"""
PulseTrack — Pharmacy Bronze ingestion.

Reads Avro pharmacy events from Kafka topic ``pharmacy_events`` (produced by
``data_generators/openfda_producer.py``), decodes via the Confluent
Schema Registry wire format, and writes to the Bronze pharmacy Delta table.

Same Bronze contract as the wearable path: every record is written with raw
Avro bytes + decoded struct + envelope metadata + ``is_parseable`` flag. DLQ
on decode failure. Bronze GX gate is informative-only — Bronze is the source
of truth and is never blocked.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_failed,
    records_processed,
    start_metrics_server,
    streaming_query_active,
)
from schemas.registry import load_schema_str  # noqa: E402
from streaming.dlq import DLQHandler  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402
from utils.streaming import register_metrics_listener, setup_graceful_shutdown  # noqa: E402

log = get_logger(__name__)
SCHEMA_FILE = "pharmacy_event.avsc"
QUERY_NAME = "bronze-pharmacy-events"


def _decode_envelope(kafka_df: DataFrame, schema_str: str) -> DataFrame:
    """Strip the Confluent wire prefix (1 magic + 4 schema-id) and decode Avro."""
    avro_payload = F.expr("substring(value, 6, length(value) - 5)")
    return kafka_df.select(
        F.col("value").alias("raw_avro_bytes"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast(StringType()).alias("kafka_key"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingestion_date"),
        F.date_format(F.current_timestamp(), "HH").alias("ingestion_hour"),
        from_avro(avro_payload, schema_str, {"mode": "PERMISSIVE"}).alias("decoded"),
    ).withColumn(
        "is_parseable",
        F.col("decoded").isNotNull() & F.col("decoded.event_id").isNotNull(),
    )


def _make_batch_processor(dlq: DLQHandler):
    """foreachBatch closure: write Bronze, route failures to DLQ, update metrics."""

    def process(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        cached = batch_df.cache()

        (
            cached.write.format("delta")
            .mode("append")
            .partitionBy("ingestion_date", "ingestion_hour")
            .option("mergeSchema", "true")
            .save(settings.bronze_pharmacy)
        )

        valid = cached.filter(F.col("is_parseable")).count()
        invalid_df = cached.filter(~F.col("is_parseable"))
        invalid = invalid_df.count()

        records_processed.labels(layer="bronze", source="pharmacy").inc(valid + invalid)

        if invalid > 0:
            dlq_input = invalid_df.select(
                F.col("kafka_topic").alias("topic"),
                F.col("kafka_partition").alias("partition"),
                F.col("kafka_offset").alias("offset"),
                F.col("kafka_key").alias("key"),
                F.col("raw_avro_bytes").cast(StringType()).alias("value"),
            )
            dlq.publish_dataframe(
                dlq_input,
                error_type="avro_deserialization_failure",
                error_message="Pharmacy Avro payload failed decoding",
            )
            records_failed.labels(
                layer="bronze",
                source="pharmacy",
                reason="avro_parse",
            ).inc(invalid)

        cached.unpersist()
        log.info(
            "Pharmacy bronze batch processed",
            extra={"extra_data": {"batch_id": batch_id, "valid": valid, "invalid": invalid}},
        )

    return process


def run_pharmacy_bronze(
    metrics_port: int = settings.metrics_port_bronze_pharmacy,
    dlq: Optional[DLQHandler] = None,
):
    start_metrics_server(metrics_port)
    spark = get_spark_session("PulseTrack-Bronze-Pharmacy")
    register_metrics_listener(spark, layer="bronze")
    schema_str = load_schema_str(SCHEMA_FILE)
    dlq = dlq or DLQHandler(spark)

    log.info(
        "PulseTrack Pharmacy → Bronze ingestion starting",
        extra={
            "extra_data": {
                "source_topic": settings.kafka_topic_pharmacy,
                "kafka_bootstrap": settings.kafka_bootstrap,
                "sink": settings.bronze_pharmacy,
                "checkpoint": settings.checkpoint_bronze_pharmacy,
            }
        },
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic_pharmacy)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
    )
    bronze_df = _decode_envelope(kafka_df, schema_str)

    query = (
        bronze_df.writeStream.foreachBatch(_make_batch_processor(dlq))
        .option("checkpointLocation", settings.checkpoint_bronze_pharmacy)
        .trigger(processingTime=settings.trigger_interval)
        .queryName(QUERY_NAME)
        .start()
    )
    streaming_query_active.labels(query_name=QUERY_NAME).set(1)

    setup_graceful_shutdown(query, spark)
    log.info("Pharmacy Bronze running", extra={"extra_data": {"query_id": str(query.id)}})

    try:
        query.awaitTermination()
    finally:
        streaming_query_active.labels(query_name=QUERY_NAME).set(0)
        dlq.flush()


if __name__ == "__main__":
    run_pharmacy_bronze()
