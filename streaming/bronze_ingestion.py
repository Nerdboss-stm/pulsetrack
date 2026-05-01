"""
PulseTrack — Bronze ingestion (Avro + Schema Registry edition).

Reads from Kafka topic ``sensor_readings``, decodes Avro payloads using the
schema in ``schemas/sensor_reading.avsc``, and writes:

* Bronze Delta — every record with raw bytes, the decoded struct, kafka
  envelope metadata, and an ``is_parseable`` flag. Partitioned by
  ``ingestion_date / ingestion_hour``.
* DLQ Delta + Kafka — only the records that fail Avro decoding (PERMISSIVE
  mode → ``decoded`` is null or ``decoded.reading_id`` is null).

Backpressure: ``maxOffsetsPerTrigger`` from settings. Checkpoints under
``settings.checkpoint_bronze_sensor``. SIGTERM/SIGINT trigger a graceful
``query.stop()`` so the checkpoint is left consistent.
"""
from __future__ import annotations

import os
import sys
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import settings  # noqa: E402
from data_quality.expectations.bronze_sensor_suite import (  # noqa: E402
    SUITE_NAME as BRONZE_SUITE,
    prepare_for_validation as prepare_bronze,
)
from data_quality.gx_config import validate as gx_validate  # noqa: E402
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
from utils.streaming import setup_graceful_shutdown  # noqa: E402

log = get_logger(__name__)
SCHEMA_FILE = "sensor_reading.avsc"
QUERY_NAME = "bronze-sensor-readings"


def _decode_envelope(kafka_df: DataFrame, schema_str: str) -> DataFrame:
    """Strip the Confluent wire prefix (1 magic + 4 schema-id) and decode Avro."""
    avro_payload = F.expr("substring(value, 6, length(value) - 5)")
    return (
        kafka_df.select(
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
        )
        .withColumn(
            "is_parseable",
            F.col("decoded").isNotNull() & F.col("decoded.reading_id").isNotNull(),
        )
    )


def _make_batch_processor(dlq: DLQHandler):
    """foreachBatch closure: write Bronze, route failures to DLQ, update metrics."""
    def process(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        cached = batch_df.cache()

        # Bronze: every record (raw bytes + envelope + decoded struct + is_parseable)
        (
            cached.write.format("delta")
            .mode("append")
            .partitionBy("ingestion_date", "ingestion_hour")
            .option("mergeSchema", "true")
            .save(settings.bronze_sensor)
        )

        valid = cached.filter(F.col("is_parseable")).count()
        invalid_df = cached.filter(~F.col("is_parseable"))
        invalid = invalid_df.count()

        records_processed.labels(layer="bronze", source="sensor").inc(valid + invalid)

        # Failed Avro decoding → DLQ (Delta + Kafka)
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
                error_message="Schema Registry payload failed Avro decoding",
            )
            records_failed.labels(
                layer="bronze", source="sensor", reason="avro_parse",
            ).inc(invalid)

        # Informative quality gate — Bronze is the source of truth, so we
        # never block the write, just publish the result + metric.
        if valid > 0:
            gx_validate(
                prepare_bronze(cached),
                suite_name=BRONZE_SUITE,
                layer="bronze",
                source="sensor",
            )

        cached.unpersist()
        log.info(
            "Bronze batch processed",
            extra={"extra_data": {
                "batch_id": batch_id,
                "valid": valid,
                "invalid": invalid,
            }},
        )

    return process


def run_wearable_bronze(
    metrics_port: int = 8000,
    dlq: Optional[DLQHandler] = None,
):
    start_metrics_server(metrics_port)
    spark = get_spark_session("PulseTrack-Bronze-Wearables")
    schema_str = load_schema_str(SCHEMA_FILE)
    dlq = dlq or DLQHandler(spark)

    log.info(
        "PulseTrack Wearable → Bronze ingestion starting",
        extra={"extra_data": {
            "source_topic": settings.kafka_topic_sensor,
            "kafka_bootstrap": settings.kafka_bootstrap,
            "schema_registry": settings.schema_registry_url,
            "sink": settings.bronze_sensor,
            "checkpoint": settings.checkpoint_bronze_sensor,
            "max_offsets_per_trigger": settings.max_offsets_per_trigger,
        }},
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic_sensor)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
    )
    bronze_df = _decode_envelope(kafka_df, schema_str)

    query = (
        bronze_df.writeStream
        .foreachBatch(_make_batch_processor(dlq))
        .option("checkpointLocation", settings.checkpoint_bronze_sensor)
        .trigger(processingTime=settings.trigger_interval)
        .queryName(QUERY_NAME)
        .start()
    )
    streaming_query_active.labels(query_name=QUERY_NAME).set(1)

    setup_graceful_shutdown(query, spark)
    log.info("Bronze running", extra={"extra_data": {"query_id": str(query.id)}})

    try:
        query.awaitTermination()
    finally:
        streaming_query_active.labels(query_name=QUERY_NAME).set(0)
        dlq.flush()


if __name__ == "__main__":
    run_wearable_bronze()
