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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import settings  # noqa: E402
from data_quality.expectations.bronze_sensor_suite import (  # noqa: E402
    SUITE_NAME as BRONZE_SUITE,
)
from data_quality.expectations.bronze_sensor_suite import (
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
from lakehouse.format_writer import FormatWriter, TableIdentity  # noqa: E402
from lakehouse.partition_strategy import (  # noqa: E402
    PartitionStrategy,
    ReversedIdStrategy,
    iceberg_partition_transforms,
)
from streaming.dlq import DLQHandler  # noqa: E402
from streaming.kafka_helpers import spark_msk_iam_options  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402
from utils.streaming import register_metrics_listener, setup_graceful_shutdown  # noqa: E402

log = get_logger(__name__)
SCHEMA_FILE = "sensor_reading.avsc"
QUERY_NAME = "bronze-sensor-readings"

# DDL for the bronze sensor_readings table — used by FormatWriter.create_table
# when fmt=iceberg. Must stay aligned with ``_decode_envelope``'s projection.
# The nested ``decoded`` struct mirrors the SensorReading Avro schema.
BRONZE_SENSOR_DDL = (
    "raw_avro_bytes BINARY, "
    "kafka_topic STRING, "
    "kafka_partition INT, "
    "kafka_offset BIGINT, "
    "kafka_timestamp TIMESTAMP, "
    "kafka_key STRING, "
    "ingestion_timestamp TIMESTAMP, "
    "ingestion_date STRING, "
    "ingestion_hour STRING, "
    "decoded STRUCT<"
    "reading_id: STRING, "
    "device_id: STRING, "
    "device_type: STRING, "
    "user_device_account_id: STRING, "
    "patient_email: STRING, "
    "metrics: MAP<STRING, DOUBLE>, "
    "firmware_version: STRING, "
    "battery_pct: INT, "
    "event_timestamp: TIMESTAMP, "
    "sync_timestamp: TIMESTAMP, "
    "source_type: STRING"
    ">, "
    "is_parseable BOOLEAN, "
    # Reversed device_id — primary partition column under
    # ``ReversedIdStrategy`` (see lakehouse/partition_strategy.py).
    # Adding to DDL here so fresh-cluster deploys don't need V005;
    # V005 covers the upgrade path for existing bronze tables.
    "rid STRING"
)


def _make_bronze_writer(
    spark, fmt: str, strategy: PartitionStrategy
) -> FormatWriter:
    """Construct the bronze sensor writer + ensure the Iceberg table exists.

    Partition layout is driven by ``strategy``:
      * ``ReversedIdStrategy`` → ``(rid, days(ingestion_timestamp))``
      * ``DateFirstStrategy``  → ``(days(ingestion_timestamp), device_id)``
      * ``HashBucketStrategy`` → ``(bucket(N, device_id), days(ingestion_timestamp))``

    Default is ``ReversedIdStrategy`` — see
    ``docs/s3_partitioning_analysis.md`` for the WHOOP-style rationale and
    real-S3 benchmark results.
    """
    writer = FormatWriter(
        spark=spark,
        identity=TableIdentity(
            path=settings.bronze_sensor,
            catalog=settings.iceberg_catalog,
            database=settings.glue_db_bronze,
            table="sensor_readings",
        ),
        fmt=fmt,
    )
    if fmt == "iceberg":
        writer.create_table(
            schema_ddl=BRONZE_SENSOR_DDL,
            partition_transforms=iceberg_partition_transforms(strategy),
            sort_order=["kafka_offset"],
        )
    return writer


def _decode_envelope(
    kafka_df: DataFrame,
    schema_str: str,
    strategy: PartitionStrategy,
) -> DataFrame:
    """Strip the Confluent wire prefix (1 magic + 4 schema-id), decode Avro,
    and emit the partition columns required by ``strategy``.
    """
    avro_payload = F.expr("substring(value, 6, length(value) - 5)")
    decoded = (
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
    # Materialize partition columns via the strategy — for reversed_id the
    # writer needs ``rid`` as a real column on each row (Iceberg also tracks
    # it as a partition field). add_partition_columns is idempotent.
    decoded = strategy.add_partition_columns(decoded)
    # Drop strategy helper columns that aren't part of the bronze Iceberg
    # schema. ``dt`` is redundant — Iceberg's ``days(ingestion_timestamp)``
    # partition transform is hidden, computed from the existing column.
    # ``device_id`` (top-level, projected by date_first) duplicates
    # ``decoded.device_id`` and isn't part of the bronze DDL.
    helper_cols = [c for c in ("dt", "device_id", "hb") if c in decoded.columns]
    schema_cols = [
        "raw_avro_bytes", "kafka_topic", "kafka_partition", "kafka_offset",
        "kafka_timestamp", "kafka_key", "ingestion_timestamp",
        "ingestion_date", "ingestion_hour", "decoded", "is_parseable", "rid",
    ]
    keep_cols = [c for c in schema_cols if c in decoded.columns]
    return decoded.select(*keep_cols)


def _make_batch_processor(dlq: DLQHandler, writer: FormatWriter):
    """foreachBatch closure: write Bronze, route failures to DLQ, update metrics."""

    def process(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        cached = batch_df.cache()

        # Bronze: every record (raw bytes + envelope + decoded struct + is_parseable).
        # Iceberg uses hidden ``days(ingestion_timestamp)`` partitioning set
        # at create_table; Delta uses the explicit columns passed here.
        writer.append(
            cached,
            partition_columns=["ingestion_date", "ingestion_hour"],
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
                layer="bronze",
                source="sensor",
                reason="avro_parse",
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
            extra={
                "extra_data": {
                    "batch_id": batch_id,
                    "valid": valid,
                    "invalid": invalid,
                }
            },
        )

    return process


def run_wearable_bronze(
    metrics_port: int = settings.metrics_port_bronze_sensor,
    dlq: Optional[DLQHandler] = None,
    trigger_mode: str = "processing",
    fmt: str = "delta",
    partition_strategy: PartitionStrategy = ReversedIdStrategy(),
):
    start_metrics_server(metrics_port)
    spark = get_spark_session("PulseTrack-Bronze-Wearables")
    register_metrics_listener(spark, layer="bronze")
    schema_str = load_schema_str(SCHEMA_FILE)
    dlq = dlq or DLQHandler(spark, fmt=fmt)

    bronze_writer = _make_bronze_writer(spark, fmt, partition_strategy)

    # MSK IAM auth options (kafka.* prefixed) when running on cloud (SASL_SSL).
    # Local PLAINTEXT mode returns an empty dict, so the readStream chain is
    # identical in both modes.
    msk_options = spark_msk_iam_options()

    log.info(
        "PulseTrack Wearable → Bronze ingestion starting",
        extra={
            "extra_data": {
                "source_topic": settings.kafka_topic_sensor,
                "kafka_bootstrap": settings.kafka_bootstrap,
                "schema_registry": settings.schema_registry_url,
                "sink": (
                    bronze_writer.identity.fqn
                    if fmt == "iceberg"
                    else bronze_writer.identity.path
                ),
                "checkpoint": settings.checkpoint_bronze_sensor,
                "max_offsets_per_trigger": settings.max_offsets_per_trigger,
                "trigger_mode": trigger_mode,
                "format": fmt,
                "sasl_enabled": bool(msk_options),
                "partition_strategy": partition_strategy.name,
                "partition_columns": list(partition_strategy.partition_columns),
            }
        },
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .options(**msk_options)
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic_sensor)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .load()
    )
    bronze_df = _decode_envelope(kafka_df, schema_str, partition_strategy)

    stream_writer = (
        bronze_df.writeStream.foreachBatch(_make_batch_processor(dlq, bronze_writer))
        .option("checkpointLocation", settings.checkpoint_bronze_sensor)
        .queryName(QUERY_NAME)
    )
    # `available_now` processes everything currently on the topic and exits
    # cleanly (checkpoint advances). Used for one-shot end-to-end runs.
    # Default `processing` keeps the query alive at trigger_interval.
    if trigger_mode == "available_now":
        stream_writer = stream_writer.trigger(availableNow=True)
    else:
        stream_writer = stream_writer.trigger(processingTime=settings.trigger_interval)
    query = stream_writer.start()
    streaming_query_active.labels(query_name=QUERY_NAME).set(1)

    setup_graceful_shutdown(query, spark)
    log.info("Bronze running", extra={"extra_data": {"query_id": str(query.id)}})

    try:
        query.awaitTermination()
    finally:
        streaming_query_active.labels(query_name=QUERY_NAME).set(0)
        dlq.flush()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trigger",
        choices=["processing", "available_now"],
        default="processing",
        help=(
            "processing: long-running with processingTime trigger. "
            "available_now: process all current offsets, advance checkpoint, exit."
        ),
    )
    parser.add_argument(
        "--format",
        choices=["delta", "iceberg"],
        default="delta",
        help="Sink format. Iceberg writes to glue_iceberg.<glue_db_bronze>.sensor_readings.",
    )
    from lakehouse.partition_strategy import get_strategy, list_strategies
    parser.add_argument(
        "--partition-strategy",
        choices=list_strategies(),
        default="reversed_id",
        help=(
            "S3 partition layout. ``reversed_id`` (default) is WHOOP's "
            "thundering-herd mitigation; ``date_first`` is the legacy "
            "layout; ``hash_bucket`` distributes uniformly but isn't "
            "grep-able. See lakehouse/partition_strategy.py for details."
        ),
    )
    args = parser.parse_args()
    run_wearable_bronze(
        trigger_mode=args.trigger,
        fmt=args.format,
        partition_strategy=get_strategy(args.partition_strategy),
    )
