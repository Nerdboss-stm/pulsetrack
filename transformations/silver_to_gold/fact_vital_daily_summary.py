"""
fact_vital_daily_summary — Gold layer (streaming + batch).

Grain: 1 patient × 1 metric × 1 calendar day.
Source: Silver sensor_readings (one row per metric per reading).

Two entrypoints:

* :func:`run_streaming` — readStream from Silver, ``foreachBatch`` recomputes
  aggregates over every grain key touched by the batch (re-reading the
  affected slices of Silver), then MERGEs into the Gold table. This keeps the
  daily aggregates correct even when Silver receives backfilled or late
  readings.
* :func:`run_batch` (a.k.a. :func:`main`) — original full-overwrite recompute
  used by the existing tests.

patient_key lookup: device_account_id → identity bridge
(``device_account_id → patient_key sha256``) → numeric surrogate. Fallback:
``abs(hash(device_account_id))`` for unregistered devices.

sleep_stage is excluded from the avg/min/max bucket because it is a
categorical ordinal (0–3); it is still surfaced via fact_vital_reading.
"""

from __future__ import annotations

import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from data_quality.expectations.gold_vitals_suite import (  # noqa: E402
    SUITE_NAME as GOLD_SUITE,
)
from data_quality.expectations.gold_vitals_suite import (
    prepare_for_validation as prepare_gold,
)
from data_quality.gx_config import validate as gx_validate  # noqa: E402
from lakehouse import make_writer_for  # noqa: E402
from lakehouse.format_writer import FormatWriter  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_processed,
    start_metrics_server,
    streaming_query_active,
)
from streaming.spark_config import get_spark_session  # noqa: E402
from utils.streaming import register_metrics_listener, setup_graceful_shutdown  # noqa: E402

# Schema for the gold fact, used by the Iceberg create_table call. Must stay
# aligned with the projection at the bottom of ``_aggregate``.
GOLD_FACT_VITAL_DAILY_DDL = (
    "patient_key BIGINT, "
    "metric_key BIGINT, "
    "date_key INT, "
    "avg_value DOUBLE, "
    "min_value DOUBLE, "
    "max_value DOUBLE, "
    "reading_count BIGINT, "
    "anomaly_count BIGINT, "
    "pct_in_normal_range DOUBLE"
)


def _make_fact_writer(spark: SparkSession, fmt: str) -> FormatWriter:
    """Construct the gold fact writer.

    The Iceberg table DDL (partition spec, sort order, TBLPROPERTIES) is
    the migration framework's source of truth — V001 creates this table
    with ``PARTITIONED BY (date_key)`` and ``WRITE ORDERED BY
    (patient_key, metric_key, date_key)``. The ``GOLD_FACT_VITAL_DAILY_DDL``
    constant above is kept in sync as documentation but is not used for
    auto-creation here.
    """
    return make_writer_for(
        spark,
        fmt,
        path=settings.gold_fact_vital_daily,
        table_name="fact_vital_daily_summary",
        layer="gold",
    )


def _read_silver_batch(spark: SparkSession, fmt: str) -> DataFrame:
    """Format-aware batch read of silver sensor_readings.

    Iceberg mode addresses the Glue-registered table by FQN; Delta mode
    keeps the path-based read so this transform composes with legacy
    pipelines that haven't migrated their silver layer yet.
    """
    if fmt == "iceberg":
        return spark.read.table(
            f"{settings.iceberg_catalog}.{settings.glue_db_silver}.sensor_readings"
        )
    return spark.read.format("delta").load(settings.silver_sensor)


def _read_silver_stream(spark: SparkSession, fmt: str) -> DataFrame:
    """Format-aware streaming read of silver sensor_readings.

    Iceberg: silver writes via ``MERGE INTO ... WHEN NOT MATCHED THEN
    Iceberg classifies any MERGE INTO write (even INSERT-only) as
    ``overwrite`` because the writer may rewrite data files for
    partition compaction. The streaming source rejects overwrite by
    default → set ``streaming-skip-overwrite-snapshots=true`` so gold
    skips file-rewrite snapshots and only consumes append snapshots.

    Trade-off: gold loses retract semantics for silver UPDATEs that
    rewrite existing rows. Daily aggregates here are idempotent over
    (patient_key, metric_key, date_key) — the next batch tick
    reconciles. See § "Known limitations" in docs/PRODUCTION_RUNBOOK.md.

    Delta: full MERGE INTO with both branches; Delta CDF handles
    UPDATE-driven snapshots natively for the streaming gold reader.
    """
    if fmt == "iceberg":
        return (
            spark.readStream.format("iceberg")
            .option("streaming-skip-overwrite-snapshots", "true")
            .load(f"{settings.iceberg_catalog}.{settings.glue_db_silver}.sensor_readings")
        )
    return (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(settings.silver_sensor)
    )


def _read_dim_metric(spark: SparkSession, fmt: str) -> DataFrame:
    """Format-aware read of dim_metric — the gold transform joins on it."""
    if fmt == "iceberg":
        return spark.read.table(
            f"{settings.iceberg_catalog}.{settings.glue_db_gold}.dim_metric"
        )
    return spark.read.format("delta").load(settings.gold_dim_metric)

log = get_logger(__name__)
QUERY_NAME = "gold-fact-vital-daily-summary"

_EMPTY_SCHEMA = StructType(
    [
        StructField("patient_key", LongType()),
        StructField("metric_key", LongType()),
        StructField("date_key", IntegerType()),
        StructField("avg_value", DoubleType()),
        StructField("min_value", DoubleType()),
        StructField("max_value", DoubleType()),
        StructField("reading_count", LongType()),
        StructField("anomaly_count", LongType()),
        StructField("pct_in_normal_range", DoubleType()),
    ]
)


# ── Aggregation core ──────────────────────────────────────────────────────────
def _aggregate(silver_subset: DataFrame, dim_metric: DataFrame, bridge_df) -> DataFrame:
    """Aggregate Silver rows into the daily fact grain."""
    sensors = silver_subset

    if bridge_df is not None:
        device_to_patient = bridge_df.filter(
            F.col("identifier_type") == "device_account_id"
        ).select(
            F.col("identifier_value").alias("device_account_id"),
            F.abs(F.hash(F.col("patient_key"))).cast("long").alias("linked_patient_key"),
        )
        sensors = sensors.join(device_to_patient, on="device_account_id", how="left")
    else:
        sensors = sensors.withColumn("linked_patient_key", F.lit(None).cast(LongType()))

    sensors = sensors.withColumn(
        "patient_key",
        F.coalesce(
            F.col("linked_patient_key"),
            F.abs(F.hash(F.col("device_account_id"))).cast("long"),
        ),
    ).withColumn(
        "date_key",
        (
            F.year("event_timestamp") * 10000
            + F.month("event_timestamp") * 100
            + F.dayofmonth("event_timestamp")
        ).cast(IntegerType()),
    )

    sensors = sensors.join(
        dim_metric.select("metric_key", "metric_name", "device_type", "normal_low", "normal_high"),
        on=["metric_name", "device_type"],
        how="left",
    ).withColumn(
        "in_normal_range",
        F.when(
            F.col("metric_value").isNotNull()
            & F.col("normal_low").isNotNull()
            & F.col("normal_high").isNotNull(),
            (F.col("metric_value") >= F.col("normal_low"))
            & (F.col("metric_value") <= F.col("normal_high")),
        ).otherwise(F.lit(False)),
    )

    return (
        sensors.filter(F.col("metric_name") != "sleep_stage")
        .filter(F.col("metric_key").isNotNull())
        .groupBy("patient_key", "metric_key", "date_key")
        .agg(
            F.avg(F.when(F.col("is_valid"), F.col("metric_value"))).alias("avg_value"),
            F.min(F.when(F.col("is_valid"), F.col("metric_value"))).alias("min_value"),
            F.max(F.when(F.col("is_valid"), F.col("metric_value"))).alias("max_value"),
            F.count(F.lit(1)).alias("reading_count"),
            F.sum(F.when(~F.col("is_valid"), F.lit(1)).otherwise(F.lit(0))).alias("anomaly_count"),
            (
                F.sum(F.when(F.col("in_normal_range"), F.lit(1)).otherwise(F.lit(0))).cast("double")
                / F.count(F.lit(1))
                * 100
            ).alias("pct_in_normal_range"),
        )
    )


def _merge_or_seed(
    spark: SparkSession,
    aggregated: DataFrame,
    writer: FormatWriter,
) -> int:
    n = aggregated.count()
    if n == 0:
        return 0

    # GX gate — abort the MERGE if aggregate sanity checks fail.
    if not gx_validate(
        prepare_gold(aggregated),
        suite_name=GOLD_SUITE,
        layer="gold",
        source="vital_daily",
    ):
        log.error(
            "Gold gate failed — skipping MERGE for batch",
            extra={"extra_data": {"row_count": n}},
        )
        return 0

    writer.merge(
        aggregated,
        match_condition=(
            "t.patient_key = s.patient_key AND "
            "t.metric_key  = s.metric_key  AND "
            "t.date_key    = s.date_key"
        ),
    )
    return n
    return n


# ── Streaming foreachBatch ────────────────────────────────────────────────────
def _make_streaming_processor(spark: SparkSession, writer: FormatWriter):
    def process(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        cached = batch_df.cache()
        keys = cached.select(
            F.col("device_account_id"),
            F.col("metric_name"),
            F.col("device_type"),
            F.to_date("event_timestamp").alias("event_date"),
        ).distinct()
        # Re-read affected Silver slices to recompute correctly across batches
        silver = _read_silver_batch(spark, writer.fmt)
        affected = silver.join(
            keys,
            (silver["device_account_id"] == keys["device_account_id"])
            & (silver["metric_name"] == keys["metric_name"])
            & (silver["device_type"] == keys["device_type"])
            & (F.to_date(silver["event_timestamp"]) == keys["event_date"]),
            "inner",
        ).select(silver["*"])

        dim_metric = _read_dim_metric(spark, writer.fmt)
        # Identity bridge is still Delta-only today — leave it format=delta
        # for now. This is a known follow-up (V003 migration adds the silver
        # pharmacy + identity bridge Iceberg conversion).
        bridge = (
            spark.read.format("delta").load(settings.silver_identity_bridge)
            if DeltaTable.isDeltaTable(spark, settings.silver_identity_bridge)
            else None
        )

        aggregated = _aggregate(affected, dim_metric, bridge)
        n = _merge_or_seed(spark, aggregated, writer)
        records_processed.labels(layer="gold", source="vital_daily").inc(n)
        cached.unpersist()
        log.info(
            "Gold daily summary batch processed",
            extra={"extra_data": {"batch_id": batch_id, "merged_rows": n, "format": writer.fmt}},
        )

    return process


def run_streaming(
    metrics_port: int = settings.metrics_port_gold_daily,
    trigger_mode: str = "processing",
    fmt: str = "delta",
) -> None:
    start_metrics_server(metrics_port)
    spark = get_spark_session("PulseTrack-Gold-VitalDaily")
    register_metrics_listener(spark, layer="gold")

    fact_writer = _make_fact_writer(spark, fmt)

    silver_stream = _read_silver_stream(spark, fmt)

    stream_writer = (
        silver_stream.writeStream.foreachBatch(
            _make_streaming_processor(spark, fact_writer)
        )
        .option("checkpointLocation", f"{settings.checkpoint_base}/gold_vital_daily")
        .queryName(QUERY_NAME)
    )
    if trigger_mode == "available_now":
        stream_writer = stream_writer.trigger(availableNow=True)
    else:
        stream_writer = stream_writer.trigger(processingTime=settings.trigger_interval)
    query = stream_writer.start()
    streaming_query_active.labels(query_name=QUERY_NAME).set(1)
    setup_graceful_shutdown(query, spark)
    log.info("Gold daily summary stream running", extra={"extra_data": {"query_id": str(query.id)}})

    try:
        query.awaitTermination()
    finally:
        streaming_query_active.labels(query_name=QUERY_NAME).set(0)


# ── Batch entrypoint (the original full recompute, kept for tests) ────────────
def run_batch(spark: SparkSession | None = None, fmt: str = "delta") -> None:
    spark = spark or get_spark_session("PulseTrack-Gold-VitalDaily-Batch")

    fact_writer = _make_fact_writer(spark, fmt)

    # Iceberg silver-existence check is implicit in the table-name lookup;
    # for Delta we still gate on the Delta log so an unseeded silver path
    # produces an empty fact rather than crashing.
    if fmt == "delta" and not DeltaTable.isDeltaTable(spark, settings.silver_sensor):
        log.warning("Silver sensor_readings not found — writing empty fact table")
        df = spark.createDataFrame([], _EMPTY_SCHEMA)
        fact_writer.overwrite(df)
        log.info(
            "fact_vital_daily_summary written (empty seed)",
            extra={"extra_data": {"row_count": 0, "format": fmt}},
        )
        return

    sensors = _read_silver_batch(spark, fmt)
    dim_metric = _read_dim_metric(spark, fmt)
    bridge = (
        spark.read.format("delta").load(settings.silver_identity_bridge)
        if DeltaTable.isDeltaTable(spark, settings.silver_identity_bridge)
        else None
    )

    df = _aggregate(sensors, dim_metric, bridge)
    fact_writer.overwrite(df)
    log.info(
        "fact_vital_daily_summary written",
        extra={
            "extra_data": {
                "row_count": df.count(),
                "format": fmt,
                "target": (
                    fact_writer.identity.fqn
                    if fmt == "iceberg"
                    else fact_writer.identity.path
                ),
            }
        },
    )


def main() -> None:
    """Backwards-compat alias used by tests and existing scripts."""
    run_batch()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["streaming", "batch"], default="batch")
    parser.add_argument(
        "--trigger",
        choices=["processing", "available_now"],
        default="processing",
        help="Only used when --mode streaming. available_now exits after catching up.",
    )
    parser.add_argument(
        "--format",
        choices=["delta", "iceberg"],
        default="delta",
        help="Sink format. Iceberg writes to glue_iceberg.<glue_db_gold>.fact_vital_daily_summary.",
    )
    args = parser.parse_args()
    if args.mode == "streaming":
        run_streaming(trigger_mode=args.trigger, fmt=args.format)
    else:
        run_batch(fmt=args.format)
