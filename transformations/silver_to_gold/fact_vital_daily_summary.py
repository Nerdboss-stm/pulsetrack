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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_processed,
    start_metrics_server,
    streaming_query_active,
)
from streaming.spark_config import get_spark_session  # noqa: E402
from utils.streaming import setup_graceful_shutdown  # noqa: E402

log = get_logger(__name__)
QUERY_NAME = "gold-fact-vital-daily-summary"

_EMPTY_SCHEMA = StructType([
    StructField("patient_key",         LongType()),
    StructField("metric_key",          LongType()),
    StructField("date_key",            IntegerType()),
    StructField("avg_value",           DoubleType()),
    StructField("min_value",           DoubleType()),
    StructField("max_value",           DoubleType()),
    StructField("reading_count",       LongType()),
    StructField("anomaly_count",       LongType()),
    StructField("pct_in_normal_range", DoubleType()),
])


# ── Aggregation core ──────────────────────────────────────────────────────────
def _aggregate(silver_subset: DataFrame, dim_metric: DataFrame, bridge_df) -> DataFrame:
    """Aggregate Silver rows into the daily fact grain."""
    sensors = silver_subset

    if bridge_df is not None:
        device_to_patient = (
            bridge_df.filter(F.col("identifier_type") == "device_account_id")
            .select(
                F.col("identifier_value").alias("device_account_id"),
                F.abs(F.hash(F.col("patient_key"))).cast("long").alias("linked_patient_key"),
            )
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
                / F.count(F.lit(1)) * 100
            ).alias("pct_in_normal_range"),
        )
    )


def _merge_or_seed(spark: SparkSession, aggregated: DataFrame) -> int:
    n = aggregated.count()
    if n == 0:
        return 0

    if not DeltaTable.isDeltaTable(spark, settings.gold_fact_vital_daily):
        aggregated.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").save(settings.gold_fact_vital_daily)
        return n
    DeltaTable.forPath(spark, settings.gold_fact_vital_daily).alias("t").merge(
        aggregated.alias("s"),
        "t.patient_key = s.patient_key AND "
        "t.metric_key  = s.metric_key  AND "
        "t.date_key    = s.date_key",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    return n


# ── Streaming foreachBatch ────────────────────────────────────────────────────
def _make_streaming_processor(spark: SparkSession):
    def process(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        cached = batch_df.cache()
        keys = (
            cached.select(
                F.col("device_account_id"),
                F.col("metric_name"),
                F.col("device_type"),
                F.to_date("event_timestamp").alias("event_date"),
            ).distinct()
        )
        # Re-read affected Silver slices to recompute correctly across batches
        silver = spark.read.format("delta").load(settings.silver_sensor)
        affected = silver.join(
            keys,
            (silver["device_account_id"] == keys["device_account_id"])
            & (silver["metric_name"] == keys["metric_name"])
            & (silver["device_type"] == keys["device_type"])
            & (F.to_date(silver["event_timestamp"]) == keys["event_date"]),
            "inner",
        ).select(silver["*"])

        dim_metric = spark.read.format("delta").load(settings.gold_dim_metric)
        bridge = (
            spark.read.format("delta").load(settings.silver_identity_bridge)
            if DeltaTable.isDeltaTable(spark, settings.silver_identity_bridge)
            else None
        )

        aggregated = _aggregate(affected, dim_metric, bridge)
        n = _merge_or_seed(spark, aggregated)
        records_processed.labels(layer="gold", source="vital_daily").inc(n)
        cached.unpersist()
        log.info(
            "Gold daily summary batch processed",
            extra={"extra_data": {"batch_id": batch_id, "merged_rows": n}},
        )
    return process


def run_streaming(metrics_port: int = 8004) -> None:
    start_metrics_server(metrics_port)
    spark = get_spark_session("PulseTrack-Gold-VitalDaily")

    silver_stream = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(settings.silver_sensor)
    )

    query = (
        silver_stream.writeStream
        .foreachBatch(_make_streaming_processor(spark))
        .option("checkpointLocation", f"{settings.checkpoint_base}/gold_vital_daily")
        .trigger(processingTime=settings.trigger_interval)
        .queryName(QUERY_NAME)
        .start()
    )
    streaming_query_active.labels(query_name=QUERY_NAME).set(1)
    setup_graceful_shutdown(query, spark)
    log.info("Gold daily summary stream running",
             extra={"extra_data": {"query_id": str(query.id)}})

    try:
        query.awaitTermination()
    finally:
        streaming_query_active.labels(query_name=QUERY_NAME).set(0)


# ── Batch entrypoint (the original full recompute, kept for tests) ────────────
def run_batch(spark: SparkSession | None = None) -> None:
    spark = spark or get_spark_session("PulseTrack-Gold-VitalDaily-Batch")

    if not DeltaTable.isDeltaTable(spark, settings.silver_sensor):
        log.warning("Silver sensor_readings not found — writing empty fact table")
        df = spark.createDataFrame([], _EMPTY_SCHEMA)
        df.write.format("delta").mode("overwrite").save(settings.gold_fact_vital_daily)
        log.info(
            "fact_vital_daily_summary written (empty seed)",
            extra={"extra_data": {"row_count": 0, "path": settings.gold_fact_vital_daily}},
        )
        return

    sensors = spark.read.format("delta").load(settings.silver_sensor)
    dim_metric = spark.read.format("delta").load(settings.gold_dim_metric)
    bridge = (
        spark.read.format("delta").load(settings.silver_identity_bridge)
        if DeltaTable.isDeltaTable(spark, settings.silver_identity_bridge)
        else None
    )

    df = _aggregate(sensors, dim_metric, bridge)
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(settings.gold_fact_vital_daily)
    log.info(
        "fact_vital_daily_summary written",
        extra={"extra_data": {
            "row_count": df.count(),
            "path": settings.gold_fact_vital_daily,
        }},
    )


def main() -> None:
    """Backwards-compat alias used by tests and existing scripts."""
    run_batch()


if __name__ == "__main__":
    run_batch()
