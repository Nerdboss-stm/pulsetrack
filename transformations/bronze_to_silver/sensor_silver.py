"""
PulseTrack — Silver sensor transform.

Reads the new Avro-decoded Bronze (``decoded`` struct + envelope), explodes the
nested ``metrics`` map into one row per (reading, metric), tags rows with
``is_valid`` / ``is_late_arriving``, and merges the valid rows into the Silver
Delta table while sending the invalid rows to the quarantine sink.

Two entrypoints:

* :func:`run_streaming` — true Structured Streaming with watermark +
  ``dropDuplicatesWithinWatermark`` + ``foreachBatch`` MERGE.
* :func:`run_batch` — same logic invoked once over the current Bronze
  contents. Use this for backfills (the Kappa "same code, batch execution"
  pattern).

``run_sensor_silver(spark)`` is preserved as a backwards-compatible wrapper
around :func:`run_batch` for existing callers.
"""
from __future__ import annotations

import os
import sys
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import settings  # noqa: E402
from data_quality.expectations.silver_sensor_suite import (  # noqa: E402
    SUITE_NAME as SILVER_SUITE,
    prepare_for_validation as prepare_silver,
)
from data_quality.gx_config import validate as gx_validate  # noqa: E402
from data_quality.quarantine import quarantine_records  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_processed,
    start_metrics_server,
    streaming_query_active,
)
from streaming.spark_config import get_spark_session  # noqa: E402
from utils.streaming import setup_graceful_shutdown  # noqa: E402

log = get_logger(__name__)
QUERY_NAME = "silver-sensor-readings"

# Valid ranges per metric — drives the `is_valid` flag.
METRIC_RANGES: dict[str, tuple[float, float]] = {
    "heart_rate_bpm":     (30,   220),
    "spo2_pct":           (70,   100),
    "steps_since_last":   (0,    10000),
    "skin_temp_celsius":  (30,   42),
    "hrv_ms":             (5,    200),
    "respiration_rate":   (8,    40),
    "sleep_stage":        (0,    4),
    "blood_glucose_mgdl": (40,   400),
    "bp_systolic_mmhg":   (70,   220),
    "bp_diastolic_mmhg":  (40,   130),
}


# ── Transform pipeline ────────────────────────────────────────────────────────
def parse_and_explode(bronze: DataFrame) -> DataFrame:
    """Project decoded Avro fields out of Bronze and explode metrics map."""
    parsed = (
        bronze.filter(F.col("is_parseable"))
        .select(
            F.col("decoded.reading_id").alias("reading_id"),
            F.col("decoded.device_id").alias("device_id"),
            F.col("decoded.device_type").alias("device_type"),
            F.col("decoded.user_device_account_id").alias("device_account_id"),
            F.col("decoded.patient_email").alias("patient_email"),
            F.col("decoded.firmware_version").alias("firmware_version"),
            F.col("decoded.battery_pct").cast(IntegerType()).alias("battery_pct"),
            F.col("decoded.metrics").alias("metrics_map"),
            F.col("decoded.event_timestamp").alias("event_timestamp"),
            F.col("decoded.sync_timestamp").alias("sync_timestamp"),
            F.col("ingestion_timestamp"),
        )
    )
    return parsed.select(
        F.col("reading_id"),
        F.col("device_id"),
        F.col("device_type"),
        F.col("device_account_id"),
        F.col("patient_email"),
        F.col("firmware_version"),
        F.col("battery_pct"),
        F.col("event_timestamp"),
        F.col("sync_timestamp"),
        F.col("ingestion_timestamp"),
        F.explode_outer("metrics_map").alias("metric_name", "metric_value"),
    )


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Build is_valid via per-metric range checks; flag late-arriving rows."""
    valid_expr = F.lit(True)
    for metric, (low, high) in METRIC_RANGES.items():
        valid_expr = F.when(
            F.col("metric_name") == metric,
            F.col("metric_value").between(low, high),
        ).otherwise(valid_expr)

    return (
        df.withColumn(
            "is_valid",
            F.when(F.col("metric_value").isNull(), F.lit(False)).otherwise(valid_expr),
        )
        .withColumn(
            "is_late_arriving",
            (F.unix_timestamp("sync_timestamp") - F.unix_timestamp("event_timestamp"))
            > settings.late_arrival_threshold_seconds,
        )
    )


def transform(bronze: DataFrame) -> DataFrame:
    return add_quality_flags(parse_and_explode(bronze))


# ── Sink: foreachBatch closure shared by streaming and batch ──────────────────
def _process_batch(spark: SparkSession, batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        log.info("Silver batch empty", extra={"extra_data": {"batch_id": batch_id}})
        return

    cached = batch_df.cache()
    valid = cached.filter(F.col("is_valid"))
    n_valid = valid.count()
    n_total = cached.count()
    n_invalid = n_total - n_valid

    if n_valid > 0:
        # GX gate — block the MERGE if the validated rows fail expectations.
        # Quarantine still runs below so bad records aren't lost.
        gate_pass = gx_validate(
            prepare_silver(valid),
            suite_name=SILVER_SUITE,
            layer="silver",
            source="sensor",
        )
        if gate_pass:
            if not DeltaTable.isDeltaTable(spark, settings.silver_sensor):
                valid.write.format("delta").save(settings.silver_sensor)
            else:
                DeltaTable.forPath(spark, settings.silver_sensor).alias("t").merge(
                    valid.alias("s"),
                    "t.reading_id = s.reading_id AND t.metric_name = s.metric_name",
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            records_processed.labels(layer="silver", source="sensor").inc(n_valid)
        else:
            log.error(
                "Silver gate failed — skipping MERGE for batch",
                extra={"extra_data": {"batch_id": batch_id, "valid_rows": n_valid}},
            )

    if n_invalid > 0:
        quarantine_records(cached, "is_valid", layer="silver", source="sensor")

    cached.unpersist()
    log.info(
        "Silver batch processed",
        extra={"extra_data": {
            "batch_id": batch_id,
            "valid": n_valid,
            "quarantined": n_invalid,
        }},
    )


# ── Streaming entrypoint ──────────────────────────────────────────────────────
def run_streaming(metrics_port: int = 8003) -> None:
    start_metrics_server(metrics_port)
    spark = get_spark_session("PulseTrack-Silver-Sensors")

    log.info(
        "PulseTrack Silver sensor stream starting",
        extra={"extra_data": {
            "source": settings.bronze_sensor,
            "sink": settings.silver_sensor,
            "checkpoint": f"{settings.checkpoint_base}/silver_sensors",
            "watermark": settings.watermark_delay,
        }},
    )

    bronze = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(settings.bronze_sensor)
    )

    silver = (
        transform(bronze)
        .withWatermark("event_timestamp", settings.watermark_delay)
        .dropDuplicatesWithinWatermark(["reading_id", "metric_name"])
    )

    query = (
        silver.writeStream
        .foreachBatch(lambda df, bid: _process_batch(spark, df, bid))
        .option("checkpointLocation", f"{settings.checkpoint_base}/silver_sensors")
        .trigger(processingTime=settings.trigger_interval)
        .queryName(QUERY_NAME)
        .start()
    )
    streaming_query_active.labels(query_name=QUERY_NAME).set(1)

    setup_graceful_shutdown(query, spark)
    log.info("Silver sensor stream running",
             extra={"extra_data": {"query_id": str(query.id)}})

    try:
        query.awaitTermination()
    finally:
        streaming_query_active.labels(query_name=QUERY_NAME).set(0)


# ── Batch / backfill entrypoint (Kappa: same code, batch execution) ───────────
def run_batch(spark: Optional[SparkSession] = None) -> None:
    spark = spark or get_spark_session("PulseTrack-Silver-Sensors-Batch")

    bronze = spark.read.format("delta").load(settings.bronze_sensor)
    bronze_count = bronze.count()
    log.info("Silver batch starting",
             extra={"extra_data": {"bronze_rows": bronze_count}})

    silver = transform(bronze).dropDuplicates(["reading_id", "metric_name"])
    _process_batch(spark, silver, batch_id=-1)
    log.info("Silver batch complete")


# ── Backwards-compat wrapper ──────────────────────────────────────────────────
def run_sensor_silver(spark: Optional[SparkSession] = None) -> None:
    """Alias preserved for existing callers (legacy entrypoint)."""
    run_batch(spark)


if __name__ == "__main__":
    run_streaming()
