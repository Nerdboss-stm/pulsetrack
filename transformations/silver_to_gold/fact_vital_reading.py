"""
fact_vital_reading — Gold layer (atomic per-reading fact, streaming).

Grain: 1 patient × 1 metric × 1 event_timestamp.
Source: streaming Silver sensor_readings (one row per metric per reading).

Joins:
  - identity bridge (device_account_id → patient_key)
  - dim_metric      (metric_name + device_type → metric_key, normal range)
  - dim_date is implied by date_key, derived from event_timestamp

MERGE key: (patient_key, metric_key, event_timestamp). Late-arriving rows
update the existing fact row in place. Records that fail to resolve a
patient_key fall back to ``abs(hash(device_account_id))`` so unregistered
devices still produce facts.
"""

from __future__ import annotations

import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_processed,
    start_metrics_server,
    streaming_query_active,
)
from streaming.spark_config import get_spark_session  # noqa: E402
from utils.streaming import register_metrics_listener, setup_graceful_shutdown  # noqa: E402

log = get_logger(__name__)
QUERY_NAME = "gold-fact-vital-reading"

FACT_SCHEMA = StructType(
    [
        StructField("patient_key", LongType(), True),
        StructField("metric_key", LongType(), True),
        StructField("date_key", IntegerType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("is_valid", BooleanType(), True),
        StructField("is_late_arriving", BooleanType(), True),
        StructField("source_type", StringType(), True),
    ]
)


def _seed_empty_table(spark: SparkSession) -> None:
    if DeltaTable.isDeltaTable(spark, settings.gold_fact_vital_reading):
        return
    empty = spark.createDataFrame([], FACT_SCHEMA)
    empty.write.format("delta").mode("overwrite").save(settings.gold_fact_vital_reading)


def _build_facts(
    silver: DataFrame,
    dim_metric: DataFrame,
    bridge_df: DataFrame | None,
) -> DataFrame:
    """Resolve keys + project Silver rows into the fact-grain DataFrame."""
    if bridge_df is not None:
        device_to_patient = bridge_df.filter(
            F.col("identifier_type") == "device_account_id"
        ).select(
            F.col("identifier_value").alias("device_account_id"),
            F.abs(F.hash(F.col("patient_key"))).cast("long").alias("linked_patient_key"),
        )
        silver = silver.join(device_to_patient, on="device_account_id", how="left")
    else:
        silver = silver.withColumn("linked_patient_key", F.lit(None).cast(LongType()))

    silver = silver.withColumn(
        "patient_key",
        F.coalesce(
            F.col("linked_patient_key"),
            F.abs(F.hash(F.col("device_account_id"))).cast("long"),
        ),
    )

    silver = silver.join(
        dim_metric.select("metric_key", "metric_name", "device_type"),
        on=["metric_name", "device_type"],
        how="left",
    )

    if "source_type" not in silver.columns:
        silver = silver.withColumn("source_type", F.lit("simulator"))
    return silver.filter(F.col("metric_key").isNotNull()).select(
        F.col("patient_key"),
        F.col("metric_key"),
        (
            F.year("event_timestamp") * 10000
            + F.month("event_timestamp") * 100
            + F.dayofmonth("event_timestamp")
        )
        .cast(IntegerType())
        .alias("date_key"),
        F.col("event_timestamp"),
        F.col("metric_value").cast(DoubleType()).alias("value"),
        F.col("is_valid"),
        F.col("is_late_arriving"),
        F.col("source_type"),
    )


def _merge_facts(spark: SparkSession, facts: DataFrame) -> int:
    # Dedupe on the MERGE grain. Multiple source events can land on the same
    # (patient_key, metric_key, event_timestamp) — e.g., WHOOP cycle.end and
    # recovery.created_at often coincide to the second for the same metric.
    # Without dedupe, Delta's MERGE raises DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE.
    facts = facts.dropDuplicates(["patient_key", "metric_key", "event_timestamp"])
    n = facts.count()
    if n == 0:
        return 0
    DeltaTable.forPath(spark, settings.gold_fact_vital_reading).alias("t").merge(
        facts.alias("s"),
        "t.patient_key = s.patient_key AND "
        "t.metric_key  = s.metric_key  AND "
        "t.event_timestamp = s.event_timestamp",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    return n


def _make_processor(spark: SparkSession):
    def process(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        dim_metric = spark.read.format("delta").load(settings.gold_dim_metric)
        bridge = (
            spark.read.format("delta").load(settings.silver_identity_bridge)
            if DeltaTable.isDeltaTable(spark, settings.silver_identity_bridge)
            else None
        )
        facts = _build_facts(batch_df, dim_metric, bridge)
        n = _merge_facts(spark, facts)
        records_processed.labels(layer="gold", source="vital_reading").inc(n)
        log.info(
            "Gold vital_reading batch processed",
            extra={"extra_data": {"batch_id": batch_id, "merged_rows": n}},
        )

    return process


def run_streaming(metrics_port: int = settings.metrics_port_gold_reading) -> None:
    start_metrics_server(metrics_port)
    spark = get_spark_session("PulseTrack-Gold-VitalReading")
    register_metrics_listener(spark, layer="gold")
    _seed_empty_table(spark)

    log.info(
        "Gold fact_vital_reading stream starting",
        extra={
            "extra_data": {
                "source": settings.silver_sensor,
                "sink": settings.gold_fact_vital_reading,
            }
        },
    )

    silver_stream = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(settings.silver_sensor)
    )

    query = (
        silver_stream.writeStream.foreachBatch(_make_processor(spark))
        .option("checkpointLocation", f"{settings.checkpoint_base}/gold_vital_reading")
        .trigger(processingTime=settings.trigger_interval)
        .queryName(QUERY_NAME)
        .start()
    )
    streaming_query_active.labels(query_name=QUERY_NAME).set(1)
    setup_graceful_shutdown(query, spark)

    try:
        query.awaitTermination()
    finally:
        streaming_query_active.labels(query_name=QUERY_NAME).set(0)


def run_batch(spark: SparkSession | None = None) -> None:
    spark = spark or get_spark_session("PulseTrack-Gold-VitalReading-Batch")
    _seed_empty_table(spark)

    if not DeltaTable.isDeltaTable(spark, settings.silver_sensor):
        log.warning("Silver sensor_readings not found — fact_vital_reading remains empty")
        return

    silver = spark.read.format("delta").load(settings.silver_sensor)
    dim_metric = spark.read.format("delta").load(settings.gold_dim_metric)
    bridge = (
        spark.read.format("delta").load(settings.silver_identity_bridge)
        if DeltaTable.isDeltaTable(spark, settings.silver_identity_bridge)
        else None
    )
    facts = _build_facts(silver, dim_metric, bridge)
    n = _merge_facts(spark, facts)
    log.info(
        "fact_vital_reading written",
        extra={
            "extra_data": {
                "row_count": n,
                "path": settings.gold_fact_vital_reading,
            }
        },
    )


def main() -> None:
    run_batch()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["streaming", "batch"], default="batch")
    args = parser.parse_args()
    if args.mode == "streaming":
        run_streaming()
    else:
        run_batch()
