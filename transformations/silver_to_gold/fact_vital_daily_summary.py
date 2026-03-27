"""
fact_vital_daily_summary — Gold layer
======================================
Grain: 1 patient × 1 metric × 1 calendar day
Source: Silver sensor_readings (exploded metric rows, one per metric per reading)

patient_key lookup:
  device_account_id → identity bridge (device_account_id → patient_key sha256) → numeric surrogate.
  Fallback: abs(hash(device_account_id)) as proxy patient_key for unregistered devices.

sleep_stage is excluded — it is categorical and stored as null in Silver because the
metrics JSON parser uses DoubleType (sensor_silver.py MapType(StringType, DoubleType)).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, DoubleType
)
from delta.tables import DeltaTable
from streaming.spark_config import get_spark_session

GOLD_BASE           = "/tmp/pulsetrack-lakehouse/gold"
SILVER_BASE         = "/tmp/pulsetrack-lakehouse/silver"
SILVER_SENSORS_PATH = f"{SILVER_BASE}/sensor_readings"
BRIDGE_PATH         = f"{SILVER_BASE}/identity/patient_identity_bridge"

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


def main():
    spark = get_spark_session("GoldFactVitalDailySummary")

    if not DeltaTable.isDeltaTable(spark, SILVER_SENSORS_PATH):
        print("  WARNING: Silver sensor_readings not found — writing empty fact table.")
        df = spark.createDataFrame([], _EMPTY_SCHEMA)
        df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/fact_vital_daily_summary")
        print("✅ fact_vital_daily_summary rows written: 0 rows (Silver not yet populated)")
        df.printSchema()
        return

    sensors    = spark.read.format("delta").load(SILVER_SENSORS_PATH)
    dim_metric = spark.read.format("delta").load(f"{GOLD_BASE}/dim_metric")

    # ── Patient key via identity bridge ────────────────────────────────
    if DeltaTable.isDeltaTable(spark, BRIDGE_PATH):
        bridge = spark.read.format("delta").load(BRIDGE_PATH)
        device_to_patient = (
            bridge
            .filter(F.col("identifier_type") == "device_account_id")
            .select(
                F.col("identifier_value").alias("device_account_id"),
                F.abs(F.hash(F.col("patient_key"))).cast("long").alias("linked_patient_key"),
            )
        )
        sensors = sensors.join(device_to_patient, on="device_account_id", how="left")
    else:
        sensors = sensors.withColumn("linked_patient_key", F.lit(None).cast(LongType()))

    # Fallback: unregistered devices get a device-derived proxy patient_key
    sensors = sensors.withColumn(
        "patient_key",
        F.coalesce(
            F.col("linked_patient_key"),
            F.abs(F.hash(F.col("device_account_id"))).cast("long"),
        )
    )

    # ── Date key ───────────────────────────────────────────────────────
    sensors = sensors.withColumn(
        "date_key",
        (F.year("event_timestamp") * 10000
         + F.month("event_timestamp") * 100
         + F.dayofmonth("event_timestamp")).cast("int")
    )

    # ── Join dim_metric for metric_key + normal ranges ──────────────────
    sensors = sensors.join(
        dim_metric.select("metric_key", "metric_name", "device_type", "normal_low", "normal_high"),
        on=["metric_name", "device_type"],
        how="left",
    )

    # Flag each valid reading as in/out of normal range
    sensors = sensors.withColumn(
        "in_normal_range",
        F.when(
            F.col("metric_value").isNotNull()
            & F.col("normal_low").isNotNull()
            & F.col("normal_high").isNotNull(),
            (F.col("metric_value") >= F.col("normal_low"))
            & (F.col("metric_value") <= F.col("normal_high")),
        ).otherwise(F.lit(False))
    )

    # ── Aggregate (exclude sleep_stage — categorical, null in Silver) ───
    df = (
        sensors
        .filter(F.col("metric_name") != "sleep_stage")
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

    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/fact_vital_daily_summary")
    print(f"✅ fact_vital_daily_summary rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
