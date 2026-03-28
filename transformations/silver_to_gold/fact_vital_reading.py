"""
fact_vital_reading — Gold layer (Atomic)
=========================================
Grain: 1 patient × 1 metric × 1 reading timestamp (finest grain).
Source: Silver sensor_readings WHERE is_valid = true.

patient_key lookup:
  device_account_id → identity bridge → patient_key sha256 → numeric surrogate.
  Fallback: abs(hash(device_account_id)) for unregistered devices.

device_key:  abs(hash(device_id|firmware_version))
metric_key:  abs(hash(metric_name|device_type)) — matches dim_metric grain
reading_key: abs(hash(reading_id|metric_name)) — stable surrogate, idempotent on replay
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, DoubleType,
    BooleanType, StringType,
)
from delta.tables import DeltaTable
from streaming.spark_config import get_spark_session

GOLD_BASE           = "/tmp/pulsetrack-lakehouse/gold"
SILVER_BASE         = "/tmp/pulsetrack-lakehouse/silver"
SILVER_SENSORS_PATH = f"{SILVER_BASE}/sensor_readings"
BRIDGE_PATH         = f"{SILVER_BASE}/identity/patient_identity_bridge"

_EMPTY_SCHEMA = StructType([
    StructField("reading_key",   LongType()),
    StructField("patient_key",   LongType()),
    StructField("device_key",    LongType()),
    StructField("metric_key",    LongType()),
    StructField("date_key",      IntegerType()),
    StructField("time_key",      IntegerType()),
    StructField("metric_value",  DoubleType()),
    StructField("quality_score", DoubleType()),
    StructField("is_anomaly",    BooleanType()),
    StructField("anomaly_type",  StringType()),
])


def main():
    spark = get_spark_session("GoldFactVitalReading")

    if not DeltaTable.isDeltaTable(spark, SILVER_SENSORS_PATH):
        print("  WARNING: Silver sensor_readings not found — writing empty fact table.")
        df = spark.createDataFrame([], _EMPTY_SCHEMA)
        df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/fact_vital_reading")
        print("✅ fact_vital_reading rows written: 0 rows (Silver not yet populated)")
        df.printSchema()
        return

    sensors    = spark.read.format("delta").load(SILVER_SENSORS_PATH).filter(F.col("is_valid"))
    dim_metric = spark.read.format("delta").load(f"{GOLD_BASE}/dim_metric")
    dim_device = spark.read.format("delta").load(f"{GOLD_BASE}/dim_device") \
        .select("device_key", "device_id", "firmware_version")

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

    sensors = sensors.withColumn(
        "patient_key",
        F.coalesce(
            F.col("linked_patient_key"),
            F.abs(F.hash(F.col("device_account_id"))).cast("long"),
        )
    )

    # ── FK lookups ─────────────────────────────────────────────────────
    sensors = sensors.join(dim_device, on=["device_id", "firmware_version"], how="left")
    sensors = sensors.join(
        dim_metric.select("metric_key", "metric_name", "device_type"),
        on=["metric_name", "device_type"],
        how="left",
    )

    # ── Date / time keys ───────────────────────────────────────────────
    sensors = sensors.withColumn(
        "date_key",
        (F.year("event_timestamp") * 10000
         + F.month("event_timestamp") * 100
         + F.dayofmonth("event_timestamp")).cast("int")
    ).withColumn(
        "time_key",
        (F.hour("event_timestamp") * 100 + F.minute("event_timestamp")).cast("int")
    )

    # ── Surrogate + quality flags ──────────────────────────────────────
    df = (
        sensors
        .filter(F.col("metric_key").isNotNull())
        .withColumn("reading_key",
            F.abs(F.hash(
                F.concat_ws("|", F.col("reading_id"), F.col("metric_name"))
            )).cast("long"))
        .withColumn("quality_score", F.lit(1.0))
        .withColumn("is_anomaly",    F.lit(False))
        .withColumn("anomaly_type",  F.lit(None).cast(StringType()))
        .select(
            "reading_key", "patient_key", "device_key", "metric_key",
            "date_key", "time_key", "metric_value",
            "quality_score", "is_anomaly", "anomaly_type",
        )
    )

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(f"{GOLD_BASE}/fact_vital_reading")
    print(f"✅ fact_vital_reading rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
