"""
dim_device — Gold layer (SCD2)
==============================
Source: Silver sensor_readings (distinct device × firmware combinations).
SCD2 triggers: (1) firmware_version update, (2) device assigned to new patient.

patient_key lookup:
  device_account_id → identity bridge → patient_key sha256 → numeric surrogate.
  Devices pending registration get patient_key = NULL.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType
from delta.tables import DeltaTable
from streaming.spark_config import get_spark_session

GOLD_BASE   = "/tmp/pulsetrack-lakehouse/gold"
SILVER_BASE = "/tmp/pulsetrack-lakehouse/silver"

MODEL_MAP = {
    "SW": "PulseTrack Watch",
    "CS": "PulseTrack Chest Strap",
    "SR": "PulseTrack Sleep Ring",
    "GM": "PulseTrack Glucose Monitor",
}
ZONE_MAP = {
    "smartwatch":      "wrist",
    "chest_strap":     "chest",
    "sleep_ring":      "finger",
    "glucose_monitor": "abdomen",
}


def main():
    spark = get_spark_session("GoldDimDevice")

    if not DeltaTable.isDeltaTable(spark, f"{SILVER_BASE}/sensor_readings"):
        print("  WARNING: Silver sensor_readings not found — skipping dim_device.")
        return

    sensors = (
        spark.read.format("delta").load(f"{SILVER_BASE}/sensor_readings")
        .select("device_id", "device_type", "device_account_id", "firmware_version")
        .distinct()
    )

    # ── Patient key via identity bridge ────────────────────────────────
    if DeltaTable.isDeltaTable(spark, f"{SILVER_BASE}/identity/patient_identity_bridge"):
        bridge = spark.read.format("delta").load(
            f"{SILVER_BASE}/identity/patient_identity_bridge"
        )
        device_to_patient = (
            bridge
            .filter(F.col("identifier_type") == "device_account_id")
            .select(
                F.col("identifier_value").alias("device_account_id"),
                F.abs(F.hash(F.col("patient_key"))).cast("long").alias("patient_key"),
            )
        )
        sensors = sensors.join(device_to_patient, on="device_account_id", how="left")
    else:
        sensors = sensors.withColumn("patient_key", F.lit(None).cast(LongType()))

    model_udf = F.udf(lambda did: MODEL_MAP.get(did[:2], "Unknown") if did else "Unknown")
    zone_udf  = F.udf(lambda dt:  ZONE_MAP.get(dt, "unknown") if dt else "unknown")

    df = (
        sensors
        .withColumn("device_key",
            F.abs(F.hash(
                F.concat_ws("|", F.col("device_id"), F.col("firmware_version"))
            )).cast("long"))
        .withColumn("model",             model_udf(F.col("device_id")))
        .withColumn("body_zone",         zone_udf(F.col("device_type")))
        .withColumn("registration_date", F.to_date(F.current_timestamp()))
        .select(
            "device_key", "device_id", "device_type", "model",
            "firmware_version", "patient_key", "registration_date", "body_zone",
        )
    )

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(f"{GOLD_BASE}/dim_device")
    print(f"✅ dim_device rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
