"""
dim_patient — Gold layer
========================
Sources (in priority order):
  1. Silver identity bridge   → patient_key (sha256), linked MRN ↔ email pairs
  2. Raw EHR batch JSON files → patient_birth_year for age_group
     (Silver ehr_silver.py only captures entries, not patient-level demographics)
  3. Silver ehr_conditions    → primary_condition_key (first active condition)
  4. Silver sensor_readings   → device_count, first_reading_date (via bridge)

PII masking: patient_id (MRN) is sha256-hashed. Email is dropped entirely.

Fallback: if Silver identity bridge does not exist yet, dim_patient is built
directly from the raw EHR batch files so the table is always populated.
"""
import sys, os, json, glob
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType
)
from delta.tables import DeltaTable
from streaming.spark_config import get_spark_session
from datetime import datetime

GOLD_BASE     = "/tmp/pulsetrack-lakehouse/gold"
SILVER_BASE   = "/tmp/pulsetrack-lakehouse/silver"
BRIDGE_PATH   = f"{SILVER_BASE}/identity/patient_identity_bridge"
EHR_BATCH_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'ehr_batches')


def load_ehr_demographics(spark):
    """
    Read patient_id, patient_email, patient_birth_year from raw EHR batch JSON files.
    Silver ehr_silver.py does not capture these patient-level fields — only the entries
    (Condition, MedicationStatement, Observation) are stored in Silver.
    """
    rows = []
    for batch_file in glob.glob(os.path.join(EHR_BATCH_DIR, "*/ehr_batch.json")):
        with open(batch_file) as f:
            data = json.load(f)
        for p in data["patients"]:
            rows.append((
                p["patient_id"],
                p.get("patient_email", ""),
                int(p.get("patient_birth_year", 0)),
            ))

    if not rows:
        return spark.createDataFrame([], StructType([
            StructField("patient_id",         StringType()),
            StructField("patient_email",       StringType()),
            StructField("patient_birth_year",  IntegerType()),
        ]))

    current_year = datetime.utcnow().year
    return (
        spark.createDataFrame(rows, ["patient_id", "patient_email", "patient_birth_year"])
        .dropDuplicates(["patient_id"])
        .withColumn("age", F.lit(current_year) - F.col("patient_birth_year"))
        .withColumn("age_group",
            F.when(F.col("age") < 18,  "0-17")
             .when(F.col("age") < 35,  "18-34")
             .when(F.col("age") < 50,  "35-49")
             .when(F.col("age") < 65,  "50-64")
             .otherwise("65+")
        )
    )


def main():
    spark = get_spark_session("GoldDimPatient")

    demo_df = load_ehr_demographics(spark)
    print(f"  EHR demographics loaded: {demo_df.count()} patients")

    # ── Fallback: no Silver bridge yet ─────────────────────────────────
    if not DeltaTable.isDeltaTable(spark, BRIDGE_PATH):
        print("  WARNING: Identity bridge not found — building from EHR batch files only.")
        df = (
            demo_df
            .withColumn("patient_key",
                F.abs(F.hash(F.sha2(F.lower(F.trim(F.col("patient_email"))), 256))).cast("long"))
            .withColumn("patient_id_masked",
                F.sha2(F.lower(F.trim(F.col("patient_id"))), 256))
            .withColumn("gender",                F.lit("Unknown"))
            .withColumn("primary_condition_key", F.lit(None).cast(LongType()))
            .withColumn("device_count",          F.lit(0).cast(LongType()))
            .withColumn("first_reading_date",    F.lit(None).cast(DateType()))
            .select("patient_key", "patient_id_masked", "age_group", "gender",
                    "primary_condition_key", "device_count", "first_reading_date")
            .dropDuplicates(["patient_key"])
        )
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_BASE}/dim_patient")
        print(f"✅ dim_patient rows written: {df.count()} rows (seeded from EHR batch files)")
        df.printSchema()
        return

    # ── Full build from Silver ──────────────────────────────────────────
    bridge = spark.read.format("delta").load(BRIDGE_PATH)

    mrn_rows = (
        bridge.filter(F.col("identifier_type") == "hospital_mrn")
              .select(F.col("patient_key").alias("patient_key_sha256"),
                      F.col("identifier_value").alias("patient_id"))
    )
    email_rows = (
        bridge.filter(F.col("identifier_type") == "email")
              .select(F.col("patient_key").alias("patient_key_sha256"),
                      F.col("identifier_value").alias("patient_email"))
    )

    linked = mrn_rows.join(email_rows, on="patient_key_sha256", how="inner")

    patients = (
        linked
        .join(demo_df.select("patient_id", "age_group"), on="patient_id", how="left")
        .withColumn("age_group", F.coalesce(F.col("age_group"), F.lit("Unknown")))
    )

    # ── Primary condition (first active ICD-10 per patient) ─────────────
    if DeltaTable.isDeltaTable(spark, f"{SILVER_BASE}/ehr_conditions"):
        conds_silver = spark.read.format("delta").load(f"{SILVER_BASE}/ehr_conditions")
        dim_cond     = spark.read.format("delta").load(f"{GOLD_BASE}/dim_condition")

        cond_lookup = dim_cond.select(
            F.col("condition_key"),
            F.col("condition_code").alias("icd10_code"),
        )
        primary_cond = (
            conds_silver
            .filter(F.col("status") == "active")
            .join(cond_lookup, on="icd10_code", how="left")
            .groupBy("patient_id")
            .agg(F.first("condition_key", ignorenulls=True).alias("primary_condition_key"))
        )
        patients = patients.join(primary_cond, on="patient_id", how="left")
    else:
        patients = patients.withColumn("primary_condition_key", F.lit(None).cast(LongType()))

    # ── Device count + first reading date (via bridge) ──────────────────
    if DeltaTable.isDeltaTable(spark, f"{SILVER_BASE}/sensor_readings"):
        sensors = spark.read.format("delta").load(f"{SILVER_BASE}/sensor_readings")

        device_bridge = (
            bridge
            .filter(F.col("identifier_type") == "device_account_id")
            .select(F.col("identifier_value").alias("device_account_id"),
                    F.col("patient_key").alias("patient_key_sha256"))
        )
        device_stats = (
            sensors
            .join(device_bridge, on="device_account_id", how="left")
            .groupBy("patient_key_sha256")
            .agg(
                F.countDistinct("device_id").alias("device_count"),
                F.min(F.to_date("event_timestamp")).alias("first_reading_date"),
            )
        )
        patients = patients.join(device_stats, on="patient_key_sha256", how="left")
    else:
        patients = (
            patients
            .withColumn("device_count",       F.lit(0).cast(LongType()))
            .withColumn("first_reading_date", F.lit(None).cast(DateType()))
        )

    # ── Surrogate key + PII masking + final select ──────────────────────
    df = (
        patients
        .withColumn("patient_key",
            F.abs(F.hash(F.col("patient_key_sha256"))).cast("long"))
        .withColumn("patient_id_masked",
            F.sha2(F.lower(F.trim(F.col("patient_id"))), 256))
        .withColumn("gender", F.lit("Unknown"))
        .fillna({"device_count": 0})
        .select("patient_key", "patient_id_masked", "age_group", "gender",
                "primary_condition_key", "device_count", "first_reading_date")
        .dropDuplicates(["patient_key"])
    )

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_BASE}/dim_patient")
    print(f"✅ dim_patient rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
