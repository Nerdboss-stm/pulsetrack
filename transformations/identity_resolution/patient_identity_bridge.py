import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

SILVER_BASE = "/tmp/pulsetrack-lakehouse/silver"
BRIDGE_PATH = f"{SILVER_BASE}/identity/patient_identity_bridge"


def build_ehr_identities(spark: SparkSession) -> DataFrame:
    """
    From EHR silver — we have both patient_id (MRN) and patient_email.
    These are fully linked: MRN ↔ email is a confirmed match.
    Use UNION of conditions + medications to maximize coverage
    (a patient might appear in one but not the other).
    """
    conditions = spark.read.format("delta") \
        .load(f"{SILVER_BASE}/ehr_conditions") \
        .select("patient_id", "patient_email") \
        .filter(F.col("patient_id").isNotNull())

    medications = spark.read.format("delta") \
        .load(f"{SILVER_BASE}/ehr_medications") \
        .select("patient_id", "patient_email") \
        .filter(F.col("patient_id").isNotNull())

    # Union + deduplicate — same patient appears in multiple batch files
    ehr_identities = conditions.union(medications) \
        .dropDuplicates(["patient_id", "patient_email"])

    # Build unified patient_key from email (same hashing logic as GhostKitchen)
    return ehr_identities.withColumn(
        "patient_key",
        F.sha2(F.lower(F.trim(F.col("patient_email"))), 256)
    )


def build_ehr_bridge_rows(ehr_df: DataFrame) -> DataFrame:
    """
    Each EHR patient produces TWO bridge rows:
      Row 1: identifier_type=hospital_mrn,  value=MRN-xxxxx,          linked to patient_key
      Row 2: identifier_type=email,          value=patient@example.com, linked to patient_key
    """
    mrn_rows = ehr_df.select(
        F.col("patient_key"),
        F.lit("hospital_mrn").alias("identifier_type"),
        F.col("patient_id").alias("identifier_value"),
        F.lit("ehr_batch").alias("source"),
        F.lit("linked").alias("link_status"),
        F.lit("exact_mrn_email").alias("match_method"),
    )

    email_rows = ehr_df.select(
        F.col("patient_key"),
        F.lit("email").alias("identifier_type"),
        F.col("patient_email").alias("identifier_value"),
        F.lit("ehr_batch").alias("source"),
        F.lit("linked").alias("link_status"),
        F.lit("exact_mrn_email").alias("match_method"),
    )

    return mrn_rows.union(email_rows) \
                   .dropDuplicates(["patient_key", "identifier_type", "identifier_value"])


def build_device_bridge_rows(spark: SparkSession) -> DataFrame:
    """
    Device accounts from sensor silver.
    No email available yet — patient_key is NULL until registration source arrives.
    link_status = pending_registration signals this explicitly.
    """
    sensors = spark.read.format("delta") \
        .load(f"{SILVER_BASE}/sensor_readings") \
        .select("device_account_id") \
        .filter(F.col("device_account_id").isNotNull()) \
        .dropDuplicates(["device_account_id"])

    return sensors.select(
        F.lit(None).cast(StringType()).alias("patient_key"),
        F.lit("device_account_id").alias("identifier_type"),
        F.col("device_account_id").alias("identifier_value"),
        F.lit("wearable").alias("source"),
        F.lit("pending_registration").alias("link_status"),
        F.lit("none").alias("match_method"),
    )


def load_bridge(bridge_df: DataFrame, spark: SparkSession):
    bridge_df = bridge_df \
        .withColumn("first_seen", F.current_timestamp()) \
        .withColumn("last_seen",  F.current_timestamp())

    if not DeltaTable.isDeltaTable(spark, BRIDGE_PATH):
        bridge_df.write.format("delta").save(BRIDGE_PATH)
        print(f"  patient_identity_bridge created: {bridge_df.count()} rows")
        return

    DeltaTable.forPath(spark, BRIDGE_PATH).alias("bridge").merge(
        bridge_df.alias("new"),
        """bridge.identifier_type  = new.identifier_type
           AND bridge.identifier_value = new.identifier_value"""
    ).whenMatchedUpdate(set={
        "last_seen":    "new.last_seen",
        "link_status":  "new.link_status",
        "patient_key":  "new.patient_key",
    }).whenNotMatchedInsertAll().execute()
    print("  patient_identity_bridge merged")


def run_identity_bridge(spark: SparkSession):
    print("Building EHR identities...")
    ehr_df = build_ehr_identities(spark)
    print(f"  Unique EHR patients: {ehr_df.count()}")

    ehr_bridge    = build_ehr_bridge_rows(ehr_df)
    device_bridge = build_device_bridge_rows(spark)

    bridge_df = ehr_bridge.union(device_bridge)

    print("\nLoading bridge...")
    load_bridge(bridge_df, spark)

    print("\n=== Bridge breakdown ===")
    final = spark.read.format("delta").load(BRIDGE_PATH)
    final.groupBy("identifier_type", "link_status").count() \
         .orderBy("identifier_type").show()

    linked = final.filter(F.col("link_status") == "linked") \
                  .select("patient_key").distinct().count()
    pending = final.filter(F.col("link_status") == "pending_registration").count()
    print(f"  Fully linked patients: {linked}")
    print(f"  Pending device accounts: {pending}")

    print("\n✅ Identity bridge complete")


if __name__ == "__main__":
    from streaming.spark_config import get_spark_session
    spark = get_spark_session("PatientIdentityBridge")
    run_identity_bridge(spark)