"""
fact_lab_result — Gold layer
=============================
Grain: 1 patient × 1 lab test × 1 test date
Source: Silver ehr_lab_results (Observation entries from FHIR batches)

patient_key lookup:
  patient_id (hospital MRN) → identity bridge (hospital_mrn → patient_key sha256) → numeric surrogate.
  Fallback: abs(hash(patient_id)) when bridge is absent.

condition_key:
  Inferred from a clinical mapping of lab test codes to ICD-10 conditions.
  HbA1c → E11.9 (Diabetes), LDL → E78.5 (Hyperlipidemia), BP_systolic → I10 (Hypertension).
  Tests without a mapping get condition_key = NULL.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, DoubleType, StringType, BooleanType
)
from delta.tables import DeltaTable
from streaming.spark_config import get_spark_session

GOLD_BASE        = "/tmp/pulsetrack-lakehouse/gold"
SILVER_BASE      = "/tmp/pulsetrack-lakehouse/silver"
SILVER_LABS_PATH = f"{SILVER_BASE}/ehr_lab_results"
BRIDGE_PATH      = f"{SILVER_BASE}/identity/patient_identity_bridge"

# Clinical linkage: lab test code → ICD-10 condition code
LAB_TO_CONDITION = [
    ("HbA1c",       "E11.9"),  # Glycated haemoglobin → Type 2 diabetes
    ("LDL",         "E78.5"),  # LDL cholesterol       → Hyperlipidemia
    ("BP_systolic", "I10"),    # Systolic blood pressure→ Essential hypertension
]

_EMPTY_SCHEMA = StructType([
    StructField("patient_key",          LongType()),
    StructField("date_key",             IntegerType()),
    StructField("lab_test_name",        StringType()),
    StructField("result_value",         DoubleType()),
    StructField("result_unit",          StringType()),
    StructField("reference_range_low",  DoubleType()),
    StructField("reference_range_high", DoubleType()),
    StructField("is_abnormal",          BooleanType()),
    StructField("condition_key",        LongType()),
])


def main():
    spark = get_spark_session("GoldFactLabResult")

    if not DeltaTable.isDeltaTable(spark, SILVER_LABS_PATH):
        print("  WARNING: Silver ehr_lab_results not found — writing empty fact table.")
        df = spark.createDataFrame([], _EMPTY_SCHEMA)
        df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/fact_lab_result")
        print("✅ fact_lab_result rows written: 0 rows (Silver not yet populated)")
        df.printSchema()
        return

    labs          = spark.read.format("delta").load(SILVER_LABS_PATH)
    dim_condition = spark.read.format("delta").load(f"{GOLD_BASE}/dim_condition")

    # ── Patient key via identity bridge ────────────────────────────────
    if DeltaTable.isDeltaTable(spark, BRIDGE_PATH):
        bridge = spark.read.format("delta").load(BRIDGE_PATH)
        mrn_to_patient = (
            bridge
            .filter(F.col("identifier_type") == "hospital_mrn")
            .select(
                F.col("identifier_value").alias("patient_id"),
                F.abs(F.hash(F.col("patient_key"))).cast("long").alias("patient_key"),
            )
        )
        labs = labs.join(mrn_to_patient, on="patient_id", how="left")
        # Fallback for any MRNs not yet in bridge
        labs = labs.withColumn(
            "patient_key",
            F.coalesce(
                F.col("patient_key"),
                F.abs(F.hash(F.col("patient_id"))).cast("long"),
            )
        )
    else:
        labs = labs.withColumn(
            "patient_key",
            F.abs(F.hash(F.col("patient_id"))).cast("long"),
        )

    # ── Date key ───────────────────────────────────────────────────────
    labs = labs.withColumn(
        "date_key",
        (F.year("test_date") * 10000
         + F.month("test_date") * 100
         + F.dayofmonth("test_date")).cast("int")
    )

    # ── Condition key via clinical lab-to-condition mapping ─────────────
    lab_cond_df = spark.createDataFrame(LAB_TO_CONDITION, ["test_code", "condition_code"])

    cond_keys = dim_condition.select(
        F.col("condition_key"),
        F.col("condition_code"),
    )
    lab_cond_lookup = (
        lab_cond_df
        .join(cond_keys, on="condition_code", how="left")
        .select("test_code", "condition_key")
    )

    labs = labs.join(lab_cond_lookup, on="test_code", how="left")

    # ── Final select + dedup on natural grain ───────────────────────────
    df = (
        labs
        .select(
            F.col("patient_key"),
            F.col("date_key"),
            F.col("test_code").alias("lab_test_name"),
            F.col("value").alias("result_value"),
            F.col("unit").alias("result_unit"),
            F.col("reference_low").alias("reference_range_low"),
            F.col("reference_high").alias("reference_range_high"),
            F.col("is_abnormal"),
            F.col("condition_key"),
        )
        .dropDuplicates(["patient_key", "date_key", "lab_test_name"])
    )

    df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/fact_lab_result")
    print(f"✅ fact_lab_result rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
