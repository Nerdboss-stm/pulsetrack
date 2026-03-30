"""
fact_prescription_fill — Gold layer
=====================================
Grain: 1 row per prescription fill event.
Source: Silver pharmacy prescriptions.

patient_key lookup:
  pharmacy_patient_id has no direct EHR match (different namespace).
  Fallback: abs(hash(pharmacy_patient_id)) as proxy patient_key.

medication_key: lookup by drug name prefix against dim_medication.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType
)
from delta.tables import DeltaTable
from streaming.spark_config import get_spark_session

GOLD_BASE    = "/tmp/pulsetrack-lakehouse/gold"
SILVER_BASE  = "/tmp/pulsetrack-lakehouse/silver"
SILVER_RX_PATH = f"{SILVER_BASE}/pharmacy_prescriptions"

_EMPTY_SCHEMA = StructType([
    StructField("fill_key",          LongType()),
    StructField("patient_key",       LongType()),
    StructField("medication_key",    LongType()),
    StructField("date_key",          IntegerType()),
    StructField("days_supply",       IntegerType()),
    StructField("refill_number",     IntegerType()),
    StructField("status",            StringType()),
])

# NDC prefix → medication name (first token match against dim_medication)
NDC_TO_DRUG = {
    "00093-7": "metformin",
    "00093-52": "lisinopril",
    "00113-0": "albuterol",
    "00093-8": "atorvastatin",
    "68180-03": "sertraline",
    "68180-05": "omeprazole",
}


def main():
    spark = get_spark_session("GoldFactPrescriptionFill")

    if not DeltaTable.isDeltaTable(spark, SILVER_RX_PATH):
        print("  WARNING: Silver pharmacy_prescriptions not found — writing empty fact table.")
        df = spark.createDataFrame([], _EMPTY_SCHEMA)
        df.write.format("delta").mode("overwrite").save(f"{GOLD_BASE}/fact_prescription_fill")
        print("✅ fact_prescription_fill rows written: 0 rows (Silver not yet populated)")
        df.printSchema()
        return

    rx      = spark.read.format("delta").load(SILVER_RX_PATH)
    dim_med = spark.read.format("delta").load(f"{GOLD_BASE}/dim_medication") \
        .select(F.col("medication_key"), F.col("medication_name"))

    # ── Patient key (proxy — pharmacy has its own patient namespace) ───
    rx = rx.withColumn(
        "patient_key",
        F.abs(F.hash(F.col("pharmacy_patient_id"))).cast("long"),
    )

    # ── Medication key — match drug_name lower → dim_medication name ──
    rx = rx.withColumn(
        "drug_name_lower",
        F.lower(F.regexp_extract(F.col("drug_name"), r"^(\w+)", 1))
    ).join(
        dim_med.withColumn("medication_name_lower", F.lower(F.col("medication_name")))
               .select("medication_key", F.col("medication_name_lower").alias("drug_name_lower")),
        on="drug_name_lower",
        how="left",
    )

    # ── Date key ───────────────────────────────────────────────────────
    rx = rx.withColumn(
        "date_key",
        F.regexp_replace(F.col("fill_date"), "-", "").cast("int")
    )

    # ── Surrogate key ──────────────────────────────────────────────────
    df = (
        rx
        .withColumn("fill_key", F.abs(F.hash(F.col("rx_id"))).cast("long"))
        .select(
            "fill_key", "patient_key", "medication_key",
            "date_key",
            F.col("days_supply").cast("int"),
            F.col("refill_number").cast("int"),
            "status",
        )
        .dropDuplicates(["fill_key"])
    )

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(f"{GOLD_BASE}/fact_prescription_fill")
    print(f"✅ fact_prescription_fill rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    main()
