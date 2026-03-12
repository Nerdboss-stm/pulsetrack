import sys, os, json, glob
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

EHR_BATCH_DIR = "data/ehr_batches"
SILVER_BASE   = "/tmp/pulsetrack-lakehouse/silver"


def load_all_batches(spark: SparkSession) -> list[dict]:
    """
    Read all ehr_batch.json files from all date folders.
    Returns flat list of (patient_bundle, batch_date) dicts.
    """
    records = []
    for batch_file in glob.glob(f"{EHR_BATCH_DIR}/*/ehr_batch.json"):
        batch_date = batch_file.split("/")[-2]
        with open(batch_file) as f:
            data = json.load(f)
        for patient in data["patients"]:
            patient["batch_date"] = batch_date
            records.append(patient)
    print(f"  Loaded {len(records)} patient bundles across all batch files")
    return records


def build_conditions_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """
    Flatten Condition entries from all patient bundles.
    Patient-level fields (patient_id, patient_email) joined to each entry.
    """
    rows = []
    for p in records:
        patient_id    = p["patient_id"]
        patient_email = p["patient_email"]
        batch_date    = p["batch_date"]
        for entry in p["entries"]:
            if entry["resource_type"] != "Condition":
                continue
            rows.append({
                "patient_id":    patient_id,
                "patient_email": patient_email,
                "icd10_code":    entry["code"],
                "description":   entry["description"],
                "category":      entry["category"],
                "is_chronic":    entry["is_chronic"],
                "onset_date":    entry["onset_date"],
                "status":        entry["status"],
                "clinician_npi": entry["clinician_npi"],
                "batch_date":    batch_date,
            })

    schema = StructType([
        StructField("patient_id",    StringType()),
        StructField("patient_email", StringType()),
        StructField("icd10_code",    StringType()),
        StructField("description",   StringType()),
        StructField("category",      StringType()),
        StructField("is_chronic",    BooleanType()),
        StructField("onset_date",    StringType()),
        StructField("status",        StringType()),
        StructField("clinician_npi", StringType()),
        StructField("batch_date",    StringType()),
    ])

    return spark.createDataFrame(rows, schema) \
                .withColumn("onset_date", F.to_date(F.col("onset_date"))) \
                .withColumn("ingestion_timestamp", F.current_timestamp()) \
                .withColumn("row_hash", F.sha2(
                    F.concat_ws("||",
                        F.coalesce(F.col("status"),      F.lit("")),
                        F.coalesce(F.col("icd10_code"),  F.lit("")),
                    ), 256
                ))


def build_medications_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """
    Flatten MedicationStatement entries.
    SCD2 key: patient_id + medication (drug name)
    Change triggers: status change OR dosage change
    """
    rows = []
    for p in records:
        patient_id    = p["patient_id"]
        patient_email = p["patient_email"]
        batch_date    = p["batch_date"]
        for entry in p["entries"]:
            if entry["resource_type"] != "MedicationStatement":
                continue
            rows.append({
                "patient_id":    patient_id,
                "patient_email": patient_email,
                "medication":    entry["medication"],
                "generic_name":  entry["generic_name"],
                "drug_class":    entry["drug_class"],
                "dosage":        entry["dosage"],
                "frequency":     entry["frequency"],
                "start_date":    entry["start_date"],
                "end_date":      entry["end_date"],
                "status":        entry["status"],
                "prescriber_npi":entry["prescriber_npi"],
                "batch_date":    batch_date,
            })

    schema = StructType([
        StructField("patient_id",     StringType()),
        StructField("patient_email",  StringType()),
        StructField("medication",     StringType()),
        StructField("generic_name",   StringType()),
        StructField("drug_class",     StringType()),
        StructField("dosage",         StringType()),
        StructField("frequency",      StringType()),
        StructField("start_date",     StringType()),
        StructField("end_date",       StringType()),
        StructField("status",         StringType()),
        StructField("prescriber_npi", StringType()),
        StructField("batch_date",     StringType()),
    ])

    return spark.createDataFrame(rows, schema) \
                .withColumn("start_date", F.to_date(F.col("start_date"))) \
                .withColumn("end_date",   F.to_date(F.col("end_date"))) \
                .withColumn("ingestion_timestamp", F.current_timestamp()) \
                .withColumn("effective_start", F.col("start_date")) \
                .withColumn("effective_end",
                    F.when(F.col("end_date").isNotNull(), F.col("end_date"))
                     .otherwise(F.lit(None).cast(DateType()))
                ) \
                .withColumn("is_current",
                    F.col("end_date").isNull()
                ) \
                .withColumn("row_hash", F.sha2(
                    F.concat_ws("||",
                        F.coalesce(F.col("status"), F.lit("")),
                        F.coalesce(F.col("dosage"), F.lit("")),
                        F.coalesce(F.col("frequency"), F.lit("")),
                    ), 256
                ))


def build_labs_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """
    Flatten Observation entries.
    Append-only — lab results don't change after the fact.
    """
    rows = []
    for p in records:
        patient_id    = p["patient_id"]
        patient_email = p["patient_email"]
        batch_date    = p["batch_date"]
        for entry in p["entries"]:
            if entry["resource_type"] != "Observation":
                continue
            rows.append({
                "patient_id":    patient_id,
                "patient_email": patient_email,
                "test_code":     entry["code"],
                "value":         float(entry["value"]),
                "unit":          entry["unit"],
                "reference_low": float(entry["reference_low"]),
                "reference_high":float(entry["reference_high"]),
                "is_abnormal":   entry["is_abnormal"],
                "test_date":     entry["date"],
                "batch_date":    batch_date,
            })

    schema = StructType([
        StructField("patient_id",     StringType()),
        StructField("patient_email",  StringType()),
        StructField("test_code",      StringType()),
        StructField("value",          DoubleType()),
        StructField("unit",           StringType()),
        StructField("reference_low",  DoubleType()),
        StructField("reference_high", DoubleType()),
        StructField("is_abnormal",    BooleanType()),
        StructField("test_date",      StringType()),
        StructField("batch_date",     StringType()),
    ])

    return spark.createDataFrame(rows, schema) \
                .withColumn("test_date", F.to_date(F.col("test_date"))) \
                .withColumn("ingestion_timestamp", F.current_timestamp()) \
                .withColumn("observation_id", F.sha2(
                    # Natural key — patient + test + date (no UUID in generator)
                    F.concat_ws("||",
                        F.col("patient_id"),
                        F.col("test_code"),
                        F.col("test_date"),
                    ), 256
                ))


def load_conditions(df: DataFrame, spark: SparkSession):
    path = f"{SILVER_BASE}/ehr_conditions"
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        print(f"  ehr_conditions created: {df.count()} rows")
        return

    # SCD1 for conditions — status can change (active→resolved), just update in place
    DeltaTable.forPath(spark, path).alias("existing").merge(
        df.alias("new"),
        """existing.patient_id  = new.patient_id
           AND existing.icd10_code = new.icd10_code"""
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print("  ehr_conditions merged")


def load_medications(df: DataFrame, spark: SparkSession):
    """
    SCD2 for medications.
    Match key: patient_id + medication
    Change detected via row_hash (status + dosage + frequency)
    """
    path = f"{SILVER_BASE}/ehr_medications"

    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        print(f"  ehr_medications created: {df.count()} rows")
        return

    # Step 1 — expire current records where hash changed
    DeltaTable.forPath(spark, path).alias("existing").merge(
        df.alias("new"),
        """existing.patient_id  = new.patient_id
           AND existing.medication  = new.medication
           AND existing.is_current  = true
           AND existing.row_hash   != new.row_hash"""
    ).whenMatchedUpdate(set={
        "effective_end": "new.effective_start",
        "is_current":    "false"
    }).execute()

    # Step 2 — insert new versions not already present as current
    existing_current = spark.read.format("delta").load(path) \
                            .filter(F.col("is_current") == True)

    new_records = df.alias("new").join(
        existing_current.alias("cur"),
        on=[
            F.col("new.patient_id") == F.col("cur.patient_id"),
            F.col("new.medication") == F.col("cur.medication"),
            F.col("new.row_hash")   == F.col("cur.row_hash"),
        ],
        how="left_anti"
    )
    new_records.write.format("delta").mode("append").save(path)
    print("  ehr_medications updated (SCD2)")


def load_labs(df: DataFrame, spark: SparkSession):
    path = f"{SILVER_BASE}/ehr_lab_results"
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
        print(f"  ehr_lab_results created: {df.count()} rows")
        return

    # Append-only — insert new observations only
    DeltaTable.forPath(spark, path).alias("existing").merge(
        df.alias("new"),
        "existing.observation_id = new.observation_id"
    ).whenNotMatchedInsertAll().execute()
    print("  ehr_lab_results merged")


def run_ehr_silver(spark: SparkSession):
    records = load_all_batches(spark)

    conditions_df = build_conditions_df(spark, records)
    medications_df = build_medications_df(spark, records)
    labs_df        = build_labs_df(spark, records)

    print("\nLoading Silver tables...")
    load_conditions(conditions_df, spark)
    load_medications(medications_df, spark)
    load_labs(labs_df, spark)

    print("\n=== Row counts ===")
    for name, path in [
        ("ehr_conditions",  f"{SILVER_BASE}/ehr_conditions"),
        ("ehr_medications", f"{SILVER_BASE}/ehr_medications"),
        ("ehr_lab_results", f"{SILVER_BASE}/ehr_lab_results"),
    ]:
        count = spark.read.format("delta").load(path).count()
        print(f"  {name}: {count}")

    print("\n✅ EHR Silver complete")


if __name__ == "__main__":
    from streaming.spark_config import get_spark_session
    spark = get_spark_session("EHRSilver")
    run_ehr_silver(spark)