import argparse
import glob
import json
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from lakehouse import make_writer_for  # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger(__name__)


def load_all_batches(spark: SparkSession) -> list[dict]:
    """
    Read all ehr_batch.json files from all date folders.
    Returns flat list of (patient_bundle, batch_date) dicts.
    """
    records = []
    for batch_file in glob.glob(f"{settings.ehr_batch_dir}/*/ehr_batch.json"):
        batch_date = batch_file.split("/")[-2]
        with open(batch_file) as f:
            data = json.load(f)
        for patient in data["patients"]:
            patient["batch_date"] = batch_date
            records.append(patient)
    log.info(
        "EHR bundles loaded",
        extra={"extra_data": {"bundle_count": len(records)}},
    )
    return records


def build_conditions_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """
    Flatten Condition entries from all patient bundles.
    Patient-level fields (patient_id, patient_email) joined to each entry.
    """
    rows = []
    for p in records:
        patient_id = p["patient_id"]
        patient_email = p["patient_email"]
        batch_date = p["batch_date"]
        for entry in p["entries"]:
            if entry["resource_type"] != "Condition":
                continue
            rows.append(
                {
                    "patient_id": patient_id,
                    "patient_email": patient_email,
                    "icd10_code": entry["code"],
                    "description": entry["description"],
                    "category": entry["category"],
                    "is_chronic": entry["is_chronic"],
                    "onset_date": entry["onset_date"],
                    "status": entry["status"],
                    "clinician_npi": entry["clinician_npi"],
                    "batch_date": batch_date,
                }
            )

    schema = StructType(
        [
            StructField("patient_id", StringType()),
            StructField("patient_email", StringType()),
            StructField("icd10_code", StringType()),
            StructField("description", StringType()),
            StructField("category", StringType()),
            StructField("is_chronic", BooleanType()),
            StructField("onset_date", StringType()),
            StructField("status", StringType()),
            StructField("clinician_npi", StringType()),
            StructField("batch_date", StringType()),
        ]
    )

    return (
        spark.createDataFrame(rows, schema)
        .withColumn("onset_date", F.to_date(F.col("onset_date")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn(
            "row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("status"), F.lit("")),
                    F.coalesce(F.col("icd10_code"), F.lit("")),
                ),
                256,
            ),
        )
    )


def build_medications_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """
    Flatten MedicationStatement entries.
    SCD2 key: patient_id + medication (drug name)
    Change triggers: status change OR dosage change
    """
    rows = []
    for p in records:
        patient_id = p["patient_id"]
        patient_email = p["patient_email"]
        batch_date = p["batch_date"]
        for entry in p["entries"]:
            if entry["resource_type"] != "MedicationStatement":
                continue
            rows.append(
                {
                    "patient_id": patient_id,
                    "patient_email": patient_email,
                    "medication": entry["medication"],
                    "generic_name": entry["generic_name"],
                    "drug_class": entry["drug_class"],
                    "dosage": entry["dosage"],
                    "frequency": entry["frequency"],
                    "start_date": entry["start_date"],
                    "end_date": entry["end_date"],
                    "status": entry["status"],
                    "prescriber_npi": entry["prescriber_npi"],
                    "batch_date": batch_date,
                }
            )

    schema = StructType(
        [
            StructField("patient_id", StringType()),
            StructField("patient_email", StringType()),
            StructField("medication", StringType()),
            StructField("generic_name", StringType()),
            StructField("drug_class", StringType()),
            StructField("dosage", StringType()),
            StructField("frequency", StringType()),
            StructField("start_date", StringType()),
            StructField("end_date", StringType()),
            StructField("status", StringType()),
            StructField("prescriber_npi", StringType()),
            StructField("batch_date", StringType()),
        ]
    )

    return (
        spark.createDataFrame(rows, schema)
        .withColumn("start_date", F.to_date(F.col("start_date")))
        .withColumn("end_date", F.to_date(F.col("end_date")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("effective_start", F.col("start_date"))
        .withColumn(
            "effective_end",
            F.when(F.col("end_date").isNotNull(), F.col("end_date")).otherwise(
                F.lit(None).cast(DateType())
            ),
        )
        .withColumn("is_current", F.col("end_date").isNull())
        .withColumn(
            "row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("status"), F.lit("")),
                    F.coalesce(F.col("dosage"), F.lit("")),
                    F.coalesce(F.col("frequency"), F.lit("")),
                ),
                256,
            ),
        )
    )


def build_labs_df(spark: SparkSession, records: list[dict]) -> DataFrame:
    """
    Flatten Observation entries.
    Append-only — lab results don't change after the fact.
    """
    rows = []
    for p in records:
        patient_id = p["patient_id"]
        patient_email = p["patient_email"]
        batch_date = p["batch_date"]
        for entry in p["entries"]:
            if entry["resource_type"] != "Observation":
                continue
            rows.append(
                {
                    "patient_id": patient_id,
                    "patient_email": patient_email,
                    "test_code": entry["code"],
                    "value": float(entry["value"]),
                    "unit": entry["unit"],
                    "reference_low": float(entry["reference_low"]),
                    "reference_high": float(entry["reference_high"]),
                    "is_abnormal": entry["is_abnormal"],
                    "test_date": entry["date"],
                    "batch_date": batch_date,
                }
            )

    schema = StructType(
        [
            StructField("patient_id", StringType()),
            StructField("patient_email", StringType()),
            StructField("test_code", StringType()),
            StructField("value", DoubleType()),
            StructField("unit", StringType()),
            StructField("reference_low", DoubleType()),
            StructField("reference_high", DoubleType()),
            StructField("is_abnormal", BooleanType()),
            StructField("test_date", StringType()),
            StructField("batch_date", StringType()),
        ]
    )

    return (
        spark.createDataFrame(rows, schema)
        .withColumn("test_date", F.to_date(F.col("test_date")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn(
            "observation_id",
            F.sha2(
                # Natural key — patient + test + date (no UUID in generator)
                F.concat_ws(
                    "||",
                    F.col("patient_id"),
                    F.col("test_code"),
                    F.col("test_date"),
                ),
                256,
            ),
        )
    )


def load_conditions(df: DataFrame, spark: SparkSession, fmt: str) -> None:
    """SCD1 for conditions — status can change (active→resolved); update in place."""
    writer = make_writer_for(
        spark,
        fmt,
        path=settings.silver_ehr_conditions,
        table_name="ehr_conditions",
        layer="silver",
    )
    writer.merge(
        df,
        match_condition=(
            "t.patient_id = s.patient_id AND t.icd10_code = s.icd10_code"
        ),
    )
    log.info(
        "ehr_conditions merged",
        extra={"extra_data": {"format": fmt, "row_count": df.count()}},
    )


def load_medications(df: DataFrame, spark: SparkSession, fmt: str) -> None:
    """SCD2 for medications.

    Match key: patient_id + medication.
    Change detected via row_hash (status + dosage + frequency).
    Two-step pattern:
      1. Update-only MERGE — expire current rows whose row_hash drifted
         (sets effective_end and is_current=false).
      2. Append rows that are new (left-anti join against current rows).
    """
    writer = make_writer_for(
        spark,
        fmt,
        path=settings.silver_ehr_medications,
        table_name="ehr_medications",
        layer="silver",
    )

    # Step 1 — expire current records where hash changed (UPDATE-only).
    writer.merge(
        df,
        match_condition=(
            "t.patient_id = s.patient_id AND "
            "t.medication = s.medication AND "
            "t.is_current = true AND "
            "t.row_hash != s.row_hash"
        ),
        update_set={
            "effective_end": "s.effective_start",
            "is_current": "false",
        },
        with_insert=False,  # this branch only expires; new versions go to step 2
    )

    # Step 2 — insert new versions not already present as current.
    existing_current = writer.read_batch().filter(F.col("is_current"))
    new_records = df.alias("new").join(
        existing_current.alias("cur"),
        on=[
            F.col("new.patient_id") == F.col("cur.patient_id"),
            F.col("new.medication") == F.col("cur.medication"),
            F.col("new.row_hash") == F.col("cur.row_hash"),
        ],
        how="left_anti",
    )
    if new_records.count() > 0:
        writer.append(new_records)
    log.info(
        "ehr_medications updated (SCD2)", extra={"extra_data": {"format": fmt}}
    )


def load_labs(df: DataFrame, spark: SparkSession, fmt: str) -> None:
    """Append-only — insert new observations only (no updates)."""
    writer = make_writer_for(
        spark,
        fmt,
        path=settings.silver_ehr_lab_results,
        table_name="ehr_lab_results",
        layer="silver",
    )
    writer.merge(
        df,
        match_condition="t.observation_id = s.observation_id",
        with_update=False,  # never update — new observations only
    )
    log.info(
        "ehr_lab_results merged",
        extra={"extra_data": {"format": fmt, "row_count": df.count()}},
    )


def run_ehr_silver(spark: SparkSession, fmt: str = "delta") -> None:
    records = load_all_batches(spark)

    conditions_df = build_conditions_df(spark, records)
    medications_df = build_medications_df(spark, records)
    labs_df = build_labs_df(spark, records)

    log.info("Loading Silver EHR tables", extra={"extra_data": {"format": fmt}})
    load_conditions(conditions_df, spark, fmt)
    load_medications(medications_df, spark, fmt)
    load_labs(labs_df, spark, fmt)

    counts = {}
    for name, path, table in [
        ("ehr_conditions", settings.silver_ehr_conditions, "ehr_conditions"),
        ("ehr_medications", settings.silver_ehr_medications, "ehr_medications"),
        ("ehr_lab_results", settings.silver_ehr_lab_results, "ehr_lab_results"),
    ]:
        counts[name] = make_writer_for(
            spark, fmt, path=path, table_name=table, layer="silver"
        ).read_batch().count()
    log.info("EHR Silver row counts", extra={"extra_data": counts})

    log.info("EHR Silver complete")


if __name__ == "__main__":
    from streaming.spark_config import get_spark_session

    parser = argparse.ArgumentParser()
    parser.add_argument("--format", choices=["delta", "iceberg"], default="delta")
    args = parser.parse_args()
    spark = get_spark_session("EHRSilver")
    run_ehr_silver(spark, fmt=args.format)
