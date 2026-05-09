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

import glob
import json
import os
import sys
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
import argparse  # noqa: E402

from config import settings  # noqa: E402
from lakehouse import make_writer_for  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)


def load_ehr_demographics(spark):
    """
    Read patient_id, patient_email, patient_birth_year from raw EHR batch JSON files.
    Silver ehr_silver.py does not capture these patient-level fields — only the entries
    (Condition, MedicationStatement, Observation) are stored in Silver.
    """
    rows = []
    for batch_file in glob.glob(os.path.join(settings.ehr_batch_dir, "*/ehr_batch.json")):
        with open(batch_file) as f:
            data = json.load(f)
        for p in data["patients"]:
            rows.append(
                (
                    p["patient_id"],
                    p.get("patient_email", ""),
                    int(p.get("patient_birth_year", 0)),
                )
            )

    if not rows:
        return spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("patient_id", StringType()),
                    StructField("patient_email", StringType()),
                    StructField("patient_birth_year", IntegerType()),
                ]
            ),
        )

    current_year = datetime.utcnow().year
    return (
        spark.createDataFrame(rows, ["patient_id", "patient_email", "patient_birth_year"])
        .dropDuplicates(["patient_id"])
        .withColumn("age", F.lit(current_year) - F.col("patient_birth_year"))
        .withColumn(
            "age_group",
            F.when(F.col("age") < 18, "0-17")
            .when(F.col("age") < 35, "18-34")
            .when(F.col("age") < 50, "35-49")
            .when(F.col("age") < 65, "50-64")
            .otherwise("65+"),
        )
    )


def main(fmt: str = "delta") -> None:
    spark = get_spark_session("GoldDimPatient")

    demo_df = load_ehr_demographics(spark)
    log.info(
        "EHR demographics loaded",
        extra={"extra_data": {"patient_count": demo_df.count()}},
    )

    out_writer = make_writer_for(
        spark, fmt, path=settings.gold_dim_patient, table_name="dim_patient", layer="gold"
    )

    # ── Fallback: no Silver bridge yet ─────────────────────────────────
    bridge_writer = make_writer_for(
        spark,
        fmt,
        path=settings.silver_identity_bridge,
        table_name="identity_bridge",
        layer="silver",
    )
    bridge_available = (
        # Iceberg: table is created by V001 + identity_bridge — we treat
        # an empty table as "available" since the joins below tolerate empty
        # bridge rows. For Delta, fall back to the original isDeltaTable check.
        fmt == "iceberg"
        or DeltaTable.isDeltaTable(spark, settings.silver_identity_bridge)
    )
    if not bridge_available:
        log.warning("Identity bridge not found — building from EHR batch files only")
        df = (
            demo_df.withColumn(
                "patient_key",
                F.abs(F.hash(F.sha2(F.lower(F.trim(F.col("patient_email"))), 256))).cast("long"),
            )
            .withColumn("patient_id_masked", F.sha2(F.lower(F.trim(F.col("patient_id"))), 256))
            .withColumn("gender", F.lit("Unknown"))
            .withColumn("primary_condition_key", F.lit(None).cast(LongType()))
            .withColumn("device_count", F.lit(0).cast(LongType()))
            .withColumn("first_reading_date", F.lit(None).cast(DateType()))
            .select(
                "patient_key",
                "patient_id_masked",
                "age_group",
                "gender",
                "primary_condition_key",
                "device_count",
                "first_reading_date",
            )
            .dropDuplicates(["patient_key"])
        )
        out_writer.overwrite(df)
        log.info(
            "dim_patient written (seeded from EHR batch files)",
            extra={"extra_data": {"row_count": df.count(), "format": fmt}},
        )
        return

    # ── Full build from Silver ──────────────────────────────────────────
    bridge = bridge_writer.read_batch()

    mrn_rows = bridge.filter(F.col("identifier_type") == "hospital_mrn").select(
        F.col("patient_key").alias("patient_key_sha256"),
        F.col("identifier_value").alias("patient_id"),
    )
    email_rows = bridge.filter(F.col("identifier_type") == "email").select(
        F.col("patient_key").alias("patient_key_sha256"),
        F.col("identifier_value").alias("patient_email"),
    )

    linked = mrn_rows.join(email_rows, on="patient_key_sha256", how="inner")

    patients = linked.join(
        demo_df.select("patient_id", "age_group"), on="patient_id", how="left"
    ).withColumn("age_group", F.coalesce(F.col("age_group"), F.lit("Unknown")))

    # ── Primary condition (first active ICD-10 per patient) ─────────────
    conds_writer = make_writer_for(
        spark, fmt, path=settings.silver_ehr_conditions, table_name="ehr_conditions", layer="silver"
    )
    dim_cond_writer = make_writer_for(
        spark, fmt, path=settings.gold_dim_condition, table_name="dim_condition", layer="gold"
    )
    conds_available = fmt == "iceberg" or DeltaTable.isDeltaTable(spark, settings.silver_ehr_conditions)
    if conds_available:
        conds_silver = conds_writer.read_batch()
        dim_cond = dim_cond_writer.read_batch()

        cond_lookup = dim_cond.select(
            F.col("condition_key"),
            F.col("condition_code").alias("icd10_code"),
        )
        # Deterministic primary condition: pick the row with the lexicographically
        # smallest icd10_code per patient (ties broken by condition_key). Avoids the
        # non-determinism of F.first() over an unordered groupBy.
        primary_window = Window.partitionBy("patient_id").orderBy(
            F.col("icd10_code").asc_nulls_last(), F.col("condition_key").asc_nulls_last()
        )
        primary_cond = (
            conds_silver.filter(F.col("status") == "active")
            .join(cond_lookup, on="icd10_code", how="left")
            .withColumn("__rn", F.row_number().over(primary_window))
            .filter(F.col("__rn") == 1)
            .select(F.col("patient_id"), F.col("condition_key").alias("primary_condition_key"))
        )
        patients = patients.join(primary_cond, on="patient_id", how="left")
    else:
        patients = patients.withColumn("primary_condition_key", F.lit(None).cast(LongType()))

    # ── Device count + first reading date (via bridge) ──────────────────
    sensor_writer = make_writer_for(
        spark, fmt, path=settings.silver_sensor, table_name="sensor_readings", layer="silver"
    )
    sensors_available = fmt == "iceberg" or DeltaTable.isDeltaTable(spark, settings.silver_sensor)
    if sensors_available:
        sensors = sensor_writer.read_batch()

        device_bridge = bridge.filter(F.col("identifier_type") == "device_account_id").select(
            F.col("identifier_value").alias("device_account_id"),
            F.col("patient_key").alias("patient_key_sha256"),
        )
        device_stats = (
            sensors.join(device_bridge, on="device_account_id", how="left")
            .groupBy("patient_key_sha256")
            .agg(
                F.countDistinct("device_id").alias("device_count"),
                F.min(F.to_date("event_timestamp")).alias("first_reading_date"),
            )
        )
        patients = patients.join(device_stats, on="patient_key_sha256", how="left")
    else:
        patients = patients.withColumn("device_count", F.lit(0).cast(LongType())).withColumn(
            "first_reading_date", F.lit(None).cast(DateType())
        )

    # ── Surrogate key + PII masking + final select ──────────────────────
    df = (
        patients.withColumn("patient_key", F.abs(F.hash(F.col("patient_key_sha256"))).cast("long"))
        .withColumn("patient_id_masked", F.sha2(F.lower(F.trim(F.col("patient_id"))), 256))
        .withColumn("gender", F.lit("Unknown"))
        .fillna({"device_count": 0})
        .select(
            "patient_key",
            "patient_id_masked",
            "age_group",
            "gender",
            "primary_condition_key",
            "device_count",
            "first_reading_date",
        )
        .dropDuplicates(["patient_key"])
    )

    out_writer.overwrite(df)
    log.info(
        "dim_patient written",
        extra={"extra_data": {"row_count": df.count(), "format": fmt}},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--format", choices=["delta", "iceberg"], default="delta")
    args = parser.parse_args()
    main(fmt=args.format)
