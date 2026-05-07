"""
Patient identity bridge.

Phases (executed in order so each phase can read what the previous one wrote):

1. EHR identities — build (MRN, email) bridge rows from Silver EHR tables.
   ``patient_key`` is the sha256 of the email — the unifier across sources.

2. Device identities — read sensor Silver, look up each device's
   ``patient_email`` in the bridge to find the linked ``patient_key``.
   Devices whose email matches are ``linked``; everything else is
   ``pending_registration``.

3. Pharmacy identities (Open FDA) — when a Bronze pharmacy table exists,
   register every ``fda_report_id`` as a fourth identifier type. These rows
   are ``pending_registration`` until a future linkage source connects them
   to a patient_key.

4. Resolution metrics — log linkage KPIs.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from data_quality.identity_metrics import compute_resolution_metrics  # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger(__name__)


# ── Phase 1: EHR identities ─────────────────────────────────────────────────
def build_ehr_identities(spark: SparkSession) -> DataFrame:
    """Union (patient_id, patient_email) from EHR conditions + medications."""
    conditions = (
        spark.read.format("delta")
        .load(settings.silver_ehr_conditions)
        .select("patient_id", "patient_email")
        .filter(F.col("patient_id").isNotNull())
    )

    medications = (
        spark.read.format("delta")
        .load(settings.silver_ehr_medications)
        .select("patient_id", "patient_email")
        .filter(F.col("patient_id").isNotNull())
    )

    ehr_identities = conditions.union(medications).dropDuplicates(["patient_id", "patient_email"])

    return ehr_identities.withColumn(
        "patient_key",
        F.sha2(F.lower(F.trim(F.col("patient_email"))), 256),
    )


def build_ehr_bridge_rows(ehr_df: DataFrame) -> DataFrame:
    """Each EHR patient produces two rows: hospital_mrn + email."""
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
    return mrn_rows.union(email_rows).dropDuplicates(
        ["patient_key", "identifier_type", "identifier_value"]
    )


# ── Phase 2: Device identities (transitive via email) ───────────────────────
def build_device_bridge_rows(spark: SparkSession) -> DataFrame:
    """
    Device accounts now carry patient_email (set by the wearable producer).
    Look up email in the EHR bridge rows already in the bridge to find
    patient_key. Devices whose email matches → linked. Devices without an
    email match → pending_registration.

    Must be called AFTER the EHR bridge rows have been written.
    """
    devices = (
        spark.read.format("delta")
        .load(settings.silver_sensor)
        .select("device_account_id", "patient_email")
        .filter(F.col("device_account_id").isNotNull())
        .distinct()
    )

    ehr_emails = (
        spark.read.format("delta")
        .load(settings.silver_identity_bridge)
        .filter(F.col("identifier_type") == "email")
        .select(
            F.col("identifier_value").alias("patient_email"),
            F.col("patient_key"),
        )
    )

    linked = devices.join(ehr_emails, on="patient_email", how="left")

    return linked.select(
        F.col("patient_key"),
        F.lit("device_account_id").alias("identifier_type"),
        F.col("device_account_id").alias("identifier_value"),
        F.lit("wearable").alias("source"),
        F.when(F.col("patient_key").isNotNull(), F.lit("linked"))
        .otherwise(F.lit("pending_registration"))
        .alias("link_status"),
        F.when(F.col("patient_key").isNotNull(), F.lit("exact_email_match"))
        .otherwise(F.lit("none"))
        .alias("match_method"),
    )


# ── Phase 3: Pharmacy / FDA identifiers ─────────────────────────────────────
def build_pharmacy_bridge_rows(spark: SparkSession) -> Optional[DataFrame]:
    """
    Open FDA adverse-event reports flow into the bridge as a 4th identifier
    type (``fda_report_id``). Returns None if no pharmacy Bronze table exists
    yet — callers should treat that as "no pharmacy bridge contribution".

    The current pipeline doesn't yet have an MRN/email link for FDA
    pseudo-patients, so these rows land as ``pending_registration``. A
    future linkage source (e.g., NDC + patient consent) can update them.
    """
    if not DeltaTable.isDeltaTable(spark, settings.bronze_pharmacy):
        log.info(
            "No bronze_pharmacy table — skipping pharmacy bridge phase",
            extra={"extra_data": {"path": settings.bronze_pharmacy}},
        )
        return None

    bronze = spark.read.format("delta").load(settings.bronze_pharmacy)
    if "decoded" not in bronze.columns:
        log.warning(
            "bronze_pharmacy missing 'decoded' struct — skipping",
            extra={"extra_data": {"columns": bronze.columns}},
        )
        return None

    fda = (
        bronze.select(
            F.col("decoded.fda_report_id").alias("fda_report_id"),
            F.col("decoded.event_type").alias("event_type"),
        )
        .filter(F.col("fda_report_id").isNotNull())
        .filter(F.col("event_type") == "adverse_event")
        .distinct()
    )

    return fda.select(
        F.lit(None).cast(StringType()).alias("patient_key"),
        F.lit("fda_report_id").alias("identifier_type"),
        F.col("fda_report_id").alias("identifier_value"),
        F.lit("pharmacy_fda").alias("source"),
        F.lit("pending_registration").alias("link_status"),
        F.lit("none").alias("match_method"),
    )


# ── Bridge sink ─────────────────────────────────────────────────────────────
def load_bridge(bridge_df: DataFrame, spark: SparkSession):
    bridge_df = bridge_df.withColumn("first_seen", F.current_timestamp()).withColumn(
        "last_seen", F.current_timestamp()
    )

    if not DeltaTable.isDeltaTable(spark, settings.silver_identity_bridge):
        bridge_df.write.format("delta").save(settings.silver_identity_bridge)
        log.info(
            "patient_identity_bridge created",
            extra={"extra_data": {"row_count": bridge_df.count()}},
        )
        return

    DeltaTable.forPath(spark, settings.silver_identity_bridge).alias("bridge").merge(
        bridge_df.alias("new"),
        """bridge.identifier_type  = new.identifier_type
           AND bridge.identifier_value = new.identifier_value""",
    ).whenMatchedUpdate(
        set={
            "last_seen": "new.last_seen",
            "link_status": "new.link_status",
            "patient_key": "new.patient_key",
            "match_method": "new.match_method",
        }
    ).whenNotMatchedInsertAll().execute()
    log.info("patient_identity_bridge merged")


# ── Orchestration ───────────────────────────────────────────────────────────
def run_identity_bridge(spark: SparkSession):
    # Phase 1: EHR rows must land first so phase 2 can read them.
    log.info("Phase 1: Building EHR identities")
    ehr_df = build_ehr_identities(spark)
    log.info("Unique EHR patients", extra={"extra_data": {"count": ehr_df.count()}})
    load_bridge(build_ehr_bridge_rows(ehr_df), spark)

    # Phase 2: Devices look up email → patient_key in the bridge.
    log.info("Phase 2: Building device identities (transitive email link)")
    load_bridge(build_device_bridge_rows(spark), spark)

    # Phase 3: FDA report IDs (if a pharmacy Bronze table exists).
    log.info("Phase 3: Building pharmacy / FDA identifiers")
    pharmacy_bridge = build_pharmacy_bridge_rows(spark)
    if pharmacy_bridge is not None:
        load_bridge(pharmacy_bridge, spark)

    # Phase 4: Resolution KPIs.
    log.info("Phase 4: Computing identity resolution metrics")
    compute_resolution_metrics(spark)

    log.info("Identity bridge complete")


if __name__ == "__main__":
    from streaming.spark_config import get_spark_session

    sess = get_spark_session("PatientIdentityBridge")
    run_identity_bridge(sess)
