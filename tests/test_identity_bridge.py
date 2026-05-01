"""Identity bridge: transitive device linkage, MERGE idempotency, NULL handling."""
from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _seed_silver_ehr(spark, tmp_lakehouse):
    """Write minimal Silver EHR conditions + medications so build_ehr_identities
    has something to consume."""
    from config import settings

    rows = [
        ("MRN-1", "alice@example.com"),
        ("MRN-2", "bob@example.com"),
    ]
    df = spark.createDataFrame(rows, ["patient_id", "patient_email"])
    df.write.format("delta").save(settings.silver_ehr_conditions)
    df.write.format("delta").save(settings.silver_ehr_medications)


def _seed_silver_sensor(spark, devices: list[tuple[str, str | None]]):
    """Write Silver sensor with (device_account_id, patient_email) rows."""
    from config import settings

    df = spark.createDataFrame(devices, ["device_account_id", "patient_email"])
    df.write.format("delta").mode("overwrite").save(settings.silver_sensor)


@pytest.mark.usefixtures("tmp_lakehouse")
def test_build_ehr_identities_unions_conditions_and_medications(spark, tmp_lakehouse):
    from transformations.identity_resolution.patient_identity_bridge import (
        build_ehr_identities,
    )
    _seed_silver_ehr(spark, tmp_lakehouse)
    out = build_ehr_identities(spark).collect()
    emails = {r["patient_email"] for r in out}
    assert emails == {"alice@example.com", "bob@example.com"}
    assert all(r["patient_key"] is not None for r in out)


@pytest.mark.usefixtures("tmp_lakehouse")
def test_build_ehr_bridge_rows_emits_two_rows_per_patient(spark, tmp_lakehouse):
    from transformations.identity_resolution.patient_identity_bridge import (
        build_ehr_bridge_rows, build_ehr_identities,
    )
    _seed_silver_ehr(spark, tmp_lakehouse)
    bridge = build_ehr_bridge_rows(build_ehr_identities(spark))
    rows = bridge.collect()
    types = sorted(r["identifier_type"] for r in rows)
    assert types == ["email", "email", "hospital_mrn", "hospital_mrn"]


@pytest.mark.usefixtures("tmp_lakehouse")
def test_device_bridge_links_via_email(spark, tmp_lakehouse):
    """If a device's email matches an EHR email, status becomes 'linked'."""
    from transformations.identity_resolution.patient_identity_bridge import (
        build_device_bridge_rows, build_ehr_bridge_rows, build_ehr_identities,
        load_bridge,
    )
    _seed_silver_ehr(spark, tmp_lakehouse)
    _seed_silver_sensor(spark, [
        ("acct_known", "alice@example.com"),  # matches EHR
        ("acct_unknown", "stranger@example.com"),  # no match
        ("acct_no_email", None),  # no email at all
    ])
    # Phase 1: write EHR bridge rows
    load_bridge(build_ehr_bridge_rows(build_ehr_identities(spark)), spark)
    # Phase 2: device rows look up email in bridge
    device_rows = build_device_bridge_rows(spark).collect()
    by_id = {r["identifier_value"]: r for r in device_rows}
    assert by_id["acct_known"]["link_status"] == "linked"
    assert by_id["acct_known"]["match_method"] == "exact_email_match"
    assert by_id["acct_unknown"]["link_status"] == "pending_registration"
    assert by_id["acct_no_email"]["link_status"] == "pending_registration"


@pytest.mark.usefixtures("tmp_lakehouse")
def test_load_bridge_is_idempotent(spark, tmp_lakehouse):
    """Calling load_bridge twice with the same rows must not duplicate."""
    from config import settings
    from transformations.identity_resolution.patient_identity_bridge import (
        build_ehr_bridge_rows, build_ehr_identities, load_bridge,
    )
    _seed_silver_ehr(spark, tmp_lakehouse)
    bridge_rows = build_ehr_bridge_rows(build_ehr_identities(spark))
    load_bridge(bridge_rows, spark)
    n1 = spark.read.format("delta").load(settings.silver_identity_bridge).count()
    load_bridge(bridge_rows, spark)
    n2 = spark.read.format("delta").load(settings.silver_identity_bridge).count()
    assert n1 == n2  # MERGE on (identifier_type, identifier_value)


@pytest.mark.usefixtures("tmp_lakehouse")
def test_pharmacy_bridge_returns_none_when_bronze_missing(spark, tmp_lakehouse):
    from transformations.identity_resolution.patient_identity_bridge import (
        build_pharmacy_bridge_rows,
    )
    out = build_pharmacy_bridge_rows(spark)
    assert out is None
