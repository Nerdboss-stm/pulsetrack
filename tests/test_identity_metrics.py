"""Tests for data_quality.identity_metrics — KPI computation for the bridge."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _seed_bridge(spark, settings):
    """Write a synthetic identity-bridge Delta table at settings.silver_identity_bridge."""
    bridge_rows = [
        # patient_key, identifier_type, identifier_value, link_status
        ("p1", "email", "alice@example.com", "linked"),
        ("p1", "hospital_mrn", "MRN-1", "linked"),
        ("p1", "device_account_id", "acct_known", "linked"),
        ("p2", "email", "bob@example.com", "linked"),
        ("p2", "hospital_mrn", "MRN-2", "linked"),
        (None, "device_account_id", "acct_orphan", "pending_registration"),
        (None, "fda_report_id", "rep_001", "pending_registration"),
    ]
    df = spark.createDataFrame(
        bridge_rows,
        ["patient_key", "identifier_type", "identifier_value", "link_status"],
    )
    df.write.format("delta").mode("overwrite").save(settings.silver_identity_bridge)


@pytest.mark.usefixtures("tmp_lakehouse")
def test_compute_resolution_metrics_returns_link_rate(spark, tmp_lakehouse):
    from config import settings
    from data_quality.identity_metrics import compute_resolution_metrics

    _seed_bridge(spark, settings)
    metrics = compute_resolution_metrics(spark, fmt="delta")
    # 5 linked / 7 total ≈ 71.4 %
    assert metrics["total_bridge_rows"] == 7
    assert metrics["linked"] == 5
    assert metrics["pending"] == 2
    assert metrics["link_rate_pct"] == pytest.approx(71.4, rel=1e-2)


@pytest.mark.usefixtures("tmp_lakehouse")
def test_compute_resolution_metrics_unique_patients(spark, tmp_lakehouse):
    from config import settings
    from data_quality.identity_metrics import compute_resolution_metrics

    _seed_bridge(spark, settings)
    metrics = compute_resolution_metrics(spark, fmt="delta")
    assert metrics["unique_patients"] == 2  # p1, p2


@pytest.mark.usefixtures("tmp_lakehouse")
def test_compute_resolution_metrics_avg_ids_per_patient(spark, tmp_lakehouse):
    from config import settings
    from data_quality.identity_metrics import compute_resolution_metrics

    _seed_bridge(spark, settings)
    metrics = compute_resolution_metrics(spark, fmt="delta")
    # p1 has 3 identifiers; p2 has 2; avg = 2.5
    assert metrics["avg_identifiers_per_patient"] == pytest.approx(2.5)


@pytest.mark.usefixtures("tmp_lakehouse")
def test_compute_resolution_metrics_breakdown_has_all_types(spark, tmp_lakehouse):
    from config import settings
    from data_quality.identity_metrics import compute_resolution_metrics

    _seed_bridge(spark, settings)
    metrics = compute_resolution_metrics(spark, fmt="delta")
    types = {row["identifier_type"] for row in metrics["breakdown"]}
    assert types == {"email", "hospital_mrn", "device_account_id", "fda_report_id"}
