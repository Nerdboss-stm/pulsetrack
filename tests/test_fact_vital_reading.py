"""fact_vital_reading: per-reading fact, schema + join logic."""
from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _silver_df(spark):
    return spark.createDataFrame(
        [
            ("acct_1", "smartwatch", "heart_rate_bpm", 72.0, True, False,
             datetime(2026, 5, 3, 14, 0)),
            ("acct_1", "smartwatch", "spo2_pct", 97.0, True, False,
             datetime(2026, 5, 3, 14, 0)),
        ],
        ["device_account_id", "device_type", "metric_name", "metric_value",
         "is_valid", "is_late_arriving", "event_timestamp"],
    )


def _dim_metric(spark):
    return spark.createDataFrame(
        [
            (1, "heart_rate_bpm", "smartwatch"),
            (2, "spo2_pct", "smartwatch"),
        ],
        ["metric_key", "metric_name", "device_type"],
    )


def test_build_facts_resolves_metric_key(spark):
    from transformations.silver_to_gold.fact_vital_reading import _build_facts

    out = _build_facts(_silver_df(spark), _dim_metric(spark), bridge_df=None).collect()
    assert len(out) == 2
    by_metric = {r["metric_key"]: r for r in out}
    assert by_metric[1]["value"] == 72.0
    assert by_metric[2]["value"] == 97.0


def test_build_facts_falls_back_to_device_hash_for_unregistered(spark):
    """No bridge means patient_key derived from device_account_id."""
    from transformations.silver_to_gold.fact_vital_reading import _build_facts

    out = _build_facts(_silver_df(spark), _dim_metric(spark), bridge_df=None).collect()
    assert all(r["patient_key"] is not None for r in out)
    # Same device_account_id ⇒ same patient_key across rows
    assert len({r["patient_key"] for r in out}) == 1


def test_build_facts_drops_rows_with_unknown_metric(spark):
    from transformations.silver_to_gold.fact_vital_reading import _build_facts

    silver = spark.createDataFrame(
        [
            ("acct_1", "smartwatch", "heart_rate_bpm", 72.0, True, False,
             datetime(2026, 5, 3, 14, 0)),
            ("acct_1", "smartwatch", "unknown_metric", 1.0, True, False,
             datetime(2026, 5, 3, 14, 0)),
        ],
        ["device_account_id", "device_type", "metric_name", "metric_value",
         "is_valid", "is_late_arriving", "event_timestamp"],
    )
    out = _build_facts(silver, _dim_metric(spark), bridge_df=None).collect()
    assert len(out) == 1  # unknown metric filtered


def test_build_facts_emits_date_key_yyyymmdd(spark):
    from transformations.silver_to_gold.fact_vital_reading import _build_facts

    out = _build_facts(_silver_df(spark), _dim_metric(spark), bridge_df=None).collect()
    assert all(r["date_key"] == 20260503 for r in out)
