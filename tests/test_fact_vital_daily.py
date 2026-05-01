"""fact_vital_daily_summary: aggregation math (avg/min/max/count, normal-range %)."""
from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.mark.usefixtures("tmp_lakehouse")
def test_aggregate_basic_stats(spark, tmp_lakehouse):
    from transformations.silver_to_gold.fact_vital_daily_summary import _aggregate

    silver = spark.createDataFrame(
        [
            ("acct_1", "smartwatch", "heart_rate_bpm", 70.0, True, datetime(2026, 5, 3, 8)),
            ("acct_1", "smartwatch", "heart_rate_bpm", 80.0, True, datetime(2026, 5, 3, 12)),
            ("acct_1", "smartwatch", "heart_rate_bpm", 90.0, True, datetime(2026, 5, 3, 16)),
            ("acct_1", "smartwatch", "heart_rate_bpm", 200.0, False, datetime(2026, 5, 3, 17)),  # invalid
        ],
        ["device_account_id", "device_type", "metric_name", "metric_value",
         "is_valid", "event_timestamp"],
    )

    dim_metric = spark.createDataFrame(
        [(1234, "heart_rate_bpm", "smartwatch", 45.0, 180.0)],
        ["metric_key", "metric_name", "device_type", "normal_low", "normal_high"],
    )

    out = _aggregate(silver, dim_metric, bridge_df=None).collect()
    assert len(out) == 1
    row = out[0]
    assert row["reading_count"] == 4
    assert row["anomaly_count"] == 1  # the invalid one
    # avg/min/max use only valid rows
    assert row["avg_value"] == pytest.approx((70.0 + 80.0 + 90.0) / 3)
    assert row["min_value"] == 70.0
    assert row["max_value"] == 90.0
    # 3 of 4 readings in normal range (the 200 is out of range AND invalid)
    assert row["pct_in_normal_range"] == pytest.approx(75.0)


@pytest.mark.usefixtures("tmp_lakehouse")
def test_aggregate_excludes_sleep_stage(spark, tmp_lakehouse):
    from transformations.silver_to_gold.fact_vital_daily_summary import _aggregate

    silver = spark.createDataFrame(
        [
            ("acct_1", "sleep_ring", "sleep_stage", 2.0, True, datetime(2026, 5, 3, 3)),
            ("acct_1", "sleep_ring", "heart_rate_bpm", 60.0, True, datetime(2026, 5, 3, 3)),
        ],
        ["device_account_id", "device_type", "metric_name", "metric_value",
         "is_valid", "event_timestamp"],
    )
    dim_metric = spark.createDataFrame(
        [
            (1, "sleep_stage", "sleep_ring", 0.0, 4.0),
            (2, "heart_rate_bpm", "sleep_ring", 40.0, 100.0),
        ],
        ["metric_key", "metric_name", "device_type", "normal_low", "normal_high"],
    )
    out = _aggregate(silver, dim_metric, bridge_df=None).collect()
    metric_keys = {r["metric_key"] for r in out}
    # sleep_stage filtered out, only heart_rate_bpm aggregated
    assert metric_keys == {2}
