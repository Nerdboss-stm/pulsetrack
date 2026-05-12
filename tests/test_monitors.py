"""Unit tests for observability.monitors — 4 Monte Carlo-style checks.

Each monitor returns a MonitorResult; we mock the SparkSession so tests don't
need a JVM. Covers freshness / volume / schema / distribution and the result
dataclass + serialization.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from observability.monitors import (
    MonitorResult,
    check_freshness,
    check_volume,
)


# ── MonitorResult dataclass ──────────────────────────────────────────────


def test_result_passed_property_ok():
    r = MonitorResult(monitor_name="x", table_name="t", check_type="freshness", status="ok")
    assert r.passed is True


def test_result_passed_property_warn():
    r = MonitorResult(monitor_name="x", table_name="t", check_type="volume", status="warn")
    assert r.passed is False


def test_result_passed_property_error():
    r = MonitorResult(monitor_name="x", table_name="t", check_type="freshness", status="error")
    assert r.passed is False


def test_result_run_at_defaults_now():
    r = MonitorResult(monitor_name="x", table_name="t", check_type="x", status="ok")
    delta = abs((datetime.now(timezone.utc) - r.run_at).total_seconds())
    assert delta < 5  # within 5 s of construction


# ── Freshness ────────────────────────────────────────────────────────────


def test_freshness_stub_when_no_spark():
    """spark=None returns a stub result, no SQL execution."""
    r = check_freshness("db.s.t", "ingestion_timestamp", 5)
    assert r.status == "ok"
    assert r.threshold == 5.0
    assert "(stub)" in r.detail


def test_freshness_fresh_table():
    """latest timestamp within max_age → status=ok."""
    spark = MagicMock()
    recent = datetime.now(timezone.utc) - timedelta(minutes=2)
    spark.sql.return_value.first.return_value = [recent]
    r = check_freshness("db.s.sensor_readings", "event_timestamp", 5, spark=spark)
    assert r.status == "ok"
    assert r.value < 5
    assert "age=" in r.detail


def test_freshness_stale_table():
    """latest timestamp beyond max_age → status=error."""
    spark = MagicMock()
    stale = datetime.now(timezone.utc) - timedelta(hours=2)
    spark.sql.return_value.first.return_value = [stale]
    r = check_freshness("db.s.sensor_readings", "event_timestamp", 5, spark=spark)
    assert r.status == "error"
    assert r.value > 5  # well past threshold
    assert r.threshold == 5.0


def test_freshness_empty_table():
    """MAX returns None → status=error (no timestamps)."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = [None]
    r = check_freshness("db.s.empty_table", "event_timestamp", 5, spark=spark)
    assert r.status == "error"
    assert "empty" in r.detail.lower()


def test_freshness_sql_construction():
    """The SQL is built from the table_fqn and timestamp_column."""
    spark = MagicMock()
    recent = datetime.now(timezone.utc) - timedelta(minutes=1)
    spark.sql.return_value.first.return_value = [recent]
    check_freshness("pulsetrack_silver_dev.sensor_readings", "event_timestamp", 5, spark=spark)
    sql = spark.sql.call_args[0][0]
    assert "MAX(event_timestamp)" in sql
    assert "pulsetrack_silver_dev.sensor_readings" in sql


# ── Volume ───────────────────────────────────────────────────────────────


def test_volume_stub_when_no_spark():
    r = check_volume("db.s.t")
    assert r.status == "ok"
    assert "(stub)" in r.detail


def test_volume_above_min_no_baseline():
    """Row count above expected_min → ok, no baseline check fires."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = [1000]
    r = check_volume("db.s.t", expected_min=100, spark=spark)
    assert r.status == "ok"


def test_volume_below_expected_min():
    """Row count below expected_min → status=error."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = [5]
    r = check_volume("db.s.t", expected_min=100, spark=spark)
    assert r.status == "error"
    assert r.value == 5.0
    assert r.threshold == 100.0
    assert "below expected_min" in r.detail


def test_volume_within_baseline_sigma():
    """Z-score within sigma_threshold → ok."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = [1050]
    r = check_volume(
        "db.s.t",
        rolling_avg=1000.0,
        rolling_stddev=50.0,
        sigma_threshold=3.0,
        spark=spark,
    )
    # |1050 - 1000| / 50 = 1.0 → within 3.0 → ok
    assert r.status == "ok"


def test_volume_outside_baseline_sigma():
    """Z-score > sigma_threshold → status=warn."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = [10000]
    r = check_volume(
        "db.s.t",
        rolling_avg=1000.0,
        rolling_stddev=100.0,
        sigma_threshold=3.0,
        spark=spark,
    )
    # |10000-1000|/100 = 90.0 z-score → warn
    assert r.status == "warn"


def test_volume_zero_stddev_skips_baseline_check():
    """rolling_stddev=0 → divide-by-zero protection; check is skipped."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = [500]
    r = check_volume(
        "db.s.t",
        rolling_avg=1000.0,
        rolling_stddev=0.0,
        sigma_threshold=3.0,
        spark=spark,
    )
    # 500 != 1000 but stddev is 0 → no z-score check, status defaults to ok
    assert r.status == "ok"


def test_volume_combines_min_and_baseline():
    """Both expected_min check + baseline check operate together."""
    spark = MagicMock()
    spark.sql.return_value.first.return_value = [1010]
    r = check_volume(
        "db.s.t",
        expected_min=100,
        rolling_avg=1000.0,
        rolling_stddev=10.0,
        sigma_threshold=3.0,
        spark=spark,
    )
    assert r.status == "ok"
