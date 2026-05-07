"""sensor_silver: parse_and_explode, add_quality_flags, dedup, MERGE."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Explicit schema so the `metrics` field becomes a MapType, not a nested struct.
_DECODED_SCHEMA = StructType(
    [
        StructField("reading_id", StringType()),
        StructField("device_id", StringType()),
        StructField("device_type", StringType()),
        StructField("user_device_account_id", StringType()),
        StructField("patient_email", StringType()),
        StructField("firmware_version", StringType()),
        StructField("battery_pct", IntegerType()),
        StructField("metrics", MapType(StringType(), DoubleType())),
        StructField("event_timestamp", TimestampType()),
        StructField("sync_timestamp", TimestampType()),
    ]
)
BRONZE_TEST_SCHEMA = StructType(
    [
        StructField("decoded", _DECODED_SCHEMA),
        StructField("ingestion_timestamp", TimestampType()),
        StructField("is_parseable", BooleanType()),
    ]
)


def _bronze_row(
    reading_id: str,
    device_type: str,
    metrics: dict,
    event_ts: datetime,
    sync_ts: datetime | None = None,
    is_parseable: bool = True,
) -> dict:
    sync_ts = sync_ts or event_ts
    return {
        "is_parseable": is_parseable,
        "ingestion_timestamp": event_ts,
        "decoded": {
            "reading_id": reading_id,
            "device_id": "SW-AAA-12345",
            "device_type": device_type,
            "user_device_account_id": "acct_1",
            "patient_email": "p@example.com",
            "firmware_version": "3.4.1",
            "battery_pct": 80,
            "metrics": {k: (float(v) if v is not None else None) for k, v in metrics.items()},
            "event_timestamp": event_ts,
            "sync_timestamp": sync_ts,
        },
    }


def test_parse_and_explode_drops_unparseable_rows(spark):
    from transformations.bronze_to_silver.sensor_silver import parse_and_explode

    now = datetime(2026, 5, 3, 14, 0)
    rows = [
        _bronze_row("r1", "smartwatch", {"heart_rate_bpm": 72.0}, now),
        _bronze_row("r2", "smartwatch", {"heart_rate_bpm": 80.0}, now, is_parseable=False),
    ]
    df = spark.createDataFrame(rows, schema=BRONZE_TEST_SCHEMA)
    out = parse_and_explode(df).collect()
    assert len(out) == 1
    assert out[0]["reading_id"] == "r1"
    assert out[0]["metric_name"] == "heart_rate_bpm"
    assert out[0]["metric_value"] == 72.0


def test_parse_and_explode_explodes_one_row_per_metric(spark):
    from transformations.bronze_to_silver.sensor_silver import parse_and_explode

    now = datetime(2026, 5, 3, 14, 0)
    rows = [
        _bronze_row(
            "r1",
            "smartwatch",
            {"heart_rate_bpm": 72.0, "spo2_pct": 97.0, "hrv_ms": 45.0},
            now,
        )
    ]
    df = spark.createDataFrame(rows, schema=BRONZE_TEST_SCHEMA)
    out = parse_and_explode(df).collect()
    metric_names = {r["metric_name"] for r in out}
    assert metric_names == {"heart_rate_bpm", "spo2_pct", "hrv_ms"}


def test_quality_flags_marks_out_of_range_invalid(spark):
    from transformations.bronze_to_silver.sensor_silver import (
        add_quality_flags,
        parse_and_explode,
    )

    now = datetime(2026, 5, 3, 14, 0)
    rows = [
        _bronze_row("r1", "smartwatch", {"heart_rate_bpm": 70.0}, now),  # ok
        _bronze_row("r2", "smartwatch", {"heart_rate_bpm": 999.0}, now),  # invalid
        _bronze_row("r3", "smartwatch", {"heart_rate_bpm": None}, now),  # null → invalid
    ]
    df = spark.createDataFrame(rows, schema=BRONZE_TEST_SCHEMA)
    out = add_quality_flags(parse_and_explode(df))
    rows_out = {r["reading_id"]: r for r in out.collect()}
    assert rows_out["r1"]["is_valid"] is True
    assert rows_out["r2"]["is_valid"] is False
    assert rows_out["r3"]["is_valid"] is False


def test_quality_flags_marks_late_arriving(spark):
    from transformations.bronze_to_silver.sensor_silver import (
        add_quality_flags,
        parse_and_explode,
    )

    event_ts = datetime(2026, 5, 3, 10, 0)
    sync_ts = event_ts + timedelta(hours=3)  # > 2h late
    rows = [_bronze_row("r1", "smartwatch", {"heart_rate_bpm": 70.0}, event_ts, sync_ts)]
    df = spark.createDataFrame(rows, schema=BRONZE_TEST_SCHEMA)
    out = add_quality_flags(parse_and_explode(df)).collect()
    assert out[0]["is_late_arriving"] is True


def test_quality_flags_metric_ranges_cover_all_known_metrics(spark):
    from transformations.bronze_to_silver.sensor_silver import METRIC_RANGES

    expected_metrics = {
        "heart_rate_bpm",
        "spo2_pct",
        "hrv_ms",
        "skin_temp_celsius",
        "respiration_rate",
        "blood_glucose_mgdl",
        "bp_systolic_mmhg",
        "bp_diastolic_mmhg",
    }
    assert expected_metrics <= set(METRIC_RANGES.keys())
