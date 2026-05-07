"""Quarantine sink filters bad rows, writes Delta, bumps the metric."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.mark.usefixtures("tmp_lakehouse")
def test_quarantine_writes_only_invalid_rows(spark, tmp_lakehouse):
    from pyspark.sql import functions as F

    from data_quality.quarantine import quarantine_records

    df = spark.createDataFrame(
        [
            ("r1", "hr", 70.0, True),
            ("r2", "hr", 9999.0, False),
            ("r3", "spo2", 98.0, True),
            ("r4", "spo2", 30.0, False),
        ],
        ["reading_id", "metric_name", "metric_value", "is_valid"],
    )

    n = quarantine_records(df, "is_valid", layer="silver", source="sensor")
    assert n == 2

    out = spark.read.format("delta").load(str(tmp_lakehouse / "quarantine"))
    assert out.count() == 2
    # Sanity: only is_valid=False rows
    assert out.filter(F.col("is_valid") == True).count() == 0  # noqa: E712
    # Required metadata present
    cols = set(out.columns)
    assert {"quarantine_layer", "quarantine_source", "quarantine_reason", "quarantined_at"} <= cols


@pytest.mark.usefixtures("tmp_lakehouse")
def test_quarantine_no_op_when_all_valid(spark, tmp_lakehouse):
    from data_quality.quarantine import quarantine_records

    df = spark.createDataFrame(
        [("r1", "hr", 70.0, True), ("r2", "spo2", 98.0, True)],
        ["reading_id", "metric_name", "metric_value", "is_valid"],
    )
    n = quarantine_records(df, "is_valid", layer="silver", source="sensor")
    assert n == 0
    # No table created when nothing to write
    qpath = tmp_lakehouse / "quarantine"
    assert not qpath.exists() or not any(qpath.iterdir())
