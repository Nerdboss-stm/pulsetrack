"""maintenance.compaction smoke tests — table list shape and skip semantics."""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_table_list_covers_bronze_silver_gold_ops():
    from maintenance.compaction import TABLES

    paths = [t["path"] for t in TABLES]
    assert any("bronze/sensor_readings" in p for p in paths)
    assert any("silver/sensor_readings" in p for p in paths)
    assert any("gold/fact_vital_daily_summary" in p for p in paths)
    assert any("gold/dim_patient" in p for p in paths)
    assert any("dlq" in p for p in paths)
    assert any("quarantine" in p for p in paths)


def test_zorder_columns_set_for_high_volume_tables():
    from maintenance.compaction import TABLES

    by_path = {t["path"]: t["zorder_cols"] for t in TABLES}
    sensor_silver = next(p for p in by_path if "silver/sensor_readings" in p)
    assert by_path[sensor_silver] == ["device_type", "metric_name"]
    fact_daily = next(p for p in by_path if "fact_vital_daily_summary" in p)
    assert by_path[fact_daily] == ["patient_key", "date_key"]


def test_table_properties_contain_user_spec_values():
    from maintenance.compaction import TABLE_PROPERTIES

    assert TABLE_PROPERTIES["delta.autoOptimize.optimizeWrite"] == "true"
    assert TABLE_PROPERTIES["delta.autoOptimize.autoCompact"] == "true"
    assert TABLE_PROPERTIES["delta.logRetentionDuration"] == "interval 30 days"
    assert TABLE_PROPERTIES["delta.deletedFileRetentionDuration"] == "interval 7 days"


def test_maintain_table_skips_when_path_is_not_delta(spark, tmp_lakehouse):
    from maintenance import compaction
    from delta.tables import DeltaTable

    nonexistent = str(tmp_lakehouse / "no_such_table")
    # No exception — silent skip
    compaction._maintain_table(spark, nonexistent, [])
    assert not DeltaTable.isDeltaTable(spark, nonexistent)


def test_maintain_table_runs_optimize_on_real_table(spark, tmp_lakehouse):
    from maintenance import compaction

    path = str(tmp_lakehouse / "tiny")
    df = spark.createDataFrame(
        [(i, f"k{i % 4}", float(i)) for i in range(50)],
        ["id", "key", "val"],
    )
    df.write.format("delta").save(path)
    # Z-ORDER on a small table is fast
    compaction._maintain_table(spark, path, ["key"])
    # File still readable, count preserved
    out = spark.read.format("delta").load(path)
    assert out.count() == 50
