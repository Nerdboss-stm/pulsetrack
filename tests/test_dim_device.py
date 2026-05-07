"""dim_device SCD2: history derivation, current flagging."""

from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _silver_rows(rows: list[tuple[str, str, str, datetime]]):
    """rows: list of (device_id, device_type, firmware_version, event_timestamp)."""
    return rows


@pytest.mark.usefixtures("tmp_lakehouse")
def test_single_firmware_version_marked_current(spark, tmp_lakehouse):
    from config import settings

    rows = _silver_rows(
        [
            ("SW-AAA-00001", "smartwatch", "3.0.0", datetime(2026, 1, 1)),
            ("SW-AAA-00001", "smartwatch", "3.0.0", datetime(2026, 1, 5)),
        ]
    )
    df = spark.createDataFrame(
        rows,
        ["device_id", "device_type", "firmware_version", "event_timestamp"],
    )
    df.write.format("delta").save(settings.silver_sensor)

    from transformations.silver_to_gold.dim_device import main as dim_device_main

    dim_device_main()

    dim = spark.read.format("delta").load(settings.gold_dim_device)
    assert dim.count() == 1
    row = dim.collect()[0]
    assert row["firmware_version"] == "3.0.0"
    assert row["is_current"] is True
    assert row["effective_end"] is None


@pytest.mark.usefixtures("tmp_lakehouse")
def test_firmware_change_creates_two_scd2_rows(spark, tmp_lakehouse):
    from config import settings
    from transformations.silver_to_gold.dim_device import main as dim_device_main

    rows = [
        ("SW-AAA-00001", "smartwatch", "3.0.0", datetime(2026, 1, 1)),
        ("SW-AAA-00001", "smartwatch", "3.0.0", datetime(2026, 1, 15)),
        ("SW-AAA-00001", "smartwatch", "3.1.0", datetime(2026, 2, 1)),
        ("SW-AAA-00001", "smartwatch", "3.1.0", datetime(2026, 3, 1)),
    ]
    df = spark.createDataFrame(
        rows,
        ["device_id", "device_type", "firmware_version", "event_timestamp"],
    )
    df.write.format("delta").save(settings.silver_sensor)

    dim_device_main()
    dim = spark.read.format("delta").load(settings.gold_dim_device)
    assert dim.count() == 2

    rows_by_fw = {r["firmware_version"]: r for r in dim.collect()}
    old = rows_by_fw["3.0.0"]
    new = rows_by_fw["3.1.0"]
    assert old["is_current"] is False
    assert old["effective_end"] is not None
    assert new["is_current"] is True
    assert new["effective_end"] is None


@pytest.mark.usefixtures("tmp_lakehouse")
def test_multiple_devices_independent_history(spark, tmp_lakehouse):
    from config import settings
    from transformations.silver_to_gold.dim_device import main as dim_device_main

    rows = [
        ("SW-AAA-00001", "smartwatch", "3.0.0", datetime(2026, 1, 1)),
        ("CS-BBB-00002", "chest_strap", "2.5.0", datetime(2026, 1, 1)),
    ]
    df = spark.createDataFrame(
        rows,
        ["device_id", "device_type", "firmware_version", "event_timestamp"],
    )
    df.write.format("delta").save(settings.silver_sensor)

    dim_device_main()
    dim = spark.read.format("delta").load(settings.gold_dim_device)
    assert dim.count() == 2
    assert dim.filter(dim.is_current).count() == 2  # both are current
