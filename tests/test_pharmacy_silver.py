"""
Pharmacy Silver tests — placeholder.

There's no Bronze→Silver transform for pharmacy events yet. The Avro schema
exists (schemas/pharmacy_event.avsc) and openfda_producer.py publishes to
the pharmacy_events topic, but no Spark job materializes the Silver table.

When that job lands, replace these skips with parsing + quality-flag tests
analogous to test_sensor_silver.py.
"""
from __future__ import annotations

import pytest


@pytest.mark.skip(reason="No pharmacy_silver transform implemented yet")
def test_parse_fda_adverse_event():
    """Stub: parse Bronze pharmacy → Silver pharmacy_fills."""


@pytest.mark.skip(reason="No pharmacy_silver transform implemented yet")
def test_quality_flags_filter_bad_drug_names():
    """Stub: drug_name validation against NDC reference data."""


@pytest.mark.skip(reason="No pharmacy_silver transform implemented yet")
def test_dedup_on_safety_report_id():
    """Stub: ensure FDA safetyreportid is the dedup key."""
