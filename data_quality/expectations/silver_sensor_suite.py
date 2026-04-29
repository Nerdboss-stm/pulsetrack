"""
Silver→Gold gate: post-explosion, post-quality-flag sensor rows.

GX 1.x has no native "(reading_id, metric_name) is unique" expectation, so
we derive a concatenated key column and check it with
``ExpectColumnValuesToBeUnique``.

Per-metric physiological range checks are encoded in Silver's ``is_valid``
flag already; the gate verifies that every row passing into Gold has
``is_valid = True`` (i.e. quarantine has already siphoned off the rest).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import great_expectations as gx
import great_expectations.expectations as gxe
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from data_quality.gx_config import register_suite  # noqa: E402

SUITE_NAME = "silver_sensor"

KNOWN_METRICS = [
    "heart_rate_bpm", "spo2_pct", "steps_since_last", "skin_temp_celsius",
    "hrv_ms", "respiration_rate", "sleep_stage", "blood_glucose_mgdl",
    "bp_systolic_mmhg", "bp_diastolic_mmhg",
]


def prepare_for_validation(silver_df: DataFrame) -> DataFrame:
    """Add a derived (reading_id||metric_name) for the unique-pair expectation."""
    return silver_df.withColumn(
        "reading_metric_key",
        F.concat_ws("||", F.col("reading_id"), F.col("metric_name")),
    )


@register_suite(SUITE_NAME)
def build():
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    # No duplicate (reading_id, metric_name) pairs
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="reading_metric_key"))

    # is_valid: not null, must be true for rows propagating to Gold
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="is_valid"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
        column="is_valid", value_set=[True],
    ))

    # device_account_id — required for downstream patient resolution
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="device_account_id"))

    # metric_name — closed enum (anything else is a data plumbing bug)
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
        column="metric_name", value_set=KNOWN_METRICS,
    ))

    # event_timestamp inside watermark window (we use a generous 7d to leave
    # room for backfills and late-sync wearables)
    now = datetime.utcnow()
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="event_timestamp",
        min_value=now - timedelta(days=7),
        max_value=now,
    ))

    return suite
