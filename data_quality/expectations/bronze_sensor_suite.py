"""
Bronze→Silver gate: shape & sanity checks for decoded wearable readings.

The Bronze table stores the full Avro envelope as a ``decoded`` struct;
:func:`prepare_for_validation` flattens that into the column layout the
suite expects so we can run vanilla GX expectations against it.
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

SUITE_NAME = "bronze_sensor"

DEVICE_TYPES = ["smartwatch", "chest_strap", "sleep_ring", "blood_pressure_cuff"]
DEVICE_ID_REGEX = r"^[A-Z]{2}-[A-Z0-9]{3}-\d{5}$"
SEMVER_REGEX = r"^\d+\.\d+\.\d+$"


def prepare_for_validation(bronze_df: DataFrame) -> DataFrame:
    """Project the parseable rows of Bronze to flat columns the suite reads."""
    return bronze_df.filter(F.col("is_parseable")).select(
        F.col("decoded.reading_id").alias("reading_id"),
        F.col("decoded.device_id").alias("device_id"),
        F.col("decoded.device_type").alias("device_type"),
        F.col("decoded.battery_pct").alias("battery_pct"),
        F.col("decoded.firmware_version").alias("firmware_version"),
        F.col("decoded.event_timestamp").alias("event_timestamp"),
        F.size(F.col("decoded.metrics")).alias("metrics_size"),
    )


@register_suite(SUITE_NAME)
def build():
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    # reading_id — non-null, unique within batch
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="reading_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="reading_id"))

    # device_id — non-null, "<2 letters>-<3 alnum>-<5 digits>"
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="device_id"))
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(
        column="device_id", regex=DEVICE_ID_REGEX,
    ))

    # device_type — closed enum
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(
        column="device_type", value_set=DEVICE_TYPES,
    ))

    # event_timestamp — non-null, not in the future, not older than 30 days
    now = datetime.utcnow()
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="event_timestamp"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="event_timestamp",
        min_value=now - timedelta(days=30),
        max_value=now,
    ))

    # battery_pct — 0..100
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="battery_pct", min_value=0, max_value=100,
    ))

    # firmware_version — semver-ish "X.Y.Z"
    suite.add_expectation(gxe.ExpectColumnValuesToMatchRegex(
        column="firmware_version", regex=SEMVER_REGEX,
    ))

    # metrics map non-empty
    suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(
        column="metrics_size", min_value=1,
    ))

    return suite
