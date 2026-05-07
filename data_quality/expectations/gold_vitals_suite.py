"""
Gold gate: fact_vital_daily_summary aggregate sanity.

Two checks need preprocessing:

* ``min <= avg <= max`` — GX 1.x has no native multi-column expectation, so
  ``prepare_for_validation`` materializes the relation as ``agg_envelope_ok``.
* No orphan ``patient_key`` (i.e. every patient_key exists in dim_patient)
  needs a join across DataFrames; that lives in
  :func:`assert_no_orphan_patient_keys` and is invoked alongside the suite
  by callers that have access to the dim table.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

import great_expectations as gx
import great_expectations.expectations as gxe
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from data_quality.gx_config import register_suite  # noqa: E402

SUITE_NAME = "gold_vitals"


def prepare_for_validation(gold_df: DataFrame) -> DataFrame:
    """Derive ``agg_envelope_ok`` so we can ExpectInSet([True])."""
    return gold_df.withColumn(
        "agg_envelope_ok",
        F.when(
            # No valid readings → avg/min/max are null; envelope is vacuously OK.
            F.col("avg_value").isNull(),
            F.lit(True),
        ).otherwise(
            (F.col("min_value") <= F.col("avg_value")) & (F.col("avg_value") <= F.col("max_value"))
        ),
    )


def assert_no_orphan_patient_keys(gold_df: DataFrame, dim_patient_df: DataFrame) -> int:
    """Return the number of patient_keys present in `gold_df` but not in
    `dim_patient_df`. Callers can fail their batch if the result is non-zero.
    """
    return (
        gold_df.select("patient_key")
        .distinct()
        .join(dim_patient_df.select("patient_key"), on="patient_key", how="left_anti")
        .count()
    )


@register_suite(SUITE_NAME)
def build():
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    # patient_key not null
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="patient_key"))

    # date_key valid YYYYMMDD (1900-01-01..today)
    today_int = int(datetime.utcnow().strftime("%Y%m%d"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="date_key",
            min_value=19000101,
            max_value=today_int,
        )
    )

    # reading_count > 0
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="reading_count",
            min_value=1,
        )
    )

    # min <= avg <= max via the derived flag
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(
            column="agg_envelope_ok",
            value_set=[True],
        )
    )

    # pct_in_normal_range bounded to [0, 100]
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="pct_in_normal_range",
            min_value=0,
            max_value=100,
        )
    )

    return suite
