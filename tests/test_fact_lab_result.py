"""fact_lab_result: lab parsing, condition mapping, dedup on natural grain."""
from __future__ import annotations

import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.mark.usefixtures("tmp_lakehouse")
def test_fact_lab_result_writes_empty_table_when_silver_missing(spark, tmp_lakehouse):
    from config import settings
    from transformations.silver_to_gold.fact_lab_result import main as fact_main

    # Silver labs absent → empty fact table seed
    fact_main()
    out = spark.read.format("delta").load(settings.gold_fact_lab_result)
    assert out.count() == 0
    cols = set(out.columns)
    assert {"patient_key", "date_key", "lab_test_name", "result_value",
            "is_abnormal"} <= cols


@pytest.mark.usefixtures("tmp_lakehouse")
def test_fact_lab_result_dedups_on_natural_grain(spark, tmp_lakehouse):
    from config import settings
    from transformations.silver_to_gold.fact_lab_result import main as fact_main

    # Seed dim_condition (HbA1c maps to E11.9)
    spark.createDataFrame(
        [(123, "E11.9", "Type 2 diabetes")],
        ["condition_key", "condition_code", "condition_name"],
    ).write.format("delta").save(settings.gold_dim_condition)

    # Seed Silver labs with a duplicate (same patient + test + date)
    labs = spark.createDataFrame(
        [
            ("MRN-1", "HbA1c", 6.5, "%", 4.0, 5.6, True, date(2026, 4, 1)),
            ("MRN-1", "HbA1c", 6.5, "%", 4.0, 5.6, True, date(2026, 4, 1)),
            ("MRN-1", "LDL",   95.0, "mg/dL", 0.0, 100.0, False, date(2026, 4, 1)),
        ],
        ["patient_id", "test_code", "value", "unit",
         "reference_low", "reference_high", "is_abnormal", "test_date"],
    )
    labs.write.format("delta").save(settings.silver_ehr_lab_results)

    fact_main()
    out = spark.read.format("delta").load(settings.gold_fact_lab_result)
    assert out.count() == 2  # duplicate dropped


@pytest.mark.usefixtures("tmp_lakehouse")
def test_fact_lab_result_maps_hba1c_to_diabetes_condition(spark, tmp_lakehouse):
    from pyspark.sql import functions as F
    from config import settings
    from transformations.silver_to_gold.fact_lab_result import main as fact_main

    spark.createDataFrame(
        [(123, "E11.9", "Type 2 diabetes")],
        ["condition_key", "condition_code", "condition_name"],
    ).write.format("delta").save(settings.gold_dim_condition)

    labs = spark.createDataFrame(
        [("MRN-1", "HbA1c", 6.5, "%", 4.0, 5.6, True, date(2026, 4, 1))],
        ["patient_id", "test_code", "value", "unit",
         "reference_low", "reference_high", "is_abnormal", "test_date"],
    )
    labs.write.format("delta").save(settings.silver_ehr_lab_results)

    fact_main()
    out = spark.read.format("delta").load(settings.gold_fact_lab_result).collect()
    assert len(out) == 1
    assert out[0]["condition_key"] == 123
