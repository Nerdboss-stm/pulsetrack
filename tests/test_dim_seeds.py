"""Dimension seed-table tests — covers dim_metric, dim_condition, dim_medication,
dim_drug_class, dim_condition_category, dim_time, dim_date.

These dim tables are populated from static Python-side seed lists. The tests
verify that:
  - The seed constants have the expected shape.
  - The transform produces the expected output schema/row counts when given
    a real (test) SparkSession.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Seed constant tests (no Spark needed) ────────────────────────────────


def test_dim_metric_seed_shape():
    from transformations.silver_to_gold.dim_metric import METRIC_SEED

    # Each row: (metric_name, unit, normal_low, normal_high, device_type)
    assert all(len(row) == 5 for row in METRIC_SEED)
    device_types = {row[4] for row in METRIC_SEED}
    assert device_types == {"smartwatch", "chest_strap", "sleep_ring", "glucose_monitor"}


def test_dim_metric_seed_heart_rate_present():
    from transformations.silver_to_gold.dim_metric import METRIC_SEED

    hr_rows = [r for r in METRIC_SEED if r[0] == "heart_rate_bpm"]
    assert len(hr_rows) == 3  # smartwatch, chest_strap, sleep_ring


def test_dim_condition_seed_shape():
    from transformations.silver_to_gold.dim_condition import CONDITION_SEED

    assert all(len(row) == 3 for row in CONDITION_SEED)
    icd_codes = {row[0] for row in CONDITION_SEED}
    assert "E11.9" in icd_codes  # Type 2 diabetes
    assert "I10" in icd_codes    # Hypertension


def test_dim_medication_seed_shape():
    from transformations.silver_to_gold.dim_medication import MEDICATION_SEED

    # Each row: (medication_name, generic_name, class_name)
    assert all(len(row) == 3 for row in MEDICATION_SEED)
    names = {row[0] for row in MEDICATION_SEED}
    assert "metformin" in names
    assert "lisinopril" in names


def test_dim_drug_class_seed_shape():
    from transformations.silver_to_gold.dim_drug_class import DRUG_CLASS_SEED

    assert all(len(row) == 2 for row in DRUG_CLASS_SEED)
    classes = {row[0] for row in DRUG_CLASS_SEED}
    assert "Biguanides" in classes
    assert "Statins" in classes


def test_dim_condition_category_seed_shape():
    from transformations.silver_to_gold.dim_condition_category import CATEGORY_SEED

    assert all(len(row) == 3 for row in CATEGORY_SEED)
    cats = {row[0] for row in CATEGORY_SEED}
    assert "Endocrine" in cats
    assert "Circulatory" in cats


# ── End-to-end transformation tests (use spark fixture) ─────────────────


@pytest.mark.usefixtures("tmp_lakehouse")
def test_dim_metric_main_writes_rows(spark, tmp_lakehouse, monkeypatch):
    from config import settings

    monkeypatch.setattr(
        "transformations.silver_to_gold.dim_metric.get_spark_session", lambda app: spark
    )
    from transformations.silver_to_gold.dim_metric import main as dim_metric_main

    dim_metric_main()
    dim = spark.read.format("delta").load(settings.gold_dim_metric)
    cols = set(dim.columns)
    assert {"metric_key", "metric_name", "unit", "normal_low", "normal_high", "device_type"} <= cols
    assert dim.count() > 0


@pytest.mark.usefixtures("tmp_lakehouse")
def test_dim_time_main_writes_1440_rows(spark, tmp_lakehouse, monkeypatch):
    """dim_time is 24h * 60min = 1440 rows."""
    from config import settings

    monkeypatch.setattr(
        "transformations.silver_to_gold.dim_time.get_spark_session", lambda app: spark
    )
    from transformations.silver_to_gold.dim_time import main as dim_time_main

    dim_time_main()
    dim = spark.read.format("delta").load(settings.gold_dim_time)
    assert dim.count() == 1440
    cols = set(dim.columns)
    assert {"time_key", "hour", "minute", "time_str", "period_of_day", "is_sleep_window"} <= cols


@pytest.mark.usefixtures("tmp_lakehouse")
def test_dim_date_main_writes_3_years_of_rows(spark, tmp_lakehouse, monkeypatch):
    """dim_date covers 1096 days = 3 calendar years."""
    from config import settings

    monkeypatch.setattr(
        "transformations.silver_to_gold.dim_date.get_spark_session", lambda app: spark
    )
    from transformations.silver_to_gold.dim_date import main as dim_date_main

    dim_date_main()
    dim = spark.read.format("delta").load(settings.gold_dim_date)
    assert dim.count() == 1096
    cols = set(dim.columns)
    assert {"date_key", "year", "month", "day", "quarter", "is_holiday", "is_weekend"} <= cols


@pytest.mark.usefixtures("tmp_lakehouse")
def test_dim_drug_class_main_writes_six_classes(spark, tmp_lakehouse, monkeypatch):
    from config import settings

    monkeypatch.setattr(
        "transformations.silver_to_gold.dim_drug_class.get_spark_session", lambda app: spark
    )
    from transformations.silver_to_gold.dim_drug_class import main as dim_dc_main

    dim_dc_main()
    dim = spark.read.format("delta").load(settings.gold_dim_drug_class)
    assert dim.count() == 6


@pytest.mark.usefixtures("tmp_lakehouse")
def test_dim_condition_category_main_writes_six_categories(spark, tmp_lakehouse, monkeypatch):
    from config import settings

    monkeypatch.setattr(
        "transformations.silver_to_gold.dim_condition_category.get_spark_session",
        lambda app: spark,
    )
    from transformations.silver_to_gold.dim_condition_category import (
        main as dim_cc_main,
    )

    dim_cc_main()
    dim = spark.read.format("delta").load(settings.gold_dim_condition_category)
    assert dim.count() == 6
