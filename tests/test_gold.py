"""
Gold layer tests — PulseTrack Snowflake Schema
===============================================
Run: pytest tests/test_gold.py -v

Coverage:
  - Row count > 0 (or exact count for dim_date/dim_time)
  - No null primary keys
  - No duplicate primary keys
  - Snowflake FK integrity: dim_condition → dim_condition_category
  - Snowflake FK integrity: dim_medication → dim_drug_class
  - fact_vital_daily_summary: reading_count > 0, avg between min and max
  - fact_lab_result: no duplicate grain rows

Fact tables may be empty when Silver has not been populated yet.
Those tests auto-skip with a message rather than failing.
"""
import sys, os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import functions as F
from streaming.spark_config import get_spark_session

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"


@pytest.fixture(scope="session")
def spark():
    return get_spark_session("GoldTests")


def read_gold(spark, table_name):
    return spark.read.format("delta").load(f"{GOLD_BASE}/{table_name}")


# ── dim_date ────────────────────────────────────────────────────────────

class TestDimDate:

    def test_row_count_exact(self, spark):
        assert read_gold(spark, "dim_date").count() == 1096

    def test_no_null_primary_key(self, spark):
        assert read_gold(spark, "dim_date").filter(F.col("date_key").isNull()).count() == 0

    def test_no_duplicate_primary_key(self, spark):
        df = read_gold(spark, "dim_date")
        assert df.count() == df.select("date_key").distinct().count()


# ── dim_time ────────────────────────────────────────────────────────────

class TestDimTime:

    def test_row_count_exact(self, spark):
        assert read_gold(spark, "dim_time").count() == 1440

    def test_no_null_primary_key(self, spark):
        assert read_gold(spark, "dim_time").filter(F.col("time_key").isNull()).count() == 0

    def test_no_duplicate_primary_key(self, spark):
        df = read_gold(spark, "dim_time")
        assert df.count() == df.select("time_key").distinct().count()


# ── dim_condition_category ──────────────────────────────────────────────

class TestDimConditionCategory:

    def test_row_count_positive(self, spark):
        assert read_gold(spark, "dim_condition_category").count() > 0

    def test_no_null_primary_key(self, spark):
        df = read_gold(spark, "dim_condition_category")
        assert df.filter(F.col("condition_category_key").isNull()).count() == 0

    def test_no_duplicate_primary_key(self, spark):
        df = read_gold(spark, "dim_condition_category")
        assert df.count() == df.select("condition_category_key").distinct().count()


# ── dim_condition ───────────────────────────────────────────────────────

class TestDimCondition:

    def test_row_count_positive(self, spark):
        assert read_gold(spark, "dim_condition").count() > 0

    def test_no_null_primary_key(self, spark):
        df = read_gold(spark, "dim_condition")
        assert df.filter(F.col("condition_key").isNull()).count() == 0

    def test_no_duplicate_primary_key(self, spark):
        df = read_gold(spark, "dim_condition")
        assert df.count() == df.select("condition_key").distinct().count()

    def test_fk_to_condition_category(self, spark):
        """Snowflake FK: every condition_category_key in dim_condition must exist in dim_condition_category."""
        conditions = read_gold(spark, "dim_condition")
        categories = read_gold(spark, "dim_condition_category")
        orphans = conditions.join(
            categories.select("condition_category_key"),
            on="condition_category_key",
            how="left_anti",
        ).count()
        assert orphans == 0, f"dim_condition has {orphans} orphaned condition_category_key(s)"


# ── dim_drug_class ──────────────────────────────────────────────────────

class TestDimDrugClass:

    def test_row_count_positive(self, spark):
        assert read_gold(spark, "dim_drug_class").count() > 0

    def test_no_null_primary_key(self, spark):
        df = read_gold(spark, "dim_drug_class")
        assert df.filter(F.col("drug_class_key").isNull()).count() == 0

    def test_no_duplicate_primary_key(self, spark):
        df = read_gold(spark, "dim_drug_class")
        assert df.count() == df.select("drug_class_key").distinct().count()


# ── dim_medication ──────────────────────────────────────────────────────

class TestDimMedication:

    def test_row_count_positive(self, spark):
        assert read_gold(spark, "dim_medication").count() > 0

    def test_no_null_primary_key(self, spark):
        df = read_gold(spark, "dim_medication")
        assert df.filter(F.col("medication_key").isNull()).count() == 0

    def test_no_duplicate_primary_key(self, spark):
        df = read_gold(spark, "dim_medication")
        assert df.count() == df.select("medication_key").distinct().count()

    def test_fk_to_drug_class(self, spark):
        """Snowflake FK: every drug_class_key in dim_medication must exist in dim_drug_class."""
        meds        = read_gold(spark, "dim_medication")
        drug_classes = read_gold(spark, "dim_drug_class")
        orphans = meds.join(
            drug_classes.select("drug_class_key"),
            on="drug_class_key",
            how="left_anti",
        ).count()
        assert orphans == 0, f"dim_medication has {orphans} orphaned drug_class_key(s)"


# ── dim_metric ──────────────────────────────────────────────────────────

class TestDimMetric:

    def test_row_count_positive(self, spark):
        assert read_gold(spark, "dim_metric").count() > 0

    def test_no_null_primary_key(self, spark):
        df = read_gold(spark, "dim_metric")
        assert df.filter(F.col("metric_key").isNull()).count() == 0

    def test_no_duplicate_primary_key(self, spark):
        df = read_gold(spark, "dim_metric")
        assert df.count() == df.select("metric_key").distinct().count()


# ── dim_patient ─────────────────────────────────────────────────────────

class TestDimPatient:

    def test_row_count_positive(self, spark):
        assert read_gold(spark, "dim_patient").count() > 0

    def test_no_null_primary_key(self, spark):
        df = read_gold(spark, "dim_patient")
        assert df.filter(F.col("patient_key").isNull()).count() == 0

    def test_no_duplicate_primary_key(self, spark):
        df = read_gold(spark, "dim_patient")
        assert df.count() == df.select("patient_key").distinct().count()


# ── fact_vital_daily_summary ────────────────────────────────────────────

class TestFactVitalDailySummary:

    def test_table_readable(self, spark):
        assert read_gold(spark, "fact_vital_daily_summary").count() >= 0

    def test_no_null_patient_key(self, spark):
        df = read_gold(spark, "fact_vital_daily_summary")
        if df.count() == 0:
            pytest.skip("fact_vital_daily_summary is empty — Silver not yet populated")
        assert df.filter(F.col("patient_key").isNull()).count() == 0

    def test_no_null_metric_key(self, spark):
        df = read_gold(spark, "fact_vital_daily_summary")
        if df.count() == 0:
            pytest.skip("fact_vital_daily_summary is empty — Silver not yet populated")
        assert df.filter(F.col("metric_key").isNull()).count() == 0

    def test_reading_count_always_positive(self, spark):
        df = read_gold(spark, "fact_vital_daily_summary")
        if df.count() == 0:
            pytest.skip("fact_vital_daily_summary is empty — Silver not yet populated")
        assert df.filter(F.col("reading_count") <= 0).count() == 0, \
            "All rows must have reading_count > 0"

    def test_avg_between_min_and_max(self, spark):
        df = read_gold(spark, "fact_vital_daily_summary")
        if df.count() == 0:
            pytest.skip("fact_vital_daily_summary is empty — Silver not yet populated")
        violations = df.filter(
            F.col("avg_value").isNotNull()
            & F.col("min_value").isNotNull()
            & F.col("max_value").isNotNull()
            & (
                (F.col("avg_value") < F.col("min_value"))
                | (F.col("avg_value") > F.col("max_value"))
            )
        ).count()
        assert violations == 0, f"{violations} row(s) where avg_value is outside [min_value, max_value]"


# ── fact_lab_result ─────────────────────────────────────────────────────

class TestFactLabResult:

    def test_table_readable(self, spark):
        assert read_gold(spark, "fact_lab_result").count() >= 0

    def test_no_null_patient_key(self, spark):
        df = read_gold(spark, "fact_lab_result")
        if df.count() == 0:
            pytest.skip("fact_lab_result is empty — Silver not yet populated")
        assert df.filter(F.col("patient_key").isNull()).count() == 0

    def test_no_duplicate_grain(self, spark):
        """Grain: patient_key × date_key × lab_test_name must be unique."""
        df = read_gold(spark, "fact_lab_result")
        if df.count() == 0:
            pytest.skip("fact_lab_result is empty — Silver not yet populated")
        total    = df.count()
        distinct = df.select("patient_key", "date_key", "lab_test_name").distinct().count()
        assert total == distinct, f"fact_lab_result has {total - distinct} duplicate grain row(s)"
