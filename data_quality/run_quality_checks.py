"""
Data Quality Checks — Gold Layer
=================================
Validates Gold Delta tables after each pipeline run. Checks:
  1. Row counts — warns on empty tables
  2. Foreign key integrity — Gold FK references are valid
  3. Null rates — critical columns must not be null
  4. Metric range bounds — values within clinically plausible range
  5. HIPAA PII scan — no raw PII in Gold
  6. SCD0 stability — reference dims have expected fixed row counts
  7. Grain uniqueness — fact table primary keys are unique

Run: python data_quality/run_quality_checks.py
Exit code 0 = all checks passed (warnings only for soft failures)
Exit code 1 = one or more hard failures
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from streaming.spark_config import get_spark_session
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"

TABLES = [
    "dim_date", "dim_time", "dim_condition_category", "dim_drug_class",
    "dim_metric", "dim_patient", "dim_condition", "dim_medication", "dim_device",
    "fact_vital_reading", "fact_vital_daily_summary", "fact_lab_result",
    "fact_prescription_fill", "fact_anomaly_alert",
]

# Expected fixed row counts for SCD0 reference dims
EXPECTED_COUNTS = {
    "dim_date":               1096,   # 3 years of dates
    "dim_time":               1440,   # 24 * 60 minutes
    "dim_condition_category":    5,
    "dim_drug_class":            6,
    "dim_metric":               13,
}

# FK relationships: (child_table, child_col, parent_table, parent_col)
FK_CHECKS = [
    ("dim_condition",         "condition_category_key", "dim_condition_category", "condition_category_key"),
    ("dim_medication",        "drug_class_key",         "dim_drug_class",         "drug_class_key"),
    ("dim_device",            "patient_key",            "dim_patient",            "patient_key"),
    ("fact_vital_reading",    "patient_key",            "dim_patient",            "patient_key"),
    ("fact_vital_reading",    "metric_key",             "dim_metric",             "metric_key"),
    ("fact_vital_daily_summary", "patient_key",         "dim_patient",            "patient_key"),
    ("fact_vital_daily_summary", "metric_key",          "dim_metric",             "metric_key"),
    ("fact_lab_result",       "patient_key",            "dim_patient",            "patient_key"),
    ("fact_prescription_fill","patient_key",            "dim_patient",            "patient_key"),
    ("fact_prescription_fill","medication_key",         "dim_medication",         "medication_key"),
    ("fact_anomaly_alert",    "patient_key",            "dim_patient",            "patient_key"),
    ("fact_anomaly_alert",    "metric_key",             "dim_metric",             "metric_key"),
]

# Columns that must never be null
NOT_NULL_CHECKS = {
    "dim_patient":             ["patient_key", "age_group", "gender"],
    "dim_device":              ["device_key", "device_id", "device_type"],
    "fact_vital_reading":      ["reading_key", "patient_key", "metric_key", "date_key", "metric_value"],
    "fact_vital_daily_summary":["patient_key", "metric_key", "date_key", "avg_value"],
    "fact_lab_result":         ["patient_key", "date_key", "lab_test_name", "result_value"],
    "fact_prescription_fill":  ["fill_key", "patient_key", "date_key"],
    "fact_anomaly_alert":      ["alert_key", "patient_key", "metric_key", "alert_type", "severity"],
}

# PII column names that must NOT appear in Gold
PII_COLUMNS = {"email", "patient_email", "phone", "address", "ssn", "dob", "date_of_birth",
               "first_name", "last_name", "full_name", "mrn", "hospital_mrn"}

VITAL_BOUNDS = {
    "heart_rate":        (20, 300),
    "systolic_bp":       (50, 300),
    "diastolic_bp":      (30, 200),
    "spo2":              (50, 100),
    "blood_glucose":     (20, 800),
    "body_temperature":  (90, 115),
    "respiratory_rate":  (5, 60),
    "steps":             (0, 100000),
    "calories_burned":   (0, 10000),
}


def _load(spark, table):
    path = f"{GOLD_BASE}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def check_row_counts(spark):
    print("\n── Row Count Checks ──────────────────────────────────────────")
    failures = []
    for table in TABLES:
        df = _load(spark, table)
        if df is None:
            print(f"  WARN  {table}: table not found (run pipeline first)")
            continue
        count = df.count()
        expected = EXPECTED_COUNTS.get(table)
        if expected is not None:
            if count == expected:
                print(f"  OK    {table}: {count} rows (expected {expected})")
            else:
                print(f"  FAIL  {table}: {count} rows (expected {expected})")
                failures.append(f"{table} row count mismatch: {count} != {expected}")
        else:
            if count == 0:
                print(f"  WARN  {table}: 0 rows — pipeline may not have run yet")
            else:
                print(f"  OK    {table}: {count} rows")
    return failures


def check_fk_integrity(spark):
    print("\n── Foreign Key Integrity ─────────────────────────────────────")
    failures = []
    loaded = {}

    def _get(t):
        if t not in loaded:
            loaded[t] = _load(spark, t)
        return loaded[t]

    for child_t, child_col, parent_t, parent_col in FK_CHECKS:
        child  = _get(child_t)
        parent = _get(parent_t)
        if child is None or parent is None:
            print(f"  SKIP  {child_t}.{child_col} → {parent_t}.{parent_col}: table missing")
            continue

        # Left anti-join: child rows with no matching parent key
        orphans = (
            child.select(F.col(child_col))
            .filter(F.col(child_col).isNotNull())
            .join(parent.select(F.col(parent_col)), child_col == parent_col, "left_anti")
            .count()
        )
        if orphans == 0:
            print(f"  OK    {child_t}.{child_col} → {parent_t}.{parent_col}")
        else:
            msg = f"{child_t}.{child_col} → {parent_t}.{parent_col}: {orphans} orphan rows"
            print(f"  FAIL  {msg}")
            failures.append(msg)
    return failures


def check_not_null(spark):
    print("\n── Not-Null Checks ───────────────────────────────────────────")
    failures = []
    for table, cols in NOT_NULL_CHECKS.items():
        df = _load(spark, table)
        if df is None:
            print(f"  SKIP  {table}: table missing")
            continue
        total = df.count()
        for col in cols:
            if col not in df.columns:
                print(f"  SKIP  {table}.{col}: column missing")
                continue
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count == 0:
                print(f"  OK    {table}.{col}: 0 nulls")
            else:
                pct = 100 * null_count / max(total, 1)
                msg = f"{table}.{col}: {null_count} nulls ({pct:.1f}%)"
                print(f"  FAIL  {msg}")
                failures.append(msg)
    return failures


def check_pii_columns(spark):
    print("\n── HIPAA PII Column Scan ─────────────────────────────────────")
    failures = []
    for table in TABLES:
        df = _load(spark, table)
        if df is None:
            continue
        leaked = [c for c in df.columns if c.lower() in PII_COLUMNS]
        if leaked:
            msg = f"{table}: PII columns detected: {leaked}"
            print(f"  FAIL  {msg}")
            failures.append(msg)
        else:
            print(f"  OK    {table}: no PII column names")
    return failures


def check_grain_uniqueness(spark):
    print("\n── Grain Uniqueness ──────────────────────────────────────────")
    grain_checks = {
        "dim_date":               ["date_key"],
        "dim_time":               ["time_key"],
        "dim_metric":             ["metric_key"],
        "fact_vital_reading":     ["reading_key"],
        "fact_vital_daily_summary": ["patient_key", "metric_key", "date_key"],
        "fact_prescription_fill": ["fill_key"],
        "fact_anomaly_alert":     ["alert_key"],
    }
    failures = []
    for table, keys in grain_checks.items():
        df = _load(spark, table)
        if df is None:
            print(f"  SKIP  {table}: table missing")
            continue
        total = df.count()
        distinct = df.select(*keys).distinct().count()
        if total == distinct:
            print(f"  OK    {table}: {total} rows, all {keys} unique")
        else:
            dups = total - distinct
            msg = f"{table}: {dups} duplicate {keys} rows"
            print(f"  FAIL  {msg}")
            failures.append(msg)
    return failures


def check_vital_bounds(spark):
    print("\n── Vital Metric Range Checks ─────────────────────────────────")
    failures = []
    df = _load(spark, "fact_vital_reading")
    dim_m = _load(spark, "dim_metric")
    if df is None or dim_m is None:
        print("  SKIP  fact_vital_reading or dim_metric not found")
        return failures

    joined = df.join(dim_m.select("metric_key", "metric_name"), "metric_key", "left")
    for metric, (lo, hi) in VITAL_BOUNDS.items():
        out_of_range = (
            joined
            .filter(F.col("metric_name") == metric)
            .filter((F.col("metric_value") < lo) | (F.col("metric_value") > hi))
            .count()
        )
        if out_of_range == 0:
            print(f"  OK    {metric}: all values in [{lo}, {hi}]")
        else:
            msg = f"{metric}: {out_of_range} readings outside [{lo}, {hi}]"
            print(f"  WARN  {msg}")
            # Soft failure — out-of-range readings need investigation but don't break pipeline
    return failures


def main():
    spark = get_spark_session("PulseTrackDataQuality")
    print("=" * 60)
    print("PulseTrack Gold Layer — Data Quality Report")
    print("=" * 60)

    all_failures = []
    all_failures += check_row_counts(spark)
    all_failures += check_fk_integrity(spark)
    all_failures += check_not_null(spark)
    all_failures += check_pii_columns(spark)
    all_failures += check_grain_uniqueness(spark)
    check_vital_bounds(spark)  # soft check, warnings only

    print("\n" + "=" * 60)
    if all_failures:
        print(f"RESULT: {len(all_failures)} FAILURE(S)")
        for f in all_failures:
            print(f"  ✗ {f}")
        sys.exit(1)
    else:
        print("RESULT: ALL CHECKS PASSED ✅")


if __name__ == "__main__":
    main()
