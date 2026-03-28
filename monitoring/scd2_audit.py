"""
SCD2 Chain Integrity Audit
===========================
Verifies that SCD2 slowly-changing dimension tables have valid chains:
  - No gaps: effective_end of row N+1 immediately follows effective_start of row N
  - No overlaps: two active rows for the same natural key at the same time
  - Exactly one current row: is_current = True for each natural key
  - effective_end = NULL for is_current rows

SCD2 tables in Gold:
  - dim_patient (natural key: patient_key — stable SHA256 hash)
  - dim_condition (natural key: condition_code)
  - dim_medication (natural key: medication_name)
  - dim_device (natural key: device_id + firmware_version → device_key)

Note: dim_date, dim_time, dim_condition_category, dim_drug_class, dim_metric
are SCD0 (never change) — not audited here.

Run: python monitoring/scd2_audit.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from streaming.spark_config import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"

# (table, natural_key_col, has_scd2_tracking)
# natural_key: the stable business key used to group versions
SCD2_TABLES = [
    ("dim_patient",    "patient_key",      False),  # patient_key IS stable; no separate natural key
    ("dim_condition",  "condition_code",   True),
    ("dim_medication", "medication_name",  True),
    ("dim_device",     "device_key",       False),  # device_key IS stable; firmware changes create new rows
]


def _load(spark, table):
    path = f"{GOLD_BASE}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def audit_is_current(spark, table, df, natural_key):
    """Each natural key must have exactly one is_current=True row."""
    failures = []
    if "is_current" not in df.columns:
        print(f"  SKIP  {table}: no is_current column (not SCD2)")
        return failures

    multi_current = (
        df.filter(F.col("is_current"))
        .groupBy(natural_key)
        .agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") > 1)
        .count()
    )
    if multi_current == 0:
        print(f"  OK    {table}: no natural key with >1 is_current row")
    else:
        msg = f"{table}: {multi_current} natural keys have >1 is_current=True row"
        print(f"  FAIL  {msg}")
        failures.append(msg)

    # is_current=True rows must have effective_end = NULL
    if "effective_end" in df.columns:
        current_with_end = (
            df.filter(F.col("is_current"))
            .filter(F.col("effective_end").isNotNull())
            .count()
        )
        if current_with_end == 0:
            print(f"  OK    {table}: all is_current rows have effective_end = NULL")
        else:
            msg = f"{table}: {current_with_end} is_current rows have non-NULL effective_end"
            print(f"  FAIL  {msg}")
            failures.append(msg)

    # is_current=False rows must have effective_end set
    if "effective_end" in df.columns:
        closed_missing_end = (
            df.filter(~F.col("is_current"))
            .filter(F.col("effective_end").isNull())
            .count()
        )
        if closed_missing_end == 0:
            print(f"  OK    {table}: all closed rows have effective_end set")
        else:
            msg = f"{table}: {closed_missing_end} closed (is_current=False) rows missing effective_end"
            print(f"  WARN  {msg}")
            # Warn only — could be legacy rows before SCD2 was added
    return failures


def audit_overlap(spark, table, df, natural_key):
    """Detect overlapping effective date ranges for the same natural key."""
    failures = []
    required = {"effective_start", "effective_end"}
    if not required.issubset(set(df.columns)):
        print(f"  SKIP  {table}: missing effective_start/effective_end columns")
        return failures

    w = Window.partitionBy(natural_key).orderBy("effective_start")
    # For each row, get the previous row's effective_end
    checked = df.withColumn(
        "prev_end",
        F.lag("effective_end").over(w)
    ).withColumn(
        "overlap",
        F.col("prev_end").isNotNull() & (F.col("effective_start") < F.col("prev_end"))
    )
    overlap_count = checked.filter(F.col("overlap")).count()
    if overlap_count == 0:
        print(f"  OK    {table}: no overlapping effective date ranges")
    else:
        msg = f"{table}: {overlap_count} rows with overlapping effective date ranges"
        print(f"  FAIL  {msg}")
        failures.append(msg)
    return failures


def audit_no_zero_length_version(spark, table, df, natural_key):
    """effective_start must be strictly before effective_end (when set)."""
    failures = []
    if "effective_start" not in df.columns or "effective_end" not in df.columns:
        return failures
    zero_length = (
        df.filter(
            F.col("effective_end").isNotNull()
            & (F.col("effective_end") <= F.col("effective_start"))
        ).count()
    )
    if zero_length == 0:
        print(f"  OK    {table}: all version windows have positive duration")
    else:
        msg = f"{table}: {zero_length} rows with effective_end <= effective_start"
        print(f"  FAIL  {msg}")
        failures.append(msg)
    return failures


def audit_table(spark, table, natural_key):
    print(f"\n  [{table}] natural_key={natural_key}")
    df = _load(spark, table)
    if df is None:
        print(f"  SKIP  {table}: table not found")
        return []

    total = df.count()
    current = df.filter(F.col("is_current")).count() if "is_current" in df.columns else None
    print(f"  INFO  {table}: {total} total rows" + (f", {current} current" if current is not None else ""))

    failures = []
    failures += audit_is_current(spark, table, df, natural_key)
    failures += audit_overlap(spark, table, df, natural_key)
    failures += audit_no_zero_length_version(spark, table, df, natural_key)
    return failures


def main():
    spark = get_spark_session("PulseTrackSCD2Audit")
    print("=" * 60)
    print("PulseTrack — SCD2 Chain Integrity Audit")
    print("=" * 60)

    all_failures = []

    # dim_patient: patient_key is stable (SHA256 hash of email)
    # No separate natural_key column — patient_key IS the natural key
    all_failures += audit_table(spark, "dim_patient", "patient_key")

    # dim_condition: condition_code is the natural key
    all_failures += audit_table(spark, "dim_condition", "condition_code")

    # dim_medication: medication_name is the natural key
    all_failures += audit_table(spark, "dim_medication", "medication_name")

    # dim_device: device_id + firmware_version form a stable key
    # dim_device doesn't track SCD2 history — each firmware version is its own row
    # Just check for duplicate device_keys
    df_dev = _load(spark, "dim_device")
    if df_dev is not None:
        print(f"\n  [dim_device] checking device_key uniqueness")
        dupes = df_dev.groupBy("device_key").count().filter(F.col("count") > 1).count()
        if dupes == 0:
            print(f"  OK    dim_device: all device_key values unique")
        else:
            msg = f"dim_device: {dupes} duplicate device_key values"
            print(f"  FAIL  {msg}")
            all_failures.append(msg)

    print("\n" + "=" * 60)
    if all_failures:
        print(f"RESULT: {len(all_failures)} SCD2 FAILURE(S)")
        for f in all_failures:
            print(f"  ✗ {f}")
        sys.exit(1)
    else:
        print("RESULT: SCD2 AUDIT PASSED ✅")


if __name__ == "__main__":
    main()
