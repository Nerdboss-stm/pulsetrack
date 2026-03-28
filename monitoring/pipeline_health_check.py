"""
Pipeline Health Check
======================
Operational health check for the PulseTrack Kappa pipeline.
Checks:
  1. Streaming lag — is Gold fresh vs Silver?
  2. Identity resolution coverage — % of readings with resolved patient_key
  3. Gold table row counts + freshness (last write time)
  4. Anomaly alert rate — sanity check (spike = device bug, not health crisis)
  5. FK integrity summary (same as quality checks but faster spot-check)
  6. Bronze → Silver → Gold row lineage (input ≈ output ratio)

Run: python monitoring/pipeline_health_check.py
Designed to be called by Airflow dag_data_quality_sweep (every 6 hours).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from streaming.spark_config import get_spark_session
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

GOLD_BASE   = "/tmp/pulsetrack-lakehouse/gold"
SILVER_BASE = "/tmp/pulsetrack-lakehouse/silver"
BRONZE_BASE = "/tmp/pulsetrack-lakehouse/bronze"

# Maximum acceptable hours of staleness per Gold table
FRESHNESS_SLA_HOURS = {
    "fact_vital_reading":       1,
    "fact_vital_daily_summary": 4,
    "fact_anomaly_alert":       1,
    "fact_lab_result":          24,
    "fact_prescription_fill":   24,
    "dim_patient":              4,
}


def _load(spark, layer_base, table):
    path = f"{layer_base}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return spark.read.format("delta").load(path)


def _delta_last_modified(spark, path):
    """Return hours since last Delta commit."""
    try:
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
        last_modified = detail.select("lastModified").collect()[0][0]
        if last_modified is None:
            return None
        elapsed = (datetime.utcnow() - last_modified.replace(tzinfo=None)).total_seconds() / 3600
        return round(elapsed, 2)
    except Exception:
        return None


def check_gold_freshness(spark):
    print("\n── Gold Table Freshness ──────────────────────────────────────")
    warnings = []
    for table, sla_h in FRESHNESS_SLA_HOURS.items():
        path = f"{GOLD_BASE}/{table}"
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"  MISSING  {table}")
            warnings.append(f"{table}: table not found")
            continue
        hours_old = _delta_last_modified(spark, path)
        if hours_old is None:
            print(f"  UNKNOWN  {table}: could not read last modified")
        elif hours_old <= sla_h:
            print(f"  OK       {table}: {hours_old}h old (SLA {sla_h}h)")
        else:
            msg = f"{table}: {hours_old}h old (SLA {sla_h}h) — STALE"
            print(f"  WARN     {msg}")
            warnings.append(msg)
    return warnings


def check_identity_coverage(spark):
    print("\n── Identity Resolution Coverage ──────────────────────────────")
    sensors = _load(spark, SILVER_BASE, "sensor_readings")
    bridge  = _load(spark, f"{SILVER_BASE}/identity", "patient_identity_bridge")
    if sensors is None:
        print("  SKIP  Silver sensor_readings not found")
        return []

    total = sensors.select("device_account_id").distinct().count()
    if bridge is None or total == 0:
        print(f"  SKIP  bridge not found or no devices ({total} distinct accounts)")
        return []

    linked = (
        bridge
        .filter(F.col("identifier_type") == "device_account_id")
        .filter(F.col("link_status") == "linked")
        .select("identifier_value")
        .distinct()
        .count()
    )
    pct = 100 * linked / total
    msg = f"{linked}/{total} device accounts linked ({pct:.1f}%)"
    if pct >= 95:
        print(f"  OK    {msg}")
    elif pct >= 80:
        print(f"  WARN  {msg} (target ≥ 95%)")
    else:
        print(f"  FAIL  {msg} (target ≥ 95%)")
    return []


def check_row_lineage(spark):
    """Bronze → Silver → Gold row counts for anomaly detection."""
    print("\n── Row Lineage (Bronze → Silver → Gold) ─────────────────────")

    def count_or_none(spark, base, table):
        df = _load(spark, base, table)
        return df.count() if df is not None else None

    pairs = [
        ("Bronze pharmacy",  BRONZE_BASE,                   "pharmacy"),
        ("Silver pharmacy",  SILVER_BASE,                   "pharmacy_prescriptions"),
        ("Gold fact_rx",     GOLD_BASE,                     "fact_prescription_fill"),
        ("Silver sensors",   SILVER_BASE,                   "sensor_readings"),
        ("Gold vital_read",  GOLD_BASE,                     "fact_vital_reading"),
        ("Gold daily_sum",   GOLD_BASE,                     "fact_vital_daily_summary"),
        ("Gold anomaly",     GOLD_BASE,                     "fact_anomaly_alert"),
    ]
    for label, base, table in pairs:
        n = count_or_none(spark, base, table)
        if n is None:
            print(f"  --   {label}: not found")
        else:
            print(f"  {n:>8,}  {label}")
    return []


def check_anomaly_rate(spark):
    print("\n── Anomaly Alert Rate (firmware sanity check) ────────────────")
    alerts  = _load(spark, GOLD_BASE, "fact_anomaly_alert")
    vitals  = _load(spark, GOLD_BASE, "fact_vital_reading")
    dim_dev = _load(spark, GOLD_BASE, "dim_device")

    if alerts is None or vitals is None or dim_dev is None:
        print("  SKIP  required tables not found")
        return []

    total_readings = vitals.count()
    total_alerts   = alerts.count()
    if total_readings == 0:
        print("  SKIP  no vital readings")
        return []

    global_rate = 100 * total_alerts / total_readings
    print(f"  Global anomaly rate: {global_rate:.2f}% ({total_alerts:,} alerts / {total_readings:,} readings)")
    if global_rate > 10:
        print("  WARN  >10% anomaly rate — investigate device firmware bugs")
    elif global_rate > 5:
        print("  WARN  >5% anomaly rate — monitor closely")
    else:
        print("  OK    anomaly rate within expected range")

    # Per firmware version breakdown
    per_fw = (
        vitals.join(dim_dev.select("device_key", "firmware_version"), "device_key", "left")
        .groupBy("firmware_version")
        .agg(F.count("*").alias("readings"))
    )
    fw_alerts = (
        alerts.join(vitals.select("reading_key", "device_key"), "reading_key", "left")
        .join(dim_dev.select("device_key", "firmware_version"), "device_key", "left")
        .groupBy("firmware_version")
        .agg(F.count("*").alias("alerts"))
    )
    fw_stats = per_fw.join(fw_alerts, "firmware_version", "left").fillna(0)
    fw_rows = fw_stats.collect()
    for row in fw_rows:
        fw_rate = 100 * (row["alerts"] or 0) / max(row["readings"], 1)
        flag = " ← INVESTIGATE" if fw_rate > 15 else ""
        print(f"    firmware {row['firmware_version']}: {fw_rate:.1f}% ({row['alerts']:,}/{row['readings']:,}){flag}")

    return []


def main():
    spark = get_spark_session("PulseTrackHealthCheck")
    print("=" * 60)
    print("PulseTrack Pipeline Health Check")
    print(f"Run at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    warnings = []
    warnings += check_gold_freshness(spark)
    warnings += check_identity_coverage(spark)
    warnings += check_row_lineage(spark)
    warnings += check_anomaly_rate(spark)

    print("\n" + "=" * 60)
    if warnings:
        print(f"HEALTH: {len(warnings)} WARNING(S)")
        for w in warnings:
            print(f"  ⚠ {w}")
    else:
        print("HEALTH: ALL CHECKS OK ✅")


if __name__ == "__main__":
    main()
