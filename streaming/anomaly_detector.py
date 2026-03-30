"""
Anomaly Detector — Spark Stateful Streaming
============================================
Reads Silver sensor_readings (batch or stream) and detects anomalies
using per-patient, per-metric rolling statistics (30-day window).

Algorithm:
  - Compute rolling mean + stddev over last 30 days per (patient_key, metric_name)
  - Flag reading as anomaly if |metric_value - mean| > 2 * stddev
  - Classify: sustained_high / sustained_low / sudden_spike

Output: /tmp/pulsetrack-lakehouse/gold/fact_anomaly_alert/

Run in batch mode (development): python streaming/anomaly_detector.py
Run in streaming mode:           python streaming/anomaly_detector.py --stream
"""
import sys, os, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, DoubleType,
    BooleanType, StringType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from streaming.spark_config import get_spark_session

GOLD_BASE           = "/tmp/pulsetrack-lakehouse/gold"
SILVER_BASE         = "/tmp/pulsetrack-lakehouse/silver"
SILVER_SENSORS_PATH = f"{SILVER_BASE}/sensor_readings"
BRIDGE_PATH         = f"{SILVER_BASE}/identity/patient_identity_bridge"
ALERT_PATH          = f"{GOLD_BASE}/fact_anomaly_alert"
CHECKPOINT_PATH     = "/tmp/pulsetrack-lakehouse/checkpoints/anomaly_detector"

ANOMALY_SIGMA_THRESHOLD = 2.0
WINDOW_DAYS = 30

_EMPTY_SCHEMA = StructType([
    StructField("alert_key",        LongType()),
    StructField("patient_key",      LongType()),
    StructField("metric_key",       LongType()),
    StructField("date_key",         IntegerType()),
    StructField("time_key",         IntegerType()),
    StructField("alert_type",       StringType()),
    StructField("severity",         StringType()),
    StructField("metric_value",     DoubleType()),
    StructField("threshold_value",  DoubleType()),
    StructField("patient_baseline", DoubleType()),
    StructField("resolved_flag",    BooleanType()),
    StructField("resolved_timestamp", StringType()),
])


def _classify_anomaly(value_col, mean_col, std_col):
    """Classify anomaly type based on z-score direction and magnitude."""
    z = (value_col - mean_col) / std_col
    return (
        F.when(z > 3.0, F.lit("sudden_spike"))
        .when(z > ANOMALY_SIGMA_THRESHOLD, F.lit("sustained_high"))
        .when(z < -3.0, F.lit("sudden_spike"))
        .when(z < -ANOMALY_SIGMA_THRESHOLD, F.lit("sustained_low"))
        .otherwise(F.lit(None))
    )


def _severity(z_col):
    z = F.abs(z_col)
    return (
        F.when(z > 4.0, F.lit("critical"))
        .when(z > 3.0, F.lit("high"))
        .when(z > ANOMALY_SIGMA_THRESHOLD, F.lit("medium"))
        .otherwise(F.lit("low"))
    )


def run_batch(spark):
    """Batch mode — compute anomalies from full Silver sensor table."""
    if not DeltaTable.isDeltaTable(spark, SILVER_SENSORS_PATH):
        print("  WARNING: Silver sensor_readings not found — no anomalies to detect.")
        df = spark.createDataFrame([], _EMPTY_SCHEMA)
        df.write.format("delta").mode("overwrite").save(ALERT_PATH)
        return

    sensors    = spark.read.format("delta").load(SILVER_SENSORS_PATH).filter(F.col("is_valid"))
    dim_metric = spark.read.format("delta").load(f"{GOLD_BASE}/dim_metric") \
        .select("metric_key", "metric_name", "device_type")

    # ── Patient key ────────────────────────────────────────────────────
    if DeltaTable.isDeltaTable(spark, BRIDGE_PATH):
        bridge = spark.read.format("delta").load(BRIDGE_PATH)
        dev_pat = (
            bridge
            .filter(F.col("identifier_type") == "device_account_id")
            .select(
                F.col("identifier_value").alias("device_account_id"),
                F.abs(F.hash(F.col("patient_key"))).cast("long").alias("patient_key"),
            )
        )
        sensors = sensors.join(dev_pat, "device_account_id", "left")
    sensors = sensors.withColumn(
        "patient_key",
        F.coalesce(
            F.col("patient_key"),
            F.abs(F.hash(F.col("device_account_id"))).cast("long"),
        )
    )

    # Metric key join
    sensors = sensors.join(
        dim_metric, on=["metric_name", "device_type"], how="left"
    )

    # ── Rolling 30-day stats per (patient_key, metric_name) ───────────
    w30 = (
        Window
        .partitionBy("patient_key", "metric_name")
        .orderBy(F.unix_timestamp("event_timestamp"))
        .rangeBetween(-WINDOW_DAYS * 86400, 0)
    )

    stats = sensors.withColumn(
        "rolling_mean", F.avg("metric_value").over(w30)
    ).withColumn(
        "rolling_std", F.stddev("metric_value").over(w30)
    )

    # ── Flag anomalies ─────────────────────────────────────────────────
    anomalies = stats.filter(
        F.col("rolling_std").isNotNull()
        & (F.col("rolling_std") > 0)
    ).withColumn(
        "z_score",
        (F.col("metric_value") - F.col("rolling_mean")) / F.col("rolling_std")
    ).filter(
        F.abs(F.col("z_score")) > ANOMALY_SIGMA_THRESHOLD
    )

    # ── Build fact_anomaly_alert rows ──────────────────────────────────
    df = (
        anomalies
        .withColumn("alert_key",
            F.abs(F.hash(
                F.concat_ws("|", F.col("reading_id"), F.col("metric_name"))
            )).cast("long"))
        .withColumn("date_key",
            (F.year("event_timestamp") * 10000
             + F.month("event_timestamp") * 100
             + F.dayofmonth("event_timestamp")).cast("int"))
        .withColumn("time_key",
            (F.hour("event_timestamp") * 100 + F.minute("event_timestamp")).cast("int"))
        .withColumn("alert_type",
            _classify_anomaly(F.col("metric_value"), F.col("rolling_mean"), F.col("rolling_std")))
        .withColumn("severity",
            _severity(F.col("z_score")))
        .withColumn("threshold_value",
            F.col("rolling_mean") + ANOMALY_SIGMA_THRESHOLD * F.col("rolling_std"))
        .select(
            "alert_key", "patient_key", "metric_key",
            "date_key", "time_key",
            "alert_type", "severity",
            F.col("metric_value"),
            F.col("threshold_value"),
            F.col("rolling_mean").alias("patient_baseline"),
            F.lit(False).alias("resolved_flag"),
            F.lit(None).cast(StringType()).alias("resolved_timestamp"),
        )
    )

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(ALERT_PATH)
    print(f"✅ fact_anomaly_alert rows written: {df.count()} rows")
    df.printSchema()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", action="store_true",
                        help="Run in structured streaming mode (default: batch)")
    args = parser.parse_args()

    spark = get_spark_session("PulseTrackAnomalyDetector")
    run_batch(spark)
