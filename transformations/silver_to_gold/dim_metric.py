import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)

# Junk dimension: one row per (metric_name, device_type) combination.
# Source: DEVICE_METRICS in wearable_generator.py + METRIC_RANGES in sensor_silver.py.
# normal_low/normal_high reflect valid physiological ranges (same thresholds used by is_valid flag).
# sleep_stage is categorical; normal_low/normal_high are placeholder stage indices (0–4).
METRIC_SEED = [
    # metric_name,          unit,           normal_low, normal_high, device_type
    ("heart_rate_bpm",     "bpm",                45.0,       180.0,  "smartwatch"),
    ("spo2_pct",           "%",                  90.0,       100.0,  "smartwatch"),
    ("steps_since_last",   "steps",               0.0,     10000.0,  "smartwatch"),
    ("skin_temp_celsius",  "°C",                 30.0,        38.0,  "smartwatch"),
    ("hrv_ms",             "ms",                 10.0,       120.0,  "smartwatch"),
    ("heart_rate_bpm",     "bpm",                45.0,       200.0,  "chest_strap"),
    ("hrv_ms",             "ms",                 10.0,       120.0,  "chest_strap"),
    ("respiration_rate",   "breaths/min",         8.0,        30.0,  "chest_strap"),
    ("heart_rate_bpm",     "bpm",                40.0,       100.0,  "sleep_ring"),
    ("spo2_pct",           "%",                  88.0,       100.0,  "sleep_ring"),
    ("skin_temp_celsius",  "°C",                 31.0,        37.0,  "sleep_ring"),
    ("sleep_stage",        "stage",               0.0,         4.0,  "sleep_ring"),
    ("blood_glucose_mgdl", "mg/dL",              40.0,       400.0,  "glucose_monitor"),
]


def main():
    spark = get_spark_session("GoldDimMetric")

    df = spark.createDataFrame(
        METRIC_SEED,
        ["metric_name", "unit", "normal_low", "normal_high", "device_type"]
    )

    df = (
        df
        .withColumn("metric_key",
            F.abs(F.hash(F.concat_ws("|", F.col("metric_name"), F.col("device_type")))).cast("long"))
        .select("metric_key", "metric_name", "unit", "normal_low", "normal_high", "device_type")
    )

    df.write.format("delta").mode("overwrite").save(settings.gold_dim_metric)
    log.info(
        "dim_metric written",
        extra={"extra_data": {"row_count": df.count(), "path": settings.gold_dim_metric}},
    )


if __name__ == "__main__":
    main()
