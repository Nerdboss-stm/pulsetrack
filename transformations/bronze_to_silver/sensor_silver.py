import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

BRONZE_PATH = "/tmp/pulsetrack-lakehouse/bronze/sensor_readings"
SILVER_PATH = "/tmp/pulsetrack-lakehouse/silver/sensor_readings"

# Valid ranges per metric — used for is_valid flag
METRIC_RANGES = {
    "heart_rate_bpm":     (30,  220),
    "spo2_pct":           (70,  100),
    "steps_since_last":   (0,   10000),
    "skin_temp_celsius":  (30,  42),
    "hrv_ms":             (5,   200),
    "respiration_rate":   (8,   40),
    "sleep_stage":        (0,   4),
    "blood_glucose_mgdl": (40,  400),
}

def parse_and_explode(df: DataFrame) -> DataFrame:
    """
    Step 1: Parse raw_value JSON into structured columns
    Step 2: Explode nested metrics object into one row per metric
    """
    rv = F.col("raw_value")

    # Parse top-level fields first
    parsed = df.select(
        F.get_json_object(rv, "$.reading_id").alias("reading_id"),
        F.get_json_object(rv, "$.device_id").alias("device_id"),
        F.get_json_object(rv, "$.device_type").alias("device_type"),
        F.get_json_object(rv, "$.user_device_account_id").alias("device_account_id"),
        F.get_json_object(rv, "$.firmware_version").alias("firmware_version"),
        F.get_json_object(rv, "$.battery_pct").cast(IntegerType()).alias("battery_pct"),
        F.to_utc_timestamp(
            F.to_timestamp(F.get_json_object(rv, "$.event_timestamp")), "UTC"
        ).alias("event_timestamp"),
        F.to_utc_timestamp(
            F.to_timestamp(F.get_json_object(rv, "$.sync_timestamp")), "UTC"
        ).alias("sync_timestamp"),
        # Keep entire metrics blob for explosion
        F.get_json_object(rv, "$.metrics").alias("metrics_json"),
        F.col("ingestion_timestamp")
    )

    # Explode metrics — one row per metric name/value pair
    # map_from_entries(from_json()) converts {"heart_rate_bpm": 72, ...}
    # into a Map type that can be exploded
    metrics_schema = MapType(StringType(), DoubleType())

    exploded = parsed.withColumn(
        "metrics_map",
        F.from_json(F.col("metrics_json"), metrics_schema)
        ).select(
        F.col("reading_id"),
        F.col("device_id"),
        F.col("device_type"),
        F.col("device_account_id"),
        F.col("firmware_version"),
        F.col("battery_pct"),
        F.col("event_timestamp"),
        F.col("sync_timestamp"),
        F.col("ingestion_timestamp"),
        F.explode(F.col("metrics_map")).alias("metric_name", "metric_value")
    )

    return exploded


def add_quality_flags(df: DataFrame) -> DataFrame:
    """
    Add is_valid flag based on metric-specific ranges.
    Add is_late_arriving flag — wearables batch sync, so 2h threshold.
    """
    # Build is_valid using a CASE-style expression
    valid_expr = F.lit(True)
    for metric, (low, high) in METRIC_RANGES.items():
        valid_expr = F.when(
            F.col("metric_name") == metric,
            F.col("metric_value").between(low, high)
        ).otherwise(valid_expr)

    return df.withColumn(
        "is_valid", valid_expr
    ).withColumn(
        # Late arriving: sync_timestamp > event_timestamp + 2 hours
        "is_late_arriving",
        (F.unix_timestamp(F.col("sync_timestamp")) -
         F.unix_timestamp(F.col("event_timestamp"))) > 7200
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Dedup on (reading_id + metric_name).
    reading_id is per-device-event, metric_name distinguishes rows after explosion.
    """
    from pyspark.sql import Window
    window = Window.partitionBy("reading_id", "metric_name") \
                   .orderBy(F.col("sync_timestamp").asc())
    return df.withColumn("row_num", F.row_number().over(window)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")


def run_sensor_silver(spark: SparkSession):
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze rows: {bronze_df.count()}")

    parsed_df   = parse_and_explode(bronze_df)
    flagged_df  = add_quality_flags(parsed_df)
    deduped_df  = deduplicate(flagged_df)

    print(f"Silver rows after explosion + dedup: {deduped_df.count()}")

    deduped_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(SILVER_PATH)

    print("✅ Sensor silver written")


if __name__ == "__main__":
    from streaming.spark_config import get_spark_session
    spark = get_spark_session("SensorSilver")
    run_sensor_silver(spark)