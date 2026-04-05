"""
PulseTrack: Wearable Sensor Readings → Bronze Delta Lake
==========================================================
Reads from Kafka (sensor_readings topic) and writes to Bronze Delta on Azurite.

SAME PATTERN as GhostKitchen but:
- Kafka on port 9093 (PulseTrack's Kafka)
- Storage on Azurite (Azure Blob emulator) instead of MinIO (S3)
- Output path uses local filesystem for simplicity (Azurite + Delta + Spark
  can be tricky; local filesystem is more reliable for learning)

NOTE: For local dev, we write to the local filesystem instead of Azurite.
This avoids Azure SDK complexity. When deploying to real Azure, switch to
wasbs:// paths. The pipeline logic is IDENTICAL.
"""

import sys, os
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger(__name__)
from streaming.spark_config import get_spark_session


# Use local filesystem for Bronze (simpler for learning)
# In production Azure: "wasbs://container@account.blob.core.windows.net/bronze/..."


def run_wearable_bronze():
    log.info("💜 PulseTrack Wearable → Bronze ingestion starting...")
    log.info(f"   Source: Kafka topic 'sensor_readings' on localhost:9093")
    log.info(f"   Sink: {settings.bronze_sensor}")
    
    spark = get_spark_session("PulseTrack-Bronze-Wearables")
    
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)  # PulseTrack Kafka!
        .option("subscribe", settings.kafka_topic_sensor)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    bronze_df = (
        kafka_df.select(
            F.col("value").cast(StringType()).alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingestion_date"),
            F.date_format(F.current_timestamp(), "HH").alias("ingestion_hour"),
        )
    )
    
    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", settings.checkpoint_bronze_sensor)
        .trigger(processingTime=settings.trigger_interval)
        .partitionBy("ingestion_date", "ingestion_hour")
        .start(settings.bronze_sensor)
    )
    
    log.info("✅ PulseTrack Bronze running!")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()
        log.info("Stopped.")

if __name__ == "__main__":
    run_wearable_bronze()