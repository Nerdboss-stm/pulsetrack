"""
Pharmacy Bronze ingestion — Kafka pharmacy_events → Bronze Delta
=================================================================
Reads CDC events from the pharmacy_events Kafka topic and lands them
raw (JSON strings) into Bronze Delta. Mirrors bronze_ingestion.py for sensor_readings.

Run: python batch_ingestion/pharmacy_bronze.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from streaming.spark_config import get_spark_session
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP = "localhost:9093"
TOPIC           = "pharmacy_events"
BRONZE_PATH     = "/tmp/pulsetrack-lakehouse/bronze/pharmacy"
CHECKPOINT_PATH = "/tmp/pulsetrack-lakehouse/checkpoints/bronze_pharmacy"


def main():
    spark = get_spark_session("BronzePharmacyIngestion")

    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_value")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    query = (
        df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("path", BRONZE_PATH)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    count = spark.read.format("delta").load(BRONZE_PATH).count()
    print(f"✅ Bronze pharmacy rows: {count}")


if __name__ == "__main__":
    main()
