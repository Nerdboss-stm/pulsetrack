"""
PulseTrack: Wearable Sensor Readings → Bronze Delta Lake.
Raw-JSON ingest (Avro decode lands in a follow-up).
"""

import os
import signal
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from streaming.spark_config import get_spark_session  # noqa: E402

log = get_logger(__name__)


def _install_shutdown_handlers(query, spark):
    """Stop the streaming query and Spark session on SIGTERM/SIGINT."""
    def handler(signum, _frame):
        log.info(
            "Received shutdown signal, stopping streaming query",
            extra={"extra_data": {"signal": signum}},
        )
        try:
            query.stop()
        except Exception:
            log.exception("Error stopping streaming query")
        try:
            spark.stop()
        except Exception:
            log.exception("Error stopping Spark session")
        log.info("Shutdown complete")
        sys.exit(0)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def run_wearable_bronze():
    log.info(
        "PulseTrack Wearable → Bronze ingestion starting",
        extra={"extra_data": {
            "source_topic": settings.kafka_topic_sensor,
            "kafka_bootstrap": settings.kafka_bootstrap,
            "sink": settings.bronze_sensor,
        }},
    )

    spark = get_spark_session("PulseTrack-Bronze-Wearables")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
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

    _install_shutdown_handlers(query, spark)
    log.info("PulseTrack Bronze running", extra={"extra_data": {"query_id": str(query.id)}})

    query.awaitTermination()


if __name__ == "__main__":
    run_wearable_bronze()
