"""
PulseTrack — Silver pharmacy transform.

Reads Bronze pharmacy (Avro-decoded ``decoded`` struct), projects the typed
fields, applies quality flags, deduplicates on ``event_id``, and MERGEs into
``silver.pharmacy_fills`` Delta. Invalid rows route to quarantine.

Two entrypoints:
* :func:`run_streaming` — Structured Streaming with watermark + foreachBatch MERGE.
* :func:`run_batch` — same per-row logic, batch read of Bronze. Used for backfill.
"""

from __future__ import annotations

import os
import sys
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from data_quality.quarantine import quarantine_records  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_processed,
    start_metrics_server,
    streaming_query_active,
)
from streaming.spark_config import get_spark_session  # noqa: E402
from utils.streaming import register_metrics_listener, setup_graceful_shutdown  # noqa: E402

log = get_logger(__name__)
QUERY_NAME = "silver-pharmacy"

VALID_EVENT_TYPES = ["new_fill", "refill", "cancellation", "adverse_event"]


def parse_pharmacy(bronze: DataFrame) -> DataFrame:
    """Project decoded fields out of Bronze, drop unparseable rows."""
    return (
        bronze.filter(F.col("is_parseable"))
        .select(
            F.col("decoded.event_id").alias("event_id"),
            F.col("decoded.event_type").alias("event_type"),
            F.col("decoded.patient_id").alias("patient_id"),
            F.col("decoded.drug_name").alias("drug_name"),
            F.col("decoded.ndc_code").alias("ndc_code"),
            F.col("decoded.prescriber_npi").alias("prescriber_npi"),
            F.col("decoded.fill_date").alias("fill_date"),
            F.col("decoded.quantity").alias("quantity"),
            F.col("decoded.fda_report_id").alias("fda_report_id"),
            F.col("decoded.event_timestamp").alias("event_timestamp"),
            F.col("ingestion_timestamp"),
        )
        .filter(F.col("event_id").isNotNull())
    )


def add_quality_flags(df: DataFrame) -> DataFrame:
    """Tag rows with is_valid (event_type known + quantity sane + drug_name present)."""
    is_valid = (
        F.col("event_type").isin(VALID_EVENT_TYPES)
        & F.col("drug_name").isNotNull()
        & (F.length(F.col("drug_name")) > 0)
        & F.col("quantity").isNotNull()
        & (F.col("quantity") >= 0)
    )
    return df.withColumn("is_valid", is_valid)


def transform(bronze: DataFrame) -> DataFrame:
    """Bronze → Silver per-row transform (no watermark/dedupe — those are batch-level)."""
    return add_quality_flags(parse_pharmacy(bronze))


def _process_batch(spark: SparkSession, silver_df: DataFrame, batch_id: int) -> None:
    if silver_df.rdd.isEmpty():
        return
    cached = silver_df.cache()

    valid_df = cached.filter(F.col("is_valid"))
    invalid_df = cached.filter(~F.col("is_valid"))

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    if invalid_count > 0:
        quarantine_records(invalid_df, validity_col="is_valid", layer="silver", source="pharmacy")

    if valid_count == 0:
        cached.unpersist()
        return

    if not DeltaTable.isDeltaTable(spark, settings.silver_pharmacy):
        valid_df.write.format("delta").mode("overwrite").save(settings.silver_pharmacy)
    else:
        DeltaTable.forPath(spark, settings.silver_pharmacy).alias("t").merge(
            valid_df.alias("s"), "t.event_id = s.event_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    records_processed.labels(layer="silver", source="pharmacy").inc(valid_count)
    cached.unpersist()
    log.info(
        "Pharmacy silver batch processed",
        extra={
            "extra_data": {"batch_id": batch_id, "valid": valid_count, "invalid": invalid_count}
        },
    )


def _make_streaming_processor(spark: SparkSession):
    def process(batch_df: DataFrame, batch_id: int) -> None:
        silver = transform(batch_df).dropDuplicates(["event_id"])
        _process_batch(spark, silver, batch_id)

    return process


def run_streaming(metrics_port: int = settings.metrics_port_silver_pharmacy) -> None:
    start_metrics_server(metrics_port)
    spark = get_spark_session("PulseTrack-Silver-Pharmacy")
    register_metrics_listener(spark, layer="silver")
    bronze_stream = (
        spark.readStream.format("delta")
        .option("ignoreChanges", "true")
        .load(settings.bronze_pharmacy)
    )
    transformed = transform(bronze_stream).withWatermark(
        "ingestion_timestamp", settings.watermark_delay
    )
    deduped = transformed.dropDuplicatesWithinWatermark(["event_id"])
    query = (
        deduped.writeStream.foreachBatch(_make_streaming_processor(spark))
        .option("checkpointLocation", f"{settings.checkpoint_base}/silver_pharmacy")
        .trigger(processingTime=settings.trigger_interval)
        .queryName(QUERY_NAME)
        .start()
    )
    streaming_query_active.labels(query_name=QUERY_NAME).set(1)
    setup_graceful_shutdown(query, spark)
    log.info("Pharmacy Silver running", extra={"extra_data": {"query_id": str(query.id)}})
    try:
        query.awaitTermination()
    finally:
        streaming_query_active.labels(query_name=QUERY_NAME).set(0)


def run_batch(spark: Optional[SparkSession] = None) -> None:
    spark = spark or get_spark_session("PulseTrack-Silver-Pharmacy-Batch")
    if not DeltaTable.isDeltaTable(spark, settings.bronze_pharmacy):
        log.warning("Bronze pharmacy table missing — nothing to transform")
        return
    bronze = spark.read.format("delta").load(settings.bronze_pharmacy)
    silver = transform(bronze).dropDuplicates(["event_id"])
    _process_batch(spark, silver, batch_id=-1)
    log.info("Pharmacy silver batch complete")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["streaming", "batch"], default="batch")
    args = parser.parse_args()
    if args.mode == "streaming":
        run_streaming()
    else:
        run_batch()
