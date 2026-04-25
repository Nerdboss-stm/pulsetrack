"""
Dead Letter Queue handler.

When a record fails deserialization, schema validation, or any
unrecoverable processing error, it's written to:
  1. A DLQ Delta table (for batch reprocessing)
  2. A DLQ Kafka topic (for real-time alerting)

DLQ record schema:
  - original_topic, original_partition, original_offset
  - original_key, original_value (as raw bytes/string)
  - error_type, error_message, stack_trace
  - failed_at (timestamp)
  - retry_count

A separate DLQ reprocessor job can read the DLQ table,
attempt to fix records (e.g., apply schema migration),
and re-publish to the original topic.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import records_failed  # noqa: E402
from utils.retry import retry  # noqa: E402

log = get_logger(__name__)


# ── Delta schema for the DLQ table ─────────────────────────────────────────────
DLQ_SCHEMA = StructType([
    StructField("original_topic",     StringType(),    True),
    StructField("original_partition", IntegerType(),   True),
    StructField("original_offset",    LongType(),      True),
    StructField("original_key",       StringType(),    True),
    StructField("original_value",     StringType(),    True),
    StructField("error_type",         StringType(),    True),
    StructField("error_message",      StringType(),    True),
    StructField("stack_trace",        StringType(),    True),
    StructField("failed_at",          TimestampType(), True),
    StructField("retry_count",        IntegerType(),   True),
])


@dataclass
class DLQRecord:
    """A single failed record bound for the DLQ."""
    error_type: str
    error_message: str
    original_topic: Optional[str] = None
    original_partition: Optional[int] = None
    original_offset: Optional[int] = None
    original_key: Optional[str] = None
    original_value: Optional[str] = None
    stack_trace: Optional[str] = None
    retry_count: int = 0
    failed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class DLQHandler:
    """Publishes failed records to the DLQ Delta table and Kafka topic.

    A SparkSession is required for Delta writes; pass `None` to publish to
    Kafka only (e.g. from a non-Spark producer process).
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark
        self._producer: Optional[Producer] = None

    # ── Lazy Kafka producer ────────────────────────────────────────────────
    @property
    def producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer({
                "bootstrap.servers": settings.kafka_bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 50,
            })
        return self._producer

    # ── Single-record publish (Delta + Kafka) ──────────────────────────────
    def publish(self, record: DLQRecord, also_kafka: bool = True) -> None:
        if self.spark is not None:
            self._write_delta_single(record)
        if also_kafka:
            self._publish_kafka(record)
        records_failed.labels(
            layer=record.original_topic or "unknown",
            source="dlq",
            reason=record.error_type or "unknown",
        ).inc()
        log.warning(
            "Record sent to DLQ",
            extra={"extra_data": {
                "error_type": record.error_type,
                "error_message": record.error_message,
                "topic": record.original_topic,
                "offset": record.original_offset,
            }},
        )

    @retry(max_retries=3, backoff_factor=2.0)
    def _write_delta_single(self, record: DLQRecord) -> None:
        df = self.spark.createDataFrame([asdict(record)], schema=DLQ_SCHEMA)
        df.write.format("delta").mode("append").save(settings.dlq)

    def _publish_kafka(self, record: DLQRecord) -> None:
        payload = asdict(record)
        payload["failed_at"] = record.failed_at.isoformat()
        self.producer.produce(
            topic=settings.kafka_topic_dlq,
            key=(record.original_key or "").encode("utf-8"),
            value=json.dumps(payload, default=str).encode("utf-8"),
        )
        self.producer.poll(0)

    # ── Batch publish (Delta only — Kafka batches are out of scope) ────────
    @retry(max_retries=3, backoff_factor=2.0)
    def publish_dataframe(
        self,
        df: DataFrame,
        error_type: str,
        error_message: str,
    ) -> int:
        """Append all rows of `df` to the DLQ Delta table.

        `df` should carry the Kafka envelope columns (`topic`, `partition`,
        `offset`, `key`, `value`); any missing column is filled with NULL so
        non-Kafka batch contexts can still use this method.
        """
        if self.spark is None:
            raise RuntimeError("publish_dataframe requires a SparkSession")

        cols = set(df.columns)
        def _col_or_null(name: str, dtype):
            return F.col(name).cast(dtype) if name in cols else F.lit(None).cast(dtype)

        enriched = df.select(
            _col_or_null("topic",     StringType()).alias("original_topic"),
            _col_or_null("partition", IntegerType()).alias("original_partition"),
            _col_or_null("offset",    LongType()).alias("original_offset"),
            _col_or_null("key",       StringType()).alias("original_key"),
            _col_or_null("value",     StringType()).alias("original_value"),
            F.lit(error_type).alias("error_type"),
            F.lit(error_message).alias("error_message"),
            F.lit(None).cast(StringType()).alias("stack_trace"),
            F.current_timestamp().alias("failed_at"),
            F.lit(0).cast(IntegerType()).alias("retry_count"),
        ).cache()

        n = enriched.count()
        if n > 0:
            enriched.write.format("delta").mode("append").save(settings.dlq)
            records_failed.labels(layer="bronze", source="dlq", reason=error_type).inc(n)
            log.warning(
                "DLQ batch published",
                extra={"extra_data": {"row_count": n, "error_type": error_type}},
            )
        enriched.unpersist()
        return n

    # ── Cleanup ────────────────────────────────────────────────────────────
    def flush(self, timeout: float = 5.0) -> None:
        if self._producer is not None:
            self._producer.flush(timeout)
