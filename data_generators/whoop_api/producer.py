"""
WHOOP → Kafka producer.

First run: backfills ``settings.whoop_backfill_days`` worth of data per endpoint.
Ongoing: polls every ``settings.whoop_poll_interval_seconds`` for new records
since the last persisted offset (per endpoint).

Offsets are persisted to ``settings.whoop_offsets_path`` so we don't re-emit
records on restart.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402
from data_generators.whoop_api.client import WhoopClient  # noqa: E402
from data_generators.whoop_api.transformer import transform_records  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_failed,
    records_processed,
    start_metrics_server,
)
from schemas.registry import get_avro_serializer, register_schemas_for_environment  # noqa: E402
from streaming.kafka_helpers import apply_msk_auth  # noqa: E402

log = get_logger(__name__)

ENDPOINTS = ["cycle", "recovery", "sleep", "workout"]


def _offsets_path() -> str:
    return os.path.expanduser(settings.whoop_offsets_path)


def _load_offsets() -> dict:
    path = _offsets_path()
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def _save_offsets(offsets: dict) -> None:
    path = _offsets_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(offsets, f)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _on_delivery(err, msg):
    if err is not None:
        records_failed.labels(layer="bronze", source="whoop", reason="delivery_error").inc()
        log.error(
            "Kafka delivery failed",
            extra={"extra_data": {"topic": msg.topic(), "error": str(err)}},
        )


class WhoopProducer:
    def __init__(self, client: Optional[WhoopClient] = None):
        self.client = client or WhoopClient()
        # Cloud → Glue Schema Registry via boto3 (no HTTP to localhost:8081).
        # Local → Confluent Schema Registry on docker-compose.
        register_schemas_for_environment()
        subject = f"{settings.kafka_topic_sensor}-value"
        self.serializer = get_avro_serializer(subject)
        self.key_serializer = StringSerializer()

        producer_config = {
            "bootstrap.servers": settings.kafka_bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 100,
        }
        apply_msk_auth(producer_config)
        self.producer = Producer(producer_config)

    def _publish(self, event: dict) -> None:
        value_bytes = self.serializer(
            event, SerializationContext(settings.kafka_topic_sensor, MessageField.VALUE)
        )
        self.producer.produce(
            topic=settings.kafka_topic_sensor,
            key=self.key_serializer(event["device_id"]),
            value=value_bytes,
            on_delivery=_on_delivery,
        )

    def _emit(self, records: Iterable[dict], kind: str) -> int:
        published = 0
        for event in transform_records(records, kind):
            try:
                self._publish(event)
                published += 1
                records_processed.labels(layer="bronze", source="whoop").inc()
            except Exception:
                records_failed.labels(
                    layer="bronze", source="whoop", reason="serialize_error"
                ).inc()
                log.error("WHOOP serialize/produce failed", exc_info=True)
            self.producer.poll(0)
        return published

    def fetch_window(self, kind: str, start_iso: str, end_iso: str) -> int:
        if kind == "cycle":
            return self._emit(self.client.list_cycles(start_iso, end_iso), kind)
        if kind == "recovery":
            return self._emit(self.client.list_recovery(start_iso, end_iso), kind)
        if kind == "sleep":
            return self._emit(self.client.list_sleep(start_iso, end_iso), kind)
        if kind == "workout":
            return self._emit(self.client.list_workouts(start_iso, end_iso), kind)
        raise ValueError(f"Unknown WHOOP endpoint: {kind}")

    def run_once(self) -> None:
        """Single fetch pass: backfill on first run, otherwise incremental from saved offsets."""
        offsets = _load_offsets()
        end = datetime.now(timezone.utc)
        for kind in ENDPOINTS:
            saved_offset = offsets.get(kind)
            if saved_offset is None:
                start = end - timedelta(days=settings.whoop_backfill_days)
                log.info(
                    "WHOOP backfill",
                    extra={"extra_data": {"endpoint": kind, "days": settings.whoop_backfill_days}},
                )
            else:
                start = datetime.fromisoformat(saved_offset.replace("Z", "+00:00"))
            published = self.fetch_window(kind, _iso(start), _iso(end))
            offsets[kind] = _iso(end)
            log.info(
                "WHOOP poll completed",
                extra={
                    "extra_data": {
                        "endpoint": kind,
                        "start": _iso(start),
                        "end": _iso(end),
                        "published": published,
                    }
                },
            )
        _save_offsets(offsets)
        self.producer.flush(10)

    def run_loop(self) -> None:
        log.info(
            "WHOOP poller started",
            extra={"extra_data": {"interval_seconds": settings.whoop_poll_interval_seconds}},
        )
        while True:
            try:
                self.run_once()
            except Exception:
                records_failed.labels(layer="bronze", source="whoop", reason="api_error").inc()
                log.error("WHOOP poll iteration failed", exc_info=True)
            time.sleep(settings.whoop_poll_interval_seconds)


def main() -> None:
    start_metrics_server(settings.metrics_port_whoop)
    producer = WhoopProducer()
    producer.run_loop()


if __name__ == "__main__":
    main()
