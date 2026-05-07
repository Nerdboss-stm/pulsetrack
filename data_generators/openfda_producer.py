"""
Polls the Open FDA Drug Adverse Events API and publishes to Kafka.

API: https://api.fda.gov/drug/event.json
- Free, no auth required
- Rate limit: 240 requests/minute without API key, 120K/day with key
- Returns real adverse event reports: patient demographics, drugs, reactions

This replaces the synthetic ``data_generators/synthetic/pharmacy_generator.py``
with REAL data. Each adverse event becomes a PharmacyEvent on the
`pharmacy_events` topic.

Polling strategy:
- On first run: backfill last 30 days of reports
- Ongoing: poll every 5 minutes for new reports since last_received_date
- Offset tracking: persist last_received_date to a local file
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import requests
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_failed,
    records_processed,
    start_metrics_server,
)
from schemas.registry import (  # noqa: E402
    get_avro_serializer,
    register_all_schemas,
)
from streaming.kafka_helpers import apply_msk_auth  # noqa: E402
from utils.retry import retry  # noqa: E402

log = get_logger(__name__)

EPOCH = date(1970, 1, 1)


def _to_avro_date(d: date) -> int:
    return (d - EPOCH).days


def _to_avro_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class OpenFDAProducer:
    BASE_URL = f"{settings.openfda_base_url}/drug/event.json"

    def __init__(self, offset_file: str = ".openfda_offset", poll_interval: int = 300):
        self.poll_interval = poll_interval
        self.offset_file = Path(offset_file)
        subject = f"{settings.kafka_topic_pharmacy}-value"
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

    # ── Offset management ──────────────────────────────────────────────────
    def load_offset(self) -> Optional[str]:
        if not self.offset_file.exists():
            return None
        return self.offset_file.read_text().strip() or None

    def save_offset(self, yyyymmdd: str) -> None:
        self.offset_file.write_text(yyyymmdd)

    # ── Open FDA API ───────────────────────────────────────────────────────
    @retry(max_retries=3, backoff_factor=2.0, exceptions=(requests.RequestException,))
    def fetch_events(
        self,
        start_date: str,
        end_date: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
    ) -> list[dict]:
        """Fetch adverse-event records in [start_date, end_date]. Dates in YYYYMMDD."""
        end_date = end_date or datetime.utcnow().strftime("%Y%m%d")
        params = {
            "search": f"receivedate:[{start_date} TO {end_date}]",
            "limit": limit,
            "skip": skip,
        }
        resp = requests.get(self.BASE_URL, params=params, timeout=30)
        if resp.status_code == 404:
            # Open FDA returns 404 when zero results match
            return []
        if resp.status_code == 429:
            log.warning("Open FDA rate-limited; backing off", extra={"extra_data": {"status": 429}})
            time.sleep(60)
            return []
        resp.raise_for_status()
        return resp.json().get("results", [])

    @retry(max_retries=3, backoff_factor=2.0, exceptions=(RuntimeError,))
    def _flush_producer(self, timeout: float = 5.0) -> None:
        """Block until all queued messages are sent. Raises if any remain."""
        pending = self.producer.flush(timeout)
        if pending and pending > 0:
            raise RuntimeError(f"Kafka flush timeout: {pending} messages remaining")

    # ── Mapping ────────────────────────────────────────────────────────────
    @staticmethod
    def _safe_first(seq: Any) -> Optional[dict]:
        return seq[0] if isinstance(seq, list) and seq else None

    @staticmethod
    def _patient_pseudo_id(report_id: str, age: Any, sex: Any) -> str:
        """
        Open FDA reports do not expose patient identifiers. We synthesize a
        stable pseudo-ID per (report, age, sex) so downstream joins behave.
        """
        material = f"{report_id}|{age}|{sex}".encode()
        return "FDA-" + hashlib.sha256(material).hexdigest()[:16]

    def transform_to_pharmacy_event(self, fda_event: dict) -> Optional[dict]:
        """Map an Open FDA result row to a PharmacyEvent dict."""
        report_id = fda_event.get("safetyreportid") or str(uuid.uuid4())
        receive_str = fda_event.get("receivedate", "")
        try:
            received = datetime.strptime(receive_str, "%Y%m%d")
        except ValueError:
            return None

        patient = fda_event.get("patient") or {}
        first_drug = self._safe_first(patient.get("drug")) or {}
        drug_name = (
            first_drug.get("medicinalproduct")
            or self._safe_first((first_drug.get("openfda") or {}).get("brand_name"))
            or "UNKNOWN"
        )
        ndc_codes = (first_drug.get("openfda") or {}).get("product_ndc") or []
        ndc_code = ndc_codes[0] if ndc_codes else None

        return {
            "event_id": f"FDA-{report_id}",
            "event_type": "adverse_event",
            "patient_id": self._patient_pseudo_id(
                report_id,
                patient.get("patientonsetage"),
                patient.get("patientsex"),
            ),
            "drug_name": str(drug_name)[:200],
            "ndc_code": ndc_code,
            "prescriber_npi": None,
            "fill_date": _to_avro_date(received.date()),
            "quantity": 1,
            "fda_report_id": report_id,
            "event_timestamp": _to_avro_millis(received),
        }

    # ── Producer ───────────────────────────────────────────────────────────
    def _on_delivery(self, err, msg):
        if err is not None:
            records_failed.labels(layer="bronze", source="openfda", reason="delivery_error").inc()
            log.error(
                "Kafka delivery failed",
                extra={"extra_data": {"topic": msg.topic(), "error": str(err)}},
            )

    def _publish(self, event: dict) -> None:
        value_bytes = self.serializer(
            event,
            SerializationContext(settings.kafka_topic_pharmacy, MessageField.VALUE),
        )
        self.producer.produce(
            topic=settings.kafka_topic_pharmacy,
            key=self.key_serializer(event["event_id"]),
            value=value_bytes,
            on_delivery=self._on_delivery,
        )
        records_processed.labels(layer="bronze", source="openfda").inc()

    def _process_window(self, start: str, end: str, max_pages: int = 20) -> int:
        """Page through results in [start, end]. Returns number published."""
        published = 0
        for page in range(max_pages):
            try:
                events = self.fetch_events(start, end, limit=100, skip=page * 100)
            except requests.RequestException as exc:
                records_failed.labels(layer="bronze", source="openfda", reason="api_error").inc()
                log.error(
                    "Open FDA fetch failed", extra={"extra_data": {"page": page, "error": str(exc)}}
                )
                break
            if not events:
                break
            for ev in events:
                mapped = self.transform_to_pharmacy_event(ev)
                if not mapped:
                    records_failed.labels(
                        layer="bronze", source="openfda", reason="parse_error"
                    ).inc()
                    continue
                try:
                    self._publish(mapped)
                    published += 1
                except Exception:
                    records_failed.labels(
                        layer="bronze", source="openfda", reason="serialize_error"
                    ).inc()
                    log.error("Serialize/produce failed", exc_info=True)
            self.producer.poll(0)
            # Throttle below the 240/min unauthenticated limit
            time.sleep(0.3)
        self._flush_producer(5)
        return published

    def backfill(self, days: int = 30) -> int:
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=days)
        start = start_dt.strftime("%Y%m%d")
        end = end_dt.strftime("%Y%m%d")
        log.info("OpenFDA backfill starting", extra={"extra_data": {"start": start, "end": end}})
        n = self._process_window(start, end)
        self.save_offset(end)
        log.info(
            "OpenFDA backfill complete", extra={"extra_data": {"published": n, "checkpoint": end}}
        )
        return n

    def run(self, poll_interval_seconds: Optional[int] = None) -> None:
        poll = poll_interval_seconds or self.poll_interval

        if self.load_offset() is None:
            self.backfill(days=30)

        log.info(
            "OpenFDA polling loop started", extra={"extra_data": {"poll_interval_seconds": poll}}
        )
        try:
            while True:
                start = self.load_offset() or datetime.utcnow().strftime("%Y%m%d")
                end = datetime.utcnow().strftime("%Y%m%d")
                n = self._process_window(start, end)
                self.save_offset(end)
                log.info(
                    "OpenFDA cycle complete",
                    extra={
                        "extra_data": {
                            "start": start,
                            "end": end,
                            "published": n,
                        }
                    },
                )
                time.sleep(poll)
        except KeyboardInterrupt:
            self._flush_producer(10)
            log.info("OpenFDA polling stopped")


def main():
    start_metrics_server(8001)
    register_all_schemas()
    OpenFDAProducer().run()


if __name__ == "__main__":
    main()
