"""
WHOOP API responses → SensorReading Avro events.

Each WHOOP record produces 1+ SensorReading events (one per metric extracted).
``device_id``, ``device_account_id``, ``patient_email`` are pulled from settings
so the user's WHOOP data flows through identity resolution to the same
``patient_key`` as their EHR/wearable rows.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Iterable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import settings  # noqa: E402

# Synthetic device id for the WHOOP source — the WHOOP band itself doesn't expose
# a device serial via the API, so we use the user's account id as the namespacing.
DEFAULT_FIRMWARE_VERSION = "5.0.0"


def _device_id() -> str:
    return f"WHOOP-{settings.whoop_account_id or 'unknown'}"


def _ts_to_millis(timestamp: str) -> int:
    """ISO 8601 string → ms since epoch."""
    if timestamp.endswith("Z"):
        timestamp = timestamp.replace("Z", "+00:00")
    return int(datetime.fromisoformat(timestamp).timestamp() * 1000)


def _build(metrics: dict, event_ts_iso: str, device_type: str = "smartwatch") -> dict:
    event_ms = _ts_to_millis(event_ts_iso)
    sync_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return {
        "reading_id": str(uuid.uuid4()),
        "device_id": _device_id(),
        "device_type": device_type,
        "user_device_account_id": settings.whoop_account_id or "unknown",
        "patient_email": settings.whoop_user_email or None,
        "metrics": {k: float(v) for k, v in metrics.items() if v is not None},
        "firmware_version": DEFAULT_FIRMWARE_VERSION,
        "battery_pct": 100,  # WHOOP doesn't expose battery via developer API
        "event_timestamp": event_ms,
        "sync_timestamp": sync_ms,
        "source_type": "whoop_api",
    }


# ── Per-endpoint mappers ────────────────────────────────────────────────────


def transform_recovery(record: dict) -> dict | None:
    """Map a WHOOP recovery record to a SensorReading event."""
    score = record.get("score") or {}
    if not score:
        return None
    metrics: dict = {}
    if score.get("hrv_rmssd_milli") is not None:
        metrics["hrv_ms"] = score["hrv_rmssd_milli"]
    if score.get("resting_heart_rate") is not None:
        metrics["heart_rate_bpm"] = score["resting_heart_rate"]
    if score.get("spo2_percentage") is not None:
        metrics["spo2_pct"] = score["spo2_percentage"]
    if score.get("skin_temp_celsius") is not None:
        metrics["skin_temp_celsius"] = score["skin_temp_celsius"]
    if not metrics:
        return None
    event_ts = record.get("created_at") or record.get("updated_at")
    if event_ts is None:
        return None
    return _build(metrics, event_ts)


def transform_sleep(record: dict) -> dict | None:
    """Map a WHOOP sleep record to a SensorReading event."""
    score = record.get("score") or {}
    metrics: dict = {}
    if score.get("respiratory_rate") is not None:
        metrics["respiration_rate"] = score["respiratory_rate"]
    if not metrics:
        return None
    event_ts = record.get("end") or record.get("created_at")
    if event_ts is None:
        return None
    return _build(metrics, event_ts, device_type="sleep_ring")


def transform_workout(record: dict) -> dict | None:
    """Map a WHOOP workout record — emit average HR as a single reading."""
    score = record.get("score") or {}
    metrics: dict = {}
    if score.get("average_heart_rate") is not None:
        metrics["heart_rate_bpm"] = score["average_heart_rate"]
    if not metrics:
        return None
    event_ts = record.get("end") or record.get("start") or record.get("created_at")
    if event_ts is None:
        return None
    return _build(metrics, event_ts, device_type="chest_strap")


def transform_cycle(record: dict) -> dict | None:
    """Map a WHOOP daily cycle to RHR + HRV summary readings."""
    score = record.get("score") or {}
    metrics: dict = {}
    if score.get("resting_heart_rate") is not None:
        metrics["heart_rate_bpm"] = score["resting_heart_rate"]
    if score.get("hrv_rmssd_milli") is not None:
        metrics["hrv_ms"] = score["hrv_rmssd_milli"]
    if not metrics:
        return None
    event_ts = record.get("end") or record.get("created_at")
    if event_ts is None:
        return None
    return _build(metrics, event_ts)


# ── Batch helpers ───────────────────────────────────────────────────────────


def transform_records(records: Iterable[dict], kind: str) -> Iterable[dict]:
    """Apply the right transformer for ``kind`` and skip unmapped records."""
    mapper = {
        "cycle": transform_cycle,
        "recovery": transform_recovery,
        "sleep": transform_sleep,
        "workout": transform_workout,
    }[kind]
    for record in records:
        event = mapper(record)
        if event is not None:
            yield event
