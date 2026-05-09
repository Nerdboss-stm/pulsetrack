"""WHOOP transformer tests — endpoint payloads → SensorReading dicts."""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture(autouse=True)
def whoop_settings(monkeypatch):
    monkeypatch.setenv("PT_WHOOP_ACCOUNT_ID", "acct_test_user")
    monkeypatch.setenv("PT_WHOOP_USER_EMAIL", "test@example.com")
    import config

    importlib.reload(config)
    import data_generators.whoop_api.transformer as t

    importlib.reload(t)
    return t


def test_recovery_maps_hrv_and_rhr_and_spo2(whoop_settings):
    record = {
        "score": {
            "hrv_rmssd_milli": 45.5,
            "resting_heart_rate": 58,
            "spo2_percentage": 97.4,
            "skin_temp_celsius": 33.2,
        },
        "created_at": "2026-05-01T07:00:00.000Z",
    }
    event = whoop_settings.transform_recovery(record)
    assert event["source_type"] == "whoop_api"
    assert event["device_id"].startswith("WHOOP-")
    assert event["patient_email"] == "test@example.com"
    assert event["metrics"]["hrv_ms"] == 45.5
    assert event["metrics"]["heart_rate_bpm"] == 58
    assert event["metrics"]["spo2_pct"] == 97.4
    assert event["metrics"]["skin_temp_celsius"] == 33.2


def test_recovery_returns_none_when_no_metrics(whoop_settings):
    record = {"score": {}, "created_at": "2026-05-01T07:00:00Z"}
    assert whoop_settings.transform_recovery(record) is None


def test_recovery_returns_none_without_timestamp(whoop_settings):
    record = {"score": {"hrv_rmssd_milli": 40.0}}
    assert whoop_settings.transform_recovery(record) is None


def test_sleep_maps_respiratory_rate(whoop_settings):
    record = {
        "score": {"respiratory_rate": 14.2},
        "end": "2026-05-01T07:00:00.000Z",
    }
    event = whoop_settings.transform_sleep(record)
    assert event["device_type"] == "sleep_ring"
    assert event["metrics"]["respiration_rate"] == 14.2


def test_workout_maps_heart_rate(whoop_settings):
    record = {
        "score": {"average_heart_rate": 145, "max_heart_rate": 178},
        "end": "2026-05-01T07:00:00.000Z",
    }
    event = whoop_settings.transform_workout(record)
    assert event["device_type"] == "chest_strap"
    assert event["metrics"]["heart_rate_bpm"] == 145


def test_cycle_maps_average_heart_rate(whoop_settings):
    """WHOOP v2: cycle now exposes average_heart_rate (HRV/RHR moved to recovery)."""
    record = {
        "score": {"average_heart_rate": 66, "max_heart_rate": 192, "strain": 16.5},
        "end": "2026-05-01T07:00:00.000Z",
    }
    event = whoop_settings.transform_cycle(record)
    assert event["metrics"]["heart_rate_bpm"] == 66
    assert "hrv_ms" not in event["metrics"]


def test_transform_records_skips_unmapped(whoop_settings):
    records = [
        {"score": {}, "created_at": "2026-05-01T07:00:00Z"},  # empty → skipped
        {"score": {"average_heart_rate": 50}, "end": "2026-05-01T07:00:00Z"},  # OK
    ]
    out = list(whoop_settings.transform_records(records, "cycle"))
    assert len(out) == 1
    assert out[0]["metrics"]["heart_rate_bpm"] == 50


def test_event_timestamp_is_iso_to_millis(whoop_settings):
    from datetime import datetime, timezone

    record = {
        "score": {"hrv_rmssd_milli": 40.0},
        "created_at": "2026-05-01T07:00:00.000Z",
    }
    event = whoop_settings.transform_recovery(record)
    expected_ms = int(datetime(2026, 5, 1, 7, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert event["event_timestamp"] == expected_ms
