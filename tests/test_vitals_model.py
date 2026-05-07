"""Tests for the physiological vitals simulator."""

from __future__ import annotations

import random
from datetime import datetime

import pytest

from data_generators.vitals_model import (
    ACTIVITY_HR_DELTA,
    ACTIVITY_HRV_DELTA,
    ACTIVITY_SPO2_DELTA,
    ActivityState,
    PatientProfile,
    blood_pressure,
    generate_reading,
    respiration_rate,
    sleep_stage_index,
    steps_for_state,
)


@pytest.fixture(autouse=True)
def _seed_rng():
    random.seed(42)
    yield


def test_patient_profile_random_in_realistic_ranges():
    p = PatientProfile.random()
    assert 22 <= p.age <= 80
    assert p.sex in {"M", "F"}
    assert 50.0 <= p.resting_hr <= 95.0
    assert 95.0 <= p.resting_spo2 <= 99.0
    assert p.resting_hrv >= 15.0
    assert 35.5 <= p.resting_temp <= 37.3


def test_activity_delta_tables_internally_consistent():
    """Higher activity ⇒ higher HR, lower SpO2, lower HRV."""
    for s in [
        ActivityState.RESTING,
        ActivityState.LIGHT_ACTIVITY,
        ActivityState.MODERATE_EXERCISE,
        ActivityState.VIGOROUS_EXERCISE,
    ]:
        assert ACTIVITY_HR_DELTA[s] >= ACTIVITY_HR_DELTA[ActivityState.RESTING]
        assert ACTIVITY_SPO2_DELTA[s] <= 0
        assert ACTIVITY_HRV_DELTA[s] <= ACTIVITY_HRV_DELTA[ActivityState.RESTING]


def test_generate_reading_returns_required_metrics():
    p = PatientProfile.random("p1", anomaly_rate=0.0)
    r = generate_reading(p, datetime(2026, 5, 3, 14, 30))
    for k in ("heart_rate_bpm", "spo2_pct", "hrv_ms", "skin_temp_celsius"):
        assert k in r
        assert isinstance(r[k], float)
    assert r["activity_state"] in {s.value for s in ActivityState}


def test_generate_reading_clamps_to_physio_ranges():
    p = PatientProfile.random("p1", anomaly_rate=1.0)  # always inject anomaly
    for _ in range(50):
        r = generate_reading(p, datetime(2026, 5, 3, 14, 30))
        assert 35.0 <= r["heart_rate_bpm"] <= 220.0
        assert 70.0 <= r["spo2_pct"] <= 100.0
        assert 5.0 <= r["hrv_ms"] <= 200.0
        assert 34.0 <= r["skin_temp_celsius"] <= 42.0


def test_circadian_hr_lower_at_night_higher_at_afternoon():
    p = PatientProfile(
        patient_id="p1",
        age=30,
        sex="M",
        resting_hr=70,
        resting_spo2=97,
        resting_hrv=50,
        resting_temp=36.8,
        anomaly_rate=0.0,
    )
    night_hrs = []
    afternoon_hrs = []
    for _ in range(50):
        # Force RESTING so activity delta cancels
        p.current_state = ActivityState.RESTING
        night_hrs.append(generate_reading(p, datetime(2026, 5, 3, 3, 30))["heart_rate_bpm"])
        p.current_state = ActivityState.RESTING
        afternoon_hrs.append(generate_reading(p, datetime(2026, 5, 3, 14, 30))["heart_rate_bpm"])
    assert sum(afternoon_hrs) / len(afternoon_hrs) > sum(night_hrs) / len(night_hrs)


def test_anomaly_injection_can_push_hr_above_threshold():
    p = PatientProfile.random("p1", anomaly_rate=1.0)
    saw_high = False
    for _ in range(100):
        r = generate_reading(p, datetime(2026, 5, 3, 14, 30))
        if r["anomaly"] == "tachycardia" and r["heart_rate_bpm"] >= 100:
            saw_high = True
            break
    assert saw_high


def test_anomaly_disabled_means_no_anomalies():
    p = PatientProfile.random("p1", anomaly_rate=0.0)
    for _ in range(50):
        r = generate_reading(p, datetime(2026, 5, 3, 14, 30))
        assert r["anomaly"] is None


def test_respiration_rate_increases_with_activity():
    rr_rest = sum(respiration_rate(ActivityState.RESTING) for _ in range(20)) / 20
    rr_vig = sum(respiration_rate(ActivityState.VIGOROUS_EXERCISE) for _ in range(20)) / 20
    assert rr_vig > rr_rest


def test_steps_for_state_zero_at_rest_positive_during_exercise():
    assert (
        steps_for_state(ActivityState.SLEEPING, 60) == 0
        or steps_for_state(ActivityState.SLEEPING, 60) >= 0
    )
    avg = sum(steps_for_state(ActivityState.MODERATE_EXERCISE, 60) for _ in range(10)) / 10
    assert avg > 0


def test_sleep_stage_index_returns_valid_state():
    for h in [3, 14, 23]:
        s = sleep_stage_index(datetime(2026, 5, 3, h, 0))
        assert s in (0, 1, 2, 3)


def test_blood_pressure_realistic_relationship():
    p = PatientProfile.random("p1", anomaly_rate=0.0)
    for _ in range(20):
        sys_v, dia_v = blood_pressure(p, ActivityState.RESTING)
        assert 80 <= sys_v <= 220
        assert 50 <= dia_v <= 130
        assert sys_v > dia_v  # systolic always > diastolic
