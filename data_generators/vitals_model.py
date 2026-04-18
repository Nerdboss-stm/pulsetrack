"""
Medically-accurate vital sign simulator.

Each simulated patient has:
- A BASELINE (resting HR, SpO2, HRV, temp) drawn from age/sex-adjusted distributions
- CIRCADIAN RHYTHM: HR lowest at 3-4 AM, peaks at 2-4 PM
- ACTIVITY STATES: resting, light_activity, moderate_exercise, vigorous_exercise, sleeping
- State transitions modeled as a Markov chain with time-of-day-dependent probabilities
- NOISE: Gaussian noise on each reading (sensor measurement error)
- ANOMALY INJECTION: configurable rate of clinically-meaningful anomalies
  (tachycardia episodes, SpO2 desaturation, fever spikes)

This produces data that LOOKS real to a clinician reviewing dashboards,
not random noise that any domain expert would spot as fake in 2 seconds.
"""
from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class ActivityState(str, Enum):
    SLEEPING = "sleeping"
    RESTING = "resting"
    LIGHT_ACTIVITY = "light_activity"
    MODERATE_EXERCISE = "moderate_exercise"
    VIGOROUS_EXERCISE = "vigorous_exercise"


# ── Activity-driven physiological deltas (above resting baseline) ─────────────
ACTIVITY_HR_DELTA: dict[ActivityState, float] = {
    ActivityState.SLEEPING:           -10.0,
    ActivityState.RESTING:              0.0,
    ActivityState.LIGHT_ACTIVITY:      20.0,
    ActivityState.MODERATE_EXERCISE:   50.0,
    ActivityState.VIGOROUS_EXERCISE:   90.0,
}

ACTIVITY_SPO2_DELTA: dict[ActivityState, float] = {
    ActivityState.SLEEPING:             0.0,
    ActivityState.RESTING:              0.0,
    ActivityState.LIGHT_ACTIVITY:      -0.3,
    ActivityState.MODERATE_EXERCISE:   -1.0,
    ActivityState.VIGOROUS_EXERCISE:   -2.0,
}

# HRV is *higher* at rest, suppressed during exercise
ACTIVITY_HRV_DELTA: dict[ActivityState, float] = {
    ActivityState.SLEEPING:            10.0,
    ActivityState.RESTING:              0.0,
    ActivityState.LIGHT_ACTIVITY:     -10.0,
    ActivityState.MODERATE_EXERCISE:  -25.0,
    ActivityState.VIGOROUS_EXERCISE:  -35.0,
}

ACTIVITY_TEMP_DELTA: dict[ActivityState, float] = {
    ActivityState.SLEEPING:            -0.3,
    ActivityState.RESTING:              0.0,
    ActivityState.LIGHT_ACTIVITY:       0.1,
    ActivityState.MODERATE_EXERCISE:    0.3,
    ActivityState.VIGOROUS_EXERCISE:    0.6,
}

ACTIVITY_RR_DELTA: dict[ActivityState, float] = {
    ActivityState.SLEEPING:            -2.0,
    ActivityState.RESTING:              0.0,
    ActivityState.LIGHT_ACTIVITY:       4.0,
    ActivityState.MODERATE_EXERCISE:   10.0,
    ActivityState.VIGOROUS_EXERCISE:   18.0,
}

ACTIVITY_BP_SYS_DELTA: dict[ActivityState, float] = {
    ActivityState.SLEEPING:            -8.0,
    ActivityState.RESTING:              0.0,
    ActivityState.LIGHT_ACTIVITY:       8.0,
    ActivityState.MODERATE_EXERCISE:   25.0,
    ActivityState.VIGOROUS_EXERCISE:   45.0,
}

# Steps emitted per second per activity state (stochastic)
ACTIVITY_STEPS_PER_SEC: dict[ActivityState, float] = {
    ActivityState.SLEEPING:             0.00,
    ActivityState.RESTING:              0.05,
    ActivityState.LIGHT_ACTIVITY:       1.50,
    ActivityState.MODERATE_EXERCISE:    2.50,
    ActivityState.VIGOROUS_EXERCISE:    3.00,
}


# ── Markov transition matrix, conditioned on time-of-day bucket ───────────────
def _bucket(hour_float: float) -> str:
    if 0 <= hour_float < 6:    return "night"
    if 6 <= hour_float < 9:    return "morning"
    if 9 <= hour_float < 17:   return "day"
    if 17 <= hour_float < 22:  return "evening"
    return "late_evening"


# Probabilities for transitioning out of `current_state` in each time bucket.
# Each row sums to 1 (within float tolerance).
TRANSITIONS: dict[str, dict[ActivityState, dict[ActivityState, float]]] = {
    "night": {
        ActivityState.SLEEPING:          {ActivityState.SLEEPING: 0.97, ActivityState.RESTING: 0.03},
        ActivityState.RESTING:           {ActivityState.SLEEPING: 0.60, ActivityState.RESTING: 0.40},
        ActivityState.LIGHT_ACTIVITY:    {ActivityState.RESTING: 0.60, ActivityState.LIGHT_ACTIVITY: 0.40},
        ActivityState.MODERATE_EXERCISE: {ActivityState.RESTING: 0.50, ActivityState.LIGHT_ACTIVITY: 0.40, ActivityState.MODERATE_EXERCISE: 0.10},
        ActivityState.VIGOROUS_EXERCISE: {ActivityState.RESTING: 0.60, ActivityState.LIGHT_ACTIVITY: 0.40},
    },
    "morning": {
        ActivityState.SLEEPING:          {ActivityState.SLEEPING: 0.55, ActivityState.RESTING: 0.45},
        ActivityState.RESTING:           {ActivityState.RESTING: 0.55, ActivityState.LIGHT_ACTIVITY: 0.35, ActivityState.MODERATE_EXERCISE: 0.10},
        ActivityState.LIGHT_ACTIVITY:    {ActivityState.LIGHT_ACTIVITY: 0.55, ActivityState.RESTING: 0.30, ActivityState.MODERATE_EXERCISE: 0.15},
        ActivityState.MODERATE_EXERCISE: {ActivityState.MODERATE_EXERCISE: 0.50, ActivityState.LIGHT_ACTIVITY: 0.40, ActivityState.VIGOROUS_EXERCISE: 0.10},
        ActivityState.VIGOROUS_EXERCISE: {ActivityState.VIGOROUS_EXERCISE: 0.40, ActivityState.MODERATE_EXERCISE: 0.40, ActivityState.LIGHT_ACTIVITY: 0.20},
    },
    "day": {
        ActivityState.SLEEPING:          {ActivityState.RESTING: 0.70, ActivityState.SLEEPING: 0.30},
        ActivityState.RESTING:           {ActivityState.RESTING: 0.50, ActivityState.LIGHT_ACTIVITY: 0.40, ActivityState.MODERATE_EXERCISE: 0.10},
        ActivityState.LIGHT_ACTIVITY:    {ActivityState.LIGHT_ACTIVITY: 0.55, ActivityState.RESTING: 0.30, ActivityState.MODERATE_EXERCISE: 0.15},
        ActivityState.MODERATE_EXERCISE: {ActivityState.MODERATE_EXERCISE: 0.55, ActivityState.LIGHT_ACTIVITY: 0.35, ActivityState.VIGOROUS_EXERCISE: 0.10},
        ActivityState.VIGOROUS_EXERCISE: {ActivityState.VIGOROUS_EXERCISE: 0.50, ActivityState.MODERATE_EXERCISE: 0.40, ActivityState.LIGHT_ACTIVITY: 0.10},
    },
    "evening": {
        ActivityState.SLEEPING:          {ActivityState.RESTING: 0.70, ActivityState.SLEEPING: 0.30},
        ActivityState.RESTING:           {ActivityState.RESTING: 0.55, ActivityState.LIGHT_ACTIVITY: 0.35, ActivityState.MODERATE_EXERCISE: 0.10},
        ActivityState.LIGHT_ACTIVITY:    {ActivityState.LIGHT_ACTIVITY: 0.50, ActivityState.RESTING: 0.40, ActivityState.MODERATE_EXERCISE: 0.10},
        ActivityState.MODERATE_EXERCISE: {ActivityState.MODERATE_EXERCISE: 0.50, ActivityState.LIGHT_ACTIVITY: 0.40, ActivityState.VIGOROUS_EXERCISE: 0.10},
        ActivityState.VIGOROUS_EXERCISE: {ActivityState.VIGOROUS_EXERCISE: 0.40, ActivityState.MODERATE_EXERCISE: 0.40, ActivityState.LIGHT_ACTIVITY: 0.20},
    },
    "late_evening": {
        ActivityState.SLEEPING:          {ActivityState.SLEEPING: 0.95, ActivityState.RESTING: 0.05},
        ActivityState.RESTING:           {ActivityState.RESTING: 0.65, ActivityState.SLEEPING: 0.35},
        ActivityState.LIGHT_ACTIVITY:    {ActivityState.RESTING: 0.60, ActivityState.LIGHT_ACTIVITY: 0.30, ActivityState.SLEEPING: 0.10},
        ActivityState.MODERATE_EXERCISE: {ActivityState.LIGHT_ACTIVITY: 0.50, ActivityState.RESTING: 0.40, ActivityState.MODERATE_EXERCISE: 0.10},
        ActivityState.VIGOROUS_EXERCISE: {ActivityState.LIGHT_ACTIVITY: 0.60, ActivityState.RESTING: 0.40},
    },
}


# ── PatientProfile ─────────────────────────────────────────────────────────────
@dataclass
class PatientProfile:
    """Per-patient physiology + ongoing activity state."""
    patient_id: str
    age: int
    sex: str                                  # "M" or "F"
    resting_hr: float                         # bpm
    resting_spo2: float                       # %
    resting_hrv: float                        # ms (RMSSD-like)
    resting_temp: float                       # °C, skin/oral
    anomaly_rate: float = 0.005               # per-reading anomaly probability
    current_state: ActivityState = ActivityState.RESTING
    _last_anomaly: str | None = field(default=None, repr=False)

    @classmethod
    def random(
        cls,
        patient_id: str | None = None,
        anomaly_rate: float = 0.005,
    ) -> "PatientProfile":
        pid = patient_id or str(uuid.uuid4())
        sex = random.choice(["M", "F"])
        age = random.randint(22, 80)

        # Resting HR: higher in women on average; small drift with age
        sex_offset = 2.0 if sex == "F" else 0.0
        age_offset = (age - 30) * 0.1
        resting_hr = max(50.0, min(95.0, random.gauss(72.0 + sex_offset + age_offset, 7.0)))

        # SpO2: slight decline above ~60
        spo2_age_pen = max(0, age - 60) * 0.05
        resting_spo2 = max(95.0, min(99.0, random.gauss(97.5 - spo2_age_pen, 0.7)))

        # HRV: significant decline with age
        hrv_age_offset = -(age - 30) * 0.4
        resting_hrv = max(15.0, random.gauss(45.0 + hrv_age_offset, 10.0))

        # Skin/oral temp baseline
        resting_temp = max(35.5, min(37.3, random.gauss(36.8, 0.25)))

        return cls(
            patient_id=pid,
            age=age,
            sex=sex,
            resting_hr=resting_hr,
            resting_spo2=resting_spo2,
            resting_hrv=resting_hrv,
            resting_temp=resting_temp,
            anomaly_rate=anomaly_rate,
            current_state=ActivityState.RESTING,
        )


# ── Internal helpers ───────────────────────────────────────────────────────────
def _next_state(current: ActivityState, ts: datetime) -> ActivityState:
    bucket = _bucket(ts.hour + ts.minute / 60.0)
    dist = TRANSITIONS[bucket][current]
    states = list(dist.keys())
    weights = list(dist.values())
    return random.choices(states, weights=weights, k=1)[0]


def _circadian_hr(hour_float: float) -> float:
    """Sinusoidal: trough ~03:30, peak ~14:30. Range ±8 bpm."""
    return -8.0 * math.cos(2 * math.pi * (hour_float - 3.5) / 24.0)


def _circadian_temp(hour_float: float) -> float:
    """Diurnal body temp: low ~04:00, peak ~16:00. Range ±0.3 °C."""
    return -0.3 * math.cos(2 * math.pi * (hour_float - 4.0) / 24.0)


# ── Public API ─────────────────────────────────────────────────────────────────
def generate_reading(profile: PatientProfile, timestamp: datetime) -> dict:
    """
    Generate one physiologically-plausible reading.

    Mutates `profile.current_state` (Markov step). Returned dict is a flat
    metric map plus the chosen `activity_state` symbol so callers can reuse it.
    """
    profile.current_state = _next_state(profile.current_state, timestamp)
    state = profile.current_state
    hour = timestamp.hour + timestamp.minute / 60.0

    hr = (
        profile.resting_hr
        + _circadian_hr(hour)
        + ACTIVITY_HR_DELTA[state]
        + random.gauss(0.0, 2.0)
    )
    spo2 = (
        profile.resting_spo2
        + ACTIVITY_SPO2_DELTA[state]
        + random.gauss(0.0, 0.3)
    )
    hrv = (
        profile.resting_hrv
        + ACTIVITY_HRV_DELTA[state]
        + random.gauss(0.0, 4.0)
    )
    temp = (
        profile.resting_temp
        + _circadian_temp(hour)
        + ACTIVITY_TEMP_DELTA[state]
        + random.gauss(0.0, 0.05)
    )

    # ── Clinically-meaningful anomaly injection ──────────────────────────
    anomaly: str | None = None
    if random.random() < profile.anomaly_rate:
        anomaly = random.choices(
            ["tachycardia", "desaturation", "fever"],
            weights=[0.5, 0.3, 0.2],
            k=1,
        )[0]
        if anomaly == "tachycardia":
            hr += random.uniform(35.0, 60.0)
        elif anomaly == "desaturation":
            spo2 = random.uniform(85.0, 91.0)
        else:  # fever
            temp += random.uniform(1.0, 2.0)
    profile._last_anomaly = anomaly

    # Clamp to physiologically-possible ranges
    hr   = max(35.0,  min(220.0, hr))
    spo2 = max(70.0,  min(100.0, spo2))
    hrv  = max(5.0,   min(200.0, hrv))
    temp = max(34.0,  min(42.0,  temp))

    return {
        "heart_rate_bpm":    round(hr, 1),
        "spo2_pct":          round(spo2, 1),
        "hrv_ms":            round(hrv, 1),
        "skin_temp_celsius": round(temp, 2),
        "activity_state":    state.value,
        "anomaly":           anomaly,
    }


def respiration_rate(state: ActivityState, baseline: float = 14.0) -> int:
    """Breaths per minute. Activity-driven with Gaussian jitter."""
    rr = baseline + ACTIVITY_RR_DELTA[state] + random.gauss(0.0, 1.5)
    return int(max(8, min(40, round(rr))))


def steps_for_state(state: ActivityState, interval_seconds: float) -> int:
    """Approximate steps accumulated over `interval_seconds`."""
    rate = ACTIVITY_STEPS_PER_SEC[state]
    return max(0, int(rate * interval_seconds + random.uniform(0.0, 5.0)))


def sleep_stage_index(timestamp: datetime) -> int:
    """0=awake, 1=light, 2=deep, 3=rem. Encoded as int so it fits Avro double map."""
    hour = timestamp.hour
    if hour < 5:
        weights = [0.05, 0.25, 0.50, 0.20]
    elif hour < 7:
        weights = [0.15, 0.45, 0.20, 0.20]
    elif 22 <= hour < 24:
        weights = [0.30, 0.55, 0.10, 0.05]
    else:
        weights = [0.95, 0.04, 0.005, 0.005]
    return random.choices([0, 1, 2, 3], weights=weights, k=1)[0]


def blood_pressure(profile: PatientProfile, state: ActivityState) -> tuple[int, int]:
    """Returns (systolic, diastolic) in mmHg for cuff readings."""
    sys_baseline = 118.0 + max(0, profile.age - 30) * 0.4
    sys_v = sys_baseline + ACTIVITY_BP_SYS_DELTA[state] + random.gauss(0.0, 4.0)
    dia_v = sys_v * 0.65 + random.gauss(0.0, 3.0)
    return (
        int(max(80, min(220, round(sys_v)))),
        int(max(50, min(130, round(dia_v)))),
    )
