"""
Medically-accurate vital sign simulator — scaffolding.
"""
from __future__ import annotations

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
