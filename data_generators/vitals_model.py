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


# ── Markov transition matrix, conditioned on time-of-day bucket ───────────────
def _bucket(hour_float: float) -> str:
    if 0 <= hour_float < 6:    return "night"
    if 6 <= hour_float < 9:    return "morning"
    if 9 <= hour_float < 17:   return "day"
    if 17 <= hour_float < 22:  return "evening"
    return "late_evening"


# Probabilities for transitioning out of `current_state` in each time bucket.
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
