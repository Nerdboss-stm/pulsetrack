"""
AI-assisted clinical anomaly explanations.

For each anomaly row in `vw_anomaly_dashboard`, Claude is given:
  - The metric + value + classification (critical/warning).
  - 30-day baseline (rolling avg + stddev + z-score).
  - Patient context (active conditions, medications, recent adverse events).
  - Device context (firmware, battery, late-arrival flag).

Claude produces a 3-section explanation:
  1. What the reading means clinically.
  2. Why this patient's context makes it more/less concerning.
  3. Recommended action (clinician follow-up vs. data-quality investigation).

NOT medical advice. The output is explicit about that — it's a triage
aid for analytics teams, not a clinical decision support tool.
"""

from __future__ import annotations

import logging
from typing import Optional

from ai.client import Prompt, complete

log = logging.getLogger(__name__)


SYSTEM_PROMPT = """\
You are a clinical informatics analyst helping data engineers triage \
vital-sign anomalies. You're given one reading with patient + device \
context. Produce a Markdown response with three sections.

## Reading interpretation
2-3 sentences. What does this value mean clinically (in plain English, \
no jargon)? Reference the normal range.

## Patient context relevance
2-3 sentences. Does the patient's active conditions / medications / \
recent adverse events make this MORE or LESS concerning? Be specific. \
If context is missing, say so — don't speculate.

## Recommended action
ONE of:
- "Likely data quality issue" — when the device/firmware/battery context \
suggests a sensor problem, not a real physiological event.
- "Worth clinician follow-up" — when the value + context suggest a real \
clinical concern.
- "Within patient-specific baseline" — when the 30-day baseline (avg ± \
stddev) shows this reading isn't unusual FOR THIS PATIENT even if it's \
outside population norms.

Always end with:

> ⚕️ This is an analytics triage aid, NOT medical advice. A real \
clinical decision requires clinician review of the full chart.

Keep total response under 350 words. Be terse, no hedging language."""


def explain_anomaly(
    metric_name: str,
    metric_value: float,
    vital_status: str,
    normal_min: Optional[float] = None,
    normal_max: Optional[float] = None,
    rolling_30d_avg: Optional[float] = None,
    rolling_30d_stddev: Optional[float] = None,
    z_score_30d: Optional[float] = None,
    active_conditions: Optional[list[str]] = None,
    active_medications: Optional[list[str]] = None,
    recent_adverse_event_count: Optional[int] = None,
    health_complexity_bucket: Optional[str] = None,
    firmware_version: Optional[str] = None,
    battery_pct: Optional[int] = None,
    is_late_arriving: Optional[bool] = None,
) -> str:
    """Generate a clinical-context explanation for one anomalous reading.

    All args are explicit — caller passes whatever it has from
    `vw_anomaly_dashboard`. Missing context is fine; Claude will note it.

    Returns:
        Markdown response suitable for Slack or a Snowflake dashboard.
    """
    lines = [
        f"**Metric:** `{metric_name}` = **{metric_value}** "
        f"(status: `{vital_status}`)",
    ]
    if normal_min is not None and normal_max is not None:
        lines.append(f"**Clinical normal range:** {normal_min} – {normal_max}")
    if rolling_30d_avg is not None:
        lines.append(
            f"**Patient 30-day baseline:** avg={rolling_30d_avg:.2f}"
            + (f", σ={rolling_30d_stddev:.2f}" if rolling_30d_stddev else "")
            + (f", z={z_score_30d:.2f}" if z_score_30d is not None else "")
        )
    if health_complexity_bucket:
        lines.append(f"**Health complexity:** {health_complexity_bucket}")
    if active_conditions:
        lines.append(f"**Active conditions:** {', '.join(active_conditions)}")
    if active_medications:
        lines.append(f"**Active medications:** {', '.join(active_medications)}")
    if recent_adverse_event_count is not None:
        lines.append(
            f"**Recent adverse events (90d):** {recent_adverse_event_count}"
        )
    if firmware_version or battery_pct is not None or is_late_arriving is not None:
        lines.append("**Device context:**")
        if firmware_version:
            lines.append(f"  - firmware: `{firmware_version}`")
        if battery_pct is not None:
            lines.append(f"  - battery: {battery_pct}%")
        if is_late_arriving is not None:
            lines.append(f"  - late_arriving: {is_late_arriving}")

    return complete(
        Prompt(
            user="\n".join(lines),
            system=SYSTEM_PROMPT,
            max_tokens=1200,
        )
    ).text


def explain_anomaly_row(row: dict) -> str:
    """Convenience wrapper: take a dict matching the
    ``vw_anomaly_dashboard`` row shape and produce an explanation."""
    return explain_anomaly(
        metric_name=row["metric_name"],
        metric_value=row["metric_value"],
        vital_status=row["vital_status"],
        normal_min=row.get("normal_min"),
        normal_max=row.get("normal_max"),
        rolling_30d_avg=row.get("rolling_30d_avg"),
        rolling_30d_stddev=row.get("rolling_30d_stddev"),
        z_score_30d=row.get("z_score_30d"),
        active_conditions=row.get("active_conditions"),
        active_medications=row.get("active_medications"),
        recent_adverse_event_count=row.get("adverse_event_count"),
        health_complexity_bucket=row.get("health_complexity_bucket"),
        firmware_version=row.get("firmware_version"),
        battery_pct=row.get("battery_pct"),
        is_late_arriving=row.get("is_late_arriving"),
    )
