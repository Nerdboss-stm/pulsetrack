"""
Alert routing for observability findings.

Severity levels:
  - 'critical' → PagerDuty (page on-call)  + Slack + log
  - 'warn'     → Slack + log
  - 'info'     → log only

Each MonitorResult's status maps to severity:
  - status == 'error' → 'critical'
  - status == 'warn'  → 'warn'
  - status == 'ok'    → 'info' (logged at debug only)

All notify_* tasks are best-effort — they NEVER raise on transport
failure. A Slack outage doesn't block the next monitor run.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

from observability.monitors import MonitorResult

log = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.environ.get("PT_SLACK_WEBHOOK_URL", "")
SNS_ALERT_TOPIC_ARN = os.environ.get(
    "PT_SNS_ALERT_TOPIC_ARN",
    "arn:aws:sns:us-east-1:960341592614:pulsetrack-dev-alerts",
)
PAGERDUTY_ROUTING_KEY = os.environ.get("PT_PAGERDUTY_ROUTING_KEY", "")

SEVERITY_FROM_STATUS = {
    "error": "critical",
    "warn": "warn",
    "ok": "info",
}


def severity_for(result: MonitorResult) -> str:
    return SEVERITY_FROM_STATUS.get(result.status, "info")


def _color(severity: str) -> str:
    return {
        "critical": "#ff0000",
        "warn":     "#ffaa00",
        "info":     "#36a64f",
    }.get(severity, "#888888")


def _format_result(result: MonitorResult) -> str:
    lines = [
        f"*{result.monitor_name}* — `{result.table_name}`",
        f"  status: *{result.status.upper()}*",
        f"  check : {result.check_type}",
    ]
    if result.column:
        lines.append(f"  column: `{result.column}`")
    if result.value is not None:
        lines.append(f"  value : {result.value:.4g}")
    if result.threshold is not None:
        lines.append(f"  thresh: {result.threshold:.4g}")
    if result.detail:
        lines.append(f"  detail: {result.detail}")
    return "\n".join(lines)


def alert_slack(result: MonitorResult) -> bool:
    """Post the monitor finding to Slack via webhook.

    Returns:
        True on successful POST; False if webhook not configured or POST fails.
    """
    if not SLACK_WEBHOOK_URL:
        log.info("[no-slack] %s — %s", result.status.upper(), _format_result(result))
        return False

    sev = severity_for(result)
    text = _format_result(result)
    payload = {
        "attachments": [
            {
                "color": _color(sev),
                "title": f"PulseTrack data-quality {sev.upper()}",
                "text": text,
                "mrkdwn_in": ["text"],
            }
        ]
    }

    try:
        import urllib.request

        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5).read()
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("slack alert failed: %s", exc)
        return False


def alert_pagerduty(result: MonitorResult, dedup_key: Optional[str] = None) -> bool:
    """Trigger a PagerDuty incident via Events API v2.

    Only fires for ``status == 'error'`` (critical). Other statuses
    return False without contacting PagerDuty.
    """
    if result.status != "error":
        return False
    if not PAGERDUTY_ROUTING_KEY:
        log.warning("pagerduty alert suppressed — PT_PAGERDUTY_ROUTING_KEY not set")
        return False

    # Dedup key: same monitor+table → coalesce repeated alerts.
    dedup_key = dedup_key or f"{result.monitor_name}:{result.table_name}:{result.column or ''}"

    payload = {
        "routing_key": PAGERDUTY_ROUTING_KEY,
        "event_action": "trigger",
        "dedup_key": dedup_key,
        "payload": {
            "summary": (
                f"PulseTrack data-quality CRITICAL: "
                f"{result.monitor_name} on {result.table_name}"
            ),
            "source": "PulseTrack observability",
            "severity": "critical",
            "component": result.table_name,
            "group": "data-platform",
            "class": result.check_type,
            "custom_details": {
                "monitor_name": result.monitor_name,
                "check_type": result.check_type,
                "column": result.column,
                "value": result.value,
                "threshold": result.threshold,
                "detail": result.detail,
            },
        },
    }

    try:
        import urllib.request

        req = urllib.request.Request(
            "https://events.pagerduty.com/v2/enqueue",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5).read()
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("pagerduty alert failed: %s", exc)
        return False


def alert_sns(result: MonitorResult) -> bool:
    """Publish to SNS — email subscribers get the alert."""
    if result.status not in ("error", "warn"):
        return False

    try:
        import boto3

        sns = boto3.client("sns")
        sns.publish(
            TopicArn=SNS_ALERT_TOPIC_ARN,
            Subject=(
                f"PulseTrack {result.status.upper()}: "
                f"{result.monitor_name} on {result.table_name}"
            )[:99],
            Message=_format_result(result),
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("sns alert failed: %s", exc)
        return False


def route_alerts(results: list[MonitorResult]) -> dict:
    """Route every non-OK result to the appropriate channel(s).

    Returns:
        Dict with per-channel send counts for visibility.
    """
    sent = {"slack": 0, "sns": 0, "pagerduty": 0, "skipped": 0}
    for r in results:
        if r.status == "ok":
            sent["skipped"] += 1
            continue
        if alert_slack(r):
            sent["slack"] += 1
        if alert_sns(r):
            sent["sns"] += 1
        if r.status == "error" and alert_pagerduty(r):
            sent["pagerduty"] += 1
    log.info("alerts routed: %s", sent)
    return sent
