"""
Notification tasks — Slack, SNS, email.

Each notification task is best-effort: failures here NEVER block the
flow. The flow's actual success/failure is independent of whether the
operator got the notification.
"""

from __future__ import annotations

import json
import os
from typing import Optional

from prefect import get_run_logger, task

SLACK_WEBHOOK_URL = os.environ.get("PT_SLACK_WEBHOOK_URL", "")
SNS_ALERT_TOPIC_ARN = os.environ.get(
    "PT_SNS_ALERT_TOPIC_ARN",
    "arn:aws:sns:us-east-1:960341592614:pulsetrack-dev-alerts",
)


@task(name="notify_slack", retries=2, tags=["notification"])
def notify_slack(
    message: str,
    channel: Optional[str] = None,
    severity: str = "info",
) -> bool:
    """Post a message to Slack via webhook. Returns True on success.

    Severity drives the message color:
      - info: gray
      - warn: yellow
      - error: red

    Failures are logged but don't raise — flow continues.
    """
    log = get_run_logger()

    if not SLACK_WEBHOOK_URL:
        log.info(f"[no-slack] {severity}: {message}")
        return False

    color = {"info": "#36a64f", "warn": "#ffaa00", "error": "#ff0000"}.get(
        severity, "#888888"
    )

    payload = {
        "attachments": [
            {
                "color": color,
                "title": f"PulseTrack {severity.upper()}",
                "text": message,
                "fields": [
                    {"title": "Source", "value": "Prefect", "short": True},
                    {"title": "Severity", "value": severity, "short": True},
                ],
            }
        ]
    }
    if channel:
        payload["channel"] = channel

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
        log.warning(f"slack-notify failed: {exc}")
        return False


@task(name="notify_sns", retries=2, tags=["notification"])
def notify_sns(
    subject: str,
    message: str,
    topic_arn: str = SNS_ALERT_TOPIC_ARN,
) -> bool:
    """Publish to SNS — email subscribers get notified."""
    log = get_run_logger()
    try:
        import boto3

        sns = boto3.client("sns")
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject[:99],  # SNS subject hard limit
            Message=message,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(f"sns-notify failed: {exc}")
        return False


@task(name="notify_pipeline_complete", tags=["notification"])
def notify_pipeline_complete(
    pipeline_name: str,
    duration_seconds: float,
    metrics: Optional[dict] = None,
) -> None:
    """Composite notification: log + slack on flow completion."""
    log = get_run_logger()

    metric_lines = ""
    if metrics:
        metric_lines = "\n" + "\n".join(f"  • {k}: {v}" for k, v in metrics.items())

    message = (
        f"`{pipeline_name}` completed in {duration_seconds:.0f}s"
        + metric_lines
    )
    log.info(message)
    notify_slack.fn(message=message, severity="info")


@task(name="notify_pipeline_failure", tags=["notification"])
def notify_pipeline_failure(
    pipeline_name: str,
    error: str,
    failed_task: Optional[str] = None,
) -> None:
    """Composite notification: log + slack + SNS on flow failure."""
    log = get_run_logger()
    subject = f"PulseTrack pipeline FAILED: {pipeline_name}"
    message = (
        f"Pipeline `{pipeline_name}` failed.\n"
        + (f"Failed task: `{failed_task}`\n" if failed_task else "")
        + f"Error: {error[:1000]}"
    )
    log.error(message)
    notify_slack.fn(message=message, severity="error")
    notify_sns.fn(subject=subject, message=message)
