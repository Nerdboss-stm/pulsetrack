"""
WHOOP API poll pipeline.

Runs every 15 minutes via deployment ``interval=900``. The poller is a
Python REST client (no Spark), so it runs on the Prefect worker — quick
and cheap.

Flow:
  refresh_token (auth.py auto) → poll_endpoints → emit_to_kafka → notify

Backpressure: if a poll cycle takes longer than the interval, Prefect's
scheduler skips overlapping runs. Each cycle is idempotent (offset file
prevents duplicate publication).
"""

from __future__ import annotations

import time
from typing import Optional

from prefect import flow, get_run_logger

from orchestration.tasks.notification_tasks import (
    notify_pipeline_failure,
    notify_slack,
)
from orchestration.tasks.spark_tasks import run_whoop_poller


@flow(name="whoop-poll", log_prints=True, retries=1)
def whoop_poll(
    lookback_days: int = 1,
    interval_minutes: int = 15,
    notify_on_failure: bool = True,
) -> dict:
    """Poll WHOOP API for new cycles/recovery/sleep/workout records.

    Args:
        lookback_days: How far back to fetch. 1 day for steady-state;
            larger values for catch-up after a poller outage.
        interval_minutes: Used as the subprocess timeout (poll must
            complete within the cycle).
        notify_on_failure: If True, send Slack + SNS on failure.

    Returns:
        Dict with poll duration and status.
    """
    log = get_run_logger()
    started = time.time()
    log.info(f"whoop-poll starting (lookback={lookback_days}d)")

    try:
        result = run_whoop_poller(
            lookback_days=lookback_days,
            interval_minutes=interval_minutes,
        )

        duration = time.time() - started

        if not result.succeeded:
            if notify_on_failure:
                notify_slack.fn(
                    message=(
                        f"WHOOP poll failed.\n"
                        f"Detail: {result.detail[:500]}"
                    ),
                    severity="warn",
                )
            return {
                "status": "fail",
                "duration_seconds": duration,
                "detail": result.detail,
            }

        log.info(f"whoop-poll ok ({duration:.0f}s)")
        return {
            "status": "ok",
            "duration_seconds": duration,
            "detail": result.detail,
        }

    except Exception as exc:
        duration = time.time() - started
        if notify_on_failure:
            notify_pipeline_failure(
                pipeline_name="whoop-poll",
                error=str(exc)[:1000],
            )
        raise


if __name__ == "__main__":
    whoop_poll()
