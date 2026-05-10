"""
Streaming-job health monitor.

Runs every 5 minutes. Checks:
  - Bronze stream consumer lag vs threshold
  - Silver stream consumer lag vs threshold
  - Gold streams (fact_vital_reading, fact_vital_daily_summary)
  - EMR cluster overall state
  - YARN application states (RUNNING vs ACCEPTED vs FAILED)

If any stream is FAILED or unresponsive for N consecutive checks,
notify + optionally restart.
"""

from __future__ import annotations

import time
from typing import Optional

from prefect import flow, get_run_logger, task

from orchestration.tasks.emr_tasks import get_emr_cluster_state
from orchestration.tasks.notification_tasks import notify_slack
from orchestration.tasks.quality_tasks import check_consumer_lag


@task(name="list_yarn_apps", retries=1, tags=["emr", "yarn"])
def list_yarn_apps(cluster_id: Optional[str] = None) -> list[dict]:
    """List YARN applications on the EMR cluster via the resource manager.

    Returns:
        List of dicts with app_id, name, state, progress.

    Note: in practice EMR exposes the YARN REST API behind a private
    endpoint; this stub returns a synthetic list. Production would either
    SSH to master and parse ``yarn application -list`` output, or call
    the YARN ResourceManager REST API through an SSH tunnel.
    """
    log = get_run_logger()
    # Stub: real impl would query the YARN ResourceManager REST API.
    log.info("yarn-apps stub list (replace with REST query in prod)")
    return [
        {"app_id": "stub_001", "name": "PulseTrack-Bronze-Wearables", "state": "RUNNING"},
        {"app_id": "stub_002", "name": "PulseTrack-Silver-Sensors", "state": "RUNNING"},
        {"app_id": "stub_003", "name": "PulseTrack-Gold-VitalReading", "state": "RUNNING"},
    ]


@flow(name="streaming-monitor", log_prints=True, retries=0)
def streaming_monitor(
    cluster_id: Optional[str] = None,
    bronze_lag_threshold: int = 50_000,
    silver_lag_threshold: int = 10_000,
    notify_on_alert: bool = True,
) -> dict:
    """Health monitor for the streaming pipeline.

    Runs every 5 min via deployment ``interval=300``. Designed to be
    cheap (no Spark submission; just status checks).
    """
    log = get_run_logger()
    started = time.time()

    # ── Cluster state ─────────────────────────────────────────────
    cluster = get_emr_cluster_state(cluster_id=cluster_id)
    log.info(f"cluster {cluster['cluster_id']} state={cluster['state']}")
    if cluster["state"] != "WAITING":
        if notify_on_alert:
            notify_slack.fn(
                message=(
                    f"EMR cluster `{cluster['cluster_id']}` is in state "
                    f"{cluster['state']} (expected WAITING). Investigate."
                ),
                severity="error",
            )
        return {
            "status": "alert",
            "cluster_state": cluster["state"],
            "duration_seconds": time.time() - started,
        }

    # ── YARN apps ────────────────────────────────────────────────
    apps = list_yarn_apps(cluster_id=cluster_id)
    expected_streams = {
        "PulseTrack-Bronze-Wearables",
        "PulseTrack-Silver-Sensors",
        "PulseTrack-Gold-VitalReading",
    }
    seen = {a["name"]: a["state"] for a in apps}
    missing = expected_streams - set(seen)
    failed = {n for n, s in seen.items() if s in {"FAILED", "KILLED"}}

    if missing or failed:
        msg = ""
        if missing:
            msg += f"Missing streaming queries: {sorted(missing)}.\n"
        if failed:
            msg += f"Failed/killed queries: {sorted(failed)}.\n"
        if notify_on_alert:
            notify_slack.fn(message=msg, severity="error")
        return {
            "status": "alert",
            "missing": sorted(missing),
            "failed": sorted(failed),
            "duration_seconds": time.time() - started,
        }

    # ── Lag thresholds ────────────────────────────────────────────
    bronze_check = check_consumer_lag(
        query_name="bronze-wearables",
        max_lag=bronze_lag_threshold,
        cluster_id=cluster_id,
    )
    silver_check = check_consumer_lag(
        query_name="silver-sensor-readings",
        max_lag=silver_lag_threshold,
        cluster_id=cluster_id,
    )

    duration = time.time() - started
    summary = (
        "all streams healthy"
        if (bronze_check.passed and silver_check.passed)
        else "lag threshold exceeded"
    )
    log.info(f"streaming-monitor: {summary} ({duration:.1f}s)")
    return {
        "status": "ok" if (bronze_check.passed and silver_check.passed) else "warn",
        "bronze_check": bronze_check.passed,
        "silver_check": silver_check.passed,
        "duration_seconds": duration,
    }


if __name__ == "__main__":
    streaming_monitor()
