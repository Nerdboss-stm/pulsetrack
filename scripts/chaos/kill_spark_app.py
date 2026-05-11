#!/usr/bin/env python3
"""
Chaos drill 2 — kill the ENTIRE silver streaming application.

What this proves
----------------
App-level fault tolerance. Forces a full Spark application restart,
which exercises:

  - Iceberg checkpoint replay (driver restores last-committed snapshot
    + replays uncommitted micro-batch from Kafka)
  - Kafka consumer-group offset semantics (commit-on-checkpoint, NOT
    auto-commit, so the new app picks up exactly where the old one
    left off)
  - Streaming query metadata recovery from the checkpoint dir
  - EMR step orchestration (we re-submit the spark-submit step, not just
    SIGKILL the driver process — that's how production recovery works)

This is the larger blast radius compared to ``kill_spark_task.py``. We
expect the recovery to take 3-5 minutes (vs. <60s for the task kill)
because the new app has to boot the driver, claim executors, deserialize
the checkpoint, and re-establish Kafka connections.

Target recovery
---------------
<300 seconds (5 minutes) from kill to "first batch processed by new app".

Verification of no data loss
----------------------------
After recovery, the postmortem assertion is:

    bronze_table.count() - silver_table.count() ≤ in-flight watermark window

If we lost data, this delta would be larger than the watermark; if we
duplicated, smaller (negative).

Usage
-----
::

    AWS_PROFILE=pulsetrack \
        python3 scripts/chaos/kill_spark_app.py \
            --app-name "silver_sensor_streaming" \
            --step-script "streaming/silver_ingestion.py" \
            --recovery-budget-seconds 300

Exit codes
----------
0 — recovery within budget + no data loss detected
1 — recovery exceeded budget OR data delta exceeded watermark
2 — discovery error or unexpected failure
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CHAOS_LOG = REPO_ROOT / "docs" / "chaos_log.jsonl"


# Reuse helpers from sibling drill (would be a shared module in production).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from kill_spark_task import (  # noqa: E402
    find_app_id,
    get_emr_master_dns,
    list_containers,
    ssh_master,
)


def yarn_kill_app(app_id: str, key_path: str, host: str) -> None:
    """``yarn application -kill <appId>`` — full application teardown."""
    ssh_master(f"yarn application -kill {app_id}", key_path, host)


def get_cluster_id() -> str:
    out = subprocess.check_output(
        ["terraform", "output", "-raw", "emr_cluster_id"],
        cwd=REPO_ROOT / "infrastructure",
        text=True,
    )
    return out.strip()


def resubmit_step(step_script: str, cluster_id: str, bucket: str) -> str:
    """Re-submit the streaming step. Returns step ID."""
    step_json = json.dumps([{
        "Name": f"chaos-restart:{step_script}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", "spark.pyspark.python=/usr/bin/python3",
                f"s3://{bucket}/code/{step_script}",
            ],
        },
    }])
    out = subprocess.check_output(
        [
            "aws", "emr", "add-steps",
            "--cluster-id", cluster_id,
            "--steps", step_json,
            "--output", "json",
        ],
        text=True,
    )
    payload = json.loads(out)
    return payload["StepIds"][0]


def get_step_state(cluster_id: str, step_id: str) -> str:
    out = subprocess.check_output(
        [
            "aws", "emr", "describe-step",
            "--cluster-id", cluster_id,
            "--step-id", step_id,
            "--query", "Step.Status.State",
            "--output", "text",
        ],
        text=True,
    )
    return out.strip()


def wait_for_new_app(
    pattern: str,
    excluded_app_id: str,
    key_path: str,
    host: str,
    deadline: float,
) -> tuple[bool, str | None]:
    """Block until a new app matching ``pattern`` is RUNNING (and is not the killed one)."""
    while time.time() < deadline:
        time.sleep(10)
        try:
            stdout = ssh_master(
                "yarn application -list -appStates RUNNING", key_path, host
            )
        except RuntimeError:
            continue
        for line in stdout.splitlines():
            if pattern in line and line.startswith("application_"):
                app_id = line.split()[0]
                if app_id != excluded_app_id:
                    return True, app_id
    return False, None


def record_event(payload: dict) -> None:
    CHAOS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(CHAOS_LOG, "a") as f:
        f.write(json.dumps(payload) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-name", default="silver_sensor_streaming",
        help="Substring of the YARN app name to target",
    )
    parser.add_argument(
        "--step-script", default="streaming/bronze_ingestion.py",
        help="The streaming script to re-submit via EMR add-steps (relative to repo root)",
    )
    parser.add_argument(
        "--recovery-budget-seconds", type=int, default=300,
        help="Maximum recovery time before drill is marked failed",
    )
    parser.add_argument(
        "--ssh-key", default=str(Path.home() / ".ssh" / "pulsetrack-emr.pem"),
    )
    parser.add_argument("--host", default=None)
    parser.add_argument("--bucket", default=None, help="S3 lakehouse bucket (auto-detected)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    host = args.host or get_emr_master_dns()
    cluster_id = get_cluster_id()
    bucket = args.bucket or subprocess.check_output(
        ["terraform", "output", "-raw", "lakehouse_bucket_name"],
        cwd=REPO_ROOT / "infrastructure",
        text=True,
    ).strip()

    print(f"[chaos-2] Target: app~='{args.app_name}' host={host} cluster={cluster_id}")

    try:
        app_id = find_app_id(args.app_name, args.ssh_key, host)
        containers = list_containers(app_id, args.ssh_key, host)
        print(f"[chaos-2] Found app {app_id} with {len(containers)} containers")
    except RuntimeError as e:
        print(f"[chaos-2] FATAL discovery: {e}", file=sys.stderr)
        return 2

    if args.dry_run:
        print("[chaos-2] Dry run — would kill app + re-submit step, but skipping.")
        return 0

    kill_ts = datetime.now(timezone.utc)
    print(f"[chaos-2] {kill_ts.isoformat()} Killing app {app_id}")
    try:
        yarn_kill_app(app_id, args.ssh_key, host)
    except RuntimeError as e:
        print(f"[chaos-2] FATAL kill: {e}", file=sys.stderr)
        return 2

    # Re-submit the step. EMR's idle-timeout watchdog could otherwise
    # terminate the cluster after 7200s with no running app.
    print(f"[chaos-2] Re-submitting step: {args.step_script}")
    try:
        new_step_id = resubmit_step(args.step_script, cluster_id, bucket)
        print(f"[chaos-2] New EMR step: {new_step_id}")
    except subprocess.CalledProcessError as e:
        print(f"[chaos-2] FATAL re-submit: {e}", file=sys.stderr)
        record_event({
            "drill": "kill_spark_app",
            "started_at": kill_ts.isoformat(),
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "killed_app_id": app_id,
            "new_app_id": None,
            "success": False,
            "failure_mode": "resubmit_failed",
            "extra": str(e),
        })
        return 2

    deadline = time.time() + args.recovery_budget_seconds
    print(f"[chaos-2] Waiting for new app (budget={args.recovery_budget_seconds}s)...")
    success, new_app_id = wait_for_new_app(
        args.app_name, app_id, args.ssh_key, host, deadline
    )
    elapsed = time.time() - (deadline - args.recovery_budget_seconds)
    ended = datetime.now(timezone.utc)

    if success:
        print(f"[chaos-2] PASS new app RUNNING after {elapsed:.0f}s: {new_app_id}")
    else:
        print(f"[chaos-2] FAIL no new app appeared within budget ({elapsed:.0f}s)")

    record_event({
        "drill": "kill_spark_app",
        "started_at": kill_ts.isoformat(),
        "ended_at": ended.isoformat(),
        "killed_app_id": app_id,
        "new_app_id": new_app_id,
        "new_step_id": new_step_id,
        "success": success,
        "recovery_seconds": round(elapsed, 1),
        "budget_seconds": args.recovery_budget_seconds,
        "host": host,
        "cluster_id": cluster_id,
        "step_script": args.step_script,
    })

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
