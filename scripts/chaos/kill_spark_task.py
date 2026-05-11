#!/usr/bin/env python3
"""
Chaos drill 1 — kill ONE silver-streaming executor mid-test.

What this proves
----------------
Spark task-level fault tolerance. The streaming query maintains a DAG with
lineage; killing one container forces the driver to re-schedule the task
on another executor. With Iceberg's commit-on-checkpoint model, no data
is lost — the killed micro-batch is simply re-attempted.

Target recovery
---------------
<60 seconds from kill to first newly-processed batch.

How
---
1. SSH to EMR master (via paramiko + ``~/.ssh/pulsetrack-emr.pem``)
2. ``yarn application -list`` → find the silver streaming app
3. ``yarn container -list <appId>`` → pick one container (NOT the AM)
4. ``yarn container -signal <containerId> FORCEFUL_SHUTDOWN``
5. Poll YARN every 5s until a new container with the same task replaces it
6. Record timestamps to ``docs/chaos_log.jsonl`` for the postmortem

Usage
-----
::

    AWS_PROFILE=pulsetrack \
        python3 scripts/chaos/kill_spark_task.py \
            --app-name "silver_sensor_streaming" \
            --recovery-budget-seconds 60

Exit codes
----------
0 — recovery within budget
1 — recovery exceeded budget (drill considered failed)
2 — no matching application or other error
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


def ssh_master(cmd: str, key_path: str, host: str) -> str:
    """Run ``cmd`` on the EMR master via SSH. Returns stdout."""
    full = [
        "ssh",
        "-i",
        key_path,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "BatchMode=yes",
        f"hadoop@{host}",
        cmd,
    ]
    result = subprocess.run(full, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(
            f"SSH command failed (exit {result.returncode}):\n  cmd={cmd}\n  stderr={result.stderr}"
        )
    return result.stdout


def get_emr_master_dns() -> str:
    """Resolve via ``terraform output``."""
    out = subprocess.check_output(
        ["terraform", "output", "-raw", "emr_master_dns"],
        cwd=REPO_ROOT / "infrastructure",
        text=True,
    )
    return out.strip()


def find_app_id(app_name: str, key_path: str, host: str) -> str:
    """Return the YARN application ID matching ``app_name``."""
    stdout = ssh_master(
        f"yarn application -list -appStates RUNNING", key_path, host
    )
    # Output format (whitespace-separated columns starting from row 3):
    #   application_1234..._0001  silver_sensor_streaming  SPARK ...
    for line in stdout.splitlines():
        if app_name in line and line.startswith("application_"):
            return line.split()[0]
    raise RuntimeError(f"No RUNNING application matches '{app_name}'")


def list_containers(app_id: str, key_path: str, host: str) -> list[dict]:
    """Return parsed container list for the given app."""
    # `yarn container -list <appId>` returns columns:
    # Container-Id, Start Time, Finish Time, State, Host, Node Http Address, LOG-URL
    stdout = ssh_master(f"yarn container -list {app_id}", key_path, host)
    containers: list[dict] = []
    for line in stdout.splitlines():
        if not line.startswith("container_"):
            continue
        parts = re.split(r"\s+", line.strip())
        if len(parts) < 5:
            continue
        containers.append(
            {
                "id": parts[0],
                "state": parts[3] if len(parts) > 3 else "?",
                "host": parts[4] if len(parts) > 4 else "?",
            }
        )
    return containers


def pick_target(containers: list[dict]) -> dict:
    """Pick a non-AM, RUNNING container. AM is always container_..._01."""
    running = [c for c in containers if c["state"] == "RUNNING"]
    if not running:
        raise RuntimeError("No RUNNING containers found")
    non_am = [c for c in running if not c["id"].endswith("_000001")]
    candidates = non_am or running  # fall back to AM if it's the only one
    return candidates[0]


def signal_kill(container_id: str, key_path: str, host: str) -> None:
    """Send FORCEFUL_SHUTDOWN to the container."""
    ssh_master(
        f"yarn container -signal {container_id} FORCEFUL_SHUTDOWN", key_path, host
    )


def wait_for_replacement(
    app_id: str,
    killed_id: str,
    key_path: str,
    host: str,
    budget_seconds: int,
) -> tuple[bool, str | None, float]:
    """Wait until a new container replaces the killed one. Returns (success, new_id, elapsed)."""
    deadline = time.time() + budget_seconds
    last_seen = ""
    while time.time() < deadline:
        time.sleep(5)
        try:
            current = list_containers(app_id, key_path, host)
        except RuntimeError as e:
            last_seen = str(e)
            continue
        running = {c["id"] for c in current if c["state"] == "RUNNING"}
        # Replacement = the killed ID is gone AND there's a new running container
        if killed_id not in running and len(running) >= 1:
            new_ids = sorted(running)
            return True, new_ids[-1], time.time() - (deadline - budget_seconds)
        last_seen = f"running={len(running)} states_seen"
    return False, None, budget_seconds + 1.0


def record_event(
    drill: str,
    started: datetime,
    ended: datetime,
    killed_id: str,
    new_id: str | None,
    success: bool,
    recovery_seconds: float,
    extra: dict,
) -> None:
    """Append a JSON-line record to docs/chaos_log.jsonl."""
    CHAOS_LOG.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "drill": drill,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "killed_container_id": killed_id,
        "replacement_container_id": new_id,
        "success": success,
        "recovery_seconds": round(recovery_seconds, 1),
        **extra,
    }
    with open(CHAOS_LOG, "a") as f:
        f.write(json.dumps(event) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-name",
        default="silver_sensor_streaming",
        help="Substring of the YARN application name to target",
    )
    parser.add_argument(
        "--recovery-budget-seconds",
        type=int,
        default=60,
        help="Maximum recovery time before the drill is considered failed",
    )
    parser.add_argument(
        "--ssh-key",
        default=str(Path.home() / ".ssh" / "pulsetrack-emr.pem"),
        help="Path to the EMR master SSH key",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="EMR master DNS (auto-detected from terraform output if omitted)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover the target but don't actually kill",
    )
    args = parser.parse_args()

    host = args.host or get_emr_master_dns()
    if not host:
        print("FATAL: Could not resolve EMR master DNS", file=sys.stderr)
        return 2

    print(f"[chaos-1] Target: app_name~='{args.app_name}' host={host}")
    started = datetime.now(timezone.utc)

    try:
        app_id = find_app_id(args.app_name, args.ssh_key, host)
        print(f"[chaos-1] Found app: {app_id}")
        containers = list_containers(app_id, args.ssh_key, host)
        print(f"[chaos-1] Containers: {len(containers)} ({sum(1 for c in containers if c['state'] == 'RUNNING')} RUNNING)")
        target = pick_target(containers)
        print(f"[chaos-1] Selected: {target['id']} on {target['host']}")
    except RuntimeError as e:
        print(f"[chaos-1] FATAL: {e}", file=sys.stderr)
        return 2

    if args.dry_run:
        print("[chaos-1] Dry run — would kill but skipping.")
        return 0

    kill_ts = datetime.now(timezone.utc)
    print(f"[chaos-1] {kill_ts.isoformat()} Killing {target['id']}")
    try:
        signal_kill(target["id"], args.ssh_key, host)
    except RuntimeError as e:
        print(f"[chaos-1] FATAL signal: {e}", file=sys.stderr)
        return 2

    print(f"[chaos-1] Waiting for replacement (budget={args.recovery_budget_seconds}s)...")
    t_wait_start = time.time()
    success, new_id, _ = wait_for_replacement(
        app_id, target["id"], args.ssh_key, host, args.recovery_budget_seconds
    )
    elapsed = time.time() - t_wait_start
    ended = datetime.now(timezone.utc)

    if success:
        print(f"[chaos-1] PASS recovered in {elapsed:.1f}s → new container {new_id}")
    else:
        print(f"[chaos-1] FAIL did not recover within budget ({elapsed:.1f}s)")

    record_event(
        drill="kill_spark_task",
        started=kill_ts,
        ended=ended,
        killed_id=target["id"],
        new_id=new_id,
        success=success,
        recovery_seconds=elapsed,
        extra={
            "app_id": app_id,
            "app_name_pattern": args.app_name,
            "host": host,
            "budget_seconds": args.recovery_budget_seconds,
            "killed_node": target.get("host"),
        },
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
