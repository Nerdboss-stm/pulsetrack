"""
dbt invocations as Prefect tasks.

Wraps the dbt CLI via subprocess. Returns structured results so flows
can branch on per-model failures rather than treating the whole `dbt
build` as one atomic success/failure.

dbt's `run_results.json` is the source of truth for per-model status.
We parse it after each invocation.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass, field
from typing import Optional

from prefect import get_run_logger, task

DBT_PROJECT_DIR = os.environ.get(
    "PT_DBT_PROJECT_DIR",
    "/Users/nerdboss-stm/pulsetrack-cm/dbt_project",
)
DBT_TARGET = os.environ.get("PT_DBT_TARGET", "dev")


@dataclass
class DBTRunResult:
    """One dbt invocation's results."""

    command: str
    target: str
    exit_code: int
    pass_count: int = 0
    warn_count: int = 0
    error_count: int = 0
    skip_count: int = 0
    duration_seconds: float = 0.0
    failed_nodes: list[str] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0


def _parse_run_results(project_dir: str) -> dict:
    """Parse target/run_results.json from the last dbt invocation."""
    path = os.path.join(project_dir, "target", "run_results.json")
    if not os.path.exists(path):
        return {"results": []}
    with open(path) as f:
        return json.load(f)


def _summarize(run_results: dict) -> tuple[int, int, int, int, list[str]]:
    """Return (pass, warn, error, skip, failed_node_ids)."""
    p = w = e = s = 0
    failed: list[str] = []
    for r in run_results.get("results", []):
        status = r.get("status", "")
        if status == "pass":
            p += 1
        elif status == "warn":
            w += 1
        elif status == "error" or status == "fail":
            e += 1
            failed.append(r.get("unique_id", "?"))
        elif status == "skipped":
            s += 1
    return p, w, e, s, failed


def _dbt(args: list[str], target: str, project_dir: Optional[str]) -> DBTRunResult:
    """Run ``dbt <args>``; capture run_results.json."""
    log = get_run_logger()
    started = time.time()

    # Resolve project_dir: explicit arg > env var > module default.
    project_dir = project_dir or DBT_PROJECT_DIR
    if not project_dir:
        raise ValueError(
            "project_dir not set — pass explicitly or set PT_DBT_PROJECT_DIR"
        )

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = project_dir
    env["DBT_EXTERNAL_ROOT"] = os.path.join(project_dir, "fixtures")

    cmd = ["dbt"] + args + ["--target", target]
    log.info(f"running: {' '.join(cmd)} (cwd={project_dir})")
    result = subprocess.run(
        cmd,
        cwd=project_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=3600,
    )
    duration = time.time() - started

    if result.stdout:
        log.info(result.stdout[-1500:])
    if result.stderr and result.returncode != 0:
        log.error(result.stderr[-500:])

    p, w, e, s, failed = _summarize(_parse_run_results(project_dir))
    return DBTRunResult(
        command=" ".join(args),
        target=target,
        exit_code=result.returncode,
        pass_count=p,
        warn_count=w,
        error_count=e,
        skip_count=s,
        duration_seconds=duration,
        failed_nodes=failed,
    )


@task(name="dbt_deps", retries=2, tags=["dbt"])
def dbt_deps(
    target: str = DBT_TARGET,
    project_dir: Optional[str] = None,
) -> DBTRunResult:
    """``dbt deps`` — install package dependencies."""
    return _dbt(["deps"], target, project_dir)


@task(name="dbt_seed", retries=1, tags=["dbt"])
def dbt_seed(
    target: str = DBT_TARGET,
    project_dir: Optional[str] = None,
    full_refresh: bool = False,
) -> DBTRunResult:
    """``dbt seed`` — load reference + fixture seeds."""
    args = ["seed"]
    if full_refresh:
        args.append("--full-refresh")
    return _dbt(args, target, project_dir)


@task(name="dbt_run", retries=1, tags=["dbt"])
def dbt_run(
    select: Optional[str] = None,
    target: str = DBT_TARGET,
    project_dir: Optional[str] = None,
) -> DBTRunResult:
    """``dbt run`` — execute models. Optionally narrow with ``--select``."""
    args = ["run"]
    if select:
        args += ["--select", select]
    return _dbt(args, target, project_dir)


@task(name="dbt_test", retries=0, tags=["dbt", "test"])
def dbt_test(
    select: Optional[str] = None,
    target: str = DBT_TARGET,
    project_dir: Optional[str] = None,
) -> DBTRunResult:
    """``dbt test`` — run generic + singular tests."""
    args = ["test"]
    if select:
        args += ["--select", select]
    return _dbt(args, target, project_dir)


@task(name="dbt_build", retries=0, tags=["dbt"])
def dbt_build(
    select: Optional[str] = None,
    target: str = DBT_TARGET,
    project_dir: Optional[str] = None,
    fail_fast: bool = True,
) -> DBTRunResult:
    """``dbt build`` — compile + run + test in dependency order."""
    args = ["build"]
    if fail_fast:
        args.append("--fail-fast")
    if select:
        args += ["--select", select]
    return _dbt(args, target, project_dir)


@task(name="dbt_snapshot", retries=1, tags=["dbt", "snapshot"])
def dbt_snapshot(
    target: str = DBT_TARGET,
    project_dir: Optional[str] = None,
) -> DBTRunResult:
    """``dbt snapshot`` — refresh SCD2 snapshots (snap_dim_device)."""
    return _dbt(["snapshot"], target, project_dir)


@task(name="dbt_source_freshness", retries=1, tags=["dbt", "freshness"])
def dbt_source_freshness(
    target: str = DBT_TARGET,
    project_dir: Optional[str] = None,
) -> DBTRunResult:
    """``dbt source freshness`` — check silver-source SLAs."""
    return _dbt(["source", "freshness"], target, project_dir)


@task(name="dbt_docs_generate", retries=1, tags=["dbt", "docs"])
def dbt_docs_generate(
    target: str = DBT_TARGET,
    project_dir: Optional[str] = None,
) -> DBTRunResult:
    """``dbt docs generate`` — build the docs site artifacts."""
    return _dbt(["docs", "generate"], target, project_dir)
