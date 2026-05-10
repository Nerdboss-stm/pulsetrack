"""
dbt pipeline flow — weekly release cadence.

Runs Friday 10:00 UTC (matches WHOOP's deploy day per their published
cadence).

Order: deps → seed → build → test → snapshot → docs generate. Each
stage's failures are captured but the flow continues to the next stage
for diagnostics — at the end, the aggregate result determines pass/fail.
"""

from __future__ import annotations

import time
from typing import Optional

from prefect import flow, get_run_logger

from orchestration.tasks.dbt_tasks import (
    DBTRunResult,
    dbt_build,
    dbt_deps,
    dbt_docs_generate,
    dbt_seed,
    dbt_snapshot,
    dbt_source_freshness,
)
from orchestration.tasks.notification_tasks import (
    notify_pipeline_complete,
    notify_pipeline_failure,
)


@flow(name="dbt-weekly", log_prints=True, retries=0)
def dbt_weekly(
    target: str = "dev",
    project_dir: Optional[str] = None,
    run_snapshot: bool = True,
    run_docs: bool = True,
) -> dict:
    """Full dbt build + test + snapshot + docs.

    Args:
        target: dbt target name (dev, snowflake, prod).
        project_dir: absolute path to dbt_project. Defaults to
            ``PT_DBT_PROJECT_DIR`` env.
        run_snapshot: skip dbt snapshot for fast iteration runs.
        run_docs: skip dbt docs generate when running CI-style.

    Returns:
        Dict of per-stage results.
    """
    log = get_run_logger()
    started = time.time()
    log.info(f"dbt-weekly starting (target={target})")

    results: dict[str, DBTRunResult] = {}

    try:
        # ── deps ──────────────────────────────────────────────────
        deps = dbt_deps(target=target, project_dir=project_dir)
        results["deps"] = deps
        if not deps.succeeded:
            raise RuntimeError("dbt deps failed; aborting")

        # ── seed ──────────────────────────────────────────────────
        seed = dbt_seed(target=target, project_dir=project_dir, full_refresh=True)
        results["seed"] = seed
        if not seed.succeeded:
            log.warning("dbt seed had errors; continuing to build for diagnostics")

        # ── build (compile + run + test in DAG order) ─────────────
        build = dbt_build(target=target, project_dir=project_dir, fail_fast=False)
        results["build"] = build
        if not build.succeeded:
            log.error(
                f"dbt build had {build.error_count} errors. "
                f"Failed nodes: {build.failed_nodes[:5]}"
            )

        # ── snapshot ──────────────────────────────────────────────
        if run_snapshot:
            snap = dbt_snapshot(target=target, project_dir=project_dir)
            results["snapshot"] = snap

        # ── source freshness ──────────────────────────────────────
        freshness = dbt_source_freshness(target=target, project_dir=project_dir)
        results["source_freshness"] = freshness

        # ── docs generate ─────────────────────────────────────────
        if run_docs:
            docs = dbt_docs_generate(target=target, project_dir=project_dir)
            results["docs_generate"] = docs

        duration = time.time() - started
        any_error = any(not r.succeeded for r in results.values())
        log.info(
            f"dbt-weekly complete in {duration:.0f}s "
            f"({'errors' if any_error else 'all clean'})"
        )

        # Notification.
        metrics = {
            stage: f"{r.pass_count} pass / {r.warn_count} warn / "
                   f"{r.error_count} error / {r.skip_count} skip ({r.duration_seconds:.0f}s)"
            for stage, r in results.items()
        }
        if any_error:
            notify_pipeline_failure(
                pipeline_name="dbt-weekly",
                error=f"stages failed: {[s for s, r in results.items() if not r.succeeded]}",
                failed_task=str(results),
            )
        else:
            notify_pipeline_complete(
                pipeline_name="dbt-weekly",
                duration_seconds=duration,
                metrics=metrics,
            )

        return {
            "status": "error" if any_error else "success",
            "duration_seconds": duration,
            "stages": {s: r.succeeded for s, r in results.items()},
        }

    except Exception as exc:
        duration = time.time() - started
        notify_pipeline_failure(
            pipeline_name="dbt-weekly",
            error=str(exc)[:1000],
        )
        raise


if __name__ == "__main__":
    dbt_weekly()
