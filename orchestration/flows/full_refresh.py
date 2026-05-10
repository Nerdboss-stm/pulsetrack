"""
Full-refresh — the nuclear option.

Rebuilds the entire pipeline from bronze. Used for:
  - Schema-evolution backfills (V003 added pharmacy tables; existing
    bronze data needs reprocessing through silver+gold).
  - Recovery from corruption (silver data went bad; recompute from
    bronze evidence).
  - Onboarding new analytical models that need historical fills.

NEVER scheduled. Triggered via Prefect UI "run-now" only, with
explicit operator confirmation.

The flow is destructive — silver and gold tables are truncated before
rebuild. Bronze is the source of truth and is NEVER touched.
"""

from __future__ import annotations

import time
from typing import Optional

from prefect import flow, get_run_logger

from orchestration.tasks.dbt_tasks import dbt_build
from orchestration.tasks.notification_tasks import (
    notify_pipeline_complete,
    notify_pipeline_failure,
    notify_slack,
)
from orchestration.tasks.spark_tasks import (
    run_ehr_silver,
    run_gold_dim,
    run_gold_fact,
    run_identity_bridge,
    run_pharmacy_silver,
)


# Gold dim build order (parents first per FK dependencies).
DIM_ORDER = [
    "dim_date",                  # leaf
    "dim_time",                  # leaf
    "dim_condition_category",    # parent of dim_condition
    "dim_drug_class",            # parent of dim_medication
    "dim_metric",                # leaf
    "dim_device",                # SCD2 from silver firmware history
    "dim_condition",             # FK → dim_condition_category
    "dim_medication",            # FK → dim_drug_class
    "dim_patient",               # PII-masked; depends on identity_bridge
]

FACT_ORDER = [
    "fact_vital_daily_summary",  # silver agg → patient/metric/date grain
    "fact_vital_reading",         # atomic per-reading
    "fact_lab_result",            # EHR-sourced
    "fact_pharmacy_fill",         # FDA-sourced
]


@flow(name="full-refresh", log_prints=True, retries=0)
def full_refresh(
    aws_env: str = "dev",
    fmt: str = "iceberg",
    operator_confirmation: str = "",
    skip_dbt: bool = False,
) -> dict:
    """Rebuild silver + gold from bronze.

    Args:
        aws_env: Glue DB suffix.
        fmt: iceberg or delta.
        operator_confirmation: must equal "YES_REBUILD_<aws_env>" to proceed.
        skip_dbt: omit the dbt build pass (faster for Spark-only rebuilds).

    Raises:
        ValueError: operator_confirmation not provided.
    """
    log = get_run_logger()
    expected_confirmation = f"YES_REBUILD_{aws_env}"
    if operator_confirmation != expected_confirmation:
        raise ValueError(
            f"full_refresh requires operator_confirmation='{expected_confirmation}' — "
            f"got '{operator_confirmation}'. Aborting."
        )

    started = time.time()
    notify_slack.fn(
        message=(
            f":warning: FULL REFRESH starting for env={aws_env}. "
            f"Silver + gold tables will be rebuilt from bronze."
        ),
        severity="warn",
    )

    results: dict = {}

    try:
        # ── Stage 1: Silver ───────────────────────────────────────
        log.info("Stage 1: rebuilding silver from bronze")
        ehr = run_ehr_silver(fmt=fmt)
        results["ehr_silver"] = {"seconds": ehr.duration_seconds, "ok": ehr.succeeded}
        pharm = run_pharmacy_silver(fmt=fmt)
        results["pharmacy_silver"] = {
            "seconds": pharm.duration_seconds, "ok": pharm.succeeded
        }

        # ── Stage 2: Identity bridge ──────────────────────────────
        log.info("Stage 2: rebuilding identity bridge")
        bridge = run_identity_bridge(fmt=fmt)
        results["identity_bridge"] = {
            "seconds": bridge.duration_seconds, "ok": bridge.succeeded
        }

        # ── Stage 3: Gold dims in dependency order ────────────────
        log.info("Stage 3: rebuilding gold dims")
        for dim in DIM_ORDER:
            r = run_gold_dim(dim, fmt=fmt)
            results[dim] = {"seconds": r.duration_seconds, "ok": r.succeeded}

        # ── Stage 4: Gold facts ───────────────────────────────────
        log.info("Stage 4: rebuilding gold facts")
        for fact in FACT_ORDER:
            r = run_gold_fact(fact, mode="batch", fmt=fmt)
            results[fact] = {"seconds": r.duration_seconds, "ok": r.succeeded}

        # ── Stage 5: dbt build (warehouse-side gold rebuild) ──────
        if not skip_dbt:
            log.info("Stage 5: dbt build (re-publishing warehouse marts)")
            dbt_result = dbt_build(target="dev", fail_fast=False)
            results["dbt_build"] = {
                "seconds": dbt_result.duration_seconds,
                "ok": dbt_result.succeeded,
                "pass_count": dbt_result.pass_count,
                "error_count": dbt_result.error_count,
            }

        duration = time.time() - started
        any_failure = any(not v.get("ok") for v in results.values())
        log.info(f"full_refresh complete in {duration:.0f}s")

        if any_failure:
            notify_pipeline_failure(
                pipeline_name="full-refresh",
                error=f"stages failed: {[k for k, v in results.items() if not v.get('ok')]}",
            )
        else:
            notify_pipeline_complete(
                pipeline_name="full-refresh",
                duration_seconds=duration,
                metrics={k: f"{v.get('seconds', 0):.0f}s" for k, v in results.items()},
            )

        return {
            "status": "error" if any_failure else "success",
            "duration_seconds": duration,
            "results": results,
        }

    except Exception as exc:
        duration = time.time() - started
        notify_pipeline_failure(
            pipeline_name="full-refresh",
            error=str(exc)[:1000],
        )
        raise


if __name__ == "__main__":
    import sys
    aws_env = sys.argv[1] if len(sys.argv) > 1 else "dev"
    full_refresh(
        aws_env=aws_env,
        operator_confirmation=f"YES_REBUILD_{aws_env}",
    )
