"""
Daily EHR pipeline flow.

DAG:
                    ┌──────────────┐
                    │ ehr_silver   │  (Spark on EMR)
                    └──────┬───────┘
                           │
                  ┌────────┴─────────┐
                  ▼                  ▼
       ┌───────────────────┐    ┌─────────────────┐
       │ identity_bridge   │    │ dim_patient     │
       │  (Spark on EMR)   │    │  (gold dim)     │
       └──────────┬────────┘    └────────┬────────┘
                  │                      │
                  └──────────┬───────────┘
                             ▼
                  ┌────────────────────┐
                  │ fact_lab_result    │
                  │  (gold fact)       │
                  └──────────┬─────────┘
                             ▼
                  ┌────────────────────┐
                  │ quality_checks     │
                  └──────────┬─────────┘
                             ▼
                  ┌────────────────────┐
                  │ notify_complete    │
                  └────────────────────┘

Scheduled: daily 06:00 UTC via deployment cron ``0 6 * * *``.
"""

from __future__ import annotations

import time

from prefect import flow, get_run_logger

from orchestration.tasks.notification_tasks import (
    notify_pipeline_complete,
    notify_pipeline_failure,
)
from orchestration.tasks.quality_tasks import (
    aggregate_quality_report,
    check_identity_link_rate,
    check_table_rowcount,
)
from orchestration.tasks.spark_tasks import (
    run_ehr_silver,
    run_gold_dim,
    run_gold_fact,
    run_identity_bridge,
)


@flow(name="ehr-daily", log_prints=True, retries=0)
def ehr_daily(aws_env: str = "dev", fmt: str = "iceberg") -> dict:
    """End-to-end EHR ingestion → silver → identity → gold → quality.

    Args:
        aws_env: Glue DB suffix (dev/staging/prod).
        fmt: ``iceberg`` or ``delta``.

    Returns:
        Dict with per-stage durations + quality summary.
    """
    log = get_run_logger()
    log.info(f"ehr_daily starting (env={aws_env}, fmt={fmt})")
    started = time.time()

    try:
        # ── Stage 1: Silver ───────────────────────────────────────
        ehr_result = run_ehr_silver(fmt=fmt)

        # ── Stage 2: Identity bridge (depends on silver) ──────────
        bridge_result = run_identity_bridge(fmt=fmt, wait_for=[ehr_result])  # noqa

        # ── Stage 3: Gold dims (parallel where possible) ──────────
        # dim_patient depends on identity_bridge; the rest are
        # parallel-safe (date, time, condition_category, drug_class).
        dim_patient = run_gold_dim("dim_patient", fmt=fmt)
        dim_date = run_gold_dim("dim_date", fmt=fmt)
        dim_time = run_gold_dim("dim_time", fmt=fmt)
        dim_condition_category = run_gold_dim(
            "dim_condition_category", fmt=fmt
        )
        dim_drug_class = run_gold_dim("dim_drug_class", fmt=fmt)
        dim_condition = run_gold_dim("dim_condition", fmt=fmt)
        dim_medication = run_gold_dim("dim_medication", fmt=fmt)

        # ── Stage 4: Gold facts ───────────────────────────────────
        fact_lab = run_gold_fact("fact_lab_result", mode="batch", fmt=fmt)

        # ── Stage 5: Quality checks ───────────────────────────────
        checks = [
            check_table_rowcount(
                database=f"pulsetrack_silver_{aws_env}",
                table="ehr_conditions",
                min_rows=1,
            ),
            check_table_rowcount(
                database=f"pulsetrack_silver_{aws_env}",
                table="ehr_medications",
                min_rows=1,
            ),
            check_table_rowcount(
                database=f"pulsetrack_silver_{aws_env}",
                table="identity_bridge",
                min_rows=1,
            ),
            check_table_rowcount(
                database=f"pulsetrack_gold_{aws_env}",
                table="fact_lab_result",
                min_rows=1,
            ),
            check_identity_link_rate(threshold=0.85),
        ]
        report = aggregate_quality_report(checks=checks)

        duration = time.time() - started
        log.info(f"ehr_daily completed in {duration:.0f}s — {report.summary}")

        notify_pipeline_complete(
            pipeline_name="ehr-daily",
            duration_seconds=duration,
            metrics={
                "ehr_silver_seconds": ehr_result.duration_seconds,
                "identity_bridge_seconds": bridge_result.duration_seconds,
                "fact_lab_seconds": fact_lab.duration_seconds,
                "quality_checks": report.summary,
            },
        )

        return {
            "status": "success",
            "duration_seconds": duration,
            "quality": report.summary,
        }

    except Exception as exc:
        duration = time.time() - started
        notify_pipeline_failure(
            pipeline_name="ehr-daily",
            error=str(exc)[:1000],
            failed_task="(see Prefect run details)",
        )
        raise


if __name__ == "__main__":
    ehr_daily()
