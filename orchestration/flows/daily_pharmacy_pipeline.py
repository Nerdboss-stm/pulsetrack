"""
Daily pharmacy pipeline flow.

DAG:
    openfda_poll → pharmacy_silver → identity_bridge → fact_pharmacy_fill
                                          ↓
                                  quality_checks → notify

Scheduled: daily 07:00 UTC (offset from EHR pipeline so they don't
collide on the cluster).
"""

from __future__ import annotations

import os
import subprocess
import time

from prefect import flow, get_run_logger, task

from orchestration.tasks.notification_tasks import (
    notify_pipeline_complete,
    notify_pipeline_failure,
)
from orchestration.tasks.quality_tasks import (
    aggregate_quality_report,
    check_table_rowcount,
)
from orchestration.tasks.spark_tasks import (
    JobResult,
    run_gold_fact,
    run_identity_bridge,
    run_pharmacy_silver,
)
from orchestration.tasks.emr_tasks import DEFAULT_BUCKET, submit_emr_step


@task(name="run_openfda_poller", retries=2, tags=["producer", "openfda"])
def run_openfda_poller(window_days: int = 7) -> JobResult:
    """One OpenFDA adverse-event poll cycle.

    Runs on the Prefect worker (REST poller; no Spark). Subprocess
    invocation lets us reuse the existing data_generators code.
    """
    log = get_run_logger()
    started = time.time()
    cmd = [
        "python3",
        "data_generators/openfda_producer.py",
        "--mode",
        "once",
        "--window-days",
        str(window_days),
    ]
    log.info(f"openfda poller: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=os.environ.get("PT_PROJECT_ROOT", "/Users/nerdboss-stm/pulsetrack-cm"),
        capture_output=True,
        text=True,
        timeout=600,
    )
    duration = time.time() - started

    if result.returncode != 0:
        log.error(f"openfda poller failed: {result.stderr[-500:]}")
        return JobResult(
            job_name="openfda_poller",
            succeeded=False,
            duration_seconds=duration,
            detail=result.stderr[-500:],
        )

    log.info(f"openfda poller done in {duration:.0f}s")
    return JobResult(
        job_name="openfda_poller",
        succeeded=True,
        duration_seconds=duration,
        detail=result.stdout[-200:],
    )


@task(name="run_pharmacy_bronze_batch", retries=1, tags=["spark", "bronze"])
def run_pharmacy_bronze_batch(fmt: str = "iceberg") -> JobResult:
    """Catch-up batch of pharmacy bronze ingestion via available_now trigger.

    Pharmacy bronze normally runs continuously; this batch trigger is
    useful for backfills or after producer-only catch-up runs.
    """
    step = submit_emr_step.fn(
        job_name=f"pharmacy-bronze-batch-{fmt}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/streaming/pharmacy_bronze_ingestion.py",
        script_args=["--trigger", "available_now", "--format", fmt],
        spark_jars=SPARK_JARS_DEFAULT,
        timeout_seconds=1800,
    )
    return JobResult(
        job_name="pharmacy_bronze",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@flow(name="pharmacy-daily", log_prints=True, retries=0)
def pharmacy_daily(aws_env: str = "dev", fmt: str = "iceberg") -> dict:
    """End-to-end pharmacy ingestion → silver → identity → gold.

    Args:
        aws_env: Glue DB suffix.
        fmt: iceberg or delta.

    Returns:
        Dict with per-stage durations + quality summary.
    """
    log = get_run_logger()
    log.info(f"pharmacy_daily starting (env={aws_env}, fmt={fmt})")
    started = time.time()

    try:
        # ── Stage 1: OpenFDA poller → Kafka ───────────────────────
        poll_result = run_openfda_poller()

        # ── Stage 2: Bronze ingestion catch-up ────────────────────
        bronze_result = run_pharmacy_bronze_batch(fmt=fmt)

        # ── Stage 3: Pharmacy silver ──────────────────────────────
        silver_result = run_pharmacy_silver(fmt=fmt)

        # ── Stage 4: Identity bridge re-resolve (pharmacy phase) ──
        bridge_result = run_identity_bridge(fmt=fmt)

        # ── Stage 5: Gold pharmacy fact ───────────────────────────
        fact_result = run_gold_fact("fact_pharmacy_fill", mode="batch", fmt=fmt)

        # ── Stage 6: Quality checks ───────────────────────────────
        checks = [
            check_table_rowcount(
                database=f"pulsetrack_silver_{aws_env}",
                table="pharmacy_fills",
                min_rows=1,
            ),
            check_table_rowcount(
                database=f"pulsetrack_gold_{aws_env}",
                table="fact_pharmacy_fill",
                min_rows=1,
            ),
        ]
        report = aggregate_quality_report(checks=checks)

        duration = time.time() - started
        log.info(f"pharmacy_daily completed in {duration:.0f}s — {report.summary}")

        notify_pipeline_complete(
            pipeline_name="pharmacy-daily",
            duration_seconds=duration,
            metrics={
                "poll_seconds": poll_result.duration_seconds,
                "bronze_seconds": bronze_result.duration_seconds,
                "silver_seconds": silver_result.duration_seconds,
                "identity_bridge_seconds": bridge_result.duration_seconds,
                "fact_seconds": fact_result.duration_seconds,
                "quality": report.summary,
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
            pipeline_name="pharmacy-daily",
            error=str(exc)[:1000],
        )
        raise


if __name__ == "__main__":
    pharmacy_daily()
