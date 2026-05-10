"""
Maintenance pipeline — nightly housekeeping for the lakehouse.

Runs every day at 02:00 UTC (low-traffic window).

Tasks:
  - Iceberg OPTIMIZE TABLE (compact small files into larger ones)
  - Iceberg expire_snapshots (drop snapshots older than retention)
  - Iceberg remove_orphan_files (clean S3 data files no longer referenced)
  - Glacierbase migration check (validate pending migrations cleanly)
  - dbt source freshness (alert on stale silver)
  - Observability monitor cleanup
"""

from __future__ import annotations

import time
from typing import Optional

from prefect import flow, get_run_logger, task

from orchestration.tasks.dbt_tasks import dbt_source_freshness
from orchestration.tasks.emr_tasks import DEFAULT_BUCKET, submit_emr_step
from orchestration.tasks.spark_tasks import SPARK_JARS_DEFAULT
from orchestration.tasks.notification_tasks import (
    notify_pipeline_complete,
    notify_pipeline_failure,
    notify_slack,
)
from orchestration.tasks.spark_tasks import (
    JobResult,
    run_migration_apply,
)


@task(name="iceberg_optimize", retries=1, tags=["maintenance", "iceberg"])
def iceberg_optimize(
    table_fqn: str,
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Run ``CALL system.rewrite_data_files`` to compact small files.

    Iceberg accumulates small parquet files when a writer commits
    frequently (every 30s in our streaming pipeline). After accumulation,
    read performance degrades — too many file open/close operations.
    Rewrite_data_files merges files in a partition into target-size files.

    Cadence: nightly OPTIMIZE for the high-write tables (bronze
    sensor_readings, silver sensor_readings, gold fact_vital_reading).
    """
    log = get_run_logger()
    sql = f"CALL system.rewrite_data_files('{table_fqn}')"
    log.info(f"OPTIMIZE: {sql}")

    # Submit as a one-off Spark SQL invocation via spark-sql.
    step = submit_emr_step.fn(
        job_name=f"iceberg-optimize-{table_fqn.replace('.', '-')}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/maintenance/run_spark_sql.py",
        script_args=["--sql", sql],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
        timeout_seconds=1800,
    )
    return JobResult(
        job_name=f"optimize_{table_fqn}",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="iceberg_expire_snapshots", retries=1, tags=["maintenance", "iceberg"])
def iceberg_expire_snapshots(
    table_fqn: str,
    older_than_days: int = 7,
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Drop Iceberg snapshots older than N days.

    Iceberg keeps historical snapshots for time-travel. They consume S3
    storage even when no longer referenced. Default retention 7 days.
    """
    log = get_run_logger()
    sql = (
        f"CALL system.expire_snapshots('{table_fqn}', "
        f"TIMESTAMP '{{(now() - INTERVAL '{older_than_days}' DAY) }}')"
    )
    # Note: above templating in spark-sql can be brittle. In prod the
    # actual SQL uses a Spark expression in the EMR step's job script.
    log.info(f"expire_snapshots: {sql}")

    step = submit_emr_step.fn(
        job_name=f"iceberg-expire-{table_fqn.replace('.', '-')}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/maintenance/expire_snapshots.py",
        script_args=["--table", table_fqn, "--older-than-days", str(older_than_days)],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
        timeout_seconds=600,
    )
    return JobResult(
        job_name=f"expire_{table_fqn}",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="iceberg_remove_orphan_files", retries=1, tags=["maintenance"])
def iceberg_remove_orphan_files(
    table_fqn: str,
    older_than_days: int = 3,
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Remove S3 data files no longer referenced by any Iceberg snapshot.

    Orphans accumulate when:
      - A write commit failed after data files were written.
      - A snapshot was rolled back manually.
      - Maintenance got interrupted partway through.
    """
    log = get_run_logger()
    log.info(f"remove_orphan_files: {table_fqn} (older_than={older_than_days}d)")

    step = submit_emr_step.fn(
        job_name=f"iceberg-orphans-{table_fqn.replace('.', '-')}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/maintenance/remove_orphans.py",
        script_args=["--table", table_fqn, "--older-than-days", str(older_than_days)],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
        timeout_seconds=1800,
    )
    return JobResult(
        job_name=f"orphans_{table_fqn}",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="check_glacierbase_pending", retries=1, tags=["maintenance", "migrations"])
def check_glacierbase_pending(
    catalog: str = "glue_iceberg",
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Run ``migrations/cli.py --catalog X pending`` — alerts if non-empty.

    Catches the situation where a migration was authored but never
    applied. Could indicate a missed PR merge.
    """
    step = submit_emr_step.fn(
        job_name=f"migration-pending-{catalog}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/migrations/cli.py",
        script_args=["--catalog", catalog, "pending"],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
        timeout_seconds=300,
    )
    return JobResult(
        job_name="migration_pending",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@flow(name="maintenance-nightly", log_prints=True, retries=0)
def maintenance(
    aws_env: str = "dev",
    cluster_id: Optional[str] = None,
) -> dict:
    """Nightly lakehouse maintenance.

    Args:
        aws_env: Glue DB suffix.
        cluster_id: EMR cluster (defaults to PT_EMR_CLUSTER_ID env).

    Returns:
        Per-task status dict.
    """
    log = get_run_logger()
    started = time.time()
    log.info(f"maintenance starting (env={aws_env})")

    results: dict = {}

    try:
        # ── Optimize high-write tables ────────────────────────────
        for layer, table in [
            ("bronze", "sensor_readings"),
            ("silver", "sensor_readings"),
            ("gold", "fact_vital_reading"),
            ("gold", "fact_vital_daily_summary"),
        ]:
            fqn = f"glue_iceberg.pulsetrack_{layer}_{aws_env}.{table}"
            try:
                r = iceberg_optimize(table_fqn=fqn, cluster_id=cluster_id)
                results[f"optimize_{layer}_{table}"] = {
                    "succeeded": r.succeeded,
                    "seconds": r.duration_seconds,
                }
            except Exception as exc:  # noqa: BLE001
                log.warning(f"optimize {fqn} failed: {exc}")
                results[f"optimize_{layer}_{table}"] = {
                    "succeeded": False, "error": str(exc)[:200]
                }

        # ── Expire old snapshots ──────────────────────────────────
        for layer, table in [("bronze", "sensor_readings"), ("silver", "sensor_readings")]:
            fqn = f"glue_iceberg.pulsetrack_{layer}_{aws_env}.{table}"
            try:
                r = iceberg_expire_snapshots(
                    table_fqn=fqn, older_than_days=7, cluster_id=cluster_id
                )
                results[f"expire_{layer}_{table}"] = {
                    "succeeded": r.succeeded,
                    "seconds": r.duration_seconds,
                }
            except Exception as exc:  # noqa: BLE001
                log.warning(f"expire {fqn} failed: {exc}")
                results[f"expire_{layer}_{table}"] = {
                    "succeeded": False, "error": str(exc)[:200]
                }

        # ── Migration sanity check ────────────────────────────────
        pending = check_glacierbase_pending(cluster_id=cluster_id)
        results["migration_pending_check"] = {
            "succeeded": pending.succeeded,
            "seconds": pending.duration_seconds,
        }

        # ── dbt freshness ─────────────────────────────────────────
        freshness = dbt_source_freshness()
        results["dbt_source_freshness"] = {
            "succeeded": freshness.succeeded,
            "exit_code": freshness.exit_code,
            "seconds": freshness.duration_seconds,
        }

        duration = time.time() - started
        log.info(f"maintenance complete in {duration:.0f}s")
        notify_pipeline_complete(
            pipeline_name="maintenance-nightly",
            duration_seconds=duration,
            metrics=results,
        )
        return {"status": "ok", "duration_seconds": duration, "results": results}

    except Exception as exc:
        duration = time.time() - started
        notify_pipeline_failure(
            pipeline_name="maintenance-nightly",
            error=str(exc)[:1000],
        )
        raise


if __name__ == "__main__":
    maintenance()
