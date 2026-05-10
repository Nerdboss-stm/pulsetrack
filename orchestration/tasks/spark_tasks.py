"""
Spark job wrappers as Prefect tasks.

Two execution modes:

  - Local: ``subprocess.run`` of ``spark-submit`` on the Prefect worker
    machine. Used for tests + the producer (which doesn't need YARN).
  - Cloud: delegate to :func:`orchestration.tasks.emr_tasks.submit_emr_step`
    for everything Spark-shaped that needs cluster compute.

Each task wraps the chosen execution mode and presents a uniform
interface to the flow. The flow doesn't know (or care) whether the
job runs locally or on EMR.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import Optional

from prefect import get_run_logger, task

from orchestration.tasks.emr_tasks import (
    DEFAULT_BUCKET,
    EMRStepResult,
    submit_emr_step,
)

# Module entry-points → S3 paths. These are uploaded to S3 ahead of
# each flow run; the EMR step references the S3 path.
SPARK_PACKAGES_DEFAULT = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
    "org.apache.spark:spark-avro_2.12:3.5.6",
    "software.amazon.msk:aws-msk-iam-auth:2.2.0",
]
SPARK_JARS_DEFAULT = [
    "file:///usr/share/aws/delta/lib/delta-spark.jar",
    "file:///usr/share/aws/delta/lib/delta-storage.jar",
]


@dataclass
class JobResult:
    """Uniform return type across local + cloud execution."""

    job_name: str
    succeeded: bool
    duration_seconds: float
    detail: Optional[str] = None
    emr_step: Optional[EMRStepResult] = None


@task(name="run_ehr_silver", retries=1, tags=["spark", "silver", "ehr"])
def run_ehr_silver(
    fmt: str = "iceberg",
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Run the EHR silver transform on EMR.

    EHR silver is batch (reads local JSON fixtures or FHIR snapshots; not
    Kafka-driven). Submits as a single EMR step.
    """
    log = get_run_logger()
    step = submit_emr_step.fn(
        job_name=f"ehr-silver-{fmt}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/transformations/bronze_to_silver/ehr_silver.py",
        script_args=["--format", fmt],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
    )
    log.info(f"ehr_silver done in {step.duration_seconds:.0f}s")
    return JobResult(
        job_name="ehr_silver",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="run_pharmacy_silver", retries=1, tags=["spark", "silver", "pharmacy"])
def run_pharmacy_silver(
    fmt: str = "iceberg",
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Pharmacy silver — reads bronze pharmacy_events Iceberg, writes
    silver pharmacy_fills."""
    log = get_run_logger()
    step = submit_emr_step.fn(
        job_name=f"pharmacy-silver-{fmt}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/transformations/bronze_to_silver/pharmacy_silver.py",
        script_args=["--format", fmt],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
    )
    log.info(f"pharmacy_silver done in {step.duration_seconds:.0f}s")
    return JobResult(
        job_name="pharmacy_silver",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="run_identity_bridge", retries=1, tags=["spark", "identity"])
def run_identity_bridge(
    fmt: str = "iceberg",
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Identity bridge — phases 1-4. Reads silver EHR + silver sensor +
    silver pharmacy; writes silver identity_bridge."""
    log = get_run_logger()
    step = submit_emr_step.fn(
        job_name=f"identity-bridge-{fmt}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/transformations/identity_resolution/patient_identity_bridge.py",
        script_args=["--format", fmt],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
    )
    log.info(f"identity_bridge done in {step.duration_seconds:.0f}s")
    return JobResult(
        job_name="identity_bridge",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="run_gold_dim", retries=1, tags=["spark", "gold", "dim"])
def run_gold_dim(
    dim_name: str,
    fmt: str = "iceberg",
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Run a single gold dim transform (dim_patient, dim_metric, ...)."""
    log = get_run_logger()
    step = submit_emr_step.fn(
        job_name=f"gold-{dim_name}-{fmt}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/transformations/silver_to_gold/{dim_name}.py",
        script_args=["--format", fmt],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
    )
    log.info(f"{dim_name} done in {step.duration_seconds:.0f}s")
    return JobResult(
        job_name=dim_name,
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="run_gold_fact", retries=1, tags=["spark", "gold", "fact"])
def run_gold_fact(
    fact_name: str,
    mode: str = "batch",
    fmt: str = "iceberg",
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Run a gold fact transform in batch or streaming mode."""
    log = get_run_logger()
    step = submit_emr_step.fn(
        job_name=f"gold-{fact_name}-{mode}-{fmt}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/transformations/silver_to_gold/{fact_name}.py",
        script_args=["--mode", mode, "--format", fmt],
        spark_packages=SPARK_PACKAGES_DEFAULT if mode == "streaming" else None,
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
    )
    log.info(f"{fact_name} ({mode}) done in {step.duration_seconds:.0f}s")
    return JobResult(
        job_name=fact_name,
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="run_migration_apply", retries=1, tags=["spark", "migration"])
def run_migration_apply(
    catalog: str = "glue_iceberg",
    cluster_id: Optional[str] = None,
) -> JobResult:
    """Apply pending Glacierbase migrations via spark-submit migrations/cli.py run."""
    log = get_run_logger()
    step = submit_emr_step.fn(
        job_name=f"migration-apply-{catalog}",
        script_s3_path=f"s3://{DEFAULT_BUCKET}/code/migrations/cli.py",
        script_args=["--catalog", catalog, "run"],
        spark_jars=SPARK_JARS_DEFAULT,
        cluster_id=cluster_id,
        timeout_seconds=900,  # migrations are quick; tighter timeout
    )
    log.info(f"migrations applied in {step.duration_seconds:.0f}s")
    return JobResult(
        job_name="migration_apply",
        succeeded=step.succeeded,
        duration_seconds=step.duration_seconds,
        emr_step=step,
    )


@task(name="run_whoop_poller", retries=2, tags=["whoop", "producer"])
def run_whoop_poller(
    lookback_days: int = 7,
    interval_minutes: int = 30,
) -> JobResult:
    """Run one WHOOP API poll cycle on the Prefect worker (NOT on EMR).

    Why on the worker: the WHOOP API connector is a Python REST client +
    Kafka producer. It doesn't need Spark. Running on a Prefect worker
    is simpler than an EMR step.

    For continuous polling, schedule this task with an interval-based
    deployment (every 15-30 min).
    """
    import time as time_mod

    log = get_run_logger()
    started = time_mod.time()

    cmd = [
        "python3",
        "data_generators/whoop_api/producer.py",
        "--mode",
        "once",  # Prefect handles the schedule
        "--lookback-days",
        str(lookback_days),
    ]
    log.info(f"WHOOP poller starting: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=os.environ.get("PT_PROJECT_ROOT", "/home/hadoop/pulsetrack-cm"),
        capture_output=True,
        text=True,
        timeout=interval_minutes * 60 - 60,  # leave 1 min buffer
    )
    duration = time_mod.time() - started

    if result.returncode != 0:
        log.error(f"whoop_poller failed: {result.stderr[-500:]}")
        return JobResult(
            job_name="whoop_poller",
            succeeded=False,
            duration_seconds=duration,
            detail=result.stderr[-500:],
        )

    log.info(f"whoop_poller done in {duration:.0f}s")
    return JobResult(
        job_name="whoop_poller",
        succeeded=True,
        duration_seconds=duration,
        detail=result.stdout[-200:],
    )
