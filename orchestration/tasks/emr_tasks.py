"""
EMR step submission as Prefect tasks.

Pattern: every Spark job run in the cloud is an EMR step submitted via
the AWS API (NOT spark-submit over SSH). EMR steps:
  1. Are first-class API objects with their own lifecycle (PENDING →
     RUNNING → COMPLETED/FAILED/CANCELLED).
  2. Persist in EMR's step history (audit trail beats SSH-based runs).
  3. Get YARN-tracked + CloudWatch-logged automatically.
  4. Honor EMR's `concurrencyLevel` and the cluster's auto-termination
     policy correctly.

Each task here returns a structured result (step_id + final state +
log paths) so downstream Prefect tasks can chain on success or notify
on failure.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional

from prefect import get_run_logger, task
from prefect.tasks import exponential_backoff

# Cluster + bucket config from Prefect Blocks / env (in production these
# come from Prefect Secrets; locally from .env or AWS Parameter Store).
DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DEFAULT_EMR_CLUSTER_ID = os.environ.get("PT_EMR_CLUSTER_ID", "")
DEFAULT_BUCKET = os.environ.get("PT_LAKEHOUSE_BUCKET", "")

# Terminal step states — once in one of these, polling stops.
TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}
SUCCESS_STATES = {"COMPLETED"}


@dataclass
class EMRStepResult:
    """Outcome of a single EMR step. Returned from ``submit_emr_step``.

    Carries enough context for downstream notification tasks: the step
    name (human-readable), id (for the EMR console URL), final state,
    and CloudWatch log path (for incident drill-down).
    """

    step_id: str
    step_name: str
    state: str
    cluster_id: str
    duration_seconds: float
    stdout_s3: Optional[str] = None
    stderr_s3: Optional[str] = None
    failure_reason: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return self.state in SUCCESS_STATES


@task(
    name="submit_emr_step",
    retries=2,
    retry_delay_seconds=exponential_backoff(backoff_factor=60),
    tags=["emr", "spark"],
)
def submit_emr_step(
    job_name: str,
    script_s3_path: str,
    script_args: Optional[list[str]] = None,
    spark_packages: Optional[list[str]] = None,
    spark_jars: Optional[list[str]] = None,
    spark_conf: Optional[dict[str, str]] = None,
    cluster_id: Optional[str] = None,
    poll_interval_seconds: int = 15,
    timeout_seconds: int = 7200,
) -> EMRStepResult:
    """Submit a Spark job to EMR and wait for completion.

    Args:
        job_name: Operator-visible name (appears in EMR console step list
            and CloudWatch metrics). Convention: ``<flow>-<task>-<date>``.
        script_s3_path: S3 URI of the .py file to run. Must be uploaded
            ahead of time via ``upload_project_to_s3``.
        script_args: Positional + flag args to pass through to the script.
        spark_packages: Maven coords for ``--packages``. Empty list ok.
        spark_jars: ``--jars`` file paths (local to EMR nodes).
        spark_conf: ``--conf key=value`` overrides. The Python-3.11 +
            executor caps from prompt 4 are applied unless overridden.
        cluster_id: Defaults to ``PT_EMR_CLUSTER_ID`` env var.
        poll_interval_seconds: How often to check step state.
        timeout_seconds: Fail the task if step doesn't terminate by then.

    Returns:
        EMRStepResult with step_id + final state + log paths.

    Raises:
        RuntimeError: step entered a non-success terminal state, OR
            timeout exceeded. Prefect's retry decorator handles transient
            EMR API failures (rate-limited add_steps, etc.).
    """
    import boto3

    log = get_run_logger()
    cluster_id = cluster_id or DEFAULT_EMR_CLUSTER_ID
    if not cluster_id:
        raise ValueError(
            "cluster_id missing — set PT_EMR_CLUSTER_ID or pass cluster_id"
        )

    # Default Spark config matches what we landed on in prompt 4 + 5
    # (right-sized for the 2-node dev cluster; cap executors so three
    # streams + one batch fit concurrently).
    default_conf = {
        "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "/usr/bin/python3.11",
        "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3.11",
        "spark.executor.memory": "1g",
        "spark.executor.cores": "1",
        "spark.executor.instances": "2",
        "spark.dynamicAllocation.enabled": "false",
    }
    if spark_conf:
        default_conf.update(spark_conf)

    # Build the spark-submit command line.
    args: list[str] = ["spark-submit", "--master", "yarn", "--deploy-mode", "client"]
    if spark_packages:
        args += ["--packages", ",".join(spark_packages)]
    if spark_jars:
        args += ["--jars", ",".join(spark_jars)]
    for k, v in default_conf.items():
        args += ["--conf", f"{k}={v}"]
    args.append(script_s3_path)
    if script_args:
        args += script_args

    emr = boto3.client("emr", region_name=DEFAULT_REGION)
    log.info(f"Submitting EMR step '{job_name}' to cluster {cluster_id}")

    resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                "Name": job_name,
                "ActionOnFailure": "CONTINUE",  # don't terminate the cluster on step fail
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": args,
                },
            }
        ],
    )
    step_id = resp["StepIds"][0]
    log.info(f"Step submitted: id={step_id}")

    # Poll loop.
    started = time.time()
    while True:
        elapsed = time.time() - started
        if elapsed > timeout_seconds:
            log.error(f"Step {step_id} exceeded timeout {timeout_seconds}s")
            # Best-effort cancel so the cluster doesn't keep working on
            # something the operator wrote off.
            try:
                emr.cancel_steps(ClusterId=cluster_id, StepIds=[step_id])
            except Exception:  # noqa: BLE001
                pass
            raise RuntimeError(
                f"EMR step '{job_name}' timeout after {elapsed:.0f}s "
                f"(step_id={step_id})"
            )

        info = emr.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]
        state = info["Status"]["State"]

        if state in TERMINAL_STATES:
            failure_reason = (
                info["Status"].get("FailureDetails", {}).get("Reason")
                if state != "COMPLETED"
                else None
            )
            log_uri = info.get("Status", {}).get("FailureDetails", {}).get("LogFile")
            result = EMRStepResult(
                step_id=step_id,
                step_name=job_name,
                state=state,
                cluster_id=cluster_id,
                duration_seconds=elapsed,
                stdout_s3=log_uri,
                stderr_s3=log_uri,
                failure_reason=failure_reason,
            )

            if not result.succeeded:
                log.error(
                    f"EMR step '{job_name}' ended in {state}: "
                    f"{failure_reason or '(no reason)'}"
                )
                raise RuntimeError(
                    f"EMR step '{job_name}' failed (state={state}): "
                    f"{failure_reason or 'check CloudWatch logs'}"
                )

            log.info(
                f"EMR step '{job_name}' completed in {elapsed:.1f}s "
                f"(step_id={step_id})"
            )
            return result

        log.info(f"step {step_id} state={state} elapsed={elapsed:.0f}s")
        time.sleep(poll_interval_seconds)


@task(name="get_emr_cluster_state", retries=1, tags=["emr"])
def get_emr_cluster_state(cluster_id: Optional[str] = None) -> dict:
    """Return basic cluster state (used by streaming_monitor flow).

    Returns:
        Dict with state, master DNS, YARN app counts.
    """
    import boto3

    cluster_id = cluster_id or DEFAULT_EMR_CLUSTER_ID
    emr = boto3.client("emr", region_name=DEFAULT_REGION)

    info = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
    return {
        "cluster_id": cluster_id,
        "state": info["Status"]["State"],
        "master_dns": info.get("MasterPublicDnsName"),
        "release_label": info.get("ReleaseLabel"),
    }


@task(name="upload_project_to_s3", retries=2, tags=["emr"])
def upload_project_to_s3(
    project_root: str = "/Users/nerdboss-stm/pulsetrack-cm",
    s3_prefix: Optional[str] = None,
) -> str:
    """Tar + upload the project to S3 so EMR steps can fetch it.

    Returns:
        S3 URI of the tarball.
    """
    import subprocess
    import tempfile

    import boto3

    log = get_run_logger()
    bucket = DEFAULT_BUCKET
    s3_prefix = s3_prefix or "code/orchestration"

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tarball_path = tmp.name

    subprocess.run(
        [
            "tar",
            "-czf",
            tarball_path,
            "-C",
            project_root,
            "--exclude=.git",
            "--exclude=.venv",
            "--exclude=__pycache__",
            "--exclude=target",
            "--exclude=dbt_packages",
            "--exclude=logs",
            ".",
        ],
        check=True,
    )

    key = f"{s3_prefix}/project-{int(time.time())}.tar.gz"
    s3_uri = f"s3://{bucket}/{key}"

    s3 = boto3.client("s3", region_name=DEFAULT_REGION)
    s3.upload_file(tarball_path, bucket, key)
    os.unlink(tarball_path)

    log.info(f"Uploaded project tarball: {s3_uri}")
    return s3_uri
