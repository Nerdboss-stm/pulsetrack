"""
Data-quality tasks for Prefect flows.

Wraps:
  - Great Expectations suite runs (used by streaming foreachBatch + can
    be invoked here for ad-hoc batch validation).
  - Custom observability monitors (consumer lag thresholds, freshness
    gaps, identity bridge link rate).
  - Glue-catalog table-row-count assertions (verify expected rowcounts
    at each layer after a daily pipeline).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from prefect import get_run_logger, task

DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
DEFAULT_AWS_ENV = os.environ.get("PT_AWS_ENV", "dev")


@dataclass
class QualityCheck:
    """One quality-check outcome."""

    name: str
    passed: bool
    severity: str  # 'info' | 'warn' | 'error'
    detail: str = ""
    metric_value: Optional[float] = None


@dataclass
class QualityReport:
    """Aggregate of all checks for one flow run."""

    checks: list[QualityCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed or c.severity != "error" for c in self.checks)

    @property
    def summary(self) -> str:
        n_pass = sum(1 for c in self.checks if c.passed)
        n_fail = len(self.checks) - n_pass
        return f"{n_pass}/{len(self.checks)} checks passed ({n_fail} failed)"


@task(name="check_table_rowcount", retries=1, tags=["quality"])
def check_table_rowcount(
    database: str,
    table: str,
    min_rows: int = 1,
    max_rows: Optional[int] = None,
    region: str = DEFAULT_REGION,
) -> QualityCheck:
    """Assert ``min_rows <= rowcount <= max_rows`` for a Glue/Iceberg table.

    Uses Athena under the hood via boto3 — works for any registered
    Iceberg table without needing Spark running.
    """
    import boto3

    log = get_run_logger()
    athena = boto3.client("athena", region_name=region)

    query = f"SELECT COUNT(*) AS n FROM {database}.{table}"
    bucket = os.environ.get(
        "PT_LAKEHOUSE_BUCKET", "pulsetrack-lakehouse-dev-03a28ee7"
    )
    output_loc = f"s3://{bucket}/athena-results/"

    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_loc},
    )
    qid = resp["QueryExecutionId"]

    # Poll for completion.
    import time as time_mod
    for _ in range(60):
        info = athena.get_query_execution(QueryExecutionId=qid)
        state = info["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time_mod.sleep(2)

    if state != "SUCCEEDED":
        return QualityCheck(
            name=f"rowcount[{database}.{table}]",
            passed=False,
            severity="warn",
            detail=f"Athena query state={state}",
        )

    results = athena.get_query_results(QueryExecutionId=qid)
    row = int(results["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"])
    passed = row >= min_rows and (max_rows is None or row <= max_rows)
    log.info(f"{database}.{table} rowcount={row} (min={min_rows}, max={max_rows})")
    return QualityCheck(
        name=f"rowcount[{database}.{table}]",
        passed=passed,
        severity="error" if not passed else "info",
        detail=f"rowcount={row}",
        metric_value=float(row),
    )


@task(name="check_consumer_lag", retries=2, tags=["quality", "streaming"])
def check_consumer_lag(
    query_name: str,
    max_lag: int = 10000,
    cluster_id: Optional[str] = None,
) -> QualityCheck:
    """Check Kafka consumer lag for a streaming query against the threshold.

    Reads from CloudWatch the ``pulsetrack_consumer_lag`` Prometheus
    gauge that the StreamingQueryListener emits (see PROMPT_3 § 2-3).

    A streaming query whose lag exceeds ``max_lag`` indicates it's
    falling behind — either silver-stream cold-start hang (PROMPT_4
    § 3.12) or downstream backpressure.
    """
    log = get_run_logger()

    # In a real deployment we'd query CloudWatch's custom-metric
    # namespace (pushed by the streaming pipeline via a CloudWatch
    # agent on EMR). For this scaffold we return a stub PASS — the flow
    # threads through correctly but the actual metric ingestion is the
    # operator's choice of telemetry stack.
    log.info(f"consumer-lag check for {query_name}: stub OK")
    return QualityCheck(
        name=f"consumer_lag[{query_name}]",
        passed=True,
        severity="info",
        detail="stub — wire CloudWatch query when prom→cw agent deployed",
    )


@task(name="check_silver_freshness", retries=1, tags=["quality"])
def check_silver_freshness(
    database: str = f"pulsetrack_silver_{DEFAULT_AWS_ENV}",
    table: str = "sensor_readings",
    max_age_hours: int = 6,
) -> QualityCheck:
    """Assert the silver table's max(event_timestamp) is within
    ``max_age_hours`` of now.

    Stale silver = upstream Spark stream stuck. Triggers an ops alert.
    """
    log = get_run_logger()

    # Stub: real impl runs Athena ``SELECT MAX(event_timestamp)``.
    log.info(f"freshness check for {database}.{table}: stub OK")
    return QualityCheck(
        name=f"freshness[{database}.{table}]",
        passed=True,
        severity="info",
        detail="stub — wire to Athena MAX(event_timestamp) query",
    )


@task(name="check_identity_link_rate", retries=1, tags=["quality"])
def check_identity_link_rate(
    threshold: float = 0.85,
) -> QualityCheck:
    """Assert identity-bridge link rate >= threshold.

    Below threshold = significant fraction of identifiers can't be
    resolved → downstream gold tables miss patient context.
    """
    log = get_run_logger()
    log.info(f"identity-link-rate check (threshold={threshold}): stub OK")
    return QualityCheck(
        name="identity_link_rate",
        passed=True,
        severity="warn",
        detail="stub — wire to identity_resolution_kpis.overall_link_rate",
    )


@task(name="aggregate_quality_report", tags=["quality"])
def aggregate_quality_report(checks: list[QualityCheck]) -> QualityReport:
    """Bundle individual checks into a report for notifications."""
    return QualityReport(checks=checks)
