"""
Persisted state for observability — the ``monitor_runs`` ledger.

Every :class:`observability.monitors.MonitorResult` is appended to an
Iceberg/Delta table. This gives operators:

  - A queryable history of data health (the core Monte Carlo value prop).
  - Rolling-baseline computation for volume monitors.
  - Schema-snapshot diff (read prior_columns from the latest run).

Schema (matches ``observability/sql/create_monitor_runs.sql``):

    monitor_name  STRING
    table_name    STRING
    check_type    STRING       -- 'freshness' | 'volume' | 'schema' | 'distribution'
    column        STRING       -- NULL except for distribution/schema column-level
    status        STRING       -- 'ok' | 'warn' | 'error'
    value         DOUBLE
    threshold     DOUBLE
    detail        STRING
    run_at        TIMESTAMP
    run_id        STRING       -- unique per invocation; ties multi-monitor batches

Partitioned by ``days(run_at)`` so historical queries prune to the right
time window.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Optional

from observability.monitors import MonitorResult

log = logging.getLogger(__name__)

DEFAULT_TABLE = os.environ.get(
    "PT_OBSERVABILITY_TABLE",
    "glue_iceberg.pulsetrack_gold_dev.monitor_runs",
)


def persist_results(
    results: list[MonitorResult],
    spark=None,
    table: str = DEFAULT_TABLE,
    run_id: Optional[str] = None,
) -> str:
    """Append each result as a row to the monitor_runs ledger.

    Args:
        results: list from :func:`run_all_monitors`.
        spark: SparkSession. When None, this is a stub (logs only).
        table: destination table FQN.
        run_id: shared identifier for this batch. Auto-generated if None.

    Returns:
        ``run_id`` (so callers can correlate logs / alerts).
    """
    run_id = run_id or str(uuid.uuid4())

    if spark is None:
        log.warning(
            "persist_results called with spark=None — %d results NOT persisted (run_id=%s)",
            len(results), run_id,
        )
        return run_id

    from pyspark.sql import Row

    rows = [
        Row(
            monitor_name=r.monitor_name,
            table_name=r.table_name,
            check_type=r.check_type,
            column=r.column,
            status=r.status,
            value=float(r.value) if r.value is not None else None,
            threshold=float(r.threshold) if r.threshold is not None else None,
            detail=r.detail,
            run_at=r.run_at,
            run_id=run_id,
        )
        for r in results
    ]

    df = spark.createDataFrame(rows)
    df.writeTo(table).append()
    log.info(
        "appended %d monitor_runs rows to %s (run_id=%s)",
        len(rows), table, run_id,
    )
    return run_id


def load_prior_schema_columns(
    table_fqn: str,
    spark=None,
    monitor_table: str = DEFAULT_TABLE,
) -> Optional[list[dict]]:
    """Read the most recent schema snapshot for ``table_fqn``.

    Used by :func:`monitors.check_schema` to detect diff vs the
    last-recorded schema.
    """
    if spark is None:
        return None

    row = spark.sql(f"""
        SELECT detail
        FROM {monitor_table}
        WHERE table_name = '{table_fqn}'
          AND check_type = 'schema'
        ORDER BY run_at DESC
        LIMIT 1
    """).first()

    if row is None or row["detail"] is None:
        return None

    # The detail is the human-readable summary; we don't roundtrip
    # the full schema through it. Production version would either:
    #  (a) extend the table with a `schema_json` column carrying the
    #      serialized column list, OR
    #  (b) read the Iceberg metadata directly (more accurate).
    # Returning None falls back to "no prior" → records baseline.
    return None


def compute_volume_baseline(
    table_fqn: str,
    days: int = 7,
    spark=None,
    monitor_table: str = DEFAULT_TABLE,
) -> tuple[Optional[float], Optional[float]]:
    """Compute the rolling N-day mean + stddev of row counts for a table.

    Reads from the monitor_runs ledger's volume entries.

    Returns:
        ``(mean, stddev)`` or ``(None, None)`` if insufficient history.
    """
    if spark is None:
        return (None, None)

    df = spark.sql(f"""
        SELECT value
        FROM {monitor_table}
        WHERE table_name  = '{table_fqn}'
          AND check_type  = 'volume'
          AND status     != 'error'
          AND run_at >= CURRENT_TIMESTAMP() - INTERVAL '{days}' DAY
    """).toPandas()

    if len(df) < 3:
        # Need at least 3 prior runs for a meaningful stddev.
        return (None, None)

    return (float(df["value"].mean()), float(df["value"].std()))
