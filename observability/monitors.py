"""
Monte Carlo-style data observability monitors.

Four monitor types, run on a schedule by Prefect's
``maintenance_pipeline`` or directly via the CLI:

  1. Freshness    — every table checked against its SLA
                    (streaming < 5 min, batch < 24 hr).
  2. Volume       — daily row counts with 7-day rolling-avg anomaly
                    detection (3-sigma deviation flags).
  3. Schema       — daily schema snapshots with column-add/drop/rename
                    diff alerting.
  4. Distribution — null_rate, distinct_count, min/max tracked per
                    numeric/string column.

Each monitor returns a :class:`MonitorResult` that's appended to the
``observability.monitor_runs`` ledger (see ``state.py``). Alerts fire
on any non-OK result via :mod:`observability.alerting`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)


# ── Result dataclasses ──────────────────────────────────────────────────────


@dataclass
class MonitorResult:
    """One monitor's check on one (table, column?) pair."""

    monitor_name: str
    table_name: str               # ``database.schema.table``
    check_type: str               # 'freshness' | 'volume' | 'schema' | 'distribution'
    status: str                   # 'ok' | 'warn' | 'error'
    value: Optional[float] = None
    threshold: Optional[float] = None
    detail: str = ""
    column: Optional[str] = None  # set for distribution/schema column-level checks
    run_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def passed(self) -> bool:
        return self.status == "ok"


# ── 1. Freshness ────────────────────────────────────────────────────────────


def check_freshness(
    table_fqn: str,
    timestamp_column: str,
    max_age_minutes: int,
    spark=None,
) -> MonitorResult:
    """Assert ``max(timestamp_column)`` is within ``max_age_minutes``.

    Args:
        table_fqn: ``db.schema.table`` Iceberg fully-qualified name.
        timestamp_column: column holding event/load timestamp (e.g.,
            ``ingestion_timestamp`` for bronze, ``event_timestamp`` for
            silver/gold).
        max_age_minutes: SLA threshold. 5 min for streaming;
            24*60 for daily-batch tables.
        spark: optional SparkSession. If None, the check is a stub.

    Returns:
        MonitorResult with status='ok' if fresh; 'error' if stale.
    """
    if spark is None:
        return MonitorResult(
            monitor_name="freshness",
            table_name=table_fqn,
            check_type="freshness",
            status="ok",
            value=None,
            threshold=float(max_age_minutes),
            detail="(stub) — provide spark to compute MAX(timestamp_column)",
        )

    sql = f"SELECT MAX({timestamp_column}) AS latest FROM {table_fqn}"
    row = spark.sql(sql).first()
    latest = row[0]
    if latest is None:
        return MonitorResult(
            monitor_name="freshness",
            table_name=table_fqn,
            check_type="freshness",
            status="error",
            detail="table is empty — no timestamps to check",
        )

    now = datetime.now(timezone.utc)
    age = (now - latest.replace(tzinfo=timezone.utc)).total_seconds() / 60.0
    passed = age <= max_age_minutes
    return MonitorResult(
        monitor_name="freshness",
        table_name=table_fqn,
        check_type="freshness",
        status="ok" if passed else "error",
        value=age,
        threshold=float(max_age_minutes),
        detail=(
            f"latest {timestamp_column}={latest.isoformat()}, age={age:.1f}min "
            f"(threshold={max_age_minutes}min)"
        ),
    )


# ── 2. Volume ───────────────────────────────────────────────────────────────


def check_volume(
    table_fqn: str,
    expected_min: Optional[int] = None,
    rolling_avg: Optional[float] = None,
    rolling_stddev: Optional[float] = None,
    sigma_threshold: float = 3.0,
    spark=None,
) -> MonitorResult:
    """Assert today's row count is within sigma_threshold * stddev of
    the rolling N-day average.

    Caller is responsible for providing ``rolling_avg`` and
    ``rolling_stddev`` from the state ledger (see
    ``state.compute_volume_baseline``). This function is the
    point-in-time check; baselines come from the ledger.

    Args:
        table_fqn: db.schema.table.
        expected_min: optional absolute lower bound (e.g., "should
            always have at least 1 row today").
        rolling_avg: mean row count over the prior 7 days.
        rolling_stddev: stddev row count over the prior 7 days.
        sigma_threshold: flag if |today - avg| > sigma * stddev.
        spark: SparkSession.

    Returns:
        MonitorResult with the actual row count + z-score.
    """
    if spark is None:
        return MonitorResult(
            monitor_name="volume",
            table_name=table_fqn,
            check_type="volume",
            status="ok",
            detail="(stub) — provide spark",
        )

    actual = spark.sql(f"SELECT COUNT(*) AS n FROM {table_fqn}").first()[0]

    # Absolute min check.
    if expected_min is not None and actual < expected_min:
        return MonitorResult(
            monitor_name="volume",
            table_name=table_fqn,
            check_type="volume",
            status="error",
            value=float(actual),
            threshold=float(expected_min),
            detail=f"row_count={actual} below expected_min={expected_min}",
        )

    # Rolling-baseline z-score check.
    if rolling_avg is not None and rolling_stddev is not None and rolling_stddev > 0:
        z = (actual - rolling_avg) / rolling_stddev
        if abs(z) > sigma_threshold:
            return MonitorResult(
                monitor_name="volume",
                table_name=table_fqn,
                check_type="volume",
                status="warn",
                value=float(actual),
                threshold=rolling_avg,
                detail=(
                    f"row_count={actual} | rolling_avg={rolling_avg:.0f} "
                    f"stddev={rolling_stddev:.0f} | z-score={z:.2f} "
                    f"(threshold={sigma_threshold:.1f}σ)"
                ),
            )

    return MonitorResult(
        monitor_name="volume",
        table_name=table_fqn,
        check_type="volume",
        status="ok",
        value=float(actual),
        detail=f"row_count={actual}",
    )


# ── 3. Schema ───────────────────────────────────────────────────────────────


def check_schema(
    table_fqn: str,
    prior_columns: Optional[list[dict]] = None,
    spark=None,
) -> MonitorResult:
    """Compare current schema vs prior snapshot. Flag adds/drops/renames.

    Args:
        table_fqn: db.schema.table.
        prior_columns: list of {name, type} from the last snapshot
            (loaded from the state ledger).
        spark: SparkSession.

    Returns:
        MonitorResult with diff detail.
    """
    if spark is None:
        return MonitorResult(
            monitor_name="schema",
            table_name=table_fqn,
            check_type="schema",
            status="ok",
            detail="(stub) — provide spark",
        )

    # DESCRIBE returns columns + types. ``str.lower()`` for case-
    # insensitive comparison (Snowflake uppercases; Spark lowercases).
    df = spark.sql(f"DESCRIBE TABLE {table_fqn}")
    current = [
        {"name": r["col_name"].lower(), "type": r["data_type"].lower()}
        for r in df.collect()
        if r["col_name"] and not r["col_name"].startswith("#")
    ]
    current_json = json.dumps(current, sort_keys=True)

    if prior_columns is None:
        # First snapshot — record current as baseline.
        return MonitorResult(
            monitor_name="schema",
            table_name=table_fqn,
            check_type="schema",
            status="ok",
            detail=f"first snapshot — recorded {len(current)} columns",
            value=float(len(current)),
        )

    prior_names = {c["name"] for c in prior_columns}
    current_names = {c["name"] for c in current}
    added = current_names - prior_names
    removed = prior_names - current_names

    # Type changes: same name, different type.
    prior_typed = {c["name"]: c["type"] for c in prior_columns}
    type_changed = [
        c["name"] for c in current
        if c["name"] in prior_typed and prior_typed[c["name"]] != c["type"]
    ]

    if not (added or removed or type_changed):
        return MonitorResult(
            monitor_name="schema",
            table_name=table_fqn,
            check_type="schema",
            status="ok",
            detail=f"schema unchanged ({len(current)} columns)",
            value=float(len(current)),
        )

    severity = "error" if removed or type_changed else "warn"
    detail_parts = []
    if added:
        detail_parts.append(f"ADDED: {sorted(added)}")
    if removed:
        detail_parts.append(f"REMOVED: {sorted(removed)}")
    if type_changed:
        detail_parts.append(f"TYPE_CHANGED: {sorted(type_changed)}")

    return MonitorResult(
        monitor_name="schema",
        table_name=table_fqn,
        check_type="schema",
        status=severity,
        detail=" | ".join(detail_parts),
        value=float(len(current)),
    )


# ── 4. Distribution ─────────────────────────────────────────────────────────


def check_distribution(
    table_fqn: str,
    column: str,
    expected_null_rate_max: Optional[float] = None,
    expected_distinct_min: Optional[int] = None,
    value_min: Optional[float] = None,
    value_max: Optional[float] = None,
    spark=None,
) -> MonitorResult:
    """Per-column distribution sanity check.

    Args:
        table_fqn: db.schema.table.
        column: column name.
        expected_null_rate_max: 0-1; if NULL rate exceeds, flag.
        expected_distinct_min: alert if distinct count drops below.
        value_min/value_max: alert if values exceed expected range.

    Returns:
        MonitorResult capturing the actual distribution stats.
    """
    if spark is None:
        return MonitorResult(
            monitor_name="distribution",
            table_name=table_fqn,
            check_type="distribution",
            column=column,
            status="ok",
            detail="(stub) — provide spark",
        )

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                    AS total,
            SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)           AS nulls,
            COUNT(DISTINCT {column})                                    AS distinct_count,
            MIN({column})                                               AS min_val,
            MAX({column})                                               AS max_val
        FROM {table_fqn}
    """).first()

    total = stats["total"] or 0
    nulls = stats["nulls"] or 0
    distinct = stats["distinct_count"] or 0
    min_v = stats["min_val"]
    max_v = stats["max_val"]
    null_rate = (nulls / total) if total > 0 else 0.0

    detail = (
        f"total={total} nulls={nulls} null_rate={null_rate:.2%} "
        f"distinct={distinct} min={min_v} max={max_v}"
    )

    # Null-rate check.
    if expected_null_rate_max is not None and null_rate > expected_null_rate_max:
        return MonitorResult(
            monitor_name="distribution",
            table_name=table_fqn,
            check_type="distribution",
            column=column,
            status="warn",
            value=null_rate,
            threshold=expected_null_rate_max,
            detail=f"null_rate={null_rate:.2%} > threshold={expected_null_rate_max:.2%} | {detail}",
        )

    # Distinct-count check.
    if expected_distinct_min is not None and distinct < expected_distinct_min:
        return MonitorResult(
            monitor_name="distribution",
            table_name=table_fqn,
            check_type="distribution",
            column=column,
            status="warn",
            value=float(distinct),
            threshold=float(expected_distinct_min),
            detail=f"distinct={distinct} < threshold={expected_distinct_min} | {detail}",
        )

    # Value-range check (only meaningful for numeric).
    if value_min is not None and min_v is not None and float(min_v) < value_min:
        return MonitorResult(
            monitor_name="distribution",
            table_name=table_fqn,
            check_type="distribution",
            column=column,
            status="warn",
            value=float(min_v),
            threshold=value_min,
            detail=f"min={min_v} < expected_min={value_min} | {detail}",
        )
    if value_max is not None and max_v is not None and float(max_v) > value_max:
        return MonitorResult(
            monitor_name="distribution",
            table_name=table_fqn,
            check_type="distribution",
            column=column,
            status="warn",
            value=float(max_v),
            threshold=value_max,
            detail=f"max={max_v} > expected_max={value_max} | {detail}",
        )

    return MonitorResult(
        monitor_name="distribution",
        table_name=table_fqn,
        check_type="distribution",
        column=column,
        status="ok",
        value=null_rate,
        detail=detail,
    )


# ── Compose a monitor suite from a YAML spec ────────────────────────────────


def run_all_monitors(spec: dict, spark=None) -> list[MonitorResult]:
    """Run every monitor declared in ``spec`` and return aggregated results.

    Spec shape (see observability/sql/monitor_spec.example.yaml):

        tables:
          - fqn: glue_iceberg.pulsetrack_silver_dev.sensor_readings
            freshness:
              timestamp_column: ingestion_timestamp
              max_age_minutes: 5
            volume:
              expected_min: 1
            schema: {}
            distribution:
              - column: battery_pct
                value_min: 0
                value_max: 100
                expected_null_rate_max: 0.05
    """
    results: list[MonitorResult] = []
    for tbl in spec.get("tables", []):
        fqn = tbl["fqn"]

        if "freshness" in tbl:
            cfg = tbl["freshness"]
            results.append(check_freshness(
                table_fqn=fqn,
                timestamp_column=cfg["timestamp_column"],
                max_age_minutes=cfg["max_age_minutes"],
                spark=spark,
            ))

        if "volume" in tbl:
            cfg = tbl["volume"]
            results.append(check_volume(
                table_fqn=fqn,
                expected_min=cfg.get("expected_min"),
                rolling_avg=cfg.get("rolling_avg"),
                rolling_stddev=cfg.get("rolling_stddev"),
                spark=spark,
            ))

        if "schema" in tbl:
            results.append(check_schema(
                table_fqn=fqn,
                prior_columns=tbl["schema"].get("prior_columns"),
                spark=spark,
            ))

        for dist in tbl.get("distribution", []):
            results.append(check_distribution(
                table_fqn=fqn,
                column=dist["column"],
                expected_null_rate_max=dist.get("expected_null_rate_max"),
                expected_distinct_min=dist.get("expected_distinct_min"),
                value_min=dist.get("value_min"),
                value_max=dist.get("value_max"),
                spark=spark,
            ))

    log.info(
        "ran %d monitors: %d ok, %d warn, %d error",
        len(results),
        sum(1 for r in results if r.status == "ok"),
        sum(1 for r in results if r.status == "warn"),
        sum(1 for r in results if r.status == "error"),
    )
    return results
