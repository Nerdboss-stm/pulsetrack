-- ============================================================================
-- create_monitor_runs.sql
-- ============================================================================
-- The observability ledger table. One row per (monitor, table, run).
--
-- Apply this via the Glacierbase migration framework as V006, OR run
-- directly via spark-sql once if you don't want it in the migration
-- ledger.
-- ============================================================================

CREATE TABLE IF NOT EXISTS glue_iceberg.pulsetrack_gold_dev.monitor_runs (
    monitor_name   STRING       COMMENT 'Logical monitor identifier (matches monitors.py function names)',
    table_name     STRING       COMMENT 'Fully-qualified table the monitor checked (db.schema.table)',
    check_type     STRING       COMMENT 'freshness | volume | schema | distribution',
    column         STRING       COMMENT 'NULL for table-level checks; column name for distribution/schema column checks',
    status         STRING       COMMENT 'ok | warn | error',
    value          DOUBLE       COMMENT 'Observed value (row count, null rate, age in minutes, etc.)',
    threshold      DOUBLE       COMMENT 'Threshold the check used (NULL when not applicable)',
    detail         STRING       COMMENT 'Human-readable summary suitable for Slack/email',
    run_at         TIMESTAMP    COMMENT 'When the monitor ran (UTC)',
    run_id         STRING       COMMENT 'Unique per invocation; correlates multiple monitor results for one run'
)
USING iceberg
PARTITIONED BY (days(run_at))
TBLPROPERTIES (
    'write.format.default'    = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '20'
);

-- Query helpers:
--   SELECT run_at, monitor_name, table_name, status, detail
--   FROM glue_iceberg.pulsetrack_gold_dev.monitor_runs
--   WHERE status != 'ok'
--     AND run_at >= CURRENT_TIMESTAMP() - INTERVAL '7' DAY
--   ORDER BY run_at DESC;

--   -- Volume baseline: 7-day rolling avg + stddev per table.
--   SELECT table_name, AVG(value) AS rolling_avg, STDDEV(value) AS rolling_stddev
--   FROM glue_iceberg.pulsetrack_gold_dev.monitor_runs
--   WHERE check_type = 'volume'
--     AND status != 'error'
--     AND run_at >= CURRENT_TIMESTAMP() - INTERVAL '7' DAY
--   GROUP BY table_name;
