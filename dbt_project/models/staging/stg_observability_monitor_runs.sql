{{ config(
    materialized='view',
    tags=['staging', 'observability', 'silver_consumer']
) }}

-- ────────────────────────────────────────────────────────────────────────────
-- stg_observability_monitor_runs
-- ────────────────────────────────────────────────────────────────────────────
-- 1:1 staging view over the observability ``monitor_runs`` table — the
-- per-run audit ledger of the streaming pipeline's data-quality monitors
-- (Great Expectations suite executions + custom quality gates).
--
-- One row per (monitor_name, run_id) emitted by data_quality/run_all_suites.py.
-- Captures pass/fail/skipped status, row counts evaluated, and failure
-- summaries.
--
-- Used by:
--   - int_silver_quality_summary (daily roll-up across monitors)
--   - downstream BI dashboards (operations health)
--
-- Cast policy: timestamps explicit; numeric columns explicit cast; everything
-- else passes through unchanged.
-- ────────────────────────────────────────────────────────────────────────────

WITH source AS (
    SELECT * FROM {{ source('observability', 'monitor_runs') }}
),

renamed AS (
    SELECT
        -- Surrogate over (monitor_name, run_id) — guarantees uniqueness even
        -- if the run_id collides across monitors.
        {{ pulsetrack.generate_sha256_key(['monitor_name', 'run_id']) }}
            AS monitor_run_key,

        run_id,
        monitor_name,
        suite_name,
        layer,                              -- bronze | silver | gold
        table_name,
        run_status,                         -- success | failure | skipped
        CAST(rows_evaluated AS BIGINT)      AS rows_evaluated,
        CAST(rows_failed AS BIGINT)         AS rows_failed,
        CAST(expectations_run AS INTEGER)   AS expectations_run,
        CAST(expectations_failed AS INTEGER) AS expectations_failed,
        CAST(started_at AS TIMESTAMP)       AS started_at,
        CAST(finished_at AS TIMESTAMP)      AS finished_at,
        failure_summary,
        environment,                        -- prod | dev | local

        -- Derived: pass-rate as a float. NULL-safe.
        {{ pulsetrack.safe_divide(
            'expectations_run - expectations_failed',
            'expectations_run',
            default='NULL'
        ) }} AS expectation_pass_rate,

        -- Derived: duration in seconds for SLA tracking.
        EXTRACT(EPOCH FROM (finished_at - started_at))
                                            AS duration_seconds,

        -- Derived: date partition. Convenience for downstream grouping.
        CAST(started_at AS DATE)            AS run_date,

        -- Derived: is the run "healthy"? Status success AND no failed expectations.
        (run_status = 'success' AND COALESCE(expectations_failed, 0) = 0)
                                            AS is_healthy
    FROM source
)

SELECT * FROM renamed
