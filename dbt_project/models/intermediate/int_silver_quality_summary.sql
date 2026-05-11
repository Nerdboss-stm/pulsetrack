{{ config(materialized='ephemeral', tags=['intermediate', 'observability', 'quality']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- int_silver_quality_summary
-- ────────────────────────────────────────────────────────────────────────────
-- Daily silver-layer data-quality summary. Aggregates the per-run
-- observability ledger (stg_observability_monitor_runs) plus the rolling
-- validity flags inferred from stg_sensor_readings into a single row per
-- (run_date, table_name).
--
-- Powers the operations dashboard: "for each silver table, on each day,
-- how clean is the data?" — broken out by:
--   - null rate (estimated from rows_failed in the monitor ledger)
--   - validity rate (1 - invalid_reading_rate from sensor stage)
--   - late-arrival rate
--   - quality-monitor pass rate (GX expectation-pass percentage)
--
-- Grain: (run_date, table_name).
--
-- Used by:
--   - daily_health_summary (data-confidence column)
--   - patient_cohort_segmentation (drops days where data quality is poor)
-- ────────────────────────────────────────────────────────────────────────────

WITH monitor_runs AS (
    SELECT * FROM {{ ref('stg_observability_monitor_runs') }}
    WHERE layer = 'silver'
),

monitor_daily AS (
    SELECT
        run_date,
        table_name,
        COUNT(*)                                  AS monitor_run_count,
        SUM(rows_evaluated)                       AS rows_evaluated,
        SUM(rows_failed)                          AS rows_failed,
        SUM(expectations_run)                     AS expectations_run,
        SUM(expectations_failed)                  AS expectations_failed,
        AVG(expectation_pass_rate)                AS avg_expectation_pass_rate,
        SUM(CASE WHEN is_healthy THEN 1 ELSE 0 END) AS healthy_run_count,
        AVG(duration_seconds)                     AS avg_duration_seconds
    FROM monitor_runs
    GROUP BY run_date, table_name
),

-- Sensor-readings-specific summary. The sensor staging carries per-row
-- validity + late-arrival flags that the monitor ledger doesn't capture.
sensor_daily AS (
    SELECT
        event_date                                AS run_date,
        'sensor_readings'                         AS table_name,
        COUNT(*)                                  AS reading_count,
        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) AS valid_count,
        SUM(CASE WHEN is_late_arriving THEN 1 ELSE 0 END) AS late_arrival_count,
        SUM(CASE WHEN battery_pct < 20 THEN 1 ELSE 0 END) AS low_battery_count,
        {{ pulsetrack.safe_divide(
            'SUM(CASE WHEN is_valid THEN 1 ELSE 0 END)',
            'COUNT(*)',
            default='NULL'
        ) }} AS validity_rate,
        {{ pulsetrack.safe_divide(
            'SUM(CASE WHEN is_late_arriving THEN 1 ELSE 0 END)',
            'COUNT(*)',
            default='NULL'
        ) }} AS late_arrival_rate
    FROM {{ ref('stg_sensor_readings') }}
    GROUP BY event_date
)

SELECT
    -- Surrogate over (run_date, table_name). Primary key.
    {{ pulsetrack.generate_sha256_key(['m.run_date', 'm.table_name']) }}
        AS quality_summary_key,

    m.run_date,
    m.table_name,
    m.monitor_run_count,
    m.rows_evaluated,
    m.rows_failed,
    m.expectations_run,
    m.expectations_failed,
    m.avg_expectation_pass_rate,
    m.healthy_run_count,
    m.avg_duration_seconds,

    -- Sensor-specific metrics (NULL for non-sensor tables).
    s.reading_count,
    s.valid_count,
    s.late_arrival_count,
    s.low_battery_count,
    s.validity_rate,
    s.late_arrival_rate,

    -- Aggregate "null %" proxy from monitor: failed rows / evaluated rows.
    {{ pulsetrack.safe_divide(
        'm.rows_failed',
        'm.rows_evaluated',
        default='NULL'
    ) }} AS null_rate_estimate
FROM monitor_daily AS m
LEFT JOIN sensor_daily AS s
    ON  s.run_date   = m.run_date
    AND s.table_name = m.table_name
