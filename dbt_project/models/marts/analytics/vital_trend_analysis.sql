{{ config(materialized='table', tags=['gold', 'analytics', 'trend']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- vital_trend_analysis
-- ────────────────────────────────────────────────────────────────────────────
-- 7-day, 14-day, 30-day rolling windows for each (patient × metric × day).
-- Powers BI line charts: "is this patient's heart rate trending up vs their
-- 30-day baseline?".
--
-- Implementation: window functions over fact_vital_daily_summary partitioned
-- by (patient, metric) and ordered by date.
-- ────────────────────────────────────────────────────────────────────────────

WITH daily AS (
    SELECT
        patient_key,
        metric_key,
        metric_name,
        date_key,
        event_date,
        avg_value,
        valid_reading_count
    FROM {{ ref('fact_vital_daily_summary') }}
    WHERE valid_reading_count > 0
)

SELECT
    patient_key,
    metric_key,
    metric_name,
    date_key,
    event_date,
    avg_value AS daily_avg,

    -- 7-day rolling
    AVG(avg_value) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg,

    -- 14-day rolling
    AVG(avg_value) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS rolling_14d_avg,

    -- 30-day rolling avg + stddev (for z-score)
    AVG(avg_value) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_avg,
    STDDEV(avg_value) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_stddev,

    -- Z-score vs 30-day rolling baseline. Anomaly threshold from project var.
    {{ pulsetrack.safe_divide(
        '(avg_value - AVG(avg_value) OVER (PARTITION BY patient_key, metric_key ORDER BY event_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))',
        'STDDEV(avg_value) OVER (PARTITION BY patient_key, metric_key ORDER BY event_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)',
        default='NULL'
    ) }} AS z_score_30d,

    -- Day-over-day change.
    avg_value - LAG(avg_value, 1) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
    ) AS day_over_day_change,

    -- Week-over-week change.
    avg_value - LAG(avg_value, 7) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
    ) AS week_over_week_change,

    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM daily
