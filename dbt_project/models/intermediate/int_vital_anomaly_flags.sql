{{ config(materialized='ephemeral', tags=['intermediate', 'vital', 'anomaly']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- int_vital_anomaly_flags
-- ────────────────────────────────────────────────────────────────────────────
-- Joins the per-patient daily vital aggregate (int_vital_daily_agg) with the
-- ``metric_normal_ranges`` seed to flag anomalies at the day grain.
--
-- Anomalies surfaced here power:
--   - daily_health_summary (one column per anomaly flag for the BI table)
--   - clinical_outcome_correlation (input feature; "days with anomalies
--     in the 30-day window before a hospitalization")
--
-- Flag taxonomy:
--   - is_high_anomaly    — avg_value > normal_max
--   - is_low_anomaly     — avg_value < normal_min
--   - is_critical_high   — avg_value > critical_max (plausibility breach)
--   - is_critical_low    — avg_value < critical_min
--   - any_anomaly        — short-circuit OR of the four above
--
-- We use AVG (not MIN/MAX) for the day-level flag because a single spike
-- isn't the same as a sustained issue. anomaly_investigation handles the
-- per-reading critical detection.
-- ────────────────────────────────────────────────────────────────────────────

WITH daily AS (
    SELECT * FROM {{ ref('int_vital_daily_agg') }}
),

ranges AS (
    SELECT
        metric_name,
        CAST(normal_min AS DOUBLE)    AS normal_min,
        CAST(normal_max AS DOUBLE)    AS normal_max,
        CAST(critical_min AS DOUBLE)  AS critical_min,
        CAST(critical_max AS DOUBLE)  AS critical_max,
        category
    FROM {{ ref('metric_normal_ranges') }}
),

flagged AS (
    SELECT
        d.patient_key,
        d.metric_name,
        d.event_date,
        d.valid_reading_count,
        d.invalid_reading_count,
        d.avg_value,
        d.min_value,
        d.max_value,
        d.stddev_value,

        r.normal_min,
        r.normal_max,
        r.critical_min,
        r.critical_max,
        r.category                          AS metric_category,

        -- Per-day anomaly flags. Uses avg_value so the day's "central
        -- tendency" must clear the range — a single warning reading doesn't
        -- flag the whole day, but a sustained warning does.
        (d.avg_value > r.normal_max)        AS is_high_anomaly,
        (d.avg_value < r.normal_min)        AS is_low_anomaly,
        (d.max_value > r.critical_max)      AS is_critical_high,
        (d.min_value < r.critical_min)      AS is_critical_low,

        -- Any anomaly — short-circuit OR. Used as the headline flag.
        (
            d.avg_value > r.normal_max
            OR d.avg_value < r.normal_min
            OR d.max_value > r.critical_max
            OR d.min_value < r.critical_min
        )                                   AS any_anomaly,

        -- Distance-from-normal (in normal-range widths). Convenience for
        -- "how bad?" sorting in dashboards.
        {{ pulsetrack.safe_divide(
            'GREATEST(d.avg_value - r.normal_max, r.normal_min - d.avg_value, 0)',
            'r.normal_max - r.normal_min',
            default='0'
        ) }} AS anomaly_severity_score
    FROM daily AS d
    INNER JOIN ranges AS r
        ON d.metric_name = r.metric_name
)

SELECT * FROM flagged
