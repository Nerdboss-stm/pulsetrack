{{ config(
    materialized='table',
    tags=['gold', 'core', 'fact', 'vital'],
    indexes=[
      {'columns': ['patient_key', 'metric_key', 'date_key'], 'unique': true}
    ]
) }}

-- ────────────────────────────────────────────────────────────────────────────
-- fact_vital_daily_summary
-- ────────────────────────────────────────────────────────────────────────────
-- Per-patient × metric × day summary. The published gold table for BI
-- vital-trend dashboards.
--
-- Grain: (patient_key, metric_key, date_key) — unique. The MERGE INTO at
-- streaming gold (transformations/silver_to_gold/fact_vital_daily_summary.py)
-- is idempotent over this grain.
--
-- Source: int_vital_daily_agg (the aggregation) + dim_metric for metric_key.
-- ────────────────────────────────────────────────────────────────────────────

WITH agg AS (
    SELECT * FROM {{ ref('int_vital_daily_agg') }}
),

metric_lookup AS (
    SELECT metric_key, metric_name, unit
    FROM {{ ref('dim_metric') }}
)

SELECT
    a.patient_key,
    m.metric_key,
    CAST(STRFTIME(a.event_date, '%Y%m%d') AS INTEGER)         AS date_key,

    a.event_date,
    a.metric_name,
    m.unit,

    a.valid_reading_count,
    a.invalid_reading_count,

    a.avg_value,
    a.min_value,
    a.max_value,
    a.stddev_value,
    a.median_value,
    a.p10_value,
    a.p90_value,

    -- Validity rate for this patient×metric×day.
    {{ pulsetrack.safe_divide(
        'a.valid_reading_count',
        'a.valid_reading_count + a.invalid_reading_count',
        default='1.0'
    ) }} AS validity_rate,

    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM agg AS a
INNER JOIN metric_lookup AS m USING (metric_name)
