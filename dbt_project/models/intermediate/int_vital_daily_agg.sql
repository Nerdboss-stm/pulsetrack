{{ config(materialized='ephemeral', tags=['intermediate', 'vital']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- int_vital_daily_agg
-- ────────────────────────────────────────────────────────────────────────────
-- Patient × metric × date aggregation over staging sensor readings.
-- Intermediate (ephemeral) — never materialized; compiled into the
-- ``fact_vital_daily_summary`` mart on the next layer down.
--
-- Joins to identity_bridge to resolve device_account_id → patient_key.
-- Filters to is_valid only (out-of-range readings don't count toward
-- aggregates) but keeps a ``invalid_reading_count`` for QA.
-- ────────────────────────────────────────────────────────────────────────────

WITH sensor AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
),

bridge AS (
    SELECT
        identifier_value AS device_account_id,
        patient_key
    FROM {{ ref('stg_identity_bridge') }}
    WHERE identifier_type = 'device_account_id'
      AND link_status = 'linked'
),

joined AS (
    SELECT
        b.patient_key,
        s.metric_name,
        s.event_date,
        s.metric_value,
        s.is_valid
    FROM sensor AS s
    INNER JOIN bridge AS b USING (device_account_id)
    -- Drop rows without a resolved patient — they don't belong in gold yet.
    WHERE b.patient_key IS NOT NULL
),

aggregated AS (
    SELECT
        patient_key,
        metric_name,
        event_date,

        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END)             AS valid_reading_count,
        SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END)         AS invalid_reading_count,

        AVG(CASE WHEN is_valid THEN metric_value END)         AS avg_value,
        MIN(CASE WHEN is_valid THEN metric_value END)         AS min_value,
        MAX(CASE WHEN is_valid THEN metric_value END)         AS max_value,
        STDDEV(CASE WHEN is_valid THEN metric_value END)      AS stddev_value,

        QUANTILE_CONT(
            CASE WHEN is_valid THEN metric_value END,
            0.5
        )                                                       AS median_value,
        QUANTILE_CONT(
            CASE WHEN is_valid THEN metric_value END,
            0.10
        )                                                       AS p10_value,
        QUANTILE_CONT(
            CASE WHEN is_valid THEN metric_value END,
            0.90
        )                                                       AS p90_value
    FROM joined
    GROUP BY 1, 2, 3
)

SELECT * FROM aggregated
