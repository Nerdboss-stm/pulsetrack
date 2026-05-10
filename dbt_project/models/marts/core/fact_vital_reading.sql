{{ config(
    materialized='table',
    tags=['gold', 'core', 'fact', 'vital', 'atomic']
) }}

-- ────────────────────────────────────────────────────────────────────────────
-- fact_vital_reading
-- ────────────────────────────────────────────────────────────────────────────
-- Atomic per-reading per-metric fact. One row per (reading_id, metric_name).
-- Used by ML pipelines that need granular per-reading data (the daily
-- summary aggregates away too much for fine-grained models).
--
-- Grain: (reading_metric_key) — unique. Maps to silver_sensor_readings 1:1
-- post explode-and-dedup.
-- ────────────────────────────────────────────────────────────────────────────

WITH sensor AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
    WHERE is_valid = TRUE   -- gold facts only contain valid readings
),

bridge AS (
    SELECT
        identifier_value AS device_account_id,
        patient_key
    FROM {{ ref('stg_identity_bridge') }}
    WHERE identifier_type = 'device_account_id'
      AND link_status = 'linked'
),

metric_lookup AS (
    SELECT metric_key, metric_name FROM {{ ref('dim_metric') }}
)

SELECT
    s.reading_metric_key,
    b.patient_key,
    m.metric_key,
    CAST(STRFTIME(s.event_date, '%Y%m%d') AS INTEGER)        AS date_key,

    s.reading_id,
    s.device_id,
    s.device_type,
    s.metric_name,
    s.metric_value,
    s.event_timestamp,
    s.sync_timestamp,
    s.sync_latency_seconds,
    s.is_late_arriving,
    s.battery_pct,
    s.firmware_version,
    s.source_type,

    {{ pulsetrack.classify_vital_range('s.metric_name', 's.metric_value') }}
        AS vital_status,

    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM sensor AS s
INNER JOIN bridge        AS b USING (device_account_id)
INNER JOIN metric_lookup AS m USING (metric_name)
WHERE b.patient_key IS NOT NULL
