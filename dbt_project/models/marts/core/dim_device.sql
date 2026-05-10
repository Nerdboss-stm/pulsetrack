{{ config(materialized='table', tags=['gold', 'core', 'dim', 'scd2']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- dim_device — SCD2 device dimension.
--
-- Sourced from int_device_firmware_scd2 (the derivation from sensor history)
-- with tracking on firmware_version. One row per (device_id, firmware_version)
-- with valid_from/valid_to semantics.
--
-- For point-in-time fact joins:
--   SELECT *
--   FROM fact_vital_reading f
--   INNER JOIN dim_device d
--     ON  f.device_id = d.device_id
--     AND f.event_timestamp >= d.valid_from
--     AND f.event_timestamp <  d.valid_to
-- ────────────────────────────────────────────────────────────────────────────

WITH scd AS (
    SELECT * FROM {{ ref('int_device_firmware_scd2') }}
),

device_meta AS (
    -- Most recent device_type seen for each device_id.
    SELECT
        device_id,
        ANY_VALUE(device_type)            AS device_type,
        MAX(event_timestamp)              AS last_seen_at,
        COUNT(*)                          AS reading_count
    FROM {{ ref('stg_sensor_readings') }}
    GROUP BY device_id
)

SELECT
    s.device_scd_key                                  AS device_key,
    s.device_id,
    dm.device_type,
    s.firmware_version,
    s.valid_from,
    s.valid_to,
    s.is_current,
    dm.last_seen_at,
    dm.reading_count,
    CURRENT_TIMESTAMP                                 AS dbt_loaded_at
FROM scd AS s
LEFT JOIN device_meta AS dm USING (device_id)
