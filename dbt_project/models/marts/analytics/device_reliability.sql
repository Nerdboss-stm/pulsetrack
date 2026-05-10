{{ config(materialized='table', tags=['gold', 'analytics', 'device']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- device_reliability
-- ────────────────────────────────────────────────────────────────────────────
-- Per-(device_type, firmware_version) reliability scoreboard. Used by
-- ops to decide firmware-rollback or recall decisions.
--
-- Reliability proxies:
--   - invalid_reading_rate: fraction of readings flagged is_valid=FALSE
--   - late_arrival_rate: fraction with is_late_arriving=TRUE
--   - low_battery_rate: fraction with battery_pct < 20 at sync time
-- ────────────────────────────────────────────────────────────────────────────

WITH base AS (
    SELECT
        s.device_type,
        s.firmware_version,
        s.device_id,
        s.is_valid,
        s.is_late_arriving,
        s.battery_pct
    FROM {{ ref('stg_sensor_readings') }} AS s
)

SELECT
    device_type,
    firmware_version,

    COUNT(*)                                                AS total_readings,
    COUNT(DISTINCT device_id)                               AS distinct_devices,

    SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END)           AS invalid_count,
    {{ pulsetrack.safe_divide(
        'SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END)',
        'COUNT(*)',
        default='0'
    ) }} AS invalid_reading_rate,

    SUM(CASE WHEN is_late_arriving THEN 1 ELSE 0 END)       AS late_arrival_count,
    {{ pulsetrack.safe_divide(
        'SUM(CASE WHEN is_late_arriving THEN 1 ELSE 0 END)',
        'COUNT(*)',
        default='0'
    ) }} AS late_arrival_rate,

    SUM(CASE WHEN battery_pct < 20 THEN 1 ELSE 0 END)       AS low_battery_count,
    {{ pulsetrack.safe_divide(
        'SUM(CASE WHEN battery_pct < 20 THEN 1 ELSE 0 END)',
        'COUNT(*)',
        default='0'
    ) }} AS low_battery_rate,

    -- Composite reliability score: lower is better.
    {{ pulsetrack.safe_divide(
        'SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) + SUM(CASE WHEN is_late_arriving THEN 1 ELSE 0 END)',
        'COUNT(*)',
        default='0'
    ) }} AS composite_failure_rate,

    CURRENT_TIMESTAMP                                       AS dbt_loaded_at
FROM base
GROUP BY 1, 2
ORDER BY 1, 2
