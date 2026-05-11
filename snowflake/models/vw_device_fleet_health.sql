-- ============================================================================
-- VW_DEVICE_FLEET_HEALTH
-- ============================================================================
-- Per-(device_type, firmware_version) reliability scoreboard. Used by ops
-- to decide firmware-rollback or recall decisions.
--
-- Tracked proxies:
--   - invalid_reading_rate: fraction is_valid=FALSE
--   - late_arrival_rate: fraction is_late_arriving=TRUE
--   - low_battery_rate: fraction battery_pct<20
--   - composite_failure_rate: sum of the above
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_DEVICE_FLEET_HEALTH
COMMENT = 'Per-(device_type, firmware_version) failure-rate scoreboard'
AS

WITH base AS (
    SELECT
        device_type,
        firmware_version,
        device_id,
        is_valid,
        is_late_arriving,
        battery_pct,
        event_timestamp
    FROM PULSETRACK.SILVER.SENSOR_READINGS
),

per_firmware AS (
    SELECT
        device_type,
        firmware_version,

        COUNT(*)                                                            AS total_readings,
        COUNT(DISTINCT device_id)                                           AS distinct_devices,

        SUM(IFF(NOT is_valid, 1, 0))                                        AS invalid_count,
        DIV0(SUM(IFF(NOT is_valid, 1, 0))::DOUBLE, COUNT(*)::DOUBLE)        AS invalid_reading_rate,

        SUM(IFF(is_late_arriving, 1, 0))                                    AS late_arrival_count,
        DIV0(SUM(IFF(is_late_arriving, 1, 0))::DOUBLE, COUNT(*)::DOUBLE)    AS late_arrival_rate,

        SUM(IFF(battery_pct < 20, 1, 0))                                    AS low_battery_count,
        DIV0(SUM(IFF(battery_pct < 20, 1, 0))::DOUBLE, COUNT(*)::DOUBLE)    AS low_battery_rate,

        DIV0(
            (SUM(IFF(NOT is_valid, 1, 0)) + SUM(IFF(is_late_arriving, 1, 0)))::DOUBLE,
            COUNT(*)::DOUBLE
        )                                                                   AS composite_failure_rate,

        MIN(event_timestamp)                                                AS first_seen,
        MAX(event_timestamp)                                                AS last_seen
    FROM base
    GROUP BY 1, 2
),

per_firmware_dim AS (
    -- Join SCD2 dim_device for `is_current` flag of each firmware row.
    SELECT
        pf.*,
        ANY_VALUE(d.is_current) AS firmware_is_current_anywhere
    FROM per_firmware pf
    LEFT JOIN PULSETRACK.GOLD.DIM_DEVICE d
        ON  d.device_type = pf.device_type
        AND d.firmware_version = pf.firmware_version
    GROUP BY pf.device_type, pf.firmware_version, pf.total_readings,
             pf.distinct_devices, pf.invalid_count, pf.invalid_reading_rate,
             pf.late_arrival_count, pf.late_arrival_rate, pf.low_battery_count,
             pf.low_battery_rate, pf.composite_failure_rate, pf.first_seen, pf.last_seen
)

SELECT
    *,
    -- Reliability tier for dashboard color.
    CASE
        WHEN composite_failure_rate > 0.10 THEN 'CRITICAL'
        WHEN composite_failure_rate > 0.05 THEN 'WARNING'
        WHEN composite_failure_rate > 0.01 THEN 'WATCH'
        ELSE                                    'OK'
    END AS reliability_tier,
    CURRENT_TIMESTAMP AS view_built_at
FROM per_firmware_dim
ORDER BY device_type, firmware_version;
