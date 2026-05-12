-- ============================================================================
-- VW_DEVICE_FLEET_HEALTH
-- ============================================================================
-- Per-(device_type, firmware_version) reliability scoreboard. Used by ops
-- to decide firmware-rollback or recall decisions.
--
-- Tracked proxies:
--   - invalid_reading_rate: fraction is_valid=FALSE
--   - late_arrival_rate: fraction is_late_arriving=TRUE
--   - composite_failure_rate: invalid + late combined
--
-- Schema note: battery_pct lives only on the bronze sensor schema; silver
-- drops it during cleansing. The original aspirational version of this
-- view computed `low_battery_rate` from silver — that column doesn't
-- exist on the silver table, so the metric is dropped here (and would be
-- re-added when the silver schema is extended to preserve battery_pct
-- through the cleansing transform).
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
        ON  d.device_type      = pf.device_type
        AND d.firmware_version = pf.firmware_version
    GROUP BY pf.device_type, pf.firmware_version, pf.total_readings,
             pf.distinct_devices, pf.invalid_count, pf.invalid_reading_rate,
             pf.late_arrival_count, pf.late_arrival_rate,
             pf.composite_failure_rate, pf.first_seen, pf.last_seen
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
