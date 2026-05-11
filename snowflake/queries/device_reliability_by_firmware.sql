-- ============================================================================
-- device_reliability_by_firmware.sql
-- ============================================================================
-- Per-firmware reliability scoreboard sorted by composite_failure_rate.
-- Used by ops: "should we roll firmware 1.1.0 back?"
-- ============================================================================

USE WAREHOUSE PULSETRACK_WH;
USE ROLE PULSETRACK_READER;
USE DATABASE PULSETRACK;

SELECT
    device_type,
    firmware_version,
    distinct_devices,
    total_readings,
    ROUND(invalid_reading_rate   * 100.0, 2) AS invalid_pct,
    ROUND(late_arrival_rate      * 100.0, 2) AS late_arrival_pct,
    ROUND(low_battery_rate       * 100.0, 2) AS low_battery_pct,
    ROUND(composite_failure_rate * 100.0, 2) AS composite_failure_pct,
    reliability_tier,
    first_seen,
    last_seen
FROM ANALYTICS.VW_DEVICE_FLEET_HEALTH
WHERE last_seen >= CURRENT_TIMESTAMP - INTERVAL '30 days'
ORDER BY composite_failure_rate DESC, total_readings DESC;
