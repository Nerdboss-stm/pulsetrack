-- ============================================================================
-- top_anomalies_this_week.sql
-- ============================================================================
-- Top 50 anomalous vital readings in the last 7 days with full patient
-- context. The "what should I look at first?" query for an on-call
-- clinician or data engineer.
--
-- Run via:  USE WAREHOUSE PULSETRACK_WH;
--           USE ROLE PULSETRACK_READER;
--           !source snowflake/queries/top_anomalies_this_week.sql
-- ============================================================================

USE WAREHOUSE PULSETRACK_WH;
USE ROLE PULSETRACK_READER;
USE DATABASE PULSETRACK;
USE SCHEMA ANALYTICS;

SELECT
    severity_label,
    event_timestamp,
    patient_key,
    metric_name,
    metric_value,
    -- The 30-day baseline for context.
    rolling_30d_avg,
    ROUND(z_score_30d, 2)              AS z_score,
    -- Patient context.
    health_complexity_bucket,
    active_conditions,
    active_medications,
    -- Device context for triage (is it a hardware issue?).
    device_id,
    firmware_version,
    battery_pct,
    is_late_arriving,
    -- Recent adverse-events context (was the patient on a new med?).
    adverse_event_count
FROM VW_ANOMALY_DASHBOARD
WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY
    CASE severity_label
        WHEN 'CRITICAL_ANOMALY'    THEN 1
        WHEN 'CRITICAL'            THEN 2
        WHEN 'STATISTICAL_ANOMALY' THEN 3
        ELSE                            4
    END,
    ABS(z_score_30d) DESC NULLS LAST,
    event_timestamp DESC
LIMIT 50;
