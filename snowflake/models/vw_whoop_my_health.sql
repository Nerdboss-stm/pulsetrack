-- ============================================================================
-- VW_WHOOP_MY_HEALTH
-- ============================================================================
-- The operator's personal WHOOP dashboard. Filters the fact tables
-- to ``source_type = 'whoop_api'`` AND ``patient_key`` matching the
-- operator's WHOOP-account-derived identity.
--
-- Configure the personal patient_key via the WHOOP_OPERATOR_PATIENT_KEY
-- variable (set during Snowflake user setup). The view will return zero
-- rows if not yet wired through the WHOOP API connector.
--
-- This view turns the lakehouse into a personal-health analytics tool:
--   "what's my 30-day HR trend?"  → SELECT * FROM vw_whoop_my_health
--   "did my HRV drop last week?"  → look at week_over_week_change
--   "any anomalous readings?"     → filter where vital_status != 'normal'
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_WHOOP_MY_HEALTH
COMMENT = "Personal WHOOP dashboard — operator's WHOOP-only readings + trends + recovery context"
AS

-- Set WHOOP_OPERATOR_EMAIL session var via:
--   ALTER SESSION SET WHOOP_OPERATOR_EMAIL = 'operator@example.com';
-- Then this view derives the patient_key via SHA-256(lower(email)).

WITH operator_patient AS (
    SELECT
        SHA2(LOWER(CURRENT_SESSION_VARIABLE('WHOOP_OPERATOR_EMAIL', 'unknown@example.com')), 256)
            AS patient_key
),

readings AS (
    SELECT
        f.reading_metric_key,
        f.patient_key,
        f.metric_name,
        f.metric_value,
        f.event_timestamp,
        CAST(f.event_timestamp AS DATE) AS event_date,
        f.device_id,
        f.firmware_version,
        f.battery_pct,
        f.source_type,
        f.vital_status
    FROM PULSETRACK.GOLD.FACT_VITAL_READING AS f
    INNER JOIN operator_patient            AS op USING (patient_key)
    WHERE f.source_type = 'whoop_api'
)

SELECT
    r.event_date,
    r.event_timestamp,
    r.metric_name,
    r.metric_value,
    r.vital_status,
    -- Reference: clinical normal range for context.
    dm.unit,
    dm.normal_min,
    dm.normal_max,
    dm.critical_min,
    dm.critical_max,
    -- Rolling trends from the trends view.
    t.rolling_7d_avg,
    t.rolling_30d_avg,
    t.z_score_30d,
    t.day_over_day_change,
    t.week_over_week_change,
    -- Device + firmware for cross-firmware comparisons.
    r.device_id,
    r.firmware_version,
    r.battery_pct,
    -- Source-of-truth columns.
    r.source_type,
    r.reading_metric_key
FROM readings AS r
LEFT JOIN PULSETRACK.GOLD.DIM_METRIC          AS dm USING (metric_name)
LEFT JOIN PULSETRACK.ANALYTICS.VW_VITAL_TRENDS AS t
    ON  t.patient_key = r.patient_key
    AND t.metric_name = r.metric_name
    AND t.event_date  = r.event_date
ORDER BY r.event_timestamp DESC;
