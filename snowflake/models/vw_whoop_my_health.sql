-- ============================================================================
-- VW_WHOOP_MY_HEALTH
-- ============================================================================
-- The operator's personal WHOOP dashboard. Filters the fact tables
-- to ``source_type = 'whoop_api'`` AND ``patient_key`` matching the
-- operator's WHOOP-account-derived identity.
--
-- Configure the personal email via session variable:
--   ALTER SESSION SET WHOOP_OPERATOR_EMAIL = 'operator@example.com';
--
-- The view will return zero rows if the WHOOP poller hasn't run yet
-- (source_type != 'whoop_api' for any reading).
--
-- Schema note: device_id / firmware_version / battery_pct don't propagate
-- through the gold transform — they're available on the silver layer
-- (sensor_readings). The original aspirational version of this view
-- selected them from fact_vital_reading; the actual gold schema only
-- carries patient_key/metric_key/date_key/event_timestamp/value.
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_WHOOP_MY_HEALTH
COMMENT = 'Personal WHOOP dashboard — operator''s WHOOP-only readings + trends'
AS

WITH operator_patient AS (
    -- Email → patient_key via identity_bridge.
    -- Email comparison is case-insensitive.
    SELECT
        TRY_CAST(patient_key AS BIGINT) AS patient_key
    FROM PULSETRACK.SILVER.IDENTITY_BRIDGE
    WHERE link_status     = 'linked'
      AND identifier_type = 'email'
      AND LOWER(identifier_value) = LOWER(
              CASE WHEN GETVARIABLE('WHOOP_OPERATOR_EMAIL') IS NULL
                   THEN 'unknown@example.com'
                   ELSE GETVARIABLE('WHOOP_OPERATOR_EMAIL')::STRING
              END
          )
),

readings AS (
    SELECT
        f.patient_key,
        f.metric_key,
        m.metric_name,
        f.value                                       AS metric_value,
        f.event_timestamp,
        CAST(f.event_timestamp AS DATE)               AS event_date,
        f.source_type,
        CASE
            WHEN m.normal_low IS NULL OR m.normal_high IS NULL THEN 'unknown'
            WHEN f.value < m.normal_low  THEN 'warning'
            WHEN f.value > m.normal_high THEN 'warning'
            ELSE 'normal'
        END                                           AS vital_status
    FROM PULSETRACK.GOLD.FACT_VITAL_READING AS f
    LEFT JOIN PULSETRACK.GOLD.DIM_METRIC    AS m USING (metric_key)
    INNER JOIN operator_patient             AS op USING (patient_key)
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
    dm.normal_low                                     AS normal_min,
    dm.normal_high                                    AS normal_max,
    -- Rolling trends from the trends view.
    t.rolling_7d_avg,
    t.rolling_30d_avg,
    t.z_score_30d,
    t.day_over_day_change,
    t.week_over_week_change,
    -- Source-of-truth columns.
    r.source_type,
    r.patient_key                                     AS patient_key
FROM readings                                       AS r
LEFT JOIN PULSETRACK.GOLD.DIM_METRIC                AS dm USING (metric_key)
LEFT JOIN PULSETRACK.ANALYTICS.VW_VITAL_TRENDS      AS t
    ON  t.patient_key = r.patient_key
    AND t.metric_name = r.metric_name
    AND t.event_date  = r.event_date
ORDER BY r.event_timestamp DESC;
