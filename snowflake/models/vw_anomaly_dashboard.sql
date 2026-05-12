-- ============================================================================
-- VW_ANOMALY_DASHBOARD
-- ============================================================================
-- One row per anomalous vital reading with patient + device context.
-- Drives the "incident review" Snowflake dashboard.
--
-- Anomaly criteria (any of):
--   - vital_status IN ('critical', 'warning') (computed via dim_metric range)
--   - |z_score_30d| > 3 (statistical outlier vs 30-day patient baseline)
--
-- Schema note: the EMR-built fact_vital_reading is minimal
-- (patient_key, metric_key, date_key, event_timestamp, value, is_valid,
-- is_late_arriving, source_type). vital_status is derived here via JOIN
-- to dim_metric (normal_low / normal_high). device_id / firmware_version
-- / battery_pct don't survive the gold transform — we'd need to join back
-- to silver to recover them. For dashboard purposes, those are filterable
-- separately via the device-fleet view.
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_ANOMALY_DASHBOARD
COMMENT = 'Anomalous vital readings with patient + dim_metric context'
AS

WITH classified_readings AS (
    -- Derive vital_status from dim_metric normal ranges.
    SELECT
        f.patient_key,
        f.metric_key,
        m.metric_name,
        f.value                                                   AS metric_value,
        f.event_timestamp,
        CAST(f.event_timestamp AS DATE)                           AS event_date,
        f.is_late_arriving,
        f.source_type,
        CASE
            WHEN m.normal_low IS NULL OR m.normal_high IS NULL    THEN 'unknown'
            -- Critical: more than 50% beyond normal range on either side.
            WHEN f.value < m.normal_low  - (m.normal_high - m.normal_low) * 0.5 THEN 'critical'
            WHEN f.value > m.normal_high + (m.normal_high - m.normal_low) * 0.5 THEN 'critical'
            -- Warning: outside normal range.
            WHEN f.value < m.normal_low                             THEN 'warning'
            WHEN f.value > m.normal_high                            THEN 'warning'
            ELSE                                                          'normal'
        END                                                       AS vital_status
    FROM PULSETRACK.GOLD.FACT_VITAL_READING AS f
    LEFT JOIN PULSETRACK.GOLD.DIM_METRIC    AS m USING (metric_key)
),

critical_readings AS (
    SELECT *
    FROM classified_readings
    WHERE vital_status IN ('critical', 'warning')
),

with_baseline AS (
    SELECT
        cr.*,
        t.rolling_30d_avg,
        t.rolling_30d_stddev,
        t.z_score_30d,
        CASE
            WHEN ABS(t.z_score_30d) > 3.0 THEN TRUE
            ELSE FALSE
        END AS is_z_score_anomaly
    FROM critical_readings AS cr
    LEFT JOIN PULSETRACK.ANALYTICS.VW_VITAL_TRENDS AS t
        ON  t.patient_key  = cr.patient_key
        AND t.metric_name  = cr.metric_name
        AND t.event_date   = cr.event_date
),

with_patient_context AS (
    SELECT
        wb.*,
        p.age_group,
        p.gender,
        p.device_count
    FROM with_baseline AS wb
    LEFT JOIN PULSETRACK.GOLD.DIM_PATIENT AS p USING (patient_key)
)

SELECT
    *,
    -- Severity label for the dashboard color coding.
    CASE
        WHEN vital_status = 'critical' AND is_z_score_anomaly THEN 'CRITICAL_ANOMALY'
        WHEN vital_status = 'critical'                         THEN 'CRITICAL'
        WHEN is_z_score_anomaly                                THEN 'STATISTICAL_ANOMALY'
        ELSE                                                       'WARNING'
    END AS severity_label,
    CURRENT_TIMESTAMP AS view_built_at
FROM with_patient_context
ORDER BY event_timestamp DESC;
