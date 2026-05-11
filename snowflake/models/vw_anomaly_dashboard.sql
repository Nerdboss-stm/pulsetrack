-- ============================================================================
-- VW_ANOMALY_DASHBOARD
-- ============================================================================
-- One row per anomalous vital reading with patient + device context.
-- Drives the "incident review" Snowflake dashboard.
--
-- Anomaly criteria (any of):
--   - vital_status IN ('critical', 'warning') (from per-metric range checks)
--   - |z_score_30d| > 3 (statistical outlier vs 30-day patient baseline)
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_ANOMALY_DASHBOARD
COMMENT = 'Anomalous vital readings with patient + device + recent-adverse-events context'
AS

WITH critical_readings AS (
    -- Per-reading status comes from fact_vital_reading.vital_status
    -- (computed by the dbt classify_vital_range macro).
    SELECT
        f.reading_metric_key,
        f.patient_key,
        f.metric_name,
        f.metric_value,
        f.event_timestamp,
        CAST(f.event_timestamp AS DATE)             AS event_date,
        f.device_id,
        f.firmware_version,
        f.battery_pct,
        f.is_late_arriving,
        f.vital_status
    FROM PULSETRACK.GOLD.FACT_VITAL_READING AS f
    WHERE f.vital_status IN ('critical', 'warning')
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
        p.health_complexity_bucket,
        p.health_complexity_score,
        p.active_conditions,
        p.active_medications,
        p.adverse_event_count
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
