{{ config(materialized='table', tags=['gold', 'analytics', 'anomaly']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- anomaly_investigation
-- ────────────────────────────────────────────────────────────────────────────
-- One row per anomalous vital reading with full patient context. Drives the
-- "incident review" dashboard. An anomaly is any reading classified
-- 'critical' OR with 30-day z-score > vital_anomaly_zscore.
--
-- Joined context (per anomalous reading):
--   - Patient enriched view (active conditions, medications, recent fills)
--   - Device + firmware
--   - 30-day rolling baseline at the time
-- ────────────────────────────────────────────────────────────────────────────

WITH critical_readings AS (
    SELECT
        f.reading_metric_key,
        f.patient_key,
        f.metric_name,
        f.metric_value,
        f.event_timestamp,
        f.device_id,
        f.firmware_version,
        f.battery_pct,
        f.is_late_arriving,
        f.vital_status
    FROM {{ ref('fact_vital_reading') }} AS f
    WHERE f.vital_status IN ('critical', 'warning')
),

baselines AS (
    SELECT
        patient_key,
        metric_name,
        event_date,
        rolling_30d_avg,
        rolling_30d_stddev,
        z_score_30d
    FROM {{ ref('vital_trend_analysis') }}
),

with_baseline AS (
    SELECT
        cr.*,
        CAST(cr.event_timestamp AS DATE)                    AS event_date,
        b.rolling_30d_avg,
        b.rolling_30d_stddev,
        b.z_score_30d,

        -- Z-score filter: flag if outside +/- 3 stddev of 30-day mean.
        CASE
            WHEN ABS(b.z_score_30d) > {{ var('vital_anomaly_zscore') }}
                THEN TRUE
            ELSE FALSE
        END AS is_z_score_anomaly
    FROM critical_readings AS cr
    LEFT JOIN baselines AS b
        ON  b.patient_key = cr.patient_key
        AND b.metric_name = cr.metric_name
        AND b.event_date  = CAST(cr.event_timestamp AS DATE)
),

with_context AS (
    SELECT
        wb.*,
        p.health_complexity_bucket,
        p.health_complexity_score,
        p.active_conditions,
        p.active_medications,
        p.adverse_event_count
    FROM with_baseline AS wb
    LEFT JOIN {{ ref('dim_patient') }} AS p USING (patient_key)
)

SELECT
    *,
    -- Severity label for dashboards.
    CASE
        WHEN vital_status = 'critical' AND is_z_score_anomaly THEN 'CRITICAL_ANOMALY'
        WHEN vital_status = 'critical'                         THEN 'CRITICAL'
        WHEN is_z_score_anomaly                                THEN 'STATISTICAL_ANOMALY'
        ELSE 'WARNING'
    END AS severity_label,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM with_context
