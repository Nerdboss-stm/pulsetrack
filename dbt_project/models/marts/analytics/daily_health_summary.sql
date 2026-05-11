{{ config(materialized='table', tags=['gold', 'analytics', 'daily', 'wide']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- daily_health_summary
-- ────────────────────────────────────────────────────────────────────────────
-- Wide per-(patient × day) summary. The "one row to rule them all" for the
-- patient-day BI dashboard: vital averages, anomaly counts, active
-- medications, recent adverse events, and data-quality confidence.
--
-- Grain: (patient_key, event_date).
--
-- Joins:
--   - int_vital_daily_agg     — daily metric averages per patient
--   - int_vital_anomaly_flags — anomaly counts per day
--   - int_patient_medication_active — active medication summary
--   - stg_pharmacy_fills      — same-day adverse events
--   - int_silver_quality_summary — data-quality confidence
-- ────────────────────────────────────────────────────────────────────────────

WITH vitals_wide AS (
    SELECT
        patient_key,
        event_date,
        MAX(CASE WHEN metric_name = 'heart_rate_bpm'    THEN avg_value END) AS avg_heart_rate,
        MAX(CASE WHEN metric_name = 'spo2_pct'           THEN avg_value END) AS avg_spo2,
        MAX(CASE WHEN metric_name = 'hrv_ms'             THEN avg_value END) AS avg_hrv,
        MAX(CASE WHEN metric_name = 'respiration_rate'   THEN avg_value END) AS avg_respiration_rate,
        MAX(CASE WHEN metric_name = 'bp_systolic_mmhg'   THEN avg_value END) AS avg_bp_systolic,
        MAX(CASE WHEN metric_name = 'bp_diastolic_mmhg'  THEN avg_value END) AS avg_bp_diastolic,
        MAX(CASE WHEN metric_name = 'skin_temp_celsius'  THEN avg_value END) AS avg_skin_temp,
        MAX(CASE WHEN metric_name = 'blood_glucose_mgdl' THEN avg_value END) AS avg_blood_glucose,
        SUM(valid_reading_count)                                              AS total_valid_readings,
        SUM(invalid_reading_count)                                            AS total_invalid_readings
    FROM {{ ref('int_vital_daily_agg') }}
    GROUP BY patient_key, event_date
),

anomaly_summary AS (
    SELECT
        patient_key,
        event_date,
        SUM(CASE WHEN is_high_anomaly  THEN 1 ELSE 0 END) AS high_anomaly_count,
        SUM(CASE WHEN is_low_anomaly   THEN 1 ELSE 0 END) AS low_anomaly_count,
        SUM(CASE WHEN is_critical_high THEN 1 ELSE 0 END) AS critical_high_count,
        SUM(CASE WHEN is_critical_low  THEN 1 ELSE 0 END) AS critical_low_count,
        SUM(CASE WHEN any_anomaly       THEN 1 ELSE 0 END) AS any_anomaly_count,
        MAX(anomaly_severity_score)                       AS max_anomaly_severity
    FROM {{ ref('int_vital_anomaly_flags') }}
    GROUP BY patient_key, event_date
),

active_meds AS (
    SELECT
        patient_key,
        MAX(active_medication_count)            AS active_medication_count,
        MAX(active_drug_class_count)            AS active_drug_class_count,
        STRING_AGG(medication_name, ', '
                   ORDER BY medication_name)    AS active_medication_list
    FROM {{ ref('int_patient_medication_active') }}
    GROUP BY patient_key
),

same_day_fills AS (
    SELECT
        patient_key,
        CAST(event_timestamp AS DATE)           AS event_date,
        COUNT(*)                                AS pharmacy_fill_count,
        SUM(CASE WHEN adverse_event != 'none' THEN 1 ELSE 0 END)
                                                AS adverse_event_count,
        MAX(adverse_event_score)                AS max_adverse_event_score
    FROM {{ ref('stg_pharmacy_fills') }}
    GROUP BY patient_key, CAST(event_timestamp AS DATE)
),

quality AS (
    SELECT
        run_date AS event_date,
        AVG(validity_rate)               AS sensor_validity_rate,
        AVG(avg_expectation_pass_rate)   AS expectation_pass_rate
    FROM {{ ref('int_silver_quality_summary') }}
    GROUP BY run_date
)

SELECT
    -- Surrogate over (patient_key, event_date). Primary key.
    {{ pulsetrack.generate_sha256_key(['v.patient_key', 'v.event_date']) }}
        AS daily_summary_key,

    v.patient_key,
    v.event_date,
    CAST(STRFTIME(v.event_date, '%Y%m%d') AS INTEGER)         AS date_key,

    -- Vital averages.
    v.avg_heart_rate,
    v.avg_spo2,
    v.avg_hrv,
    v.avg_respiration_rate,
    v.avg_bp_systolic,
    v.avg_bp_diastolic,
    v.avg_skin_temp,
    v.avg_blood_glucose,

    -- Volume.
    v.total_valid_readings,
    v.total_invalid_readings,

    -- Anomaly counts.
    COALESCE(a.high_anomaly_count, 0)                         AS high_anomaly_count,
    COALESCE(a.low_anomaly_count, 0)                          AS low_anomaly_count,
    COALESCE(a.critical_high_count, 0)                        AS critical_high_count,
    COALESCE(a.critical_low_count, 0)                         AS critical_low_count,
    COALESCE(a.any_anomaly_count, 0)                          AS any_anomaly_count,
    a.max_anomaly_severity,

    -- Medications.
    COALESCE(m.active_medication_count, 0)                    AS active_medication_count,
    COALESCE(m.active_drug_class_count, 0)                    AS active_drug_class_count,
    m.active_medication_list,

    -- Same-day pharmacy events.
    COALESCE(f.pharmacy_fill_count, 0)                        AS pharmacy_fill_count,
    COALESCE(f.adverse_event_count, 0)                        AS adverse_event_count,
    f.max_adverse_event_score,

    -- Data confidence.
    q.sensor_validity_rate,
    q.expectation_pass_rate,

    CURRENT_TIMESTAMP                                         AS dbt_loaded_at
FROM vitals_wide AS v
LEFT JOIN anomaly_summary AS a USING (patient_key, event_date)
LEFT JOIN active_meds     AS m ON m.patient_key = v.patient_key
LEFT JOIN same_day_fills  AS f USING (patient_key, event_date)
LEFT JOIN quality         AS q USING (event_date)
