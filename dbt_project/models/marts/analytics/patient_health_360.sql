{{ config(materialized='table', tags=['gold', 'analytics', 'patient']) }}

-- Wide patient view for the patient-360 dashboard. One row per patient_key
-- with everything a clinician/operator needs in one query.

WITH p AS (
    SELECT * FROM {{ ref('dim_patient') }}
),

last_30d_vitals AS (
    -- 30-day rolling: avg heart rate, max heart rate, min spo2, etc.
    SELECT
        patient_key,
        MAX(CASE WHEN metric_name = 'heart_rate_bpm' THEN avg_value END)
            AS avg_hr_30d,
        MAX(CASE WHEN metric_name = 'heart_rate_bpm' THEN max_value END)
            AS max_hr_30d,
        MIN(CASE WHEN metric_name = 'spo2_pct' THEN min_value END)
            AS min_spo2_30d,
        MAX(CASE WHEN metric_name = 'bp_systolic_mmhg' THEN max_value END)
            AS max_bp_systolic_30d,
        SUM(CASE WHEN metric_name = 'steps_since_last' THEN avg_value * valid_reading_count
                ELSE 0 END)
            AS total_steps_30d
    FROM {{ ref('fact_vital_daily_summary') }}
    WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY patient_key
),

active_meds AS (
    SELECT
        patient_key,
        STRING_AGG(medication_name, ', ' ORDER BY medication_name) AS active_medication_list
    FROM {{ ref('stg_ehr_medications') }}
    WHERE is_active = TRUE
    GROUP BY patient_key
),

primary_conditions AS (
    SELECT
        patient_key,
        STRING_AGG(condition_name, ', ' ORDER BY condition_name)
            AS primary_condition_list
    FROM {{ ref('stg_ehr_conditions') }}
    WHERE is_primary = TRUE
    GROUP BY patient_key
),

recent_adverse_events AS (
    SELECT
        patient_key,
        COUNT(*) AS adverse_events_90d,
        MAX(adverse_event_score) AS max_severity_90d
    FROM {{ ref('stg_pharmacy_fills') }}
    WHERE event_timestamp >= CURRENT_DATE - INTERVAL '90 days'
      AND adverse_event != 'none'
    GROUP BY patient_key
)

SELECT
    p.patient_key,
    p.patient_email_hash,
    p.health_complexity_bucket,
    p.health_complexity_score,

    -- Conditions
    p.total_conditions,
    p.active_conditions,
    pc.primary_condition_list,
    p.highest_condition_severity,

    -- Medications
    p.total_medications,
    p.active_medications,
    p.distinct_drug_classes,
    am.active_medication_list,

    -- Pharmacy / adverse events
    p.total_pharmacy_fills,
    p.adverse_event_count                                  AS total_adverse_events_lifetime,
    rae.adverse_events_90d                                 AS adverse_events_last_90d,
    rae.max_severity_90d,

    -- Recent vitals (30 day rolling)
    l30.avg_hr_30d,
    l30.max_hr_30d,
    l30.min_spo2_30d,
    l30.max_bp_systolic_30d,
    l30.total_steps_30d,

    -- Sensor coverage
    p.device_account_count,
    p.device_count,
    p.most_recent_firmware,
    p.first_sensor_event,
    p.last_sensor_event,

    CURRENT_TIMESTAMP                                      AS dbt_loaded_at
FROM p
LEFT JOIN last_30d_vitals          AS l30 USING (patient_key)
LEFT JOIN active_meds              AS am  USING (patient_key)
LEFT JOIN primary_conditions       AS pc  USING (patient_key)
LEFT JOIN recent_adverse_events    AS rae USING (patient_key)
