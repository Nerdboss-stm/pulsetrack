-- ============================================================================
-- VW_PATIENT_HEALTH_360
-- ============================================================================
-- Wide patient view — one row per patient with everything a clinician
-- needs in one query. Joins dim_patient with recent vitals, active
-- meds, primary conditions, recent adverse events.
--
-- Powers the patient-360 dashboard. Equivalent to the dbt-built
-- ``patient_health_360`` table — this view runs against the Iceberg
-- tables directly (no dbt build needed).
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_PATIENT_HEALTH_360
COMMENT = 'Wide patient view: dim_patient + 30d vitals + active meds + primary conditions + 90d adverse events'
AS

WITH last_30d_vitals AS (
    SELECT
        patient_key,
        MAX(CASE WHEN metric_name = 'heart_rate_bpm' THEN avg_value END)        AS avg_hr_30d,
        MAX(CASE WHEN metric_name = 'heart_rate_bpm' THEN max_value END)        AS max_hr_30d,
        MIN(CASE WHEN metric_name = 'spo2_pct'       THEN min_value END)        AS min_spo2_30d,
        MAX(CASE WHEN metric_name = 'bp_systolic_mmhg' THEN max_value END)      AS max_bp_systolic_30d,
        SUM(CASE WHEN metric_name = 'steps_since_last' THEN avg_value * valid_reading_count END) AS total_steps_30d
    FROM PULSETRACK.GOLD.FACT_VITAL_DAILY_SUMMARY
    WHERE event_date >= CURRENT_DATE - 30
    GROUP BY 1
),

active_meds AS (
    SELECT
        patient_key,
        LISTAGG(medication_name, ', ') WITHIN GROUP (ORDER BY medication_name)
            AS active_medication_list
    FROM PULSETRACK.SILVER.EHR_MEDICATIONS
    WHERE end_date IS NULL OR end_date >= CURRENT_DATE
    GROUP BY 1
),

primary_conditions AS (
    SELECT
        patient_key,
        LISTAGG(condition_name, ', ') WITHIN GROUP (ORDER BY condition_name)
            AS primary_condition_list
    FROM PULSETRACK.SILVER.EHR_CONDITIONS  ec
    INNER JOIN PULSETRACK.GOLD.DIM_PATIENT dp USING (patient_key)
    WHERE is_primary = TRUE
    GROUP BY 1
),

recent_adverse AS (
    SELECT
        patient_key,
        COUNT(*) AS adverse_events_90d,
        MAX(CASE adverse_event
            WHEN 'death' THEN 4
            WHEN 'severe' THEN 3
            WHEN 'moderate' THEN 2
            WHEN 'mild' THEN 1
            ELSE 0
        END) AS max_severity_90d
    FROM PULSETRACK.SILVER.PHARMACY_FILLS
    WHERE event_timestamp >= CURRENT_DATE - 90
      AND adverse_event != 'none'
    GROUP BY 1
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
    -- Meds
    p.total_medications,
    p.active_medications,
    p.distinct_drug_classes,
    am.active_medication_list,
    -- Pharmacy / adverse events
    p.total_pharmacy_fills,
    p.adverse_event_count                  AS lifetime_adverse_events,
    rae.adverse_events_90d                 AS adverse_events_last_90d,
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
    CURRENT_TIMESTAMP                      AS view_built_at
FROM PULSETRACK.GOLD.DIM_PATIENT             AS p
LEFT JOIN last_30d_vitals                    AS l30 USING (patient_key)
LEFT JOIN active_meds                        AS am  USING (patient_key)
LEFT JOIN primary_conditions                 AS pc  USING (patient_key)
LEFT JOIN recent_adverse                     AS rae USING (patient_key);
