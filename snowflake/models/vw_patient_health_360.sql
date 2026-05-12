-- ============================================================================
-- VW_PATIENT_HEALTH_360
-- ============================================================================
-- Wide patient view — one row per patient with everything a clinician
-- needs in one query. Joins dim_patient with recent vitals, active
-- meds, primary conditions, recent adverse events.
--
-- Powers the patient-360 dashboard.
--
-- Schema note: the EMR-built dim_patient is the minimal version
-- (patient_key, patient_id_masked, age_group, gender, primary_condition_key,
-- device_count, first_reading_date). The richer health-complexity columns
-- that the aspirational dbt-mart version of this view referenced
-- (health_complexity_bucket, active_conditions, etc.) are computed here
-- directly via joins to the silver EHR tables. When the dbt mart is
-- built against Snowflake target in a later iteration, those derived
-- columns will materialize into dim_patient itself and these joins can
-- be eliminated.
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_PATIENT_HEALTH_360
COMMENT = 'Wide patient view: dim_patient + 30d vitals + active meds + conditions + sensor stats'
AS

WITH last_30d_vitals AS (
    SELECT
        f.patient_key,
        MAX(CASE WHEN m.metric_name = 'heart_rate_bpm'    THEN f.avg_value END)        AS avg_hr_30d,
        MAX(CASE WHEN m.metric_name = 'heart_rate_bpm'    THEN f.max_value END)        AS max_hr_30d,
        MIN(CASE WHEN m.metric_name = 'spo2_pct'          THEN f.min_value END)        AS min_spo2_30d,
        MAX(CASE WHEN m.metric_name = 'bp_systolic_mmhg'  THEN f.max_value END)        AS max_bp_systolic_30d,
        SUM(CASE WHEN m.metric_name = 'steps_since_last'  THEN f.avg_value * f.reading_count END) AS total_steps_30d
    FROM PULSETRACK.GOLD.FACT_VITAL_DAILY_SUMMARY  AS f
    LEFT JOIN PULSETRACK.GOLD.DIM_METRIC           AS m USING (metric_key)
    LEFT JOIN PULSETRACK.GOLD.DIM_DATE             AS d USING (date_key)
    WHERE d.date >= CURRENT_DATE - 30
    GROUP BY 1
),

-- Bridge: identity_bridge.identifier_value = patient_id for email rows.
-- ehr_medications joins on patient_id (the source EHR identifier).
patient_id_to_key AS (
    SELECT
        identifier_value                       AS patient_id,
        TRY_CAST(patient_key AS BIGINT)        AS patient_key
    FROM PULSETRACK.SILVER.IDENTITY_BRIDGE
    WHERE link_status = 'linked'
      AND identifier_type IN ('hospital_mrn', 'patient_id')
),

active_meds AS (
    SELECT
        b.patient_key,
        LISTAGG(DISTINCT m.medication, ', ') WITHIN GROUP (ORDER BY m.medication)
            AS active_medication_list,
        COUNT(DISTINCT m.medication)                                       AS active_medications,
        COUNT(DISTINCT m.drug_class)                                       AS distinct_drug_classes,
        COUNT(*)                                                           AS total_medications
    FROM PULSETRACK.SILVER.EHR_MEDICATIONS AS m
    INNER JOIN patient_id_to_key            AS b ON b.patient_id = m.patient_id
    WHERE m.status = 'active' OR m.end_date IS NULL OR m.end_date >= CURRENT_DATE
    GROUP BY 1
),

primary_conditions AS (
    SELECT
        b.patient_key,
        LISTAGG(DISTINCT c.description, ', ') WITHIN GROUP (ORDER BY c.description)
            AS primary_condition_list,
        SUM(IFF(c.is_chronic, 1, 0))                                       AS active_conditions,
        COUNT(*)                                                           AS total_conditions
    FROM PULSETRACK.SILVER.EHR_CONDITIONS AS c
    INNER JOIN patient_id_to_key            AS b ON b.patient_id = c.patient_id
    WHERE c.status IN ('active', 'chronic') OR c.is_chronic
    GROUP BY 1
),

-- Sensor context: device_account_count + most_recent_firmware + first/last sensor event.
-- Silver sensor_readings has patient_email (not patient_key); we join via identity_bridge
-- where identifier_type = 'email'.
email_to_key AS (
    SELECT
        identifier_value                       AS patient_email,
        TRY_CAST(patient_key AS BIGINT)        AS patient_key
    FROM PULSETRACK.SILVER.IDENTITY_BRIDGE
    WHERE link_status = 'linked'
      AND identifier_type = 'email'
),

sensor_stats AS (
    SELECT
        e.patient_key,
        COUNT(DISTINCT s.device_account_id)        AS device_account_count,
        COUNT(DISTINCT s.device_id)                AS sensor_device_count,
        MAX(s.firmware_version)                    AS most_recent_firmware,
        MIN(s.event_timestamp)                     AS first_sensor_event,
        MAX(s.event_timestamp)                     AS last_sensor_event
    FROM PULSETRACK.SILVER.SENSOR_READINGS AS s
    INNER JOIN email_to_key                  AS e ON e.patient_email = s.patient_email
    GROUP BY 1
)

SELECT
    p.patient_key,
    p.patient_id_masked                            AS patient_id_masked,
    p.age_group,
    p.gender,
    -- Health complexity bucket — derived from condition + medication counts.
    CASE
        WHEN COALESCE(pc.active_conditions, 0) + COALESCE(am.active_medications, 0) >= 5 THEN 'high'
        WHEN COALESCE(pc.active_conditions, 0) + COALESCE(am.active_medications, 0) >= 2 THEN 'medium'
        ELSE 'low'
    END                                            AS health_complexity_bucket,
    (COALESCE(pc.active_conditions, 0) + COALESCE(am.active_medications, 0))::DOUBLE
                                                   AS health_complexity_score,
    -- Conditions
    pc.total_conditions,
    pc.active_conditions,
    pc.primary_condition_list,
    -- Meds
    am.total_medications,
    am.active_medications,
    am.distinct_drug_classes,
    am.active_medication_list,
    -- Pharmacy / adverse events (no pharmacy_fills produced in current run)
    0::BIGINT                                      AS total_pharmacy_fills,
    0::BIGINT                                      AS lifetime_adverse_events,
    0::BIGINT                                      AS adverse_events_last_90d,
    -- Recent vitals (30 day rolling)
    l30.avg_hr_30d,
    l30.max_hr_30d,
    l30.min_spo2_30d,
    l30.max_bp_systolic_30d,
    l30.total_steps_30d,
    -- Sensor coverage
    COALESCE(ss.device_account_count, 0)           AS device_account_count,
    COALESCE(ss.sensor_device_count, p.device_count) AS device_count,
    ss.most_recent_firmware,
    ss.first_sensor_event,
    ss.last_sensor_event,
    CURRENT_TIMESTAMP                              AS view_built_at
FROM PULSETRACK.GOLD.DIM_PATIENT             AS p
LEFT JOIN last_30d_vitals                    AS l30 USING (patient_key)
LEFT JOIN active_meds                        AS am  USING (patient_key)
LEFT JOIN primary_conditions                 AS pc  USING (patient_key)
LEFT JOIN sensor_stats                       AS ss  USING (patient_key);
