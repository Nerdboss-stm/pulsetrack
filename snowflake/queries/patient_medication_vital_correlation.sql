-- ============================================================================
-- patient_medication_vital_correlation.sql
-- ============================================================================
-- For each (patient, drug_class), compute the average heart rate / BP /
-- HRV in the 30 days BEFORE the prescription started vs the 30 days
-- AFTER. Surfaces "did this medication move the vital?" signals.
--
-- Caveats:
--   - Correlation, not causation. New meds often come with concurrent
--     lifestyle changes (diet, exercise, sleep).
--   - Sample sizes per (patient, drug, metric) are small.
--   - Only patients with both pre+post readings are included.
-- ============================================================================

USE WAREHOUSE PULSETRACK_WH;
USE ROLE PULSETRACK_READER;
USE DATABASE PULSETRACK;
USE SCHEMA SILVER;

WITH med_episodes AS (
    -- One row per medication episode start.
    SELECT
        patient_key,
        drug_class,
        medication_name,
        start_date
    FROM EHR_MEDICATIONS
    WHERE start_date IS NOT NULL
),

daily_by_metric AS (
    -- Daily summary already joined to patient_key.
    SELECT
        patient_key,
        metric_name,
        event_date,
        avg_value,
        valid_reading_count
    FROM PULSETRACK.GOLD.FACT_VITAL_DAILY_SUMMARY
    WHERE metric_name IN ('heart_rate_bpm', 'spo2_pct', 'hrv_ms',
                          'bp_systolic_mmhg', 'bp_diastolic_mmhg')
),

pre_post AS (
    SELECT
        m.patient_key,
        m.drug_class,
        m.medication_name,
        m.start_date,
        d.metric_name,
        CASE
            WHEN d.event_date BETWEEN m.start_date - 30 AND m.start_date - 1 THEN 'pre'
            WHEN d.event_date BETWEEN m.start_date     AND m.start_date + 29 THEN 'post'
            ELSE NULL
        END AS window_label,
        d.avg_value,
        d.valid_reading_count
    FROM med_episodes AS m
    INNER JOIN daily_by_metric AS d
        ON  d.patient_key = m.patient_key
        AND d.event_date BETWEEN m.start_date - 30 AND m.start_date + 29
)

SELECT
    drug_class,
    medication_name,
    metric_name,
    COUNT(DISTINCT patient_key)                                AS patients_in_sample,
    AVG(CASE WHEN window_label = 'pre'  THEN avg_value END)    AS pre_30d_mean,
    AVG(CASE WHEN window_label = 'post' THEN avg_value END)    AS post_30d_mean,
    AVG(CASE WHEN window_label = 'post' THEN avg_value END)
        - AVG(CASE WHEN window_label = 'pre' THEN avg_value END)
                                                                AS delta,
    SUM(CASE WHEN window_label = 'pre'  THEN valid_reading_count ELSE 0 END)
                                                                AS pre_reading_count,
    SUM(CASE WHEN window_label = 'post' THEN valid_reading_count ELSE 0 END)
                                                                AS post_reading_count
FROM pre_post
WHERE window_label IS NOT NULL
GROUP BY 1, 2, 3
HAVING patients_in_sample >= 1
   AND pre_reading_count  >= 3
   AND post_reading_count >= 3
ORDER BY
    ABS(delta) DESC NULLS LAST;
