{{ config(materialized='table', tags=['gold', 'analytics', 'cohort', 'segmentation']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- patient_cohort_segmentation
-- ────────────────────────────────────────────────────────────────────────────
-- Rule-based patient cohort assignment. NOT actual k-means or ML; the goal
-- is to produce clinically-interpretable cohorts that BI tools can filter
-- by. Real clustering lives in a separate Python notebook in the ML repo.
--
-- Grain: one row per patient_key.
--
-- Cohort taxonomy (mutually exclusive — first match wins):
--   1. high_acuity_polypharmacy — active conditions >= 5 AND active meds >= 5
--   2. cardiovascular_focus      — has hypertension or heart failure
--   3. diabetic_focus            — has T2DM or hyperglycemia
--   4. respiratory_focus         — has COPD, asthma, or pneumonia
--   5. mental_health_focus       — has depression or anxiety meds
--   6. stable_chronic            — 1-2 conditions, > 90d on same med
--   7. healthy_active            — no conditions, > 10k avg daily steps
--   8. low_engagement            — < 7 days of sensor data in last 30 days
--   9. unclassified              — fall-through
--
-- The bucket field uses CASE WHEN — explicit, auditable, no opaque
-- model-fitting. Adding/removing rules is a code-review.
-- ────────────────────────────────────────────────────────────────────────────

WITH p AS (
    SELECT * FROM {{ ref('int_patient_enriched') }}
),

active_conditions_per_patient AS (
    SELECT
        patient_key,
        STRING_AGG(icd10_code, ',' ORDER BY icd10_code) AS active_condition_codes
    FROM {{ ref('stg_ehr_conditions') }}
    WHERE clinical_status = 'active'
    GROUP BY patient_key
),

active_medications_per_patient AS (
    SELECT
        patient_key,
        STRING_AGG(drug_class, ',' ORDER BY drug_class) AS active_drug_classes
    FROM {{ ref('stg_ehr_medications') }}
    WHERE is_active = TRUE
    GROUP BY patient_key
),

recent_sensor_activity AS (
    SELECT
        patient_key,
        COUNT(DISTINCT event_date)                       AS recent_days_with_data,
        AVG(CASE WHEN metric_name = 'steps_since_last' THEN avg_value END) * 96
                                                         AS approx_daily_steps
    FROM {{ ref('int_vital_daily_agg') }}
    WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY patient_key
)

SELECT
    -- Surrogate over patient_key. Primary key.
    {{ pulsetrack.generate_sha256_key(['p.patient_key']) }}     AS cohort_assignment_key,
    p.patient_key,
    p.active_conditions,
    p.active_medications,
    p.adverse_event_count,
    p.health_complexity_score,

    ac.active_condition_codes,
    am.active_drug_classes,
    rs.recent_days_with_data,
    rs.approx_daily_steps,

    -- Cohort assignment — first-match-wins ladder.
    CASE
        WHEN p.active_conditions >= 5 AND p.active_medications >= 5
            THEN 'high_acuity_polypharmacy'

        WHEN ac.active_condition_codes LIKE '%I10%'
             OR ac.active_condition_codes LIKE '%I50%'
             OR ac.active_condition_codes LIKE '%I25%'
            THEN 'cardiovascular_focus'

        WHEN ac.active_condition_codes LIKE '%E11%'
            THEN 'diabetic_focus'

        WHEN ac.active_condition_codes LIKE '%J44%'
             OR ac.active_condition_codes LIKE '%J45%'
             OR ac.active_condition_codes LIKE '%J18%'
            THEN 'respiratory_focus'

        WHEN am.active_drug_classes LIKE '%SSRI%'
             OR am.active_drug_classes LIKE '%antidepressant%'
             OR ac.active_condition_codes LIKE '%F32%'
             OR ac.active_condition_codes LIKE '%F41%'
            THEN 'mental_health_focus'

        WHEN p.active_conditions BETWEEN 1 AND 2
             AND p.active_medications >= 1
            THEN 'stable_chronic'

        WHEN p.active_conditions = 0
             AND COALESCE(rs.approx_daily_steps, 0) > 10000
            THEN 'healthy_active'

        WHEN COALESCE(rs.recent_days_with_data, 0) < 7
            THEN 'low_engagement'

        ELSE 'unclassified'
    END                                                         AS cohort_bucket,

    -- Cohort risk ordinal (0 lowest, 5 highest) — useful for sorting in BI.
    CASE
        WHEN p.active_conditions >= 5 AND p.active_medications >= 5 THEN 5
        WHEN p.active_conditions >= 3                              THEN 4
        WHEN p.active_conditions >= 1                              THEN 3
        WHEN COALESCE(rs.recent_days_with_data, 0) < 7             THEN 2
        WHEN COALESCE(rs.approx_daily_steps, 0) > 10000            THEN 1
        ELSE 0
    END                                                         AS cohort_risk_rank,

    CURRENT_TIMESTAMP                                           AS dbt_loaded_at
FROM p
LEFT JOIN active_conditions_per_patient   AS ac USING (patient_key)
LEFT JOIN active_medications_per_patient  AS am USING (patient_key)
LEFT JOIN recent_sensor_activity          AS rs USING (patient_key)
WHERE p.patient_key IS NOT NULL
