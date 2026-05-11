{{ config(materialized='table', tags=['gold', 'core', 'dim']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- dim_medication
-- ────────────────────────────────────────────────────────────────────────────
-- Medication dimension. One row per distinct medication observed across the
-- (patient × medication × episode) timeline. Encodes RxNorm + drug_class FK
-- + the patient counts and activity stats BI tools want at-glance.
--
-- Source chain:
--   - stg_ehr_medications: raw observations
--   - int_medication_timeline: episode-aware aggregation (used to count
--     distinct episodes and re-prescriptions)
--   - int_patient_medication_active: active-only details for the "active"
--     summary columns below
--
-- Grain: per (medication_name) — not (rxnorm_code) because some synthetic
-- meds may not have an RxNorm assigned yet. medication_name is the cleanest
-- low-cardinality natural key.
-- ────────────────────────────────────────────────────────────────────────────

WITH timeline AS (
    SELECT * FROM {{ ref('int_medication_timeline') }}
),

active AS (
    SELECT
        medication_name,
        COUNT(*)                                              AS active_episode_count,
        COUNT(DISTINCT patient_key)                           AS active_patient_count,
        AVG(current_episode_duration_days)                    AS avg_active_episode_duration_days
    FROM {{ ref('int_patient_medication_active') }}
    GROUP BY medication_name
),

med_stats AS (
    SELECT
        medication_name,
        MIN(drug_class)                                       AS drug_class,
        MIN(rxnorm_code)                                      AS rxnorm_code,
        COUNT(*)                                              AS total_episode_count,
        COUNT(DISTINCT patient_key)                           AS total_patient_count,
        SUM(CASE WHEN is_re_prescription THEN 1 ELSE 0 END)   AS re_prescription_count,
        AVG(daily_dose_mg)                                    AS avg_daily_dose_mg,
        MIN(daily_dose_mg)                                    AS min_daily_dose_mg,
        MAX(daily_dose_mg)                                    AS max_daily_dose_mg,
        MIN(start_date)                                       AS first_prescribed_date,
        MAX(start_date)                                       AS most_recent_start_date,
        AVG(DATE_DIFF('day', start_date, effective_end_date)) AS avg_episode_duration_days
    FROM timeline
    GROUP BY medication_name
)

SELECT
    -- Surrogate key over medication_name. Primary key.
    {{ pulsetrack.generate_sha256_key(['s.medication_name']) }} AS medication_key,
    s.medication_name,
    s.rxnorm_code,
    s.drug_class,

    -- FK to dim_drug_class. Computed inline to avoid a downstream join.
    {{ pulsetrack.generate_sha256_key(['s.drug_class']) }}      AS drug_class_key,

    -- Lifetime stats.
    s.total_episode_count,
    s.total_patient_count,
    s.re_prescription_count,
    s.first_prescribed_date,
    s.most_recent_start_date,
    s.avg_episode_duration_days,

    -- Dose stats — useful for "is the dosing consistent?" QA.
    s.avg_daily_dose_mg,
    s.min_daily_dose_mg,
    s.max_daily_dose_mg,

    -- Active stats (NULL when nobody is actively taking the med).
    COALESCE(a.active_episode_count, 0)                         AS active_episode_count,
    COALESCE(a.active_patient_count, 0)                         AS active_patient_count,
    a.avg_active_episode_duration_days,

    -- Convenience flag.
    (COALESCE(a.active_episode_count, 0) > 0)                   AS has_active_prescriptions,

    CURRENT_TIMESTAMP                                           AS dbt_loaded_at
FROM med_stats AS s
LEFT JOIN active AS a USING (medication_name)
