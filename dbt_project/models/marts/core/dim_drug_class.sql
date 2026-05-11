{{ config(materialized='table', tags=['gold', 'core', 'dim']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- dim_drug_class
-- ────────────────────────────────────────────────────────────────────────────
-- ATC-style drug-class dimension. One row per distinct drug_class observed
-- in the medication and pharmacy-fill streams.
--
-- Source: derived from stg_ehr_medications + stg_pharmacy_fills (the EHR is
-- the authoritative source; the FDA fills are supplemental). Both sources
-- emit the same drug_class taxonomy from the synthetic generators
-- (data_generators/synthetic/pharmacy_generator.py).
--
-- Used by:
--   - dim_medication (FK to drug_class)
--   - medication_adherence (groups by class)
--   - patient_cohort_segmentation (poly-pharmacy cohorts)
-- ────────────────────────────────────────────────────────────────────────────

WITH med_classes AS (
    SELECT
        drug_class,
        COUNT(*)                                AS medication_count,
        COUNT(DISTINCT medication_name)         AS distinct_drug_count,
        COUNT(DISTINCT patient_key)             AS distinct_patient_count,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_medication_count,
        AVG(daily_dose_mg)                      AS avg_daily_dose_mg
    FROM {{ ref('stg_ehr_medications') }}
    WHERE drug_class IS NOT NULL
    GROUP BY drug_class
),

fill_classes AS (
    SELECT
        drug_class,
        COUNT(*)                                AS fill_count,
        COUNT(DISTINCT patient_key)             AS distinct_filled_patient_count,
        SUM(CASE WHEN adverse_event != 'none' THEN 1 ELSE 0 END)
                                                AS adverse_event_count,
        MAX(adverse_event_score)                AS max_adverse_event_score
    FROM {{ ref('stg_pharmacy_fills') }}
    WHERE drug_class IS NOT NULL
    GROUP BY drug_class
),

-- Union the two universes — a class observed in either source appears.
all_classes AS (
    SELECT drug_class FROM med_classes
    UNION
    SELECT drug_class FROM fill_classes
)

SELECT
    -- Surrogate over drug_class. Primary key.
    {{ pulsetrack.generate_sha256_key(['ac.drug_class']) }}     AS drug_class_key,
    ac.drug_class,

    -- Human-readable label. Title-cased + underscore-stripped. The dbt-side
    -- text-cleanup is intentional — we don't want clinicians to see
    -- "ACE_inhibitor" in a dashboard.
    REPLACE(ac.drug_class, '_', ' ')                            AS drug_class_label,

    -- Medication counts (NULL-safe — present if any meds exist for the class).
    COALESCE(m.medication_count, 0)                             AS medication_count,
    COALESCE(m.distinct_drug_count, 0)                          AS distinct_drug_count,
    COALESCE(m.distinct_patient_count, 0)                       AS distinct_patient_count,
    COALESCE(m.active_medication_count, 0)                      AS active_medication_count,
    m.avg_daily_dose_mg,

    -- Pharmacy-fill counts (NULL-safe).
    COALESCE(f.fill_count, 0)                                   AS fill_count,
    COALESCE(f.distinct_filled_patient_count, 0)                AS distinct_filled_patient_count,
    COALESCE(f.adverse_event_count, 0)                          AS adverse_event_count,
    COALESCE(f.max_adverse_event_score, 0)                      AS max_adverse_event_score,

    -- Risk bucket — composite of adverse-event severity.
    CASE
        WHEN COALESCE(f.max_adverse_event_score, 0) >= 3 THEN 'high_risk'
        WHEN COALESCE(f.max_adverse_event_score, 0) = 2  THEN 'moderate_risk'
        WHEN COALESCE(f.adverse_event_count, 0) > 0      THEN 'low_risk'
        ELSE 'no_observed_risk'
    END                                                         AS risk_bucket,

    CURRENT_TIMESTAMP                                           AS dbt_loaded_at
FROM all_classes  AS ac
LEFT JOIN med_classes  AS m USING (drug_class)
LEFT JOIN fill_classes AS f USING (drug_class)
