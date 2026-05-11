{{ config(materialized='table', tags=['gold', 'core', 'dim']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- dim_condition
-- ────────────────────────────────────────────────────────────────────────────
-- Full ICD-10 condition dimension. One row per ICD-10 code from the
-- ``icd10_codes`` seed, enriched with the parent chapter from
-- ``dim_condition_category``. This is the dimension fact_condition_active
-- (and other condition-aware facts) join against.
--
-- Source-of-truth chain:
--   1. seeds/icd10_codes.csv — manual curation (clinician-reviewed).
--   2. dim_condition (this model) — published gold dimension.
--   3. fact_lab_result / fact_condition_active — join against this dim.
--
-- Why a dim_condition when stg_ehr_conditions already exists?
--   - dim_condition is the dimensional model the BI tools see: chapter +
--     severity + parent rollup ready to filter on.
--   - stg_ehr_conditions is per-patient-condition observation grain;
--     dim_condition is per-distinct-ICD-10-code grain. Star schema 101.
-- ────────────────────────────────────────────────────────────────────────────

WITH codes AS (
    SELECT
        icd10_code,
        condition_name,
        category_code,
        severity_default
    FROM {{ ref('icd10_codes') }}
),

categories AS (
    SELECT
        category_code,
        category_name,
        icd10_chapter,
        description AS category_description
    FROM {{ ref('dim_condition_category') }}
),

-- Observed occurrence counts from the patient stream. Useful for prevalence
-- rankings in BI tools without a separate aggregate.
occurrence AS (
    SELECT
        icd10_code,
        COUNT(*)                                            AS observation_count,
        COUNT(DISTINCT patient_key)                         AS distinct_patient_count,
        SUM(CASE WHEN clinical_status = 'active' THEN 1 ELSE 0 END)
                                                            AS active_observation_count,
        SUM(CASE WHEN is_primary THEN 1 ELSE 0 END)         AS primary_observation_count,
        MIN(onset_date)                                     AS earliest_onset_date,
        MAX(recorded_at)                                    AS latest_recorded_at
    FROM {{ ref('stg_ehr_conditions') }}
    GROUP BY icd10_code
)

SELECT
    -- Surrogate key over icd10_code. Primary key.
    {{ pulsetrack.generate_sha256_key(['c.icd10_code']) }}      AS condition_key,
    c.icd10_code,
    c.condition_name,
    c.category_code,
    cat.category_name,
    cat.icd10_chapter,
    cat.category_description,
    c.severity_default,

    -- Severity rank (0 = none, 4 = severe) — handy for SUM/AVG-style rolling
    -- severity scoring in analytics.
    CASE c.severity_default
        WHEN 'minimal'  THEN 0
        WHEN 'mild'     THEN 1
        WHEN 'moderate' THEN 2
        WHEN 'severe'   THEN 3
        WHEN 'critical' THEN 4
        ELSE 0
    END                                                       AS severity_rank,

    -- Observed occurrence stats (NULL-safe — never-observed codes still appear).
    COALESCE(o.observation_count, 0)                          AS observation_count,
    COALESCE(o.distinct_patient_count, 0)                     AS distinct_patient_count,
    COALESCE(o.active_observation_count, 0)                   AS active_observation_count,
    COALESCE(o.primary_observation_count, 0)                  AS primary_observation_count,
    o.earliest_onset_date,
    o.latest_recorded_at,

    -- Convenience: is the condition ever observed in our data?
    (COALESCE(o.observation_count, 0) > 0)                    AS is_observed,

    CURRENT_TIMESTAMP                                         AS dbt_loaded_at
FROM codes AS c
LEFT JOIN categories  AS cat ON c.category_code = cat.category_code
LEFT JOIN occurrence  AS o   ON c.icd10_code    = o.icd10_code
