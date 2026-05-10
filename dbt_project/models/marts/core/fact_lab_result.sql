{{ config(materialized='table', tags=['gold', 'core', 'fact', 'lab']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- fact_lab_result
-- ────────────────────────────────────────────────────────────────────────────
-- LOINC-coded lab result fact. Sourced from EHR conditions for now (the
-- full EHR FHIR generator emits separate lab_results events; this dbt
-- placeholder uses condition records as a proxy until the EHR pipeline
-- adds a dedicated silver_ehr_lab_results stream).
--
-- Grain: (condition_key) — one row per condition; downstream lab-result
-- treatment derivable.
-- ────────────────────────────────────────────────────────────────────────────

WITH conditions AS (
    SELECT
        condition_key,
        patient_key,
        icd10_code,
        condition_name,
        category_code,
        category_name,
        severity_default,
        onset_date,
        recorded_at,
        clinical_status,
        is_primary
    FROM {{ ref('stg_ehr_conditions') }}
)

SELECT
    condition_key                                             AS lab_result_key,
    patient_key,
    CAST(STRFTIME(onset_date, '%Y%m%d') AS INTEGER)           AS date_key,

    icd10_code,
    condition_name                                            AS test_name,
    category_code,
    category_name,
    severity_default,
    clinical_status,
    is_primary                                                AS is_primary_diagnosis,
    onset_date                                                AS result_date,
    recorded_at,
    CURRENT_TIMESTAMP                                         AS dbt_loaded_at
FROM conditions
