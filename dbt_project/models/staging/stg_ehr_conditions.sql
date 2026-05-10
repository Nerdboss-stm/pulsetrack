{{ config(materialized='view', tags=['staging', 'ehr']) }}

-- 1:1 staging view over silver ``ehr_conditions``. Adds surrogate keys and
-- joins to the ``condition_categories`` seed for the ICD-10 chapter rollup.

WITH source AS (
    SELECT * FROM {{ source('silver', 'ehr_conditions') }}
),

categories AS (
    SELECT
        category_code,
        category_name,
        icd10_chapter
    FROM {{ ref('condition_categories') }}
),

icd10 AS (
    SELECT
        icd10_code,
        condition_name,
        category_code,
        severity_default
    FROM {{ ref('icd10_codes') }}
),

joined AS (
    SELECT
        {{ pulsetrack.generate_sha256_key(['s.condition_id']) }} AS condition_key,
        {{ pulsetrack.generate_sha256_key(['s.patient_email']) }} AS patient_key,

        s.condition_id,
        s.patient_email,
        s.mrn,
        s.icd10_code,
        i.condition_name,
        i.category_code,
        c.category_name,
        c.icd10_chapter,
        i.severity_default,
        CAST(s.onset_date AS DATE)       AS onset_date,
        CAST(s.recorded_at AS TIMESTAMP) AS recorded_at,
        s.clinical_status,
        CAST(s.is_primary AS BOOLEAN)    AS is_primary
    FROM source AS s
    LEFT JOIN icd10 AS i USING (icd10_code)
    LEFT JOIN categories AS c USING (category_code)
)

SELECT * FROM joined
