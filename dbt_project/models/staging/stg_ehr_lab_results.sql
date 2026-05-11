{{ config(
    materialized='view',
    tags=['staging', 'ehr', 'lab', 'silver_consumer']
) }}

-- ────────────────────────────────────────────────────────────────────────────
-- stg_ehr_lab_results
-- ────────────────────────────────────────────────────────────────────────────
-- 1:1 staging view over silver ``ehr_lab_results``. This is the canonical
-- LOINC-coded lab-result stream emitted by the FHIR producer's Observation
-- resources. Replaces the condition-as-lab proxy currently powering
-- fact_lab_result; the gold fact will switch to this source once the model
-- has stabilized in production.
--
-- Responsibilities at this layer:
--   1. Cast types where the silver table has ambiguous string-like types.
--   2. Add a deterministic ``lab_result_key`` surrogate for fact joins.
--   3. Add a derived ``patient_key`` so downstream joins skip the bridge
--      for the most-common case of email-resolved patients.
--   4. Derive ``is_abnormal`` from the reference range when the LOINC code
--      ships one, falling back to NULL when the range is unknown.
--
-- The view materialization is intentional: thin facade over silver, no
-- transformation logic. Logic goes in intermediate or marts layers.
-- ────────────────────────────────────────────────────────────────────────────

WITH source AS (
    SELECT * FROM {{ source('silver', 'ehr_lab_results') }}
),

renamed AS (
    SELECT
        -- Surrogate key for downstream fact-table joins. Per WHOOP commons,
        -- SHA-256 not MD5 (see macros/commons/generate_sha256_key.sql).
        {{ pulsetrack.generate_sha256_key(['lab_result_id']) }}
            AS lab_result_key,

        -- Patient key — joins to dim_patient. Hash matches the bridge's
        -- canonical email-key derivation.
        {{ pulsetrack.generate_sha256_key(['patient_email']) }}
            AS patient_key,

        lab_result_id,
        patient_email,
        mrn,
        loinc_code,
        test_name,
        test_category,
        CAST(result_value AS DOUBLE)         AS result_value,
        result_unit,
        CAST(reference_min AS DOUBLE)        AS reference_min,
        CAST(reference_max AS DOUBLE)        AS reference_max,
        result_status,
        CAST(collected_at AS TIMESTAMP)      AS collected_at,
        CAST(resulted_at AS TIMESTAMP)       AS resulted_at,
        CAST(recorded_at AS TIMESTAMP)       AS recorded_at,
        ordering_provider_id,

        -- Derived flag: is the result outside the LOINC-published reference
        -- range? NULL when the range is unknown (some LOINC codes don't
        -- publish numeric bounds — e.g., qualitative results).
        CASE
            WHEN reference_min IS NULL OR reference_max IS NULL THEN NULL
            WHEN result_value < reference_min OR result_value > reference_max
                THEN TRUE
            ELSE FALSE
        END                                  AS is_abnormal,

        -- Derived: turnaround time in hours. Useful for ops KPIs.
        EXTRACT(EPOCH FROM (resulted_at - collected_at)) / 3600.0
                                             AS turnaround_hours,

        -- Derived: date partition for downstream marts.
        CAST(collected_at AS DATE)           AS collected_date
    FROM source
)

SELECT * FROM renamed
