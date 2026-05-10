-- ────────────────────────────────────────────────────────────────────────────
-- assert_bridge_no_duplicate_identifiers
-- ────────────────────────────────────────────────────────────────────────────
-- Asserts no two bridge rows share the same (identifier_type,
-- identifier_value). Duplicates would mean the same external identifier
-- maps to multiple patient_keys → identity-resolution ambiguity → wrong
-- patient on the dashboard.
--
-- The unique constraint should be enforced upstream at silver, but we
-- assert at the dbt layer too as defense-in-depth.
-- ────────────────────────────────────────────────────────────────────────────

{{ config(severity='error') }}

SELECT
    identifier_type,
    identifier_value,
    COUNT(*) AS duplicate_count,
    ARRAY_AGG(DISTINCT patient_key) AS distinct_patient_keys
FROM {{ ref('stg_identity_bridge') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
