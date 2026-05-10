-- ────────────────────────────────────────────────────────────────────────────
-- assert_patient_key_not_orphaned
-- ────────────────────────────────────────────────────────────────────────────
-- Singular test. Asserts every patient_key in the fact tables exists in
-- dim_patient. dbt convention: tests pass when this query returns ZERO rows.
-- Returning rows = orphaned patient_keys (FK violation).
--
-- We could express this with relationship tests at column level, but a
-- singular test is more visible in CI failures: "12 patient_keys orphaned
-- across 3 fact tables" tells the operator immediately where to look.
-- ────────────────────────────────────────────────────────────────────────────

WITH dim AS (
    SELECT patient_key FROM {{ ref('dim_patient') }}
),
fact_keys AS (
    SELECT 'fact_vital_daily_summary' AS table_name, patient_key
    FROM {{ ref('fact_vital_daily_summary') }}
    UNION ALL
    SELECT 'fact_vital_reading', patient_key
    FROM {{ ref('fact_vital_reading') }}
    UNION ALL
    SELECT 'fact_lab_result', patient_key
    FROM {{ ref('fact_lab_result') }}
)
SELECT
    fact_keys.table_name,
    fact_keys.patient_key,
    COUNT(*) AS orphan_row_count
FROM fact_keys
LEFT JOIN dim USING (patient_key)
WHERE dim.patient_key IS NULL
GROUP BY fact_keys.table_name, fact_keys.patient_key
