-- ────────────────────────────────────────────────────────────────────────────
-- assert_fact_vital_referential_integrity
-- ────────────────────────────────────────────────────────────────────────────
-- Comprehensive FK check on fact_vital_reading. Asserts every FK column
-- (patient_key, metric_key, date_key) joins to the corresponding dim.
--
-- The relationships() column-level tests catch this individually, but a
-- singular consolidated test produces clearer failures: "47 fact rows have
-- broken FKs across 3 dimensions" with a column showing which dim is
-- missing.
-- ────────────────────────────────────────────────────────────────────────────

{{ config(severity='error') }}

WITH fact AS (
    SELECT reading_metric_key, patient_key, metric_key, date_key
    FROM {{ ref('fact_vital_reading') }}
),
patients AS (SELECT patient_key FROM {{ ref('dim_patient') }}),
metrics  AS (SELECT metric_key  FROM {{ ref('dim_metric') }}),
dates    AS (SELECT date_key    FROM {{ ref('dim_date') }}),

violations AS (
    SELECT
        f.reading_metric_key,
        CASE
            WHEN p.patient_key IS NULL THEN 'dim_patient'
            WHEN m.metric_key  IS NULL THEN 'dim_metric'
            WHEN d.date_key    IS NULL THEN 'dim_date'
        END AS missing_dim,
        f.patient_key,
        f.metric_key,
        f.date_key
    FROM fact AS f
    LEFT JOIN patients AS p ON p.patient_key = f.patient_key
    LEFT JOIN metrics  AS m ON m.metric_key  = f.metric_key
    LEFT JOIN dates    AS d ON d.date_key    = f.date_key
    WHERE p.patient_key IS NULL
       OR m.metric_key  IS NULL
       OR d.date_key    IS NULL
)
SELECT * FROM violations
