{{ config(materialized='table', tags=['gold', 'core', 'dim', 'pii']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- dim_patient
-- ────────────────────────────────────────────────────────────────────────────
-- Patient dimension. PII-MASKED: emails and MRNs hashed, no full names.
-- Joins to identity bridge are via patient_key only.
--
-- Source: int_patient_enriched (which already aggregates EHR + meds + fills
-- + sensor recency by patient_key).
-- ────────────────────────────────────────────────────────────────────────────

WITH src AS (
    SELECT * FROM {{ ref('int_patient_enriched') }}
)

SELECT
    patient_key,

    -- PII masking — store hashes for searchable joins, not the originals.
    -- The bridge table holds the raw values for runtime resolution; the
    -- gold table is what BI tools see and PII never leaks via dim_patient.
    {{ pulsetrack.generate_sha256_key(['patient_email']) }} AS patient_email_hash,
    {{ pulsetrack.generate_sha256_key(['mrn']) }}           AS mrn_hash,

    device_account_count,
    device_count,
    most_recent_firmware,

    total_conditions,
    active_conditions,
    primary_conditions,
    highest_condition_severity,

    total_medications,
    active_medications,
    distinct_drug_classes,

    total_pharmacy_fills,
    adverse_event_count,
    max_adverse_event_score,

    first_sensor_event,
    last_sensor_event,

    -- Bucket the health-complexity score for BI dashboards.
    CASE
        WHEN health_complexity_score >= 10 THEN 'high'
        WHEN health_complexity_score >= 5  THEN 'medium'
        WHEN health_complexity_score >  0  THEN 'low'
        ELSE 'none'
    END AS health_complexity_bucket,
    health_complexity_score,

    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM src
WHERE patient_key IS NOT NULL
