{{ config(materialized='view', tags=['staging', 'ehr', 'medication']) }}

-- 1:1 staging view over silver ``ehr_medications``. Computes ``is_active``
-- (no end_date OR end_date is in the future) for downstream timeline models.

WITH source AS (
    SELECT * FROM {{ source('silver', 'ehr_medications') }}
),

renamed AS (
    SELECT
        {{ pulsetrack.generate_sha256_key(['medication_id']) }} AS medication_key,
        {{ pulsetrack.generate_sha256_key(['patient_email']) }} AS patient_key,

        medication_id,
        patient_email,
        mrn,
        rxnorm_code,
        medication_name,
        drug_class,
        CAST(prescribed_date AS DATE)            AS prescribed_date,
        CAST(start_date AS DATE)                 AS start_date,
        TRY_CAST(end_date AS DATE)               AS end_date_cast,
        CAST(daily_dose_mg AS DOUBLE)            AS daily_dose_mg
    FROM source
),

derived AS (
    SELECT
        *,
        end_date_cast AS end_date,

        -- Derived: still on the medication today?
        CASE
            WHEN end_date_cast IS NULL THEN TRUE
            WHEN end_date_cast >= CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_active,

        -- Derived: cumulative days on this medication.
        CASE
            WHEN end_date_cast IS NULL THEN
                DATE_DIFF('day', start_date, CURRENT_DATE)
            ELSE
                DATE_DIFF('day', start_date, end_date_cast)
        END AS days_on_medication
    FROM renamed
)

SELECT
    medication_key,
    patient_key,
    medication_id,
    patient_email,
    mrn,
    rxnorm_code,
    medication_name,
    drug_class,
    prescribed_date,
    start_date,
    end_date,
    daily_dose_mg,
    is_active,
    days_on_medication
FROM derived
