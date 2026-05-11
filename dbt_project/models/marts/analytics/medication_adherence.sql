{{ config(materialized='table', tags=['gold', 'analytics', 'medication', 'adherence']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- medication_adherence
-- ────────────────────────────────────────────────────────────────────────────
-- Per-(patient × medication) adherence scorecard, computed from the gap
-- between fill events. The proxy metric here is the standard PDC (Proportion
-- of Days Covered): given N fills with X days_supply each over a window of
-- Y days, PDC = SUM(X) / Y, capped at 1.
--
-- Grain: (patient_key, drug_class, drug_name).
--
-- Joins:
--   - stg_pharmacy_fills      — fill events (the primary input)
--   - stg_ehr_medications     — prescribed days for the window denominator
--
-- PDC interpretation:
--   - >= 0.80 — adherent (clinical standard)
--   - 0.50–0.79 — partial adherence
--   - < 0.50 — non-adherent
-- ────────────────────────────────────────────────────────────────────────────

WITH fills AS (
    SELECT
        patient_key,
        drug_name,
        drug_class,
        fill_quantity,
        days_supply,
        event_timestamp,
        CAST(event_timestamp AS DATE)         AS fill_date
    FROM {{ ref('stg_pharmacy_fills') }}
    WHERE patient_key IS NOT NULL
      AND days_supply IS NOT NULL
      AND days_supply > 0
),

prescriptions AS (
    SELECT
        patient_key,
        medication_name AS drug_name,
        drug_class,
        MIN(start_date)                       AS first_prescription_date,
        MAX(COALESCE(end_date, CURRENT_DATE)) AS last_prescription_date,
        COUNT(*)                              AS prescription_count
    FROM {{ ref('stg_ehr_medications') }}
    GROUP BY patient_key, medication_name, drug_class
),

fill_summary AS (
    SELECT
        patient_key,
        drug_name,
        drug_class,
        COUNT(*)                              AS fill_count,
        SUM(fill_quantity)                    AS total_units_dispensed,
        SUM(days_supply)                      AS total_days_supplied,
        MIN(fill_date)                        AS first_fill_date,
        MAX(fill_date)                        AS last_fill_date,
        AVG(days_supply)                      AS avg_days_supply_per_fill
    FROM fills
    GROUP BY patient_key, drug_name, drug_class
)

SELECT
    -- Surrogate over (patient_key, drug_class, drug_name). Primary key.
    {{ pulsetrack.generate_sha256_key([
        'COALESCE(p.patient_key, f.patient_key)',
        'COALESCE(p.drug_class, f.drug_class)',
        'COALESCE(p.drug_name, f.drug_name)'
    ]) }} AS adherence_key,

    COALESCE(p.patient_key, f.patient_key)                  AS patient_key,
    COALESCE(p.drug_name, f.drug_name)                      AS drug_name,
    COALESCE(p.drug_class, f.drug_class)                    AS drug_class,

    -- Prescription window.
    p.first_prescription_date,
    p.last_prescription_date,
    p.prescription_count,

    -- Fill totals.
    COALESCE(f.fill_count, 0)                               AS fill_count,
    COALESCE(f.total_units_dispensed, 0)                    AS total_units_dispensed,
    COALESCE(f.total_days_supplied, 0)                      AS total_days_supplied,
    f.first_fill_date,
    f.last_fill_date,
    f.avg_days_supply_per_fill,

    -- Window length in days. NULL-safe.
    CASE
        WHEN p.first_prescription_date IS NULL THEN NULL
        ELSE DATE_DIFF('day', p.first_prescription_date, p.last_prescription_date) + 1
    END                                                     AS prescription_window_days,

    -- PDC = days_supplied / window_days. Capped at 1.0.
    LEAST(
        {{ pulsetrack.safe_divide(
            'f.total_days_supplied',
            'CASE WHEN p.first_prescription_date IS NULL THEN NULL ELSE DATE_DIFF(\'day\', p.first_prescription_date, p.last_prescription_date) + 1 END',
            default='NULL'
        ) }},
        1.0
    )                                                       AS pdc_score,

    -- Adherence band — clinical-standard breakpoints.
    CASE
        WHEN p.first_prescription_date IS NULL THEN 'no_prescription'
        WHEN f.total_days_supplied IS NULL OR f.fill_count = 0 THEN 'no_fills'
        WHEN LEAST(
                {{ pulsetrack.safe_divide(
                    'f.total_days_supplied',
                    'DATE_DIFF(\'day\', p.first_prescription_date, p.last_prescription_date) + 1',
                    default='0'
                ) }},
                1.0
             ) >= 0.80 THEN 'adherent'
        WHEN LEAST(
                {{ pulsetrack.safe_divide(
                    'f.total_days_supplied',
                    'DATE_DIFF(\'day\', p.first_prescription_date, p.last_prescription_date) + 1',
                    default='0'
                ) }},
                1.0
             ) >= 0.50 THEN 'partial'
        ELSE 'non_adherent'
    END                                                     AS adherence_band,

    CURRENT_TIMESTAMP                                       AS dbt_loaded_at
FROM prescriptions AS p
FULL OUTER JOIN fill_summary AS f
    ON  f.patient_key = p.patient_key
    AND f.drug_name   = p.drug_name
