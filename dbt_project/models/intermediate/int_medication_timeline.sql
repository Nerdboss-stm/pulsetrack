{{ config(materialized='ephemeral', tags=['intermediate', 'medication']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- int_medication_timeline
-- ────────────────────────────────────────────────────────────────────────────
-- Per-patient chronological medication timeline. For each patient, lists each
-- medication episode with start/end + duration + sequence number + lag from
-- the prior medication.
--
-- Used by:
--   - patient_health_360 (timeline-aware patient view)
--   - anomaly_investigation (correlate vital changes with medication starts)
--
-- The "episode" concept: same patient + same medication restarting after a
-- gap is a new episode. Episodes are bounded by start_date and the next
-- start_date (or end_date if available).
-- ────────────────────────────────────────────────────────────────────────────

WITH meds AS (
    SELECT
        patient_key,
        medication_key,
        medication_name,
        drug_class,
        rxnorm_code,
        start_date,
        end_date,
        daily_dose_mg,
        is_active,
        days_on_medication
    FROM {{ ref('stg_ehr_medications') }}
),

with_sequence AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_key, medication_name
            ORDER BY start_date
        ) AS episode_seq,

        -- Gap from previous episode of the same medication (in days).
        DATE_DIFF(
            'day',
            LAG(end_date) OVER (
                PARTITION BY patient_key, medication_name
                ORDER BY start_date
            ),
            start_date
        ) AS days_since_prior_episode,

        -- Cumulative count of distinct medications the patient has been on.
        DENSE_RANK() OVER (
            PARTITION BY patient_key
            ORDER BY medication_name
        ) AS medication_rank_within_patient
    FROM meds
)

SELECT
    *,

    -- Episode is "ongoing" if active OR end_date is null/future.
    is_active AS is_ongoing,

    -- Episode end: end_date if set, else today (for active rows).
    COALESCE(end_date, CURRENT_DATE) AS effective_end_date,

    -- Was there a long gap before this episode? (likely re-prescription
    -- after discontinuation rather than ongoing therapy)
    CASE
        WHEN days_since_prior_episode IS NULL THEN FALSE       -- first episode
        WHEN days_since_prior_episode > 90 THEN TRUE
        ELSE FALSE
    END AS is_re_prescription
FROM with_sequence
