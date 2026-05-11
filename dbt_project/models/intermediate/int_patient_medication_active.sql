{{ config(materialized='ephemeral', tags=['intermediate', 'medication', 'patient']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- int_patient_medication_active
-- ────────────────────────────────────────────────────────────────────────────
-- Per-(patient × active-medication) row covering the current state of every
-- ongoing medication. Built on top of ``int_medication_timeline`` so episode-
-- aware logic (re-prescription detection, days_since_prior_episode) is
-- available downstream without re-walking the timeline.
--
-- Active = is_ongoing = TRUE in the timeline (no end_date or end_date in
-- the future). Each row carries:
--   - medication identity (name, drug_class, rxnorm_code)
--   - episode position (episode_seq, is_re_prescription)
--   - duration (days_active, current_episode_duration_days)
--   - dose context (daily_dose_mg)
--
-- Used by:
--   - dim_medication (one row per current medication state for BI)
--   - medication_adherence (cross-references fill cadence against active
--     episodes)
--   - daily_health_summary (active_medication_list dimension)
-- ────────────────────────────────────────────────────────────────────────────

WITH timeline AS (
    SELECT * FROM {{ ref('int_medication_timeline') }}
    WHERE is_ongoing = TRUE
),

with_duration AS (
    SELECT
        patient_key,
        medication_key,
        medication_name,
        drug_class,
        rxnorm_code,
        start_date,
        end_date,
        effective_end_date,
        daily_dose_mg,
        episode_seq,
        days_since_prior_episode,
        is_re_prescription,
        medication_rank_within_patient,

        -- Days the patient has been on the current episode.
        DATE_DIFF('day', start_date, CURRENT_DATE)
            AS current_episode_duration_days,

        -- Day-count of the longest episode for this (patient, medication).
        -- Computed at the timeline grain; null for the first observation.
        DATE_DIFF('day', start_date, effective_end_date)
            AS episode_total_days
    FROM timeline
),

patient_summary AS (
    -- Pre-aggregate per-patient counts. We expose these on every row so
    -- downstream dim_medication can produce a one-row-per-patient view by
    -- selecting any row's summary columns.
    SELECT
        patient_key,
        COUNT(*)                                  AS active_medication_count,
        COUNT(DISTINCT drug_class)                AS active_drug_class_count,
        SUM(CASE WHEN is_re_prescription THEN 1 ELSE 0 END)
                                                  AS re_prescription_count,
        MAX(current_episode_duration_days)        AS longest_current_episode_days
    FROM with_duration
    GROUP BY patient_key
)

SELECT
    wd.*,
    ps.active_medication_count,
    ps.active_drug_class_count,
    ps.re_prescription_count,
    ps.longest_current_episode_days,

    -- Surrogate over (patient_key, medication_key, episode_seq) — primary
    -- key. Unique even when the same patient × medication has multiple
    -- closed-and-reopened episodes.
    {{ pulsetrack.generate_sha256_key([
        'wd.patient_key', 'wd.medication_key', 'wd.episode_seq'
    ]) }} AS active_medication_key
FROM with_duration AS wd
LEFT JOIN patient_summary AS ps USING (patient_key)
