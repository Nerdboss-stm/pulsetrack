{{ config(materialized='table', tags=['gold', 'analytics', 'outcome', 'correlation']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- clinical_outcome_correlation
-- ────────────────────────────────────────────────────────────────────────────
-- Correlates vital trends in the 30 days prior to a condition becoming
-- active with the condition outcome. One row per (patient_key, icd10_code,
-- condition_first_active_date). Used to power:
--   - "What vital signs predicted this CHF flare-up?"
--   - "Do diabetic patients show HRV-drop 2 weeks before hyperglycemia?"
--
-- Methodology:
--   1. For each (patient × condition × first_active_date), find the
--      30-day window before condition_first_active_date.
--   2. Pull the average + slope of each vital from int_vital_daily_agg
--      during that window.
--   3. Compute correlation-proxy: did the vital trend up or down before
--      the condition activated?
--
-- This is intentionally NOT a real correlation coefficient (no t-test, no
-- p-value) — just trend direction. Cohort analysts can take it from here.
-- ────────────────────────────────────────────────────────────────────────────

WITH conditions_active AS (
    SELECT
        patient_key,
        icd10_code,
        condition_name,
        severity_default,
        MIN(onset_date)                              AS condition_first_active_date,
        MAX(recorded_at)                             AS condition_last_recorded_at,
        COUNT(*)                                     AS condition_observation_count,
        SUM(CASE WHEN clinical_status = 'active' THEN 1 ELSE 0 END)
                                                     AS active_observations
    FROM {{ ref('stg_ehr_conditions') }}
    WHERE clinical_status IN ('active', 'resolved')
    GROUP BY patient_key, icd10_code, condition_name, severity_default
),

vital_windows AS (
    SELECT
        v.patient_key,
        v.metric_name,
        v.event_date,
        v.avg_value,
        v.valid_reading_count
    FROM {{ ref('int_vital_daily_agg') }} AS v
    WHERE v.valid_reading_count > 0
),

-- For each (patient × condition), pull 30 days of vitals BEFORE first active.
prior_window AS (
    SELECT
        ca.patient_key,
        ca.icd10_code,
        ca.condition_first_active_date,
        v.metric_name,
        v.event_date,
        v.avg_value,

        -- Day offset (negative = before).
        DATE_DIFF('day', ca.condition_first_active_date, v.event_date)
            AS days_from_condition
    FROM conditions_active AS ca
    INNER JOIN vital_windows AS v ON v.patient_key = ca.patient_key
    WHERE v.event_date BETWEEN ca.condition_first_active_date - INTERVAL '30 days'
                          AND ca.condition_first_active_date - INTERVAL '1 day'
),

trend_per_metric AS (
    SELECT
        patient_key,
        icd10_code,
        condition_first_active_date,
        metric_name,
        AVG(avg_value)                                AS prior_30d_avg,
        STDDEV(avg_value)                             AS prior_30d_stddev,

        -- Linear-slope proxy: difference between last 7 days and first 7 days
        -- of the prior window.
        AVG(CASE WHEN days_from_condition >= -7  THEN avg_value END)
            - AVG(CASE WHEN days_from_condition <= -23 THEN avg_value END)
                                                      AS trend_delta_late_minus_early,
        COUNT(*)                                      AS observation_days
    FROM prior_window
    GROUP BY patient_key, icd10_code, condition_first_active_date, metric_name
)

SELECT
    -- Surrogate over (patient_key, icd10_code, condition_first_active_date,
    -- metric_name). Primary key.
    {{ pulsetrack.generate_sha256_key([
        't.patient_key', 't.icd10_code',
        't.condition_first_active_date', 't.metric_name'
    ]) }} AS correlation_key,

    t.patient_key,
    t.icd10_code,
    ca.condition_name,
    ca.severity_default,
    t.condition_first_active_date,
    t.metric_name,
    t.observation_days,
    t.prior_30d_avg,
    t.prior_30d_stddev,
    t.trend_delta_late_minus_early,

    -- Trend label.
    CASE
        WHEN t.trend_delta_late_minus_early IS NULL                THEN 'insufficient_data'
        WHEN ABS(t.trend_delta_late_minus_early) < 1               THEN 'stable'
        WHEN t.trend_delta_late_minus_early > 0                    THEN 'trending_up'
        ELSE 'trending_down'
    END                                                AS trend_label,

    -- Days of observation in the window, normalized.
    {{ pulsetrack.safe_divide('t.observation_days', '30', default='0') }}
                                                       AS window_coverage_rate,

    CURRENT_TIMESTAMP                                  AS dbt_loaded_at
FROM trend_per_metric AS t
INNER JOIN conditions_active AS ca
    ON  ca.patient_key                = t.patient_key
    AND ca.icd10_code                 = t.icd10_code
    AND ca.condition_first_active_date = t.condition_first_active_date
