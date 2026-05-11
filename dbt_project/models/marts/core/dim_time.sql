{{ config(materialized='table', tags=['gold', 'core', 'dim']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- dim_time
-- ────────────────────────────────────────────────────────────────────────────
-- Time-of-day dimension. Exactly 1,440 rows (24 hours × 60 minutes). One
-- row per minute-of-day. Useful for hourly + minute-level vital
-- aggregations where dim_date is too coarse.
--
-- Surrogate key = HHMM as integer (e.g., 0930 → 930, 1330 → 1330). Joins to
-- fact tables via EXTRACT(HOUR ...) * 100 + EXTRACT(MINUTE ...).
--
-- Captures clinical "shift" attributes useful for ER/inpatient analytics:
--   - is_business_hours (09:00–17:00)
--   - is_overnight      (22:00–05:59)
--   - is_morning_round  (06:00–09:59)
--   - is_evening_round  (18:00–21:59)
--   - shift             (day / evening / night) — standard nursing shifts
--
-- Generated via the dbt_utils.generate_series macro (1440 rows). No external
-- seed — purely computed.
-- ────────────────────────────────────────────────────────────────────────────

WITH minutes AS (
    {{ dbt_utils.generate_series(upper_bound=1440) }}
),

-- generated_number runs 1..1440; convert to 0..1439 for minute-of-day.
spine AS (
    SELECT
        (generated_number - 1)                                AS minute_of_day
    FROM minutes
)

SELECT
    -- Surrogate: HHMM as integer (matching dim_date's YYYYMMDD pattern).
    (FLOOR(minute_of_day / 60) * 100 + (minute_of_day % 60))  AS time_key,

    minute_of_day,
    FLOOR(minute_of_day / 60)                                 AS hour_of_day,
    (minute_of_day % 60)                                      AS minute_of_hour,

    -- 24-hour clock string (HH:MM). For BI display.
    LPAD(CAST(FLOOR(minute_of_day / 60) AS VARCHAR), 2, '0')
        || ':'
        || LPAD(CAST(minute_of_day % 60 AS VARCHAR), 2, '0')  AS time_24h_label,

    -- 12-hour clock string (HH:MM AM/PM).
    CASE
        WHEN FLOOR(minute_of_day / 60) = 0       THEN '12:'
        WHEN FLOOR(minute_of_day / 60) > 12      THEN LPAD(CAST(FLOOR(minute_of_day / 60) - 12 AS VARCHAR), 2, '0') || ':'
        ELSE LPAD(CAST(FLOOR(minute_of_day / 60) AS VARCHAR), 2, '0') || ':'
    END
    || LPAD(CAST(minute_of_day % 60 AS VARCHAR), 2, '0')
    || CASE WHEN FLOOR(minute_of_day / 60) < 12 THEN ' AM' ELSE ' PM' END
        AS time_12h_label,

    -- Quarter-hour bucket label (00, 15, 30, 45).
    CASE
        WHEN minute_of_day % 60 < 15  THEN '00'
        WHEN minute_of_day % 60 < 30  THEN '15'
        WHEN minute_of_day % 60 < 45  THEN '30'
        ELSE '45'
    END                                                       AS quarter_hour_bucket,

    -- Period-of-day labels.
    CASE
        WHEN FLOOR(minute_of_day / 60) BETWEEN 5  AND 11  THEN 'morning'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 12 AND 16  THEN 'afternoon'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 17 AND 20  THEN 'evening'
        ELSE 'night'
    END                                                       AS period_of_day,

    -- Clinical-shift labels — standard 3-shift nursing.
    CASE
        WHEN FLOOR(minute_of_day / 60) BETWEEN 7  AND 14  THEN 'day_shift'
        WHEN FLOOR(minute_of_day / 60) BETWEEN 15 AND 22  THEN 'evening_shift'
        ELSE 'night_shift'
    END                                                       AS clinical_shift,

    -- Boolean flags useful for filters.
    (FLOOR(minute_of_day / 60) BETWEEN 9 AND 16)              AS is_business_hours,
    (FLOOR(minute_of_day / 60) >= 22 OR FLOOR(minute_of_day / 60) <= 5)
                                                              AS is_overnight,
    (FLOOR(minute_of_day / 60) BETWEEN 6 AND 9)               AS is_morning_round,
    (FLOOR(minute_of_day / 60) BETWEEN 18 AND 21)             AS is_evening_round,

    CURRENT_TIMESTAMP                                         AS dbt_loaded_at
FROM spine
