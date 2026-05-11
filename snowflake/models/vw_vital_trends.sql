-- ============================================================================
-- VW_VITAL_TRENDS
-- ============================================================================
-- 7/14/30-day rolling windows for each (patient × metric × day) with
-- LAG/LEAD for day-over-day and week-over-week changes. Plus a 30-day
-- z-score for anomaly detection.
--
-- Powers BI line charts ("is heart rate trending up vs baseline?") and
-- feeds the anomaly dashboard via ABS(z_score_30d) > 3 threshold.
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_VITAL_TRENDS
COMMENT = 'Rolling 7/14/30-day vital averages + z-score + DoD/WoW change'
AS

WITH daily AS (
    SELECT
        patient_key,
        metric_key,
        metric_name,
        date_key,
        event_date,
        avg_value,
        valid_reading_count
    FROM PULSETRACK.GOLD.FACT_VITAL_DAILY_SUMMARY
    WHERE valid_reading_count > 0
)

SELECT
    patient_key,
    metric_key,
    metric_name,
    date_key,
    event_date,
    avg_value                                              AS daily_avg,

    -- 7-day rolling.
    AVG(avg_value) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg,

    -- 14-day rolling.
    AVG(avg_value) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS rolling_14d_avg,

    -- 30-day rolling avg + stddev (for z-score).
    AVG(avg_value) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_avg,
    STDDEV(avg_value) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_stddev,

    -- Z-score vs 30-day baseline. NULL-safe via CASE.
    CASE
        WHEN STDDEV(avg_value) OVER (
                PARTITION BY patient_key, metric_key
                ORDER BY event_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
             ) = 0 THEN NULL
        ELSE (avg_value - AVG(avg_value) OVER (
                PARTITION BY patient_key, metric_key
                ORDER BY event_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
              ))
             / NULLIF(STDDEV(avg_value) OVER (
                PARTITION BY patient_key, metric_key
                ORDER BY event_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
             ), 0)
    END AS z_score_30d,

    -- Day-over-day change.
    avg_value - LAG(avg_value, 1) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
    ) AS day_over_day_change,

    -- Week-over-week change.
    avg_value - LAG(avg_value, 7) OVER (
        PARTITION BY patient_key, metric_key
        ORDER BY event_date
    ) AS week_over_week_change

FROM daily;
