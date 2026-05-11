{{ config(materialized='ephemeral', tags=['intermediate', 'device', 'battery']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- int_device_battery_trends
-- ────────────────────────────────────────────────────────────────────────────
-- Per-(device × day) battery trend with 7-day moving average + day-over-day
-- delta. Identifies devices on a steady downward battery slope (replacement
-- needed) vs noisy readings (single bad day).
--
-- Grain: (device_id, event_date).
--
-- Used by:
--   - device_reliability (battery-degradation rate is a reliability proxy)
--   - daily_health_summary (battery confidence column)
--   - downstream alerting (predict device-failure 7 days out)
--
-- The 7-day moving average smooths spurious 1-day dips (device-not-worn
-- still reports last-known battery). The day-over-day delta is the leading
-- indicator: a -10 % single-day drop usually means the device was unworn,
-- but a -10 % 7-day trend is a battery issue.
-- ────────────────────────────────────────────────────────────────────────────

WITH daily_battery AS (
    SELECT
        device_id,
        device_type,
        firmware_version,
        event_date,
        AVG(CAST(battery_pct AS DOUBLE))      AS avg_battery_pct,
        MIN(CAST(battery_pct AS DOUBLE))      AS min_battery_pct,
        MAX(CAST(battery_pct AS DOUBLE))      AS max_battery_pct,
        COUNT(*)                              AS reading_count,
        SUM(CASE WHEN battery_pct < 20 THEN 1 ELSE 0 END) AS low_battery_reading_count
    FROM {{ ref('stg_sensor_readings') }}
    WHERE battery_pct IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

with_windows AS (
    SELECT
        device_id,
        device_type,
        firmware_version,
        event_date,
        avg_battery_pct,
        min_battery_pct,
        max_battery_pct,
        reading_count,
        low_battery_reading_count,

        -- 7-day trailing moving average of daily mean.
        AVG(avg_battery_pct) OVER (
            PARTITION BY device_id
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS battery_7d_moving_avg,

        -- Day-over-day delta on the moving average; isolates the slope from
        -- the day-by-day noise.
        avg_battery_pct - LAG(avg_battery_pct, 1) OVER (
            PARTITION BY device_id
            ORDER BY event_date
        ) AS battery_dod_delta,

        -- 7-day delta on the moving average. Negative = trending down.
        AVG(avg_battery_pct) OVER (
            PARTITION BY device_id
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) - LAG(
            AVG(avg_battery_pct) OVER (
                PARTITION BY device_id
                ORDER BY event_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            7
        ) OVER (
            PARTITION BY device_id
            ORDER BY event_date
        ) AS battery_7d_delta
    FROM daily_battery
)

SELECT
    -- Surrogate over (device_id, event_date). Primary key.
    {{ pulsetrack.generate_sha256_key(['device_id', 'event_date']) }}
        AS device_battery_day_key,

    device_id,
    device_type,
    firmware_version,
    event_date,
    avg_battery_pct,
    min_battery_pct,
    max_battery_pct,
    reading_count,
    low_battery_reading_count,
    battery_7d_moving_avg,
    battery_dod_delta,
    battery_7d_delta,

    -- Risk flag: 7-day trend below 30 % AND moving avg below 50 %.
    (battery_7d_delta < -10 AND battery_7d_moving_avg < 50) AS is_battery_at_risk
FROM with_windows
