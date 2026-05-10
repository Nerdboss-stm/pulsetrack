-- ────────────────────────────────────────────────────────────────────────────
-- data_freshness_report (analysis)
-- ────────────────────────────────────────────────────────────────────────────
-- Per-source freshness (how recent is the latest data?) and gap analysis
-- (which dates have suspiciously few/no records?). One row per source per
-- recent date.
--
-- Run via: dbt compile --select analysis:data_freshness_report
-- ────────────────────────────────────────────────────────────────────────────

WITH spine AS (
    SELECT date_day AS event_date
    FROM ({{ pulsetrack.date_spine(
        start_date='2026-04-01',
        end_date='2026-05-15'
    ) }}) AS s
),

sensor_per_day AS (
    SELECT
        event_date,
        COUNT(*) AS row_count,
        COUNT(DISTINCT device_id) AS distinct_devices,
        MAX(sync_timestamp) AS latest_sync
    FROM {{ ref('stg_sensor_readings') }}
    GROUP BY event_date
),

ehr_per_day AS (
    SELECT
        CAST(recorded_at AS DATE) AS event_date,
        COUNT(*)   AS row_count,
        MAX(recorded_at) AS latest_record
    FROM {{ ref('stg_ehr_conditions') }}
    GROUP BY CAST(recorded_at AS DATE)
),

pharmacy_per_day AS (
    SELECT
        CAST(event_timestamp AS DATE) AS event_date,
        COUNT(*) AS row_count,
        MAX(fda_received_at) AS latest_fda_received
    FROM {{ ref('stg_pharmacy_fills') }}
    GROUP BY CAST(event_timestamp AS DATE)
)

SELECT
    spine.event_date,
    COALESCE(s.row_count, 0)        AS sensor_rows,
    COALESCE(e.row_count, 0)        AS ehr_condition_rows,
    COALESCE(p.row_count, 0)        AS pharmacy_fill_rows,
    s.latest_sync                   AS sensor_latest_sync,
    e.latest_record                 AS ehr_latest_record,
    p.latest_fda_received           AS pharmacy_latest_received,

    -- Gap flags
    CASE WHEN COALESCE(s.row_count, 0) = 0 THEN TRUE ELSE FALSE END AS sensor_gap,
    CASE WHEN COALESCE(e.row_count, 0) = 0 THEN TRUE ELSE FALSE END AS ehr_gap,
    CASE WHEN COALESCE(p.row_count, 0) = 0 THEN TRUE ELSE FALSE END AS pharmacy_gap
FROM spine
LEFT JOIN sensor_per_day   AS s USING (event_date)
LEFT JOIN ehr_per_day      AS e USING (event_date)
LEFT JOIN pharmacy_per_day AS p USING (event_date)
ORDER BY spine.event_date DESC
