{{ config(materialized='table', tags=['gold', 'analytics', 'whoop', 'personal']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- whoop_personal_health
-- ────────────────────────────────────────────────────────────────────────────
-- The operator's own WHOOP data, isolated. The PulseTrack platform ingests
-- the operator's WHOOP API account (source_type = 'whoop_api') alongside
-- simulator + clinical data. This model exposes only those whoop_api rows
-- so the operator can see "their own dashboard" without the synthetic
-- patients commingling.
--
-- Grain: (event_date) — one row per day, all WHOOP metrics pivoted wide.
--
-- Source filter: source_type = 'whoop_api'. By construction the WHOOP API
-- attaches to exactly one device_account_id, so there is no patient
-- ambiguity to resolve.
-- ────────────────────────────────────────────────────────────────────────────

WITH whoop_only AS (
    SELECT
        event_date,
        metric_name,
        metric_value,
        is_valid,
        battery_pct,
        firmware_version,
        device_id,
        event_timestamp,
        sync_latency_seconds
    FROM {{ ref('stg_sensor_readings') }}
    WHERE source_type = 'whoop_api'
),

daily_pivot AS (
    SELECT
        event_date,
        -- Pivot vital metrics into columns.
        AVG(CASE WHEN metric_name = 'heart_rate_bpm'  AND is_valid THEN metric_value END) AS avg_heart_rate,
        AVG(CASE WHEN metric_name = 'hrv_ms'           AND is_valid THEN metric_value END) AS avg_hrv,
        AVG(CASE WHEN metric_name = 'spo2_pct'         AND is_valid THEN metric_value END) AS avg_spo2,
        AVG(CASE WHEN metric_name = 'respiration_rate' AND is_valid THEN metric_value END) AS avg_respiration_rate,
        AVG(CASE WHEN metric_name = 'skin_temp_celsius' AND is_valid THEN metric_value END) AS avg_skin_temp,

        MAX(CASE WHEN metric_name = 'heart_rate_max_bpm' AND is_valid THEN metric_value END) AS max_heart_rate,

        SUM(CASE WHEN metric_name = 'steps_since_last'  AND is_valid THEN metric_value ELSE 0 END) AS total_steps,
        SUM(CASE WHEN metric_name = 'energy_kj'          AND is_valid THEN metric_value ELSE 0 END) AS total_energy_kj,

        -- Sleep proxies (WHOOP emits sleep_stage as an enum value 0–4).
        AVG(CASE WHEN metric_name = 'sleep_stage' AND is_valid THEN metric_value END) AS avg_sleep_stage,

        -- Reading counts for QA.
        COUNT(*)                                                 AS total_readings,
        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END)                AS valid_readings,
        AVG(battery_pct)                                         AS avg_battery_pct,
        AVG(sync_latency_seconds)                                AS avg_sync_latency_seconds,
        COUNT(DISTINCT device_id)                                AS distinct_devices,
        MAX(firmware_version)                                    AS most_recent_firmware
    FROM whoop_only
    GROUP BY event_date
)

SELECT
    -- Surrogate over (event_date). Primary key.
    {{ pulsetrack.generate_sha256_key(['event_date']) }}        AS whoop_day_key,
    event_date,
    CAST(STRFTIME(event_date, '%Y%m%d') AS INTEGER)             AS date_key,

    avg_heart_rate,
    max_heart_rate,
    avg_hrv,
    avg_spo2,
    avg_respiration_rate,
    avg_skin_temp,

    total_steps,
    total_energy_kj,
    avg_sleep_stage,

    total_readings,
    valid_readings,
    {{ pulsetrack.safe_divide('valid_readings', 'total_readings', default='NULL') }}
                                                                AS validity_rate,

    avg_battery_pct,
    avg_sync_latency_seconds,
    distinct_devices,
    most_recent_firmware,

    -- WHOOP-style "recovery" proxy — high HRV + low resting HR + good SpO2
    -- → high recovery. Crude formula; the real WHOOP recovery is proprietary.
    CASE
        WHEN avg_hrv IS NULL OR avg_heart_rate IS NULL THEN NULL
        ELSE LEAST(100, GREATEST(0,
            (COALESCE(avg_hrv, 30) * 1.0)
            - (COALESCE(avg_heart_rate, 70) * 0.5)
            + (COALESCE(avg_spo2, 95) * 0.3)
        ))
    END                                                         AS recovery_proxy_score,

    CURRENT_TIMESTAMP                                           AS dbt_loaded_at
FROM daily_pivot
