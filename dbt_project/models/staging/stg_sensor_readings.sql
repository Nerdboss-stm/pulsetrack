{{ config(
    materialized='view',
    tags=['staging', 'sensor', 'silver_consumer']
) }}

-- ────────────────────────────────────────────────────────────────────────────
-- stg_sensor_readings
-- ────────────────────────────────────────────────────────────────────────────
-- 1:1 staging view over the silver ``sensor_readings`` table.
--
-- Responsibilities at this layer:
--   1. Cast types where the silver table has ambiguous/string-like types.
--   2. Add a deterministic surrogate ``reading_metric_key`` for downstream
--      facts to join on.
--   3. Forward source data essentially unchanged — no transformation logic
--      lives at staging by convention. Logic goes in intermediate or marts.
--
-- The view materialization is intentional: staging is meant to be a thin
-- facade. A view recomputes on read, so freshness from silver is automatic.
-- ────────────────────────────────────────────────────────────────────────────

WITH source AS (
    SELECT * FROM {{ source('silver', 'sensor_readings') }}
),

renamed AS (
    SELECT
        -- Surrogate key for downstream fact-table joins. Per WHOOP commons,
        -- SHA-256 not MD5 (see macros/commons/generate_sha256_key.sql).
        {{ pulsetrack.generate_sha256_key(['reading_id', 'metric_name']) }}
            AS reading_metric_key,

        reading_id,
        device_id,
        device_type,
        device_account_id,
        patient_email,
        metric_name,
        CAST(metric_value AS DOUBLE)        AS metric_value,
        firmware_version,
        CAST(battery_pct AS INTEGER)        AS battery_pct,
        CAST(event_timestamp AS TIMESTAMP)  AS event_timestamp,
        CAST(sync_timestamp AS TIMESTAMP)   AS sync_timestamp,
        source_type,
        CAST(is_valid AS BOOLEAN)           AS is_valid,
        CAST(is_late_arriving AS BOOLEAN)   AS is_late_arriving,

        -- Derived: latency in seconds. Used by anomaly_investigation.
        EXTRACT(EPOCH FROM (sync_timestamp - event_timestamp))
            AS sync_latency_seconds,

        -- Date partition the downstream marts use for grain definitions.
        CAST(event_timestamp AS DATE)       AS event_date
    FROM source
)

SELECT * FROM renamed
