{{ config(
    materialized='view',
    tags=['staging', 'sensor', 'bronze_consumer', 'lineage']
) }}

-- ────────────────────────────────────────────────────────────────────────────
-- stg_bronze_sensor_readings
-- ────────────────────────────────────────────────────────────────────────────
-- 1:1 staging view over the BRONZE ``sensor_readings`` table — the raw
-- Kafka-landed events before silver applies validation, deduplication, or
-- range checks.
--
-- Why a separate staging model for bronze when stg_sensor_readings already
-- exists for silver?
--   1. LINEAGE: dbt's exposure graph + Elementary's quality dashboard need
--      both layers represented so we can quantify "rows dropped at silver"
--      and "rows fixed up at silver" without a custom audit query.
--   2. DLQ DEBUG: when silver MERGE rejects rows (range fail, schema fail,
--      late-arrival > 7d), the analyst needs the bronze copy to debug.
--      ``stg_bronze_sensor_readings`` exposes that without granting raw
--      catalog access.
--   3. AUDITS: HIPAA/SOC2 audits ask "what raw data did we receive?". Bronze
--      is the answer; staging it as dbt-managed makes it part of the
--      documentation tree.
--
-- This view does NOT filter or transform. Bronze is bronze. Casts are
-- limited to making downstream uniqueness tests possible.
-- ────────────────────────────────────────────────────────────────────────────

WITH source AS (
    SELECT * FROM {{ source('bronze', 'sensor_readings') }}
),

renamed AS (
    SELECT
        -- Surrogate over (reading_id, metric_name, ingestion_timestamp) —
        -- bronze CAN have duplicates (at-least-once Kafka delivery), so the
        -- ingestion_timestamp differentiates them. Silver dedups; bronze
        -- keeps the history for audit.
        {{ pulsetrack.generate_sha256_key([
            'reading_id', 'metric_name', 'ingestion_timestamp'
        ]) }} AS bronze_reading_key,

        reading_id,
        device_id,
        device_type,
        device_account_id,
        patient_email,
        metric_name,
        CAST(metric_value AS DOUBLE)         AS metric_value,
        firmware_version,
        CAST(battery_pct AS INTEGER)         AS battery_pct,
        CAST(event_timestamp AS TIMESTAMP)   AS event_timestamp,
        CAST(sync_timestamp AS TIMESTAMP)    AS sync_timestamp,
        CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
        source_type,
        kafka_topic,
        kafka_partition,
        CAST(kafka_offset AS BIGINT)         AS kafka_offset,

        -- Derived: end-to-end latency in seconds. event → kafka land.
        EXTRACT(EPOCH FROM (ingestion_timestamp - event_timestamp))
                                             AS ingestion_latency_seconds,

        -- Derived: date partition.
        CAST(event_timestamp AS DATE)        AS event_date
    FROM source
)

SELECT * FROM renamed
