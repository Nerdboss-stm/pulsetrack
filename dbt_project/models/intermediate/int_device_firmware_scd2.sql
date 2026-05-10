{{ config(materialized='ephemeral', tags=['intermediate', 'device', 'scd2']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- int_device_firmware_scd2
-- ────────────────────────────────────────────────────────────────────────────
-- Reconstructs SCD2 firmware history from sensor readings. For each
-- (device_id, firmware_version) combination:
--   - valid_from = first event_timestamp seen with this firmware
--   - valid_to   = first event_timestamp seen with the NEXT firmware
--                  (or far_future if currently active)
--   - is_current = whether this is the latest firmware for the device
--
-- Used by:
--   - dim_device (the gold dimension)
--   - device_reliability analytics (firmware-version failure-rate analysis)
--
-- Note: this is a derivation FROM the streaming pipeline's history. The
-- canonical SCD2 lives in the snapshot at snapshots/snap_dim_device.sql.
-- We compute it here so the gold dim can be regenerated at any time without
-- the snapshot file (e.g., for backfills).
-- ────────────────────────────────────────────────────────────────────────────

WITH sensor AS (
    SELECT DISTINCT
        device_id,
        firmware_version,
        event_timestamp
    FROM {{ ref('stg_sensor_readings') }}
    WHERE device_id IS NOT NULL
      AND firmware_version IS NOT NULL
),

firmware_starts AS (
    -- For each (device, firmware), the first time we saw it.
    SELECT
        device_id,
        firmware_version,
        MIN(event_timestamp) AS valid_from
    FROM sensor
    GROUP BY 1, 2
),

with_next AS (
    -- Compute the NEXT firmware's start = this firmware's end (valid_to).
    SELECT
        device_id,
        firmware_version,
        valid_from,
        LEAD(valid_from) OVER (
            PARTITION BY device_id
            ORDER BY valid_from
        ) AS next_firmware_starts_at
    FROM firmware_starts
),

scd2 AS (
    SELECT
        {{ pulsetrack.generate_sha256_key(['device_id', 'firmware_version', 'valid_from']) }}
            AS device_scd_key,
        device_id,
        firmware_version,
        valid_from,
        COALESCE(
            next_firmware_starts_at,
            CAST('{{ var("scd2_far_future_date") }}' AS TIMESTAMP)
        ) AS valid_to,
        next_firmware_starts_at IS NULL AS is_current
    FROM with_next
)

SELECT * FROM scd2
