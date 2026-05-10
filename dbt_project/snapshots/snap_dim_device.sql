{# ──────────────────────────────────────────────────────────────────────────
   snap_dim_device — dbt-managed SCD2 snapshot.

   Tracks (device_id, firmware_version) over time. Each ``dbt snapshot`` run:
     1. Reads the current state from the ``stg_sensor_readings`` query.
     2. Compares against the snapshot's prior state.
     3. Closes existing rows (sets ``dbt_valid_to``) where the tracked
        columns changed.
     4. Inserts new rows for the changes.

   Why both this snapshot AND ``int_device_firmware_scd2``:
     - Snapshot: dbt-managed; survives source-data deletes, has ``dbt_valid_from``
       / ``dbt_valid_to`` automatically. Authoritative.
     - int_device_firmware_scd2: derivation from current source state.
       Reproducible from a backfill but doesn't preserve closed rows whose
       source data has been GC'd.

   Run cadence: weekly (matches the dbt release cadence). The Spark
   streaming pipeline updates dim_device too on its own cadence; the dbt
   snapshot is the warehouse-side authority.
   ────────────────────────────────────────────────────────────────────────── #}

{% snapshot snap_dim_device %}
{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='device_id',
      check_cols=['firmware_version', 'device_type'],
    )
}}

WITH device_state AS (
    SELECT
        device_id,
        ANY_VALUE(device_type)         AS device_type,
        ANY_VALUE(firmware_version)    AS firmware_version,
        MAX(event_timestamp)           AS last_seen_at
    FROM {{ ref('stg_sensor_readings') }}
    WHERE device_id IS NOT NULL
    GROUP BY device_id
)
SELECT * FROM device_state

{% endsnapshot %}
