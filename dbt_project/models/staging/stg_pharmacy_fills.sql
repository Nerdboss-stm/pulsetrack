{{ config(materialized='view', tags=['staging', 'pharmacy']) }}

-- 1:1 staging view over silver ``pharmacy_fills``. Adds surrogate keys and a
-- numeric severity score for the ``adverse_event`` enum so downstream
-- analytics can rank/aggregate.

WITH source AS (
    SELECT * FROM {{ source('silver', 'pharmacy_fills') }}
),

renamed AS (
    SELECT
        {{ pulsetrack.generate_sha256_key(['fill_id']) }} AS fill_key,
        {{ pulsetrack.generate_sha256_key(['patient_email']) }} AS patient_key,

        fill_id,
        report_id,
        patient_email,
        drug_name,
        drug_class,
        CAST(fill_quantity AS INTEGER)        AS fill_quantity,
        CAST(days_supply AS INTEGER)          AS days_supply,
        adverse_event,

        -- Numeric severity for ranking. Matches the SensorReading source_type
        -- enum's severity ordering: none < mild < moderate < severe < death.
        CASE adverse_event
            WHEN 'none'     THEN 0
            WHEN 'mild'     THEN 1
            WHEN 'moderate' THEN 2
            WHEN 'severe'   THEN 3
            WHEN 'death'    THEN 4
            ELSE NULL
        END AS adverse_event_score,

        CAST(event_timestamp AS TIMESTAMP)    AS event_timestamp,
        CAST(fda_received_at AS TIMESTAMP)    AS fda_received_at,
        CAST(is_valid AS BOOLEAN)             AS is_valid,

        -- Derived: how long FDA took to process the report.
        EXTRACT(EPOCH FROM (fda_received_at - event_timestamp)) / 86400.0
            AS fda_processing_days
    FROM source
)

SELECT * FROM renamed
