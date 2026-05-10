{{ config(materialized='view', tags=['staging', 'identity']) }}

-- 1:1 staging view over silver ``identity_bridge``. Adds a deterministic
-- bridge_key surrogate so downstream uniqueness tests can target a single
-- column.

WITH source AS (
    SELECT * FROM {{ source('silver', 'identity_bridge') }}
),

renamed AS (
    SELECT
        {{ pulsetrack.generate_sha256_key(['identifier_type', 'identifier_value']) }}
            AS bridge_key,

        identifier_type,
        identifier_value,
        patient_key,
        link_status,
        link_method,
        CAST(recorded_at AS TIMESTAMP) AS recorded_at,

        -- Derived: is the row resolved to an actual patient?
        CASE
            WHEN patient_key IS NOT NULL AND link_status = 'linked' THEN TRUE
            ELSE FALSE
        END AS is_resolved
    FROM source
)

SELECT * FROM renamed
