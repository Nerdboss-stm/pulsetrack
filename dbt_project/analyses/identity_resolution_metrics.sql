-- ────────────────────────────────────────────────────────────────────────────
-- identity_resolution_metrics (analysis)
-- ────────────────────────────────────────────────────────────────────────────
-- Analyses are one-off SQL queries the team uses for ad-hoc analytics.
-- They're compiled (via `dbt compile`) into target/compiled/.../analyses/
-- but NOT executed during `dbt run` or `dbt build`.
--
-- This one mirrors data_quality/identity_metrics.py at the dbt layer.
-- Run via:  dbt compile --select analysis:identity_resolution_metrics
-- Then copy the SQL out of target/compiled/... and execute in your warehouse.
-- ────────────────────────────────────────────────────────────────────────────

WITH bridge AS (
    SELECT * FROM {{ ref('stg_identity_bridge') }}
),

per_type AS (
    SELECT
        identifier_type,
        COUNT(*)                                                    AS row_count,
        SUM(CASE WHEN link_status = 'linked' THEN 1 ELSE 0 END)     AS linked_count,
        SUM(CASE WHEN link_status != 'linked' THEN 1 ELSE 0 END)    AS pending_count,
        COUNT(DISTINCT patient_key)                                 AS unique_patients,
        {{ pulsetrack.safe_divide(
            "SUM(CASE WHEN link_status = 'linked' THEN 1 ELSE 0 END)",
            "COUNT(*)",
            default='0'
        ) }} AS link_rate
    FROM bridge
    GROUP BY identifier_type
),

per_method AS (
    SELECT
        link_method,
        link_status,
        COUNT(*)                            AS row_count,
        COUNT(DISTINCT patient_key)         AS unique_patients
    FROM bridge
    GROUP BY 1, 2
),

bridge_total AS (
    SELECT
        COUNT(*)                                                    AS total_rows,
        COUNT(DISTINCT patient_key)                                 AS unique_patients,
        SUM(CASE WHEN link_status = 'linked' THEN 1 ELSE 0 END)     AS linked,
        SUM(CASE WHEN link_status != 'linked' THEN 1 ELSE 0 END)    AS pending,
        {{ pulsetrack.safe_divide(
            "SUM(CASE WHEN link_status = 'linked' THEN 1 ELSE 0 END) * 100.0",
            "COUNT(*)",
            default='0'
        ) }} AS link_rate_pct
    FROM bridge
)

SELECT
    'per_type' AS section,
    identifier_type AS dim_1,
    NULL            AS dim_2,
    row_count,
    linked_count,
    pending_count,
    unique_patients,
    link_rate
FROM per_type

UNION ALL

SELECT
    'per_method' AS section,
    link_method  AS dim_1,
    link_status  AS dim_2,
    row_count,
    NULL,
    NULL,
    unique_patients,
    NULL
FROM per_method

UNION ALL

SELECT
    'overall' AS section,
    'all'     AS dim_1,
    NULL,
    total_rows,
    linked,
    pending,
    unique_patients,
    link_rate_pct
FROM bridge_total
