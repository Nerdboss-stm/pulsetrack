{{ config(materialized='table', tags=['gold', 'analytics', 'identity']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- identity_resolution_kpis
-- ────────────────────────────────────────────────────────────────────────────
-- KPIs on the identity bridge. Mirrors data_quality/identity_metrics.py
-- output but as a dbt mart so analytics/BI tools can chart it over time.
--
-- One row per (identifier_type, link_status, link_method).
-- A second roll-up to bridge-wide totals for headline KPI.
-- ────────────────────────────────────────────────────────────────────────────

WITH bridge AS (
    SELECT * FROM {{ ref('stg_identity_bridge') }}
),

by_breakdown AS (
    SELECT
        identifier_type,
        link_status,
        link_method,
        COUNT(*) AS row_count,
        COUNT(DISTINCT patient_key) AS unique_patients
    FROM bridge
    GROUP BY 1, 2, 3
),

totals AS (
    SELECT
        COUNT(*)                                                AS total_bridge_rows,
        SUM(CASE WHEN link_status = 'linked' THEN 1 ELSE 0 END) AS total_linked,
        SUM(CASE WHEN link_status != 'linked' THEN 1 ELSE 0 END)
                                                                AS total_pending,
        COUNT(DISTINCT patient_key)                             AS total_unique_patients
    FROM bridge
)

SELECT
    -- Per-breakdown rows
    'breakdown'                                          AS row_kind,
    by_breakdown.identifier_type,
    by_breakdown.link_status,
    by_breakdown.link_method,
    by_breakdown.row_count,
    by_breakdown.unique_patients,
    NULL::DOUBLE                                         AS overall_link_rate,
    CURRENT_TIMESTAMP                                    AS dbt_loaded_at
FROM by_breakdown

UNION ALL

SELECT
    -- Single-row rollup (headline KPI)
    'rollup'                                             AS row_kind,
    NULL                                                 AS identifier_type,
    NULL                                                 AS link_status,
    NULL                                                 AS link_method,
    total_bridge_rows                                    AS row_count,
    total_unique_patients                                AS unique_patients,
    {{ pulsetrack.safe_divide('total_linked', 'total_bridge_rows', default='0') }}
                                                         AS overall_link_rate,
    CURRENT_TIMESTAMP                                    AS dbt_loaded_at
FROM totals
