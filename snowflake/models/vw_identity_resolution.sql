-- ============================================================================
-- VW_IDENTITY_RESOLUTION
-- ============================================================================
-- Identity-bridge KPIs. Snowflake equivalent of the dbt
-- ``identity_resolution_kpis`` mart + the
-- ``data_quality/identity_metrics.py`` Prometheus gauges.
--
-- Two row kinds in the same view (UNION ALL):
--   - 'breakdown': per (identifier_type, link_status, link_method)
--   - 'rollup':    one row with overall link_rate KPI
--
-- Schema note: the Glue table column is `match_method`; aliased here to
-- `link_method` to preserve the analytics naming convention (dbt mart
-- equivalent + Prometheus gauge label both use `link_method`).
-- ============================================================================

CREATE OR REPLACE VIEW PULSETRACK.ANALYTICS.VW_IDENTITY_RESOLUTION
COMMENT = 'Identity bridge link-rate KPIs — breakdown rows + headline rollup'
AS

WITH bridge AS (
    SELECT
        patient_key,
        identifier_type,
        link_status,
        match_method                          AS link_method
    FROM PULSETRACK.SILVER.IDENTITY_BRIDGE
),

by_breakdown AS (
    SELECT
        identifier_type,
        link_status,
        link_method,
        COUNT(*)                              AS row_count,
        COUNT(DISTINCT patient_key)           AS unique_patients
    FROM bridge
    GROUP BY 1, 2, 3
),

totals AS (
    SELECT
        COUNT(*)                                       AS total_bridge_rows,
        SUM(IFF(link_status = 'linked', 1, 0))         AS total_linked,
        SUM(IFF(link_status <> 'linked', 1, 0))        AS total_pending,
        COUNT(DISTINCT patient_key)                    AS total_unique_patients
    FROM bridge
)

SELECT
    'breakdown'                            AS row_kind,
    identifier_type,
    link_status,
    link_method,
    row_count,
    unique_patients,
    CAST(NULL AS DOUBLE)                   AS overall_link_rate,
    CURRENT_TIMESTAMP                      AS view_built_at
FROM by_breakdown

UNION ALL

SELECT
    'rollup'                               AS row_kind,
    NULL                                   AS identifier_type,
    NULL                                   AS link_status,
    NULL                                   AS link_method,
    total_bridge_rows                      AS row_count,
    total_unique_patients                  AS unique_patients,
    DIV0(total_linked::DOUBLE, total_bridge_rows::DOUBLE) AS overall_link_rate,
    CURRENT_TIMESTAMP                      AS view_built_at
FROM totals;
