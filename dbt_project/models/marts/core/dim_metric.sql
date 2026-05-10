{{ config(materialized='table', tags=['gold', 'core', 'dim']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- dim_metric — junk dimension for sensor metrics.
--
-- Junk dim = pack low-cardinality, frequently-joined attributes into a
-- single dim. Saves on storage (one row per metric, not per fact) and on
-- join cost.
--
-- Source: ``metric_normal_ranges`` seed. Authoritative for clinical bounds;
-- updates flow through ``dbt seed`` → ``dbt run --select +dim_metric``.
-- ────────────────────────────────────────────────────────────────────────────

WITH src AS (
    SELECT
        metric_name,
        unit,
        normal_min,
        normal_max,
        critical_min,
        critical_max,
        category
    FROM {{ ref('metric_normal_ranges') }}
)

SELECT
    {{ pulsetrack.generate_sha256_key(['metric_name']) }} AS metric_key,
    metric_name,
    unit,
    CAST(normal_min AS DOUBLE)    AS normal_min,
    CAST(normal_max AS DOUBLE)    AS normal_max,
    CAST(critical_min AS DOUBLE)  AS critical_min,
    CAST(critical_max AS DOUBLE)  AS critical_max,
    category,

    -- Convenience derived columns for BI tools.
    normal_max - normal_min       AS normal_range_width,
    CURRENT_TIMESTAMP             AS dbt_loaded_at
FROM src
