{# ──────────────────────────────────────────────────────────────────────────
   date_spine
   ──────────────────────────────────────────────────────────────────────────
   Generate a contiguous date series between ``start_date`` and ``end_date``.

   Used by:
     - dim_date (one row per calendar day with medical-calendar attributes)
     - vital_trend_analysis (rolling-window aggregations need a complete
       date axis even on dates with zero readings)
     - data_freshness_report (gap detection — find dates with no data)

   Why a custom macro instead of dbt_utils.date_spine:
     - dbt_utils.date_spine is fine for most cases; this wrapper:
       1. Defaults the range to project-wide dates (timezone aware via var).
       2. Adds medical-calendar attributes the moment they're computed
          (consolidates dim_date generation).
     - Clean separation: the wrapper sets policy (range, timezone),
       dbt_utils does the row generation.

   Usage:
     {{ pulsetrack.date_spine('2020-01-01', '2030-12-31') }}

   Returns a CTE with a single column ``date_day`` of type DATE.
   ────────────────────────────────────────────────────────────────────────── #}
{% macro date_spine(start_date='2020-01-01', end_date='2035-12-31') %}
    SELECT date_day::DATE AS date_day
    FROM (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('" ~ start_date ~ "' as date)",
            end_date="cast('" ~ end_date ~ "' as date)"
        ) }}
    ) AS spine
{% endmacro %}
