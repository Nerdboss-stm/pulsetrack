{# ──────────────────────────────────────────────────────────────────────────
   classify_vital_range
   ──────────────────────────────────────────────────────────────────────────
   Given (metric_name, metric_value), return one of:
     'normal'     — within clinical normal range
     'warning'    — outside normal but plausible
     'critical'   — outside plausible range (clinical emergency)
     'unknown'    — no normal-range definition for the metric

   The thresholds come from the ``metric_normal_ranges`` seed which is the
   single source of truth for clinical bounds. Updates flow through:
     1. Edit seeds/metric_normal_ranges.csv
     2. dbt seed
     3. Models that reference this macro recompute on next dbt build

   Generates a CASE WHEN block at compile time so the SQL has the per-metric
   bounds inlined. Avoids a runtime JOIN to the seed for every row, which
   would be O(N) over the fact-table size.

   Usage in a SELECT:
     SELECT
         metric_name,
         metric_value,
         {{ pulsetrack.classify_vital_range('metric_name', 'metric_value') }} AS vital_status
     FROM {{ ref('stg_sensor_readings') }}
   ────────────────────────────────────────────────────────────────────────── #}
{% macro classify_vital_range(metric_col, value_col) %}
    {%- set ranges_query -%}
        SELECT metric_name, normal_min, normal_max, critical_min, critical_max
        FROM {{ ref('metric_normal_ranges') }}
    {%- endset -%}

    {%- set ranges = run_query(ranges_query) -%}

    {% if execute and ranges %}
        CASE
        {% for row in ranges %}
            WHEN {{ metric_col }} = '{{ row.metric_name }}' THEN
                CASE
                    WHEN {{ value_col }} IS NULL THEN 'unknown'
                    WHEN {{ value_col }} < {{ row.critical_min }} OR {{ value_col }} > {{ row.critical_max }}
                        THEN 'critical'
                    WHEN {{ value_col }} < {{ row.normal_min }} OR {{ value_col }} > {{ row.normal_max }}
                        THEN 'warning'
                    ELSE 'normal'
                END
        {% endfor %}
            ELSE 'unknown'
        END
    {% else %}
        {# Compile-time: ranges seed not yet available. Emit a placeholder
           that returns 'unknown' for everything; the next ``dbt run`` after
           ``dbt seed`` will pick up real thresholds. #}
        'unknown'
    {% endif %}
{% endmacro %}
