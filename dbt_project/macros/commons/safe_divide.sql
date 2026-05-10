{# ──────────────────────────────────────────────────────────────────────────
   safe_divide
   ──────────────────────────────────────────────────────────────────────────
   NULL-safe division. Returns NULL instead of error or NaN when the
   denominator is zero or NULL.

   The case for a project-level macro: SQL's default behavior on
   division-by-zero varies by warehouse:
     - Snowflake: raises DIVISION_BY_ZERO error (default; configurable)
     - DuckDB:    returns NaN (silent — bad for downstream aggregations)
     - Spark:     returns NULL (acceptable but inconsistent with others)

   Centralizing in one macro:
     1. Makes the warehouse-portability explicit (every divide goes through here).
     2. Lets the team change semantics in one place (e.g. swap NULL for 0 if a
        downstream consumer prefers).
     3. Reads more naturally: ``safe_divide(numerator, denominator, default=NULL)``
        instead of ``CASE WHEN denominator = 0 OR denominator IS NULL THEN NULL
        ELSE numerator / denominator END``.

   Optional ``default`` arg lets callers specify a non-NULL fallback (e.g.
   when computing a per-patient rate: 0 readings → 0/0 → fallback to 0
   instead of NULL).

   Usage:
     {{ pulsetrack.safe_divide('valid_readings', 'total_readings') }} AS valid_rate
     {{ pulsetrack.safe_divide('linked_count', 'total_count', default=0) }} AS link_rate
   ────────────────────────────────────────────────────────────────────────── #}
{% macro safe_divide(numerator, denominator, default='NULL') %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
            THEN {{ default }}
        ELSE ({{ numerator }})::DOUBLE / ({{ denominator }})::DOUBLE
    END
{% endmacro %}
