{# ──────────────────────────────────────────────────────────────────────────
   grant_select_to_role
   ──────────────────────────────────────────────────────────────────────────
   On-run-end hook (configured in dbt_project.yml) that grants SELECT on all
   marts to a downstream-consumer role.

   Why this is a project macro and not a commons macro:
     - It's PulseTrack-specific (encodes our role hierarchy).
     - It depends on the warehouse adapter (Snowflake-specific syntax).
     - It's a "policy" macro, not a reusable utility.

   Behavior:
     - Snowflake: emits ``GRANT SELECT ON ALL TABLES IN SCHEMA ... TO ROLE ...``
       for every schema dbt wrote to in this run.
     - DuckDB / other: no-op (no role-based access control).
     - Skips if --target is dev (we don't grant in local CI runs).

   Usage in dbt_project.yml:
     on-run-end:
       - "{{ pulsetrack.grant_select_to_role('PULSETRACK_READER') }}"
   ────────────────────────────────────────────────────────────────────────── #}
{% macro grant_select_to_role(role_name) %}
    {%- if target.type == 'snowflake' and target.name != 'dev' -%}
        {%- set schemas = schemas if schemas else [target.schema] -%}
        {%- for schema in schemas -%}
            GRANT SELECT ON ALL TABLES IN SCHEMA {{ target.database }}.{{ schema }}
              TO ROLE {{ role_name }};
            GRANT SELECT ON FUTURE TABLES IN SCHEMA {{ target.database }}.{{ schema }}
              TO ROLE {{ role_name }};
        {%- endfor -%}
    {%- else -%}
        -- {{ target.type }} target: no role-based access control or running locally.
        SELECT 1 AS skipped;
    {%- endif -%}
{% endmacro %}
