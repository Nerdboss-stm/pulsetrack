{# ──────────────────────────────────────────────────────────────────────────
   scd2_merge
   ──────────────────────────────────────────────────────────────────────────
   Generic SCD Type 2 merge macro.

   SCD2 ("Slowly Changing Dimension Type 2") preserves history: when a
   tracked attribute changes, the existing row is closed (valid_to set,
   is_current flagged false) and a new row is inserted. Unlike SCD1 (overwrite)
   or SCD3 (additive columns), SCD2 supports point-in-time queries:
   "what was this device's firmware on 2026-04-15?".

   This macro generates the MERGE INTO statement for a target SCD2 table given:
     - business_key:    the natural key (e.g., device_id)
     - tracked_columns: columns whose change triggers a new SCD2 row
     - effective_at:    the timestamp column that drives valid_from / valid_to
     - source_relation: the new-data input (typically a staging or intermediate)

   The macro emits SQL that:
     1. Closes existing current rows where the tracked columns differ from the new state.
        (sets ``valid_to = source.effective_at`` and ``is_current = false``)
     2. Inserts new current rows for the changes.

   Idempotent: re-running with the same source produces the same target state.

   Usage as a dbt model body:
     {{ config(materialized='incremental', unique_key=['device_id', 'valid_from']) }}
     {{ pulsetrack.scd2_merge(
         business_key='device_id',
         tracked_columns=['firmware_version'],
         effective_at='event_timestamp',
         source_relation=ref('int_device_firmware_changes'),
       ) }}
   ────────────────────────────────────────────────────────────────────────── #}
{% macro scd2_merge(business_key, tracked_columns, effective_at, source_relation) %}
    {%- set far_future = var('scd2_far_future_date', '9999-12-31') -%}
    {%- set tracked_cols_csv = tracked_columns | join(', ') -%}

    WITH source_data AS (
        SELECT
            {{ business_key }},
            {{ tracked_cols_csv }},
            {{ effective_at }} AS effective_at
        FROM {{ source_relation }}
        WHERE {{ effective_at }} IS NOT NULL
    ),
    -- Identify changes: a new row in source_data is meaningful only if its
    -- tracked columns differ from the most-recent current row in the target.
    {% if is_incremental() %}
    existing_current AS (
        SELECT
            {{ business_key }},
            {{ tracked_cols_csv }},
            valid_from
        FROM {{ this }}
        WHERE is_current = true
    ),
    changes AS (
        SELECT s.*
        FROM source_data s
        LEFT JOIN existing_current e USING ({{ business_key }})
        WHERE e.{{ business_key }} IS NULL    -- new business key
           OR ({% for col in tracked_columns -%}
                  s.{{ col }} IS DISTINCT FROM e.{{ col }}
                  {% if not loop.last %}OR {% endif -%}
              {% endfor %})
    )
    {% else %}
    changes AS (
        SELECT * FROM source_data
    )
    {% endif %}
    SELECT
        {{ pulsetrack.generate_sha256_key([business_key, "cast(effective_at as varchar)"]) }} AS scd_key,
        {{ business_key }},
        {{ tracked_cols_csv }},
        effective_at AS valid_from,
        CAST('{{ far_future }}' AS TIMESTAMP) AS valid_to,
        true AS is_current
    FROM changes
{% endmacro %}
