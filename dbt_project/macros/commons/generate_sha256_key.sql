{# ──────────────────────────────────────────────────────────────────────────
   generate_sha256_key
   ──────────────────────────────────────────────────────────────────────────
   SHA-256 surrogate-key generator. Drop-in replacement for
   ``dbt_utils.generate_surrogate_key`` which uses MD5.

   Why SHA-256 instead of MD5:
     * MD5 has known collision vulnerabilities. For non-adversarial keys
       it's mathematically fine, but for healthcare data (potentially PHI-
       adjacent identifiers) the cryptographic property matters for audit.
     * SHA-256 is the WHOOP-published standard for surrogate keys
       (matches the patient_key generation in the streaming identity bridge,
       see transformations/identity_resolution/patient_identity_bridge.py
       which uses ``sha2(lower(col), 256)``).
     * 256-bit output → effective collision-free at our scale.

   Behavior:
     * NULL columns are coerced to a literal '_dbt_utils_surrogate_key_null_'
       so a NULL email + 'foo' produces a different key than 'foo' + NULL.
     * Columns are lower-cased before hashing (case-insensitivity is the
       default for email/MRN-style identifiers).
     * Concatenated with a sentinel '||' between columns to prevent
       'a' + 'bc' colliding with 'ab' + 'c'.

   Usage:
     {{ pulsetrack.generate_sha256_key(['patient_email']) }}
     {{ pulsetrack.generate_sha256_key(['device_account_id', 'firmware_version']) }}
   ────────────────────────────────────────────────────────────────────────── #}
{% macro generate_sha256_key(field_list) %}
    {%- set fields = [] -%}
    {%- for field in field_list -%}
        {%- do fields.append(
            "coalesce(cast(lower(cast(" ~ field ~ " as " ~ dbt.type_string() ~ ")) as " ~ dbt.type_string() ~ "), '_dbt_utils_surrogate_key_null_')"
        ) -%}
        {%- if not loop.last -%}
            {%- do fields.append("'||'") -%}
        {%- endif -%}
    {%- endfor -%}
    {{ dbt.hash(dbt.concat(fields)) }}
{% endmacro %}

{# DuckDB-specific override: ``hash`` defaults to MD5; override to SHA-256.
   For other adapters, dbt.hash falls through to the adapter default (which is
   MD5 on most). We want SHA-256 explicitly; redefine via dispatch when running
   on DuckDB. #}
{% macro duckdb__hash(field) %}
    sha256(cast({{ field }} as varchar))
{% endmacro %}

{% macro snowflake__hash(field) %}
    sha2({{ field }}, 256)
{% endmacro %}
