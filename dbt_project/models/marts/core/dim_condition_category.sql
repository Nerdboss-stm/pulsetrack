{{ config(materialized='table', tags=['gold', 'core', 'dim']) }}

-- ICD-10 chapter rollup dimension. One row per chapter; serves as the parent
-- of dim_condition (not implemented here — would join icd10_codes to this).

SELECT
    category_key,
    category_code,
    category_name,
    icd10_chapter,
    description,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM {{ ref('condition_categories') }}
