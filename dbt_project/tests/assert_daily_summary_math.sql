-- ────────────────────────────────────────────────────────────────────────────
-- assert_daily_summary_math
-- ────────────────────────────────────────────────────────────────────────────
-- Asserts arithmetic consistency in fact_vital_daily_summary:
--   - min_value <= avg_value <= max_value
--   - p10_value <= median_value <= p90_value
--   - validity_rate = valid / (valid + invalid) within float tolerance
--   - valid_reading_count + invalid_reading_count > 0 (no zero-count rows)
--
-- Failures here are not just bad data — they're computed bad data, which
-- means a logic bug in int_vital_daily_agg or fact_vital_daily_summary.
-- Block-severity in CI.
-- ────────────────────────────────────────────────────────────────────────────

{{ config(severity='error') }}

SELECT *
FROM {{ ref('fact_vital_daily_summary') }}
WHERE
    -- Min should not exceed avg (only when both are non-null)
    (min_value IS NOT NULL AND avg_value IS NOT NULL AND min_value > avg_value)
 OR (avg_value IS NOT NULL AND max_value IS NOT NULL AND avg_value > max_value)
 OR (p10_value IS NOT NULL AND median_value IS NOT NULL AND p10_value > median_value)
 OR (median_value IS NOT NULL AND p90_value IS NOT NULL AND median_value > p90_value)

 -- Validity rate consistency (within 0.01 tolerance for float math)
 OR (validity_rate IS NOT NULL
     AND ABS(validity_rate
             - (valid_reading_count::DOUBLE / (valid_reading_count + invalid_reading_count)::DOUBLE))
         > 0.01)

 -- A row exists in the fact only because there was at least one reading.
 OR (valid_reading_count + invalid_reading_count = 0)
