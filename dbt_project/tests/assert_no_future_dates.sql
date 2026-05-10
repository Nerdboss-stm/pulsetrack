-- ────────────────────────────────────────────────────────────────────────────
-- assert_no_future_dates
-- ────────────────────────────────────────────────────────────────────────────
-- Asserts no event_timestamp in fact tables is in the future. Future
-- timestamps indicate either a producer-side clock skew or a malicious
-- payload — both worth surfacing.
--
-- Allow a 1-hour future tolerance for clock-skew-on-edge-devices (some
-- wearables drift; we don't want to nuisance-alert on an hour off).
-- ────────────────────────────────────────────────────────────────────────────

{{ config(severity='warn') }}

SELECT *
FROM {{ ref('fact_vital_reading') }}
WHERE event_timestamp > CURRENT_TIMESTAMP + INTERVAL '1 hour'
   OR sync_timestamp > CURRENT_TIMESTAMP + INTERVAL '1 hour'
