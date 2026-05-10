{{ config(materialized='ephemeral', tags=['intermediate', 'patient']) }}

-- ────────────────────────────────────────────────────────────────────────────
-- int_patient_enriched
-- ────────────────────────────────────────────────────────────────────────────
-- Aggregate per-patient view across all silver sources. Used by:
--   - dim_patient (the gold dimension)
--   - patient_health_360 (analytics wide table)
--
-- For each patient_key, computes:
--   - Primary identifier (canonical email, MRN)
--   - Condition counts (active, primary, total)
--   - Medication counts (active, total)
--   - Pharmacy fill counts and severity stats
--   - Device count + most recent firmware
--   - Earliest and latest event timestamps across all sources
-- ────────────────────────────────────────────────────────────────────────────

WITH bridge AS (
    SELECT * FROM {{ ref('stg_identity_bridge') }}
    WHERE link_status = 'linked'
      AND patient_key IS NOT NULL
),

-- Master patient list — every distinct patient_key the bridge knows about.
-- This is the universe; downstream LEFT JOINs handle patients with partial
-- coverage (e.g., EHR-only patients without sensor data still appear).
all_patients AS (
    SELECT DISTINCT patient_key FROM bridge
),

emails AS (
    SELECT
        patient_key,
        MIN(identifier_value) AS patient_email
    FROM bridge
    WHERE identifier_type = 'email'
    GROUP BY patient_key
),

mrns AS (
    SELECT
        patient_key,
        MIN(identifier_value) AS mrn
    FROM bridge
    WHERE identifier_type = 'hospital_mrn'
    GROUP BY patient_key
),

device_accounts AS (
    SELECT
        patient_key,
        COUNT(DISTINCT identifier_value) AS device_account_count
    FROM bridge
    WHERE identifier_type = 'device_account_id'
    GROUP BY patient_key
),

conditions AS (
    SELECT
        patient_key,
        COUNT(*)                                      AS total_conditions,
        SUM(CASE WHEN clinical_status = 'active' THEN 1 ELSE 0 END)
                                                      AS active_conditions,
        SUM(CASE WHEN is_primary THEN 1 ELSE 0 END)   AS primary_conditions,
        MAX(severity_default)                         AS highest_condition_severity
    FROM {{ ref('stg_ehr_conditions') }}
    GROUP BY patient_key
),

medications AS (
    SELECT
        patient_key,
        COUNT(*)                                      AS total_medications,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END)    AS active_medications,
        COUNT(DISTINCT drug_class)                    AS distinct_drug_classes
    FROM {{ ref('stg_ehr_medications') }}
    GROUP BY patient_key
),

fills AS (
    SELECT
        patient_key,
        COUNT(*)                                      AS total_fills,
        MAX(adverse_event_score)                      AS max_adverse_event_score,
        SUM(CASE WHEN adverse_event != 'none' THEN 1 ELSE 0 END)
                                                      AS adverse_event_count
    FROM {{ ref('stg_pharmacy_fills') }}
    GROUP BY patient_key
),

sensor_recency AS (
    -- Earliest and latest sensor readings for each patient.
    SELECT
        b.patient_key,
        MIN(s.event_timestamp) AS first_sensor_event,
        MAX(s.event_timestamp) AS last_sensor_event,
        COUNT(DISTINCT s.device_id) AS device_count,
        MAX(s.firmware_version) AS most_recent_firmware
    FROM {{ ref('stg_sensor_readings') }} AS s
    INNER JOIN (
        SELECT identifier_value AS device_account_id, patient_key
        FROM bridge
        WHERE identifier_type = 'device_account_id'
    ) AS b USING (device_account_id)
    GROUP BY b.patient_key
)

SELECT
    ap.patient_key,
    e.patient_email,
    m.mrn,
    COALESCE(da.device_account_count, 0)              AS device_account_count,
    COALESCE(c.total_conditions, 0)                   AS total_conditions,
    COALESCE(c.active_conditions, 0)                  AS active_conditions,
    COALESCE(c.primary_conditions, 0)                 AS primary_conditions,
    c.highest_condition_severity,
    COALESCE(med.total_medications, 0)                AS total_medications,
    COALESCE(med.active_medications, 0)               AS active_medications,
    COALESCE(med.distinct_drug_classes, 0)            AS distinct_drug_classes,
    COALESCE(f.total_fills, 0)                        AS total_pharmacy_fills,
    COALESCE(f.adverse_event_count, 0)                AS adverse_event_count,
    f.max_adverse_event_score,
    sr.first_sensor_event,
    sr.last_sensor_event,
    COALESCE(sr.device_count, 0)                      AS device_count,
    sr.most_recent_firmware,

    -- Health summary score: high condition severity, many active meds,
    -- adverse events all push the score up. NULL-safe via COALESCE.
    (COALESCE(c.active_conditions, 0) * 2)
        + COALESCE(med.active_medications, 0)
        + (COALESCE(f.adverse_event_count, 0) * 3)    AS health_complexity_score
FROM all_patients AS ap
LEFT JOIN emails         AS e   USING (patient_key)
LEFT JOIN mrns           AS m   USING (patient_key)
LEFT JOIN device_accounts AS da USING (patient_key)
LEFT JOIN conditions     AS c   USING (patient_key)
LEFT JOIN medications    AS med USING (patient_key)
LEFT JOIN fills          AS f   USING (patient_key)
LEFT JOIN sensor_recency AS sr  USING (patient_key)
