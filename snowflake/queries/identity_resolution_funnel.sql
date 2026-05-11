-- ============================================================================
-- identity_resolution_funnel.sql
-- ============================================================================
-- Identity-bridge funnel:
--   total_identifiers → linked → unique patients → patients with EHR
--   → patients with sensor data → patients with both
--
-- Surfaces where in the funnel patients drop off. The ideal state:
--   - 100% of EHR-known patient_emails are linked.
--   - High fraction of device_account_ids transitive-linked.
--   - Most patient_keys have both EHR + sensor data (joinable).
-- ============================================================================

USE WAREHOUSE PULSETRACK_WH;
USE ROLE PULSETRACK_READER;
USE DATABASE PULSETRACK;

WITH bridge AS (
    SELECT * FROM SILVER.IDENTITY_BRIDGE
),

per_type AS (
    SELECT
        identifier_type,
        COUNT(*)                                AS total,
        SUM(IFF(link_status = 'linked', 1, 0))  AS linked,
        COUNT(DISTINCT patient_key)             AS unique_patients
    FROM bridge
    GROUP BY 1
),

patients_with_ehr AS (
    SELECT DISTINCT patient_key
    FROM SILVER.EHR_CONDITIONS
),

patients_with_sensor AS (
    -- Sensor readings → patient_key resolution via bridge.
    SELECT DISTINCT b.patient_key
    FROM SILVER.IDENTITY_BRIDGE AS b
    INNER JOIN SILVER.SENSOR_READINGS AS s
        ON s.device_account_id = b.identifier_value
    WHERE b.identifier_type = 'device_account_id'
      AND b.link_status     = 'linked'
),

patients_with_both AS (
    SELECT a.patient_key
    FROM patients_with_ehr a
    INNER JOIN patients_with_sensor b USING (patient_key)
)

SELECT
    'identifiers_total'                    AS funnel_stage,
    SUM(total)                             AS value,
    NULL                                   AS pct_of_total
FROM per_type

UNION ALL

SELECT
    'identifiers_linked'                   AS funnel_stage,
    SUM(linked),
    DIV0(SUM(linked)::DOUBLE, SUM(total)::DOUBLE) * 100.0
FROM per_type

UNION ALL

SELECT
    'unique_patient_keys',
    COUNT(DISTINCT patient_key),
    NULL
FROM bridge

UNION ALL

SELECT
    'patients_with_ehr',
    COUNT(*),
    NULL
FROM patients_with_ehr

UNION ALL

SELECT
    'patients_with_sensor',
    COUNT(*),
    NULL
FROM patients_with_sensor

UNION ALL

SELECT
    'patients_with_both',
    COUNT(*),
    NULL
FROM patients_with_both

ORDER BY
    CASE funnel_stage
        WHEN 'identifiers_total'    THEN 1
        WHEN 'identifiers_linked'   THEN 2
        WHEN 'unique_patient_keys'  THEN 3
        WHEN 'patients_with_ehr'    THEN 4
        WHEN 'patients_with_sensor' THEN 5
        WHEN 'patients_with_both'   THEN 6
        ELSE 99
    END;
