-- ============================================================================
-- 04_create_iceberg_tables.sql
-- ============================================================================
-- Snowflake Iceberg tables that wrap the EMR-written Iceberg tables in Glue.
-- This is the primary integration: Snowflake reads the same Iceberg
-- metadata + parquet files the Spark streaming pipeline writes. Single
-- source of truth on S3 + Glue, two compute engines (EMR + Snowflake)
-- consuming from it.
--
-- Iceberg in Snowflake notes:
--   - ICEBERG TABLE objects use the CATALOG INTEGRATION + EXTERNAL VOLUME
--     defined in 02_create_storage_integration.sql.
--   - Snowflake refreshes metadata on every query by default. For high-
--     read workloads, schedule REFRESH ICEBERG TABLE via task.
--   - DML (INSERT/UPDATE/DELETE) is disabled — these are EMR-written.
--     Snowflake is the read-side consumer.
-- ============================================================================

USE ROLE PULSETRACK_RW;
USE DATABASE PULSETRACK;

-- ── BRONZE Iceberg tables ───────────────────────────────────────────────────
USE SCHEMA BRONZE;

CREATE OR REPLACE ICEBERG TABLE sensor_readings
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'sensor_readings'
    CATALOG_NAMESPACE  = 'pulsetrack_bronze_dev'
    AUTO_REFRESH    = TRUE
    COMMENT         = 'Bronze sensor readings — raw Kafka → Iceberg via Spark streaming';

-- ── SILVER Iceberg tables ───────────────────────────────────────────────────
USE SCHEMA SILVER;

CREATE OR REPLACE ICEBERG TABLE sensor_readings
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'sensor_readings'
    CATALOG_NAMESPACE  = 'pulsetrack_silver_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE ehr_conditions
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'ehr_conditions'
    CATALOG_NAMESPACE  = 'pulsetrack_silver_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE ehr_medications
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'ehr_medications'
    CATALOG_NAMESPACE  = 'pulsetrack_silver_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE pharmacy_fills
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'pharmacy_fills'
    CATALOG_NAMESPACE  = 'pulsetrack_silver_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE identity_bridge
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'identity_bridge'
    CATALOG_NAMESPACE  = 'pulsetrack_silver_dev'
    AUTO_REFRESH    = TRUE;

-- ── GOLD Iceberg tables ─────────────────────────────────────────────────────
USE SCHEMA GOLD;

CREATE OR REPLACE ICEBERG TABLE dim_patient
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'dim_patient'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE dim_device
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'dim_device'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE dim_metric
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'dim_metric'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE dim_date
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'dim_date'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE fact_vital_daily_summary
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'fact_vital_daily_summary'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE fact_vital_reading
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'fact_vital_reading'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE fact_lab_result
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'fact_lab_result'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE fact_pharmacy_fill
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'fact_pharmacy_fill'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE;

CREATE OR REPLACE ICEBERG TABLE schema_migrations
    EXTERNAL_VOLUME = 'PULSETRACK_VOL'
    CATALOG         = 'PULSETRACK_GLUE'
    CATALOG_TABLE_NAME = 'schema_migrations'
    CATALOG_NAMESPACE  = 'pulsetrack_gold_dev'
    AUTO_REFRESH    = TRUE
    COMMENT         = 'Glacierbase migration ledger — read-only audit trail';

-- ── Refresh task ────────────────────────────────────────────────────────────
-- Snowflake's AUTO_REFRESH refreshes metadata on each query but it's
-- best-effort. For dashboards that need sub-minute freshness, schedule
-- explicit refresh:
CREATE OR REPLACE TASK PULSETRACK.GOLD.REFRESH_ICEBERG_METADATA
    SCHEDULE   = '5 MINUTE'
    WAREHOUSE  = PULSETRACK_WH
    COMMENT    = 'Re-discover Iceberg snapshots every 5 min for fresh metadata'
AS
    ALTER ICEBERG TABLE PULSETRACK.GOLD.fact_vital_daily_summary REFRESH;

-- Activate the task (suspended by default after create).
-- ALTER TASK PULSETRACK.GOLD.REFRESH_ICEBERG_METADATA RESUME;
