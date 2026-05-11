-- ============================================================================
-- 05_create_external_tables.sql
-- ============================================================================
-- External tables = the fallback when Glue Iceberg integration isn't
-- available (legacy accounts, accounts without Iceberg enabled).
-- Reads parquet directly from S3 via the external stages defined in 03.
--
-- Tradeoff vs Iceberg tables:
--   - No snapshot-aware metadata; every query lists S3 prefix again.
--   - No partition pruning at the catalog level — Snowflake scans
--     listed files.
--   - No schema evolution — column names match parquet schema verbatim.
--   - DROP+RECREATE if the underlying schema changes.
--
-- Use these only as a stop-gap. The Iceberg tables in 04 are the
-- production path.
-- ============================================================================

USE ROLE PULSETRACK_RW;
USE DATABASE PULSETRACK;
USE SCHEMA BRONZE;

-- ── Bronze sensor_readings external table ──────────────────────────────────
CREATE OR REPLACE EXTERNAL TABLE EXT_SENSOR_READINGS_RAW
    LOCATION                = @BRONZE_STAGE/sensor_readings/data/
    FILE_FORMAT             = (FORMAT_NAME = PARQUET_FF)
    AUTO_REFRESH            = FALSE
    PATTERN                 = '.*[.]parquet'
    COMMENT                 = 'Fallback for Iceberg unavailable. Use BRONZE.sensor_readings (iceberg) when possible.';

-- ── Silver sensor_readings external table ──────────────────────────────────
USE SCHEMA SILVER;

CREATE OR REPLACE EXTERNAL TABLE EXT_SILVER_SENSOR_READINGS
    LOCATION                = @SILVER_STAGE/sensor_readings/data/
    FILE_FORMAT             = (FORMAT_NAME = PARQUET_FF)
    AUTO_REFRESH            = FALSE
    PATTERN                 = '.*[.]parquet';

CREATE OR REPLACE EXTERNAL TABLE EXT_SILVER_EHR_CONDITIONS
    LOCATION                = @SILVER_STAGE/ehr_conditions/data/
    FILE_FORMAT             = (FORMAT_NAME = PARQUET_FF)
    AUTO_REFRESH            = FALSE
    PATTERN                 = '.*[.]parquet';

CREATE OR REPLACE EXTERNAL TABLE EXT_SILVER_IDENTITY_BRIDGE
    LOCATION                = @SILVER_STAGE/identity_bridge/data/
    FILE_FORMAT             = (FORMAT_NAME = PARQUET_FF)
    AUTO_REFRESH            = FALSE
    PATTERN                 = '.*[.]parquet';

-- ── Gold fact_vital_daily_summary external table ───────────────────────────
USE SCHEMA GOLD;

CREATE OR REPLACE EXTERNAL TABLE EXT_FACT_VITAL_DAILY_SUMMARY
    LOCATION                = @GOLD_STAGE/fact_vital_daily_summary/data/
    FILE_FORMAT             = (FORMAT_NAME = PARQUET_FF)
    AUTO_REFRESH            = FALSE
    PATTERN                 = '.*[.]parquet';

-- ── How to query ────────────────────────────────────────────────────────────
-- Iceberg-style nested columns become VARIANTs in external tables.
-- To project a Bronze decoded.device_id field:
--   SELECT VALUE:decoded:device_id::VARCHAR FROM EXT_SENSOR_READINGS_RAW LIMIT 10;
--
-- For typed queries, create views over the external tables that cast
-- each VARIANT field to its concrete type (see 06_create_views.sql).
