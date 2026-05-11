-- ============================================================================
-- 03_create_stage.sql
-- ============================================================================
-- External stages pointing at the S3 lakehouse. Used for:
--   (a) Loading parquet files directly via COPY INTO (for one-off backfills).
--   (b) Reading raw parquet through external tables (the Iceberg path is
--       preferred — see 04_create_iceberg_tables.sql — but external tables
--       are the fallback when Glue catalog integration isn't available).
--   (c) Snowflake's automatic file format detection during ad-hoc analytics.
-- ============================================================================

USE ROLE PULSETRACK_ADMIN;
USE DATABASE PULSETRACK;
USE SCHEMA BRONZE;

-- ── File format: Parquet ────────────────────────────────────────────────────
CREATE OR REPLACE FILE FORMAT PARQUET_FF
    TYPE                 = PARQUET
    USE_LOGICAL_TYPE     = TRUE
    BINARY_AS_TEXT       = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    COMMENT              = 'Parquet reader config for PulseTrack lakehouse';

-- ── File format: JSON (for raw Avro envelopes if ever needed) ──────────────
CREATE OR REPLACE FILE FORMAT JSON_FF
    TYPE                 = JSON
    STRIP_OUTER_ARRAY    = FALSE
    COMMENT              = 'JSON reader for ad-hoc DLQ inspection';

-- ── Stages ──────────────────────────────────────────────────────────────────

-- Bronze stage — points at the raw bronze prefix.
CREATE OR REPLACE STAGE BRONZE_STAGE
    URL                  = 's3://pulsetrack-lakehouse-dev-03a28ee7/bronze/'
    STORAGE_INTEGRATION  = PULSETRACK_S3
    FILE_FORMAT          = PARQUET_FF
    COMMENT              = 'Raw bronze parquet files. Read via SELECT @BRONZE_STAGE/sensor_readings/data/...';

USE SCHEMA SILVER;

CREATE OR REPLACE STAGE SILVER_STAGE
    URL                  = 's3://pulsetrack-lakehouse-dev-03a28ee7/silver/'
    STORAGE_INTEGRATION  = PULSETRACK_S3
    FILE_FORMAT          = PARQUET_FF;

USE SCHEMA GOLD;

CREATE OR REPLACE STAGE GOLD_STAGE
    URL                  = 's3://pulsetrack-lakehouse-dev-03a28ee7/gold/'
    STORAGE_INTEGRATION  = PULSETRACK_S3
    FILE_FORMAT          = PARQUET_FF;

-- DLQ stage — for inspecting failed records.
USE SCHEMA BRONZE;
CREATE OR REPLACE STAGE DLQ_STAGE
    URL                  = 's3://pulsetrack-lakehouse-dev-03a28ee7/dlq/'
    STORAGE_INTEGRATION  = PULSETRACK_S3
    FILE_FORMAT          = JSON_FF;

-- ── Verify ──────────────────────────────────────────────────────────────────
-- LIST @BRONZE_STAGE/sensor_readings/data/;
-- LIST @SILVER_STAGE/sensor_readings/data/;
