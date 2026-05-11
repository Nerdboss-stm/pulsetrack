-- ============================================================================
-- 01_create_infrastructure.sql
-- ============================================================================
-- Creates the Snowflake database, warehouse, role, and schemas for PulseTrack.
-- Run as ACCOUNTADMIN once per environment (dev / prod). Idempotent.
--
-- Topology:
--   DATABASE: PULSETRACK
--     ├── SCHEMA: BRONZE   (external Iceberg, read-only from EMR pipeline)
--     ├── SCHEMA: SILVER   (external Iceberg, read-only)
--     ├── SCHEMA: GOLD     (managed views + dbt-built tables)
--     ├── SCHEMA: ANALYTICS (wide tables for BI)
--     └── SCHEMA: OBSERVABILITY (monitor_runs ledger)
--   WAREHOUSE: PULSETRACK_WH  (XSMALL auto-suspend 60s)
--   ROLES:
--     ├── PULSETRACK_ADMIN     full DDL on the database
--     ├── PULSETRACK_RW        read+write GOLD/ANALYTICS (dbt + Prefect)
--     ├── PULSETRACK_READER    read-only on GOLD + ANALYTICS (BI consumers)
--     └── PULSETRACK_LOADER    write to BRONZE/SILVER (Iceberg writes from S3)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ── Database ────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS PULSETRACK
    COMMENT = 'PulseTrack streaming health-data lakehouse — gold layer published from EMR via Iceberg';

-- ── Schemas ─────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS PULSETRACK.BRONZE
    COMMENT = 'Raw ingestion. External Iceberg via Glue catalog. Read-only from Snowflake.';

CREATE SCHEMA IF NOT EXISTS PULSETRACK.SILVER
    COMMENT = 'Cleansed, business-rule-enforced. External Iceberg. Read-only.';

CREATE SCHEMA IF NOT EXISTS PULSETRACK.GOLD
    COMMENT = 'Published star schema. Managed views + dbt-built tables.';

CREATE SCHEMA IF NOT EXISTS PULSETRACK.ANALYTICS
    COMMENT = 'Wide tables + dashboards. Materialized from gold.';

CREATE SCHEMA IF NOT EXISTS PULSETRACK.OBSERVABILITY
    COMMENT = 'Monte-Carlo-style monitor results. monitor_runs ledger.';

-- ── Warehouse ───────────────────────────────────────────────────────────────
-- XSMALL is sufficient for a 10K-patient demo lakehouse. Auto-suspend
-- after 60s of inactivity to control cost (Snowflake bills per credit-second).
-- Auto-resume on demand. Multi-cluster off (warm dev, single-cluster prod).
CREATE WAREHOUSE IF NOT EXISTS PULSETRACK_WH
    WAREHOUSE_SIZE       = 'XSMALL'
    AUTO_SUSPEND         = 60
    AUTO_RESUME          = TRUE
    INITIALLY_SUSPENDED  = TRUE
    SCALING_POLICY       = 'STANDARD'
    COMMENT              = 'PulseTrack workloads — XSMALL auto-suspend 60s';

-- ── Roles ───────────────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS PULSETRACK_ADMIN
    COMMENT = 'Full DDL on PULSETRACK database. Migration runners + DBAs.';

CREATE ROLE IF NOT EXISTS PULSETRACK_RW
    COMMENT = 'Read+write on GOLD/ANALYTICS. dbt + Prefect orchestration.';

CREATE ROLE IF NOT EXISTS PULSETRACK_READER
    COMMENT = 'Read-only on GOLD + ANALYTICS. BI tools, ML training, ad-hoc analysts.';

CREATE ROLE IF NOT EXISTS PULSETRACK_LOADER
    COMMENT = 'Write to BRONZE/SILVER via Iceberg from S3. EMR pipeline service-role.';

-- ── Grants ──────────────────────────────────────────────────────────────────
-- Admin: everything.
GRANT USAGE                ON DATABASE  PULSETRACK            TO ROLE PULSETRACK_ADMIN;
GRANT USAGE                ON ALL SCHEMAS IN DATABASE PULSETRACK TO ROLE PULSETRACK_ADMIN;
GRANT ALL PRIVILEGES       ON SCHEMA    PULSETRACK.GOLD       TO ROLE PULSETRACK_ADMIN;
GRANT ALL PRIVILEGES       ON SCHEMA    PULSETRACK.ANALYTICS  TO ROLE PULSETRACK_ADMIN;
GRANT ALL PRIVILEGES       ON SCHEMA    PULSETRACK.OBSERVABILITY TO ROLE PULSETRACK_ADMIN;
GRANT MODIFY               ON SCHEMA    PULSETRACK.BRONZE     TO ROLE PULSETRACK_ADMIN;
GRANT MODIFY               ON SCHEMA    PULSETRACK.SILVER     TO ROLE PULSETRACK_ADMIN;

-- RW: read+write on GOLD + ANALYTICS + OBSERVABILITY.
GRANT USAGE                ON DATABASE  PULSETRACK            TO ROLE PULSETRACK_RW;
GRANT USAGE                ON SCHEMA    PULSETRACK.GOLD       TO ROLE PULSETRACK_RW;
GRANT USAGE                ON SCHEMA    PULSETRACK.ANALYTICS  TO ROLE PULSETRACK_RW;
GRANT USAGE                ON SCHEMA    PULSETRACK.OBSERVABILITY TO ROLE PULSETRACK_RW;
GRANT USAGE                ON SCHEMA    PULSETRACK.BRONZE     TO ROLE PULSETRACK_RW;
GRANT USAGE                ON SCHEMA    PULSETRACK.SILVER     TO ROLE PULSETRACK_RW;
GRANT SELECT               ON ALL TABLES IN SCHEMA PULSETRACK.BRONZE  TO ROLE PULSETRACK_RW;
GRANT SELECT               ON ALL TABLES IN SCHEMA PULSETRACK.SILVER  TO ROLE PULSETRACK_RW;
GRANT SELECT               ON FUTURE TABLES IN SCHEMA PULSETRACK.BRONZE TO ROLE PULSETRACK_RW;
GRANT SELECT               ON FUTURE TABLES IN SCHEMA PULSETRACK.SILVER TO ROLE PULSETRACK_RW;
GRANT CREATE TABLE,
      CREATE VIEW,
      CREATE FUNCTION      ON SCHEMA    PULSETRACK.GOLD       TO ROLE PULSETRACK_RW;
GRANT CREATE TABLE,
      CREATE VIEW          ON SCHEMA    PULSETRACK.ANALYTICS  TO ROLE PULSETRACK_RW;
GRANT CREATE TABLE         ON SCHEMA    PULSETRACK.OBSERVABILITY TO ROLE PULSETRACK_RW;

-- Reader: SELECT only.
GRANT USAGE                ON DATABASE  PULSETRACK            TO ROLE PULSETRACK_READER;
GRANT USAGE                ON SCHEMA    PULSETRACK.GOLD       TO ROLE PULSETRACK_READER;
GRANT USAGE                ON SCHEMA    PULSETRACK.ANALYTICS  TO ROLE PULSETRACK_READER;
GRANT SELECT               ON ALL TABLES IN SCHEMA PULSETRACK.GOLD       TO ROLE PULSETRACK_READER;
GRANT SELECT               ON ALL TABLES IN SCHEMA PULSETRACK.ANALYTICS  TO ROLE PULSETRACK_READER;
GRANT SELECT               ON ALL VIEWS IN SCHEMA  PULSETRACK.GOLD       TO ROLE PULSETRACK_READER;
GRANT SELECT               ON ALL VIEWS IN SCHEMA  PULSETRACK.ANALYTICS  TO ROLE PULSETRACK_READER;
GRANT SELECT               ON FUTURE TABLES IN SCHEMA PULSETRACK.GOLD     TO ROLE PULSETRACK_READER;
GRANT SELECT               ON FUTURE TABLES IN SCHEMA PULSETRACK.ANALYTICS TO ROLE PULSETRACK_READER;
GRANT SELECT               ON FUTURE VIEWS IN SCHEMA  PULSETRACK.GOLD     TO ROLE PULSETRACK_READER;
GRANT SELECT               ON FUTURE VIEWS IN SCHEMA  PULSETRACK.ANALYTICS TO ROLE PULSETRACK_READER;

-- Loader: write BRONZE/SILVER from Iceberg-S3 path.
GRANT USAGE                ON DATABASE  PULSETRACK            TO ROLE PULSETRACK_LOADER;
GRANT USAGE                ON SCHEMA    PULSETRACK.BRONZE     TO ROLE PULSETRACK_LOADER;
GRANT USAGE                ON SCHEMA    PULSETRACK.SILVER     TO ROLE PULSETRACK_LOADER;
GRANT CREATE EXTERNAL TABLE,
      CREATE ICEBERG TABLE ON SCHEMA    PULSETRACK.BRONZE     TO ROLE PULSETRACK_LOADER;
GRANT CREATE EXTERNAL TABLE,
      CREATE ICEBERG TABLE ON SCHEMA    PULSETRACK.SILVER     TO ROLE PULSETRACK_LOADER;

-- Warehouse usage to every PulseTrack role.
GRANT USAGE,
      OPERATE              ON WAREHOUSE PULSETRACK_WH         TO ROLE PULSETRACK_ADMIN;
GRANT USAGE                ON WAREHOUSE PULSETRACK_WH         TO ROLE PULSETRACK_RW;
GRANT USAGE                ON WAREHOUSE PULSETRACK_WH         TO ROLE PULSETRACK_READER;
GRANT USAGE                ON WAREHOUSE PULSETRACK_WH         TO ROLE PULSETRACK_LOADER;

-- ── Role hierarchy ──────────────────────────────────────────────────────────
-- PULSETRACK_ADMIN inherits all child roles (simplifies operator-level reviews).
GRANT ROLE PULSETRACK_RW       TO ROLE PULSETRACK_ADMIN;
GRANT ROLE PULSETRACK_READER   TO ROLE PULSETRACK_ADMIN;
GRANT ROLE PULSETRACK_LOADER   TO ROLE PULSETRACK_ADMIN;
GRANT ROLE PULSETRACK_READER   TO ROLE PULSETRACK_RW;

-- Assign roles to your user (replace YOUR_USER as appropriate).
-- GRANT ROLE PULSETRACK_ADMIN TO USER YOUR_USER;
