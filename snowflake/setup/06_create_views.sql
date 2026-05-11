-- ============================================================================
-- 06_create_views.sql
-- ============================================================================
-- Analytics views materialized on top of the Iceberg gold layer. These
-- are the queryable surface for BI tools (Tableau / Looker / Mode), ML
-- feature stores, and ad-hoc analysts.
--
-- The actual view bodies live in snowflake/models/*.sql for git-tracked
-- source. This file is a single deployment step that runs each
-- ``CREATE OR REPLACE VIEW`` in order. dbt would replace this in
-- production (dbt's Snowflake target builds these views as models).
-- ============================================================================

USE ROLE PULSETRACK_RW;
USE DATABASE PULSETRACK;
USE SCHEMA ANALYTICS;

-- Each view is created via !source from the models/ directory.
-- Run individually:
--   !source snowflake/models/vw_patient_health_360.sql
--   !source snowflake/models/vw_vital_trends.sql
--   !source snowflake/models/vw_anomaly_dashboard.sql
--   !source snowflake/models/vw_device_fleet_health.sql
--   !source snowflake/models/vw_identity_resolution.sql
--   !source snowflake/models/vw_whoop_my_health.sql

-- Or use the setup_snowflake.sh wrapper which runs them in dependency order.

-- ── Verify after load ───────────────────────────────────────────────────────
-- SHOW VIEWS IN SCHEMA PULSETRACK.ANALYTICS;
-- SELECT COUNT(*) FROM PULSETRACK.ANALYTICS.VW_PATIENT_HEALTH_360;
