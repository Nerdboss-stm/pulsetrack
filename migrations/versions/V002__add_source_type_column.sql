-- MIGRATION_DESCRIPTION: Add source_type column to fact_vital_daily_summary and fact_lab_result
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- depends_on: V001
-- V002: Add source_type to remaining fact tables.
-- ----------------------------------------------------------------------------
-- fact_vital_reading already has source_type from V001 (it is part of the
-- streaming Gold transform's projected schema). The daily summary and lab
-- result facts predate the WHOOP integration and need the column added so
-- downstream BI can split simulator vs real-device aggregates.
-- ----------------------------------------------------------------------------

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_vital_daily_summary
    ADD COLUMN source_type STRING;

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_lab_result
    ADD COLUMN source_type STRING;
