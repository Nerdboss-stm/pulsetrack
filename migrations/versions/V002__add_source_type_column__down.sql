-- MIGRATION_DESCRIPTION: Roll back V002 — drop source_type column from the two facts
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- depends_on: V001
-- V002 (down): Drop source_type from the two facts modified by V002.

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_vital_daily_summary
    DROP COLUMN source_type;

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_lab_result
    DROP COLUMN source_type;
