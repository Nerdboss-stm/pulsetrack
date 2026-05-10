-- MIGRATION_DESCRIPTION: Roll back V001 — drop all 12 Gold Iceberg tables
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- ----------------------------------------------------------------------------
-- DROP TABLE IF EXISTS so the rollback is idempotent even if a table is gone.
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_vital_daily_summary;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_vital_reading;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_lab_result;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_patient;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_device;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_metric;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_date;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_time;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_condition;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_condition_category;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_medication;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.dim_drug_class;
