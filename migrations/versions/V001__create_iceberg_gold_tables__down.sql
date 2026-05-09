-- V001 (down): Drop all 12 Gold Iceberg tables.
-- ----------------------------------------------------------------------------
-- DROP TABLE IF EXISTS so the rollback is idempotent even if a table is gone.
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_daily_summary;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_lab_result;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_patient;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_device;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_metric;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_date;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_time;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_condition;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_condition_category;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_medication;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_drug_class;
