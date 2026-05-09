-- depends_on: V001
-- V002: Add source_type to remaining fact tables.
-- ----------------------------------------------------------------------------
-- fact_vital_reading already has source_type from V001 (it is part of the
-- streaming Gold transform's projected schema). The daily summary and lab
-- result facts predate the WHOOP integration and need the column added so
-- downstream BI can split simulator vs real-device aggregates.
-- ----------------------------------------------------------------------------

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_daily_summary
    ADD COLUMN source_type STRING;

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_lab_result
    ADD COLUMN source_type STRING;
