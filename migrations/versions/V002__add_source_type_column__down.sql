-- depends_on: V001
-- V002 (down): Drop source_type from the two facts modified by V002.

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_daily_summary
    DROP COLUMN source_type;

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_lab_result
    DROP COLUMN source_type;
