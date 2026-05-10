-- MIGRATION_DESCRIPTION: Roll back V004 — restore fact_vital_reading days(event_timestamp) partitioning
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- depends_on: V001
-- V004 (down): Revert fact_vital_reading partition spec to days() only.

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_vital_reading
    DROP PARTITION FIELD bucket(16, patient_key);

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_vital_reading
    DROP PARTITION FIELD days(event_timestamp);

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_vital_reading
    ADD PARTITION FIELD days(event_timestamp);
