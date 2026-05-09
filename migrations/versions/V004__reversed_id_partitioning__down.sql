-- depends_on: V001
-- V004 (down): Revert fact_vital_reading partition spec to days() only.

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading
    DROP PARTITION FIELD bucket(16, patient_key);

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading
    DROP PARTITION FIELD days(event_timestamp);

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading
    ADD PARTITION FIELD days(event_timestamp);
