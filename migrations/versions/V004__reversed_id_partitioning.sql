-- depends_on: V001
-- V004: Reversed-ID partitioning on fact_vital_reading.
-- ----------------------------------------------------------------------------
-- Glacierbase trick: putting bucket(patient_key) FIRST in the partition spec
-- makes patient-scoped reads (the dominant query shape — "all readings for
-- one patient over a window") prune partitions before the day-level scan.
-- The original V001 spec was days(event_timestamp) only; this evolves it to
-- (bucket(16, patient_key), days(event_timestamp)).
--
-- Iceberg supports partition-spec evolution in place: existing data files
-- keep their old spec, new writes use the new spec, and the planner unions
-- both at read time.
-- ----------------------------------------------------------------------------

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading
    DROP PARTITION FIELD days(event_timestamp);

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading
    ADD PARTITION FIELD bucket(16, patient_key);

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading
    ADD PARTITION FIELD days(event_timestamp);
