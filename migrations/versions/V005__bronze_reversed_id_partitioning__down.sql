-- MIGRATION_DESCRIPTION: Roll back V005 — drop rid partition field + column
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- ----------------------------------------------------------------------------
-- Drop in reverse order: partition field first, then the column. Iceberg
-- preserves existing data files written under the old spec — they just
-- become unpartitioned-by-rid going forward.
-- ----------------------------------------------------------------------------

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.bronze }}.sensor_readings
  DROP PARTITION FIELD rid;

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.bronze }}.sensor_readings
  DROP COLUMN rid;
