-- MIGRATION_DESCRIPTION: Add reversed-id partitioning to bronze sensor_readings (WHOOP S3 thundering-herd mitigation)
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- depends_on: V001
-- ----------------------------------------------------------------------------
-- Why this migration:
--   Date-first partitioning (the original bronze layout: days(ingestion_timestamp))
--   collapses ALL fleet writes onto a single S3 prefix at the date boundary.
--   S3's per-prefix request-rate limit (~3500 PUT/s) becomes a global
--   bottleneck and the streaming pipeline saturates with 503 SlowDown
--   responses at midnight. Reversing device_id as the leading partition
--   key spreads writes across S3 prefixes — see
--   ``lakehouse/partition_strategy.py`` and ``docs/s3_partitioning_analysis.md``.
--
-- What this migration does:
--   1. Adds the ``rid`` column (reversed device_id). Existing rows get
--      NULL — bronze ingestion populates the value going forward via
--      ``ReversedIdStrategy.add_partition_columns``.
--   2. Adds ``rid`` as a leading partition field. Iceberg supports
--      partition evolution in-place: existing data files retain their
--      old layout, new writes use the new spec. No backfill needed.
--   3. Leaves ``days(ingestion_timestamp)`` in the partition spec —
--      the secondary partition column gives operators the chronological
--      pruning they expect.
--
-- Operator note: after applying, restart the bronze stream to pick up
-- the new partition spec. Old data files don't move; new data files
-- land at ``s3://.../bronze/sensor_readings/rid=XXX/ingestion_timestamp_day=YYYY-MM-DD/``.
-- ----------------------------------------------------------------------------

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.bronze }}.sensor_readings
  ADD COLUMN rid STRING COMMENT 'Reversed device_id; primary partition column for write distribution (mitigates S3 prefix throttling)';

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.bronze }}.sensor_readings
  ADD PARTITION FIELD rid;
