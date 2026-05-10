-- MIGRATION_DESCRIPTION: Roll back V003 — drop pharmacy silver + gold tables
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- depends_on: V001
-- V003 (down): Drop pharmacy tables introduced by V003.

DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_pharmacy_fill;
DROP TABLE IF EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.silver }}.pharmacy_fills;
