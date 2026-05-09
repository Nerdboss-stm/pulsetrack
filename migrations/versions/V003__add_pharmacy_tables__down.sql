-- depends_on: V001
-- V003 (down): Drop pharmacy tables introduced by V003.

DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_pharmacy_fill;
DROP TABLE IF EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_SILVER}.pharmacy_fills;
