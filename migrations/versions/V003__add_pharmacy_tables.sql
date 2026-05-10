-- MIGRATION_DESCRIPTION: Add silver pharmacy_fills and gold fact_pharmacy_fill Iceberg tables
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- depends_on: V001
-- V003: Add Silver pharmacy_fills + Gold fact_pharmacy_fill.
-- ----------------------------------------------------------------------------
-- Silver schema mirrors transformations/bronze_to_silver/pharmacy_silver.py
-- (event_id grain, MERGE key). Gold rolls Silver up to a patient-grain fact
-- joined on dim_medication.medication_key + dim_date.date_key.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.silver }}.pharmacy_fills (
    event_id             STRING,
    event_type           STRING,
    patient_id           STRING,
    drug_name            STRING,
    ndc_code             STRING,
    prescriber_npi       STRING,
    fill_date            DATE,
    quantity             DOUBLE,
    fda_report_id        STRING,
    event_timestamp      TIMESTAMP,
    ingestion_timestamp  TIMESTAMP,
    is_valid             BOOLEAN
) USING iceberg
PARTITIONED BY (days(event_timestamp))
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'write.distribution-mode'='hash',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_pharmacy_fill (
    patient_key      BIGINT,
    medication_key   BIGINT,
    date_key         INT,
    event_type       STRING,
    quantity         DOUBLE,
    fill_count       BIGINT,
    has_fda_report   BOOLEAN,
    source_type      STRING
) USING iceberg
PARTITIONED BY (date_key)
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'write.distribution-mode'='hash',
    'format-version'='2'
);

ALTER TABLE {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_pharmacy_fill
    WRITE ORDERED BY (patient_key, medication_key, date_key);
