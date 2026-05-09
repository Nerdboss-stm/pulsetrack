-- V001: Create Iceberg Gold tables for PulseTrack lakehouse
-- ----------------------------------------------------------------------------
-- Creates all 12 Gold tables (3 facts + 9 dims) in the Glue-backed Iceberg
-- catalog. Schemas mirror the Spark DataFrame writes in
-- transformations/silver_to_gold/*.py.
--
-- Partitioning strategy
--   * Facts with a date_key int       -> PARTITIONED BY (date_key)
--   * Facts with an event_timestamp   -> PARTITIONED BY (days(event_timestamp))
--                                        (V004 adds bucket(patient_key))
--   * Dim with date column            -> PARTITIONED BY (years(date))
--   * Pure surrogate-key dims         -> no partitioning
--
-- Iceberg properties: zstd, 128 MiB target file size, hash distribution,
-- format-version 2 (so we get row-level deletes if we need them later).
-- ----------------------------------------------------------------------------

-- ── Facts ───────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_daily_summary (
    patient_key          BIGINT,
    metric_key           BIGINT,
    date_key             INT,
    avg_value            DOUBLE,
    min_value            DOUBLE,
    max_value            DOUBLE,
    reading_count        BIGINT,
    anomaly_count        BIGINT,
    pct_in_normal_range  DOUBLE
) USING iceberg
PARTITIONED BY (date_key)
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'write.distribution-mode'='hash',
    'format-version'='2'
);

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_daily_summary
    WRITE ORDERED BY (patient_key, metric_key, date_key);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading (
    patient_key       BIGINT,
    metric_key        BIGINT,
    date_key          INT,
    event_timestamp   TIMESTAMP,
    value             DOUBLE,
    is_valid          BOOLEAN,
    is_late_arriving  BOOLEAN,
    source_type       STRING
) USING iceberg
PARTITIONED BY (days(event_timestamp))
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'write.distribution-mode'='hash',
    'format-version'='2'
);

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_vital_reading
    WRITE ORDERED BY (patient_key, metric_key, event_timestamp);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_lab_result (
    patient_key           BIGINT,
    date_key              INT,
    lab_test_name         STRING,
    result_value          DOUBLE,
    result_unit           STRING,
    reference_range_low   DOUBLE,
    reference_range_high  DOUBLE,
    is_abnormal           BOOLEAN,
    condition_key         BIGINT
) USING iceberg
PARTITIONED BY (date_key)
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'write.distribution-mode'='hash',
    'format-version'='2'
);

ALTER TABLE ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.fact_lab_result
    WRITE ORDERED BY (patient_key, date_key, lab_test_name);

-- ── Dimensions ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_patient (
    patient_key            BIGINT,
    patient_id_masked      STRING,
    age_group              STRING,
    gender                 STRING,
    primary_condition_key  BIGINT,
    device_count           BIGINT,
    first_reading_date     DATE
) USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_device (
    device_key        BIGINT,
    device_id         STRING,
    device_type       STRING,
    firmware_version  STRING,
    effective_start   DATE,
    effective_end     DATE,
    is_current        BOOLEAN,
    first_event_at    TIMESTAMP,
    last_event_at     TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_metric (
    metric_key   BIGINT,
    metric_name  STRING,
    unit         STRING,
    normal_low   DOUBLE,
    normal_high  DOUBLE,
    device_type  STRING
) USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_date (
    date                    DATE,
    date_key                INT,
    year                    INT,
    month                   INT,
    day                     INT,
    quarter                 INT,
    day_of_week             INT,
    day_name                STRING,
    month_name              STRING,
    week_of_year            INT,
    is_weekend              BOOLEAN,
    is_flu_season           BOOLEAN,
    is_holiday              BOOLEAN,
    fiscal_quarter_medical  INT,
    cdc_epi_week            INT
) USING iceberg
PARTITIONED BY (years(date))
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_time (
    time_key           INT,
    hour               INT,
    minute             INT,
    time_str           STRING,
    period_of_day      STRING,
    is_sleep_window    BOOLEAN,
    is_clinical_hours  BOOLEAN
) USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_condition (
    condition_key           BIGINT,
    condition_code          STRING,
    condition_name          STRING,
    condition_category_key  BIGINT
) USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_condition_category (
    condition_category_key  BIGINT,
    category_code           STRING,
    category_name           STRING,
    icd_chapter             STRING
) USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_medication (
    medication_key   BIGINT,
    medication_name  STRING,
    generic_name     STRING,
    drug_class_key   BIGINT
) USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);

CREATE TABLE IF NOT EXISTS ${ICEBERG_CATALOG}.${GLUE_DATABASE_GOLD}.dim_drug_class (
    drug_class_key  BIGINT,
    class_name      STRING,
    drug_family     STRING
) USING iceberg
TBLPROPERTIES (
    'write.target-file-size-bytes'='134217728',
    'write.parquet.compression-codec'='zstd',
    'format-version'='2'
);
