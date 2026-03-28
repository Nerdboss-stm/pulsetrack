-- ============================================================
-- PulseTrack — Clinical Analytics Views for Apache Superset
-- ============================================================
-- These views are registered in Superset's SQL Lab and used
-- as virtual datasets for clinical dashboards.
--
-- Engine: DuckDB (reads Gold Delta tables via delta extension)
-- Alternatively: Spark SQL (same syntax, different runtime)
--
-- Usage in Superset:
--   Database: pulsetrack_gold (DuckDB or SparkSQL)
--   Add each view as a virtual dataset
-- ============================================================


-- ── 1. Patient Vital Trends ────────────────────────────────────────────
-- Daily aggregated vitals per patient per metric.
-- Powers: Patient Vital Trends dashboard panel.
-- Grain: (patient_key, metric_name, date_key) — 1 row per patient per metric per day
CREATE OR REPLACE VIEW vw_patient_vital_trends AS
SELECT
    fvd.patient_key,
    dp.age_group,
    dp.gender,
    dm.metric_name,
    dm.unit,
    dm.normal_low,
    dm.normal_high,
    fvd.date_key,
    -- Parse date_key (YYYYMMDD integer) into readable date
    CAST(
        CAST(fvd.date_key / 10000 AS VARCHAR) || '-' ||
        LPAD(CAST((fvd.date_key / 100) % 100 AS VARCHAR), 2, '0') || '-' ||
        LPAD(CAST(fvd.date_key % 100 AS VARCHAR), 2, '0')
    AS DATE) AS reading_date,
    fvd.avg_value,
    fvd.min_value,
    fvd.max_value,
    fvd.reading_count,
    fvd.anomaly_count,
    fvd.pct_in_normal_range,
    CASE
        WHEN fvd.avg_value < dm.normal_low  THEN 'below_normal'
        WHEN fvd.avg_value > dm.normal_high THEN 'above_normal'
        ELSE 'normal'
    END AS avg_status
FROM fact_vital_daily_summary fvd
JOIN dim_metric  dm ON fvd.metric_key  = dm.metric_key
JOIN dim_patient dp ON fvd.patient_key = dp.patient_key;


-- ── 2. Active Anomaly Alerts ───────────────────────────────────────────
-- Unresolved anomaly alerts with full context for clinical review.
-- Powers: Anomaly Alert panel, patient detail drill-through.
-- Grain: (alert_key) — 1 row per alert
CREATE OR REPLACE VIEW vw_active_anomaly_alerts AS
SELECT
    fa.alert_key,
    fa.patient_key,
    dp.age_group,
    dp.gender,
    dm.metric_name,
    dm.unit,
    fa.date_key,
    CAST(
        CAST(fa.date_key / 10000 AS VARCHAR) || '-' ||
        LPAD(CAST((fa.date_key / 100) % 100 AS VARCHAR), 2, '0') || '-' ||
        CAST(fa.date_key % 100 AS VARCHAR)
    AS DATE) AS alert_date,
    fa.time_key,
    fa.alert_type,
    fa.severity,
    fa.metric_value,
    fa.threshold_value,
    fa.patient_baseline,
    ROUND((fa.metric_value - fa.patient_baseline) / NULLIF(fa.patient_baseline, 0) * 100, 1) AS pct_deviation,
    fa.resolved_flag,
    fa.resolved_timestamp
FROM fact_anomaly_alert fa
JOIN dim_metric  dm ON fa.metric_key  = dm.metric_key
JOIN dim_patient dp ON fa.patient_key = dp.patient_key
WHERE fa.resolved_flag = FALSE;


-- ── 3. Medication Timeline ─────────────────────────────────────────────
-- Prescription fills per patient grouped by medication and drug class.
-- Powers: Medication Timeline panel, adherence analysis.
-- Grain: (fill_key) — 1 row per prescription fill
CREATE OR REPLACE VIEW vw_medication_timeline AS
SELECT
    fp.fill_key,
    fp.patient_key,
    dp.age_group,
    dp.gender,
    dm.medication_name,
    dm.generic_name,
    dc.drug_class_name,
    dc.drug_class_category,
    fp.date_key,
    CAST(
        CAST(fp.date_key / 10000 AS VARCHAR) || '-' ||
        LPAD(CAST((fp.date_key / 100) % 100 AS VARCHAR), 2, '0') || '-' ||
        CAST(fp.date_key % 100 AS VARCHAR)
    AS DATE) AS fill_date,
    fp.days_supply,
    fp.refill_number,
    fp.status,
    -- Flag potential gap (refill > days_supply after last fill)
    CASE WHEN fp.refill_number = 0 THEN 'new_prescription' ELSE 'refill' END AS fill_type
FROM fact_prescription_fill fp
JOIN dim_medication dm ON fp.medication_key = dm.medication_key
JOIN dim_drug_class dc ON dm.drug_class_key = dc.drug_class_key
JOIN dim_patient    dp ON fp.patient_key    = dp.patient_key;


-- ── 4. Lab Results by Condition ────────────────────────────────────────
-- Lab results linked to their associated condition category.
-- Powers: Clinical correlation panel, lab trend analysis.
-- Grain: (lab_result_key) — 1 row per lab result
CREATE OR REPLACE VIEW vw_lab_results_by_condition AS
SELECT
    fl.patient_key,
    dp.age_group,
    dp.gender,
    fl.date_key,
    CAST(
        CAST(fl.date_key / 10000 AS VARCHAR) || '-' ||
        LPAD(CAST((fl.date_key / 100) % 100 AS VARCHAR), 2, '0') || '-' ||
        CAST(fl.date_key % 100 AS VARCHAR)
    AS DATE) AS result_date,
    fl.lab_test_name,
    fl.result_value,
    fl.result_unit,
    fl.reference_range_low,
    fl.reference_range_high,
    fl.is_abnormal,
    dc.condition_name,
    dcc.category_name        AS condition_category,
    CASE
        WHEN fl.result_value < fl.reference_range_low  THEN 'low'
        WHEN fl.result_value > fl.reference_range_high THEN 'high'
        ELSE 'normal'
    END AS result_status
FROM fact_lab_result fl
LEFT JOIN dim_condition dc  ON fl.condition_key        = dc.condition_key
LEFT JOIN dim_condition_category dcc ON dc.condition_category_key = dcc.condition_category_key
JOIN dim_patient dp ON fl.patient_key = dp.patient_key;


-- ── 5. Population Analytics (fully de-identified) ─────────────────────
-- Cohort-level aggregates for research. No patient_key exposed.
-- Grain: (age_group, gender, metric_name, date_key) — population aggregate
CREATE OR REPLACE VIEW vw_population_analytics AS
SELECT
    dp.age_group,
    dp.gender,
    dm.metric_name,
    dm.unit,
    dm.normal_low,
    dm.normal_high,
    fvd.date_key,
    CAST(
        CAST(fvd.date_key / 10000 AS VARCHAR) || '-' ||
        LPAD(CAST((fvd.date_key / 100) % 100 AS VARCHAR), 2, '0') || '-' ||
        CAST(fvd.date_key % 100 AS VARCHAR)
    AS DATE) AS reading_date,
    COUNT(DISTINCT fvd.patient_key)             AS patient_count,
    ROUND(AVG(fvd.avg_value), 2)                AS cohort_avg,
    ROUND(MIN(fvd.min_value), 2)                AS cohort_min,
    ROUND(MAX(fvd.max_value), 2)                AS cohort_max,
    SUM(fvd.reading_count)                      AS total_readings,
    SUM(fvd.anomaly_count)                      AS total_anomalies,
    ROUND(AVG(fvd.pct_in_normal_range), 1)      AS avg_pct_in_normal_range
FROM fact_vital_daily_summary fvd
JOIN dim_metric  dm ON fvd.metric_key  = dm.metric_key
JOIN dim_patient dp ON fvd.patient_key = dp.patient_key
GROUP BY
    dp.age_group,
    dp.gender,
    dm.metric_name,
    dm.unit,
    dm.normal_low,
    dm.normal_high,
    fvd.date_key;
