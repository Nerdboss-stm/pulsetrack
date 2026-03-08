# PulseTrack — Complete Data Model Reference

## Project Overview
Wearable health analytics platform. 100K users with 4 device types (smartwatch, chest strap, sleep ring, glucose monitor). Also ingests EHR clinical data and pharmacy prescriptions. Detects health anomalies in near-real-time.

## Architecture Choice

### Silver Layer: CLEANED & NORMALIZED (No Data Vault)
**Why NOT Data Vault here?** PulseTrack's sources are DIFFERENT ENTITY TYPES:
- Wearables → vital sign readings (events)
- EHR → conditions, medications, lab results (clinical entities)
- Pharmacy → prescription fills (transaction events)

These don't CONFLICT with each other — they COMPLEMENT. Data Vault solves conflicting definitions of the SAME entity (3 platforms defining "customer" differently in GhostKitchen). PulseTrack doesn't have that problem.

Using Data Vault here would be OVER-ENGINEERING — too many joins for no benefit. A simpler cleaned/normalized Silver layer is appropriate.

**Knowing when NOT to use a pattern is as important as knowing when to use it.** (DM page 54: anti-pattern "using full Vault when not needed")

### Gold Layer: SNOWFLAKE SCHEMA (not Star)
**Why Snowflake instead of Star?** Healthcare dimensions are DEEPLY HIERARCHICAL and SCD2-HEAVY:
- Patient → has many Conditions (each with ICD-10 hierarchy)
- Patient → has many Medications (each with drug class hierarchy)  
- Condition → maps to ICD-10 chapter → category → specific code
- Medication → maps to drug → generic → class → therapeutic area

Cramming all of this into one wide dim_patient would create a MONSTER dimension with 50+ columns that change at different frequencies. This is the "Kitchen Sink Dimension" anti-pattern (DM page 54, 77).

**Snowflake normalizes:** dim_patient stays lean (demographics only). Conditions, medications, prescriptions, devices are separate SCD2 dimensions with FK references to hierarchy tables.

**Tradeoff:** More joins for analysts. But in columnar engines (Delta Lake, Snowflake, BigQuery), these joins are cheap. The alternative (Kitchen Sink) causes SCD2 explosion — every medication change would create a new version of the ENTIRE wide patient row.

**Comparison with GhostKitchen:** GhostKitchen uses Star Schema in Gold because its dimensions are FLAT (kitchen, brand, zone — no deep hierarchies). Different data → different model.

**PDF Reference:** Snowflake Schema (DM pages 12-13), Kitchen Sink anti-pattern (DM pages 54, 77), Normalizing large dims (DM pages 55-56)

---

## SILVER LAYER — CLEANED & NORMALIZED

### silver_sensor_readings
| Column | Type | Description |
|--------|------|-------------|
| reading_id | STRING | UUID from source device |
| device_id | STRING | Device identifier (SW-A7X-00442) |
| resolved_patient_key | BIGINT | FK → patient (nullable until identity resolved) |
| device_type | STRING | smartwatch/chest_strap/sleep_ring/glucose_monitor |
| metric_name | STRING | heart_rate_bpm/spo2_pct/blood_glucose_mgdl/etc. |
| metric_value | FLOAT | The reading value (one row per metric, not nested JSON) |
| metric_unit | STRING | bpm/percent/mgdl/celsius/ms/steps |
| firmware_version | STRING | Device firmware at time of reading |
| event_timestamp | TIMESTAMP | When reading was taken ON BODY |
| sync_timestamp | TIMESTAMP | When reading reached our servers |
| ingestion_timestamp | TIMESTAMP | When our pipeline processed it |
| quality_score | FLOAT | 0.0 = invalid, 1.0 = normal. Flags anomalous readings. |
| is_duplicate | BOOLEAN | Detected as duplicate? |

**Key transformation from Bronze:** The nested `metrics` JSON ({"heart_rate_bpm": 72, "spo2_pct": 97}) is EXPLODED into separate rows — one row per metric per reading. This normalization enables: `SELECT avg(metric_value) WHERE metric_name = 'heart_rate_bpm'` without JSON parsing.

**Why 3 timestamps?** event_timestamp = medical truth (when HR was measured). sync_timestamp = device behavior (when phone synced). ingestion_timestamp = pipeline behavior (when we processed). All three serve different purposes.

### silver_ehr_conditions
| Column | Type | Description |
|--------|------|-------------|
| condition_id | STRING | Generated: {patient_mrn}_{icd10_code} |
| patient_mrn | STRING | Hospital Medical Record Number |
| icd10_code | STRING | Diagnosis code (E11.9 = Type 2 diabetes) |
| description | STRING | Human-readable diagnosis |
| category | STRING | ICD-10 chapter (Endocrine, Circulatory, etc.) |
| onset_date | DATE | When condition was first diagnosed |
| status | STRING | active/remission/resolved |
| clinician_npi | STRING | Prescribing doctor's NPI number |
| effective_start | TIMESTAMP | SCD2: when this version became active |
| effective_end | TIMESTAMP | SCD2: when superseded (null = current) |
| is_current | BOOLEAN | Latest version? |
| batch_date | DATE | Which EHR batch this came from |

**SCD2 trigger:** When a condition's STATUS changes (active → remission), old row gets closed, new row created. This tracks the clinical timeline accurately.

### silver_ehr_medications
| Column | Type | Description |
|--------|------|-------------|
| medication_id | STRING | Generated: {patient_mrn}_{drug_name}_{start_date} |
| patient_mrn | STRING | Hospital MRN |
| drug_name | STRING | Medication name (metformin) |
| generic_name | STRING | Generic name (Metformin HCl) |
| ndc_code | STRING | National Drug Code (for pharmacy matching) |
| dosage | STRING | 500mg, 1000mg, etc. |
| frequency | STRING | once_daily, twice_daily, as_needed |
| start_date | DATE | When medication was started |
| end_date | DATE | When stopped (null = still active) |
| status | STRING | active/stopped |
| drug_class | STRING | Biguanides, ACE Inhibitors, SSRIs, etc. |
| prescriber_npi | STRING | Doctor who prescribed |
| effective_start | TIMESTAMP | SCD2 version start |
| effective_end | TIMESTAMP | SCD2 version end |
| is_current | BOOLEAN | Latest version |

**THE HARDEST SCD2 TABLE:** A medication can: be started (new row), have dosage changed (close old row, new row with new dosage), be stopped (close row with end_date), be RESTARTED (new row). Each creates a new SCD2 version. Point-in-time accuracy is CRITICAL for clinical safety: "What medications was patient X on when their glucose spiked?"

### silver_prescriptions
| Column | Type | Description |
|--------|------|-------------|
| rx_id | STRING | Pharmacy prescription ID |
| pharmacy_patient_id | STRING | Pharmacy's patient ID (different from hospital MRN!) |
| ndc_code | STRING | Drug code (links to medications via this code) |
| drug_name | STRING | Medication name |
| fill_date | DATE | When prescription was filled |
| days_supply | INT | How many days of medication |
| refill_number | INT | Which refill (0 = original fill) |
| status | STRING | filled/transferred/cancelled/returned |
| processed_timestamp | TIMESTAMP | When our pipeline processed this CDC event |

### silver_patient_identity_graph
| Column | Type | Description |
|--------|------|-------------|
| resolved_patient_key | BIGINT | The unified patient surrogate key |
| identifier_type | STRING | device_account/strava_id/apple_health_id/hospital_mrn/pharmacy_id/email/phone |
| identifier_value | STRING | The actual ID value |
| source_system | STRING | Which system provided this identifier |
| confidence_score | FLOAT | 1.0 = deterministic match, 0.6-0.9 = probabilistic |
| match_method | STRING | exact_email/transitive_closure/fuzzy_name_zip/manual |
| first_seen | TIMESTAMP | When this identifier was first linked |
| last_seen | TIMESTAMP | Most recent activity |
| is_current | BOOLEAN | Active link? |

**Multi-hop resolution algorithm:**
1. DETERMINISTIC: device_account → email → hub (device registration email matches app signup email)
2. DETERMINISTIC: hospital_mrn → email → hub (hospital registration email)
3. DETERMINISTIC: pharmacy_patient_id → prescriber_npi + drug + date → hospital_mrn (same drug, same prescriber, within 7 days = same patient)
4. PROBABILISTIC: fuzzy(name + birth_year + zip) for remaining unlinked records
5. TRANSITIVE: if device_A → patient_X and device_B belongs to same account as device_A → device_B → patient_X

---

## GOLD LAYER — SNOWFLAKE SCHEMA

### FACT TABLES

#### fact_vital_reading (Atomic)
| Column | Type | Description |
|--------|------|-------------|
| reading_key | BIGINT | Surrogate key |
| patient_key | BIGINT | FK → dim_patient |
| device_key | BIGINT | FK → dim_device |
| metric_key | BIGINT | FK → dim_metric |
| date_key | INT | FK → dim_date |
| time_key | INT | FK → dim_time |
| metric_value | FLOAT | The reading |
| quality_score | FLOAT | 0.0-1.0 |
| is_anomaly | BOOLEAN | Flagged by anomaly detector |
| anomaly_type | STRING | sustained_high/sustained_low/sudden_spike/null if not anomaly |

**Grain:** 1 row per patient × metric × reading timestamp (finest grain).
**Why atomic?** Clinicians need exact timestamps. "What was the patient's HR at 2:34pm?" requires atomic grain.

#### fact_vital_daily_summary (Pre-aggregated)
| Column | Type | Description |
|--------|------|-------------|
| patient_key | BIGINT | FK → dim_patient |
| metric_key | BIGINT | FK → dim_metric |
| date_key | INT | FK → dim_date |
| avg_value | FLOAT | Daily average |
| min_value | FLOAT | Daily minimum |
| max_value | FLOAT | Daily maximum |
| p25_value | FLOAT | 25th percentile |
| p75_value | FLOAT | 75th percentile |
| reading_count | INT | Readings in the day |
| trend_direction | STRING | up/down/stable (vs 7-day moving average) |
| trend_pct_change_7d | FLOAT | % change vs 7-day average |

**Grain:** 1 row per patient × metric × day.
**Why both atomic and daily?** Dashboards ("30-day HR trend") query daily summary (fast). Drill-down and ML query atomic (detailed). Dual-grain strategy. PDF: DM page 37.

#### fact_activity_session
| Column | Type | Description |
|--------|------|-------------|
| activity_key | BIGINT | Surrogate key |
| patient_key | BIGINT | FK → dim_patient |
| activity_type_key | BIGINT | FK → dim_activity_type (or just STRING) |
| date_key | INT | FK → dim_date |
| duration_min | FLOAT | Workout duration |
| distance_km | FLOAT | Distance covered (nullable for yoga/strength) |
| calories | INT | Calories burned |
| avg_hr | INT | Average heart rate during activity |
| max_hr | INT | Max heart rate |
| source_app | STRING | strava/apple_health/google_fit |
| dedup_status | STRING | original/deduplicated (if same workout from multiple sources) |

**Grain:** 1 row per deduplicated activity session.
**Dedup logic:** If smartwatch AND Strava both record the same run (overlapping time, same user), keep the higher-quality source and flag the other.

#### fact_lab_result
| Column | Type | Description |
|--------|------|-------------|
| lab_key | BIGINT | Surrogate key |
| patient_key | BIGINT | FK → dim_patient |
| lab_test_key | BIGINT | FK → dim_lab_test (or metric_key) |
| date_key | INT | FK → dim_date |
| value | FLOAT | Test result value |
| unit | STRING | %, mg/dL, mmHg, etc. |
| reference_low | FLOAT | Normal range lower bound |
| reference_high | FLOAT | Normal range upper bound |
| is_abnormal | BOOLEAN | Outside normal range? |

**Grain:** 1 row per lab test result. SEPARATE from vitals — different grain (labs are periodic/quarterly, vitals are continuous).
**Why separate?** Mixing lab results into fact_vital_reading would create a mixed-grain table — the anti-pattern from DM page 37.

#### fact_prescription_fill
| Column | Type | Description |
|--------|------|-------------|
| fill_key | BIGINT | Surrogate key |
| patient_key | BIGINT | FK → dim_patient |
| medication_key | BIGINT | FK → dim_medication (SCD2 — version active at fill time) |
| date_key | INT | FK → dim_date |
| days_supply | INT | Days of medication |
| refill_number | INT | Which refill |
| status | STRING | filled/transferred/cancelled/returned |

**Grain:** 1 row per prescription fill event.

#### fact_anomaly_alert
| Column | Type | Description |
|--------|------|-------------|
| alert_key | BIGINT | Surrogate key |
| patient_key | BIGINT | FK → dim_patient |
| metric_key | BIGINT | FK → dim_metric |
| date_key | INT | FK → dim_date |
| time_key | INT | FK → dim_time |
| alert_type | STRING | sustained_high/sustained_low/sudden_spike/trend_deviation |
| severity | STRING | low/medium/high/critical |
| metric_value | FLOAT | The value that triggered the alert |
| threshold_value | FLOAT | The threshold that was exceeded |
| patient_baseline | FLOAT | Patient's personal baseline for this metric |
| resolved_flag | BOOLEAN | Was the alert resolved? |
| resolved_timestamp | TIMESTAMP | When resolved (null if still active) |

**Grain:** 1 row per detected anomaly. ML-generated fact table.

### DIMENSION TABLES

#### dim_patient (SCD2 — LEAN)
| Column | Type | Description |
|--------|------|-------------|
| patient_key | BIGINT | Surrogate key (NEVER changes) |
| patient_id | STRING | Resolved best-known ID |
| age_bracket | STRING | 18-25/26-35/36-45/46-55/56-65/65+ (HIPAA: no exact age) |
| gender | STRING | M/F/Other/Unknown |
| city | STRING | City (not full address — HIPAA) |
| state | STRING | State |
| registration_date | DATE | First seen across any source |
| device_count | INT | How many devices linked |
| is_active | BOOLEAN | Active in last 30 days? |
| effective_start | TIMESTAMP | SCD2 version start |
| effective_end | TIMESTAMP | SCD2 version end |
| is_current | BOOLEAN | Latest version |

**INTENTIONALLY LEAN.** No conditions, medications, or device details. Those are separate normalized dimensions. This avoids the Kitchen Sink anti-pattern.
**HIPAA de-identification:** age_bracket instead of birth_date. City instead of address. No SSN, no exact DOB in Gold.

#### dim_condition (SCD2 — SNOWFLAKED)
| Column | Type | Description |
|--------|------|-------------|
| condition_key | BIGINT | Surrogate key |
| patient_key | BIGINT | FK → dim_patient |
| icd10_code | STRING | E11.9, I10, J45.20, etc. |
| condition_name | STRING | Type 2 diabetes, Hypertension, etc. |
| status | STRING | active/remission/resolved |
| onset_date | DATE | When diagnosed |
| resolution_date | DATE | When resolved (null if active/remission) |
| condition_category_key | BIGINT | FK → dim_condition_category (SNOWFLAKE!) |
| effective_start | TIMESTAMP | SCD2 version start |
| effective_end | TIMESTAMP | SCD2 version end |
| is_current | BOOLEAN | Latest version |

**SCD2 trigger:** Status change (active → remission). This tracks clinical timeline.
**SNOWFLAKE FK:** condition_category_key links to dim_condition_category — this is the normalization that makes it a Snowflake schema, not Star.

#### dim_condition_category (SCD0 — Reference)
| Column | Type | Description |
|--------|------|-------------|
| condition_category_key | BIGINT | Surrogate key |
| icd10_chapter | STRING | E00-E89 = Endocrine, I00-I99 = Circulatory, etc. |
| chapter_description | STRING | "Endocrine, nutritional and metabolic diseases" |
| category_code | STRING | E11 = Type 2 Diabetes |
| category_description | STRING | "Type 2 diabetes mellitus" |

**SCD Type 0:** ICD-10 codes are a medical standard. They never change (within a version).
**Why normalized?** Without this table, every row in dim_condition would repeat "Endocrine, nutritional and metabolic diseases" for every diabetes patient. Normalization saves space and makes hierarchy queries natural.

#### dim_medication (SCD2)
| Column | Type | Description |
|--------|------|-------------|
| medication_key | BIGINT | Surrogate key |
| patient_key | BIGINT | FK → dim_patient |
| drug_name | STRING | metformin, lisinopril, etc. |
| ndc_code | STRING | National Drug Code |
| dosage | STRING | 500mg, 1000mg, etc. |
| frequency | STRING | once_daily, twice_daily, as_needed |
| start_date | DATE | When started |
| end_date | DATE | When stopped (null = active) |
| status | STRING | active/stopped |
| drug_class_key | BIGINT | FK → dim_drug_class (SNOWFLAKE!) |
| effective_start | TIMESTAMP | SCD2 version start |
| effective_end | TIMESTAMP | SCD2 version end |
| is_current | BOOLEAN | Latest version |

**THE HARDEST SCD2 DIMENSION:** Changes include: dosage change (500mg → 1000mg), medication stopped, medication restarted, new medication added. Each = new SCD2 version.
**Point-in-time join:** `fact_vital_reading.event_timestamp BETWEEN dim_medication.effective_start AND dim_medication.effective_end` — answers "What meds was patient on when glucose spiked?"

#### dim_drug_class (SCD0 — Reference)
| Column | Type | Description |
|--------|------|-------------|
| drug_class_key | BIGINT | Surrogate key |
| drug_class | STRING | Biguanides, ACE Inhibitors, SSRIs, etc. |
| therapeutic_area | STRING | Antidiabetic, Cardiovascular, Mental Health, etc. |
| requires_monitoring | BOOLEAN | Drug requires regular lab monitoring? |

**Why normalized?** Drug class hierarchy (metformin → Biguanides → Antidiabetic) would repeat on every medication row without normalization.

#### dim_device (SCD2)
| Column | Type | Description |
|--------|------|-------------|
| device_key | BIGINT | Surrogate key |
| device_id | STRING | Physical device ID (SW-A7X-00442) |
| device_type | STRING | smartwatch/chest_strap/sleep_ring/glucose_monitor |
| model | STRING | Device model name |
| firmware_version | STRING | Current firmware |
| patient_key | BIGINT | FK → dim_patient (current owner) |
| registration_date | DATE | When device was registered |
| body_zone | STRING | wrist/chest/finger/abdomen |
| effective_start | TIMESTAMP | SCD2 version start |
| effective_end | TIMESTAMP | SCD2 version end |
| is_current | BOOLEAN | Latest version |

**TWO SCD2 triggers:** (1) firmware_version update, (2) device reassigned to different patient_key. Both create new versions because analytics must know: "Which firmware was this device running when it recorded this reading?" and "Who was wearing this device when this reading was taken?"

#### dim_metric (SCD0 — Junk Dimension)
| Column | Type | Description |
|--------|------|-------------|
| metric_key | BIGINT | Surrogate key |
| metric_name | STRING | heart_rate_bpm, spo2_pct, blood_glucose_mgdl, etc. |
| unit | STRING | bpm, percent, mg/dL, celsius, ms |
| normal_range_low | FLOAT | Population normal lower bound |
| normal_range_high | FLOAT | Population normal upper bound |
| critical_low | FLOAT | Clinically dangerous low |
| critical_high | FLOAT | Clinically dangerous high |
| device_types_available | STRING | Comma-separated: "smartwatch,chest_strap" |

**SCD Type 0:** Metric definitions rarely change. Updated only when medical guidelines change.

#### dim_date
| Column | Type | Description |
|--------|------|-------------|
| date_key | INT | YYYYMMDD |
| full_date | DATE | Actual date |
| year, month, day | INT | Components |
| day_of_week | STRING | Monday-Sunday |
| is_weekend | BOOLEAN | |
| is_flu_season | BOOLEAN | Oct-Mar (health-relevant) |
| is_allergy_season | BOOLEAN | Mar-Jun (health-relevant) |
| daylight_hours | FLOAT | Affects mood/activity metrics |

#### dim_time
| Column | Type | Description |
|--------|------|-------------|
| time_key | INT | HHMM |
| hour, minute | INT | Components |
| period | STRING | sleep/morning/midday/afternoon/evening/night |

### BRIDGE TABLES

#### patient_identity_bridge
| Column | Type | Description |
|--------|------|-------------|
| patient_key | BIGINT | FK → dim_patient |
| identifier_type | STRING | device_account/strava_id/apple_health_id/hospital_mrn/pharmacy_id/email/phone |
| identifier_value | STRING | The actual ID value |
| source_system | STRING | Which system provided this |
| confidence | FLOAT | 1.0 = exact match, lower = fuzzy |
| match_method | STRING | exact_email/transitive_closure/fuzzy_name_zip/prescriber_drug_date/manual |
| first_seen | DATE | When first linked |
| last_seen | DATE | Most recent activity |

**This is the HARDEST identity resolution in either project.** 7 identifier types with multi-hop matching. A patient might have: 2 device accounts, 1 Strava ID, 1 hospital MRN, 1 pharmacy ID, 1 email, 1 phone — all needing to resolve to one patient_key.

---

## GRANULARITY DECISIONS SUMMARY

| Fact Table | Grain | Why |
|------------|-------|-----|
| fact_vital_reading | 1 row per patient × metric × timestamp | Clinicians need exact timestamps |
| fact_vital_daily_summary | 1 row per patient × metric × day | Dashboards need fast 30-day trends |
| fact_activity_session | 1 row per deduplicated workout | Activities are sessions, not point-in-time |
| fact_lab_result | 1 row per lab test result | Labs are periodic (quarterly), not continuous |
| fact_prescription_fill | 1 row per fill event | Each fill is a business event |
| fact_anomaly_alert | 1 row per detected anomaly | ML-generated alerts |

**Mixed-grain avoided:** Vitals, activities, labs are SEPARATE fact tables because they have fundamentally different grains. Mixing them would be the mixed-grain anti-pattern (DM page 37).

---

## DATA SOURCES → TABLE MAPPING

| Source | Bronze | Silver | Gold |
|--------|--------|--------|------|
| Wearable Sensors (4 types) | bronze/sensor_readings/ | silver_sensor_readings (exploded, deduped, validated) | fact_vital_reading, fact_vital_daily_summary |
| Fitness Activities | bronze/fitness_activities/ | silver_activities (deduped across sources) | fact_activity_session |
| EHR FHIR Bundles | bronze/ehr/ | silver_ehr_conditions, silver_ehr_medications, silver_ehr_labs | dim_condition, dim_medication, fact_lab_result |
| Pharmacy CDC | bronze/pharmacy/ | silver_prescriptions | fact_prescription_fill |
| Device Registry | bronze/device_registry/ | silver_device_changes | dim_device |
| Reference CSVs | Direct to Gold | — | dim_condition_category, dim_drug_class, dim_metric, dim_date, dim_time |
| Identity Resolution | — | silver_patient_identity_graph | dim_patient, patient_identity_bridge |