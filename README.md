# PulseTrack 🫀
> Wearable Health Analytics & Anomaly Detection Platform

![Architecture](https://img.shields.io/badge/Architecture-Kappa-blue)
![Cloud](https://img.shields.io/badge/Cloud-Azure%20Free%20Tier-0078D4)
![Cost](https://img.shields.io/badge/Cost-%240-brightgreen)
![Stack](https://img.shields.io/badge/Stack-Kafka%20%7C%20Spark%20%7C%20Delta%20Lake-orange)

End-to-end data engineering platform that ingests wearable device telemetry, EHR clinical records, and pharmacy prescriptions to detect health anomalies using personalized baselines. 100K users. 4 device types. 3 data sources. 7 identity identifier types. One unified patient view.

---

## Architecture

**Kappa Architecture** — single streaming engine for all data paths.

Wearable sensor readings are append-only. A heart rate reading never gets corrected after the fact. No need for Lambda's dual batch+streaming complexity. Backfills replay through the same pipeline.

```
Wearable Devices ──┐
                   ├──→ Kafka ──→ Spark SS ──→ Bronze (raw JSON)
Pharmacy Events ───┘                               │
                                                   ↓
EHR FHIR Files ────────────────────────────→ Silver (exploded metrics,
                                               normalized, deduped)
                                                   │
                                                   ↓
                                             Gold (Snowflake Schema)
                                                   │
                                                   ↓
                                             Serving (Dashboards / API)
```

---

## Tech Stack

| Component | Tool | Cost |
|---|---|---|
| Event Streaming | Apache Kafka (Docker) | $0 |
| Batch Processing | Apache Spark (Docker) | $0 |
| Storage | Azurite → Azure Blob | $0 |
| Orchestration | Apache Airflow (Docker) | $0 |
| Data Quality | Great Expectations | $0 |
| Cloud Deployment | Azure Free Tier | $0 |
| **Total** | | **$0** |

---

## Data Sources

### Wearable Devices (`sensor_readings` — 6 Kafka partitions)

| Device | Metrics | Behavior |
|---|---|---|
| Smartwatch | heart_rate_bpm, spo2_pct, steps_since_last, skin_temp_celsius, hrv_ms | Continuous |
| Chest Strap | heart_rate_bpm, hrv_ms, respiration_rate | Athletic use |
| Sleep Ring | heart_rate_bpm, spo2_pct, skin_temp_celsius, sleep_stage | 10pm–7am only |
| Glucose Monitor | blood_glucose_mgdl | Every 5 minutes |

**Data quality issues injected deliberately:**
- 30% of events arrive hours late (batch sync simulation)
- Duplicate readings from network retries
- 1% null metric values (sensor malfunction)
- Out-of-order timestamps

### EHR Clinical Data (Batch)
FHIR JSON bundles. 7 days pre-generated in `data/ehr_batches/`. Contains conditions, medications, lab results.

### Pharmacy Prescriptions (`pharmacy_events` — 2 Kafka partitions)
CDC-format prescription events. Tracks fills, refills, dosage changes.

---

## Data Model: Snowflake Schema

**Why Snowflake and not Star?**

Healthcare dimensions are deeply hierarchical — ICD-10 codes have 3 levels (chapter → category → specific code), medications track drug class → medication → dosage → patient assignment. Star Schema would create a "Kitchen Sink" anti-pattern with 50+ columns in dim_patient.

### Fact Tables

| Table | Grain |
|---|---|
| `fact_vital_reading` | 1 row per patient × metric × timestamp (atomic) |
| `fact_vital_daily_summary` | 1 row per patient × metric × day (pre-aggregated) |
| `fact_activity_session` | 1 row per exercise session |
| `fact_lab_result` | 1 row per lab test |
| `fact_prescription_fill` | 1 row per pharmacy dispense |
| `fact_anomaly_alert` | 1 row per detected anomaly |

### Dimension Tables

| Table | SCD Type | Notes |
|---|---|---|
| `dim_patient` | SCD2 | Lean — no conditions/meds crammed in |
| `dim_condition` | SCD2 | FK → dim_condition_category (ICD-10) |
| `dim_condition_category` | SCD0 | ICD-10 hierarchy |
| `dim_medication` | SCD2 | Hardest SCD2: start/stop/dosage change/restart |
| `dim_drug_class` | SCD0 | Drug classification |
| `dim_device` | SCD2 | Firmware versions + patient reassignment |
| `dim_metric` | SCD0 | Junk dimension for metric metadata |
| `dim_date` | SCD0 | Standard date dimension |
| `dim_time` | SCD0 | Standard time dimension |

### Bridge Table
`patient_identity_bridge` — maps 7 identifier types to one unified `patient_key` via multi-hop graph matching.

---

## Medallion Architecture

### Bronze
Raw, immutable. Append-only. Stores everything exactly as received from Kafka.

```
/tmp/pulsetrack-lakehouse/bronze/
├── sensor_readings/     ← Kafka: wearable device events
├── ehr/                 ← Batch: FHIR JSON bundles
└── pharmacy_events/     ← Kafka: CDC prescription events
```

Every Bronze table has the same schema envelope:
```
raw_value            STRING    entire JSON blob, untouched
kafka_topic          STRING
kafka_partition      INT
kafka_offset         LONG
kafka_timestamp      TIMESTAMP
ingestion_timestamp  TIMESTAMP
ingestion_date       STRING    partition column
ingestion_hour       STRING    partition column
```

### Silver
Parsed, normalized, deduplicated. The core transformation here is **metric explosion**.

Bronze stores one row per device reading with all metrics nested:
```json
{"device_id": "SW-001", "metrics": {"heart_rate_bpm": 72, "spo2_pct": 98, "hrv_ms": 45}}
```

Silver explodes this to one row per metric:
```
SW-001 | heart_rate_bpm | 72  | true
SW-001 | spo2_pct       | 98  | true
SW-001 | hrv_ms         | 45  | true
```

Silver sensor schema:
```
reading_id           STRING    UUID dedup key
device_id            STRING
device_type          STRING    smartwatch/chest_strap/sleep_ring/glucose_monitor
device_account_id    STRING    raw account ID (for identity resolution)
firmware_version     STRING
battery_pct          INT
event_timestamp      TIMESTAMP UTC
sync_timestamp       TIMESTAMP UTC — when device synced
ingestion_timestamp  TIMESTAMP
metric_name          STRING    heart_rate_bpm / spo2_pct / etc
metric_value         DOUBLE
is_valid             BOOLEAN   within expected range for this metric
is_late_arriving     BOOLEAN   sync_timestamp > event_timestamp + 2 hours
```
```
✅ silver/sensor_readings     9,061 rows  (metric explosion)
✅ silver/ehr_conditions         908 rows  (SCD1)
✅ silver/ehr_medications         680 rows  (SCD2)
✅ silver/ehr_lab_results         431 rows  (append-only)
✅ silver/identity/patient_identity_bridge  1,159 rows
```

### Gold
Snowflake schema. Optimized for analytical queries and dashboards. (Week 3)

---

## Identity Resolution

The hardest problem in PulseTrack. Patients must be matched across 7 identifier types:

```
device_account_id → email → hospital_mrn → pharmacy_id → insurance_id → phone_hash → ssn_hash
```

Multi-hop graph matching: `device_account` links to `email`, `email` links to `hospital_mrn`, `hospital_mrn` links to `pharmacy_id` — all resolving to one unified `patient_key`.

Result stored in `silver/identity/patient_identity_bridge`.

---

## Valid Metric Ranges

| Metric | Min | Max |
|---|---|---|
| heart_rate_bpm | 30 | 220 |
| spo2_pct | 70 | 100 |
| steps_since_last | 0 | 10,000 |
| skin_temp_celsius | 30 | 42 |
| hrv_ms | 5 | 200 |
| respiration_rate | 8 | 40 |
| sleep_stage | 0 | 4 |
| blood_glucose_mgdl | 40 | 400 |

Readings outside these ranges are flagged `is_valid = false` in Silver. They are not dropped — invalid readings are kept for anomaly detection.

---

## Project Structure

```
pulsetrack/
├── data_generators/
│   ├── wearable_generator.py          # 4 device types, 30% late, personal baselines
│   ├── ehr_generator.py               # FHIR bundles: conditions, medications, labs
│   └── pharmacy_generator.py          # CDC prescription events
├── data/
│   └── ehr_batches/                   # 7 days of pre-generated FHIR files
├── streaming/
│   ├── bronze_ingestion.py            # Kafka → Bronze Delta (local filesystem)
│   └── spark_config.py                # SparkSession with Delta Lake + Azurite
├── transformations/
│   ├── bronze_to_silver/
│   │   ├── sensor_silver.py           # Metric explosion + dedup + quality flags
│   │   ├── ehr_silver.py              # FHIR parsing + SCD2 medications
│   │   └── pharmacy_silver.py         # Prescription CDC processing
│   └── identity_resolution/
│       └── patient_identity_bridge.py # Multi-hop identity matching
├── airflow_dags/                      # Orchestration DAGs (Week 3)
├── data_quality/                      # Great Expectations suites (Week 3)
├── serving/                           # API + dashboard layer (Week 3)
├── tests/
│   └── test_wearable_generator.py
├── Architecture.md
├── DataModel.md
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Quick Start

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Terminal 1 — generate wearable data
python data_generators/wearable_generator.py

# 3. Terminal 2 — ingest to Bronze
python streaming/bronze_ingestion.py

# 4. Generate EHR batch files
python data_generators/ehr_generator.py

# 5. Run Silver transforms (after Bronze has data)
python transformations/bronze_to_silver/sensor_silver.py
python transformations/bronze_to_silver/ehr_silver.py
python transformations/identity_resolution/patient_identity_bridge.py
```

---

## Docker Services

```bash
# Kafka broker
localhost:9093

# Spark Master UI
localhost:8081

# Azurite (Azure Blob emulator)
localhost:10000
```

---

## Requirements

```
pyspark
delta-spark
kafka-python
faker
pytest
azure-storage-blob
```

Install:
```bash
pip install -r requirements.txt
```

---

## Build Status

| Layer | Component | Status |
|---|---|---|
| Bronze | sensor_readings | ✅ Working |
| Bronze | EHR FHIR batch files | ✅ Working |
| Silver | sensor_silver.py (metric explosion) | ✅ Working |
| Silver | ehr_silver.py | 🔄 In Progress |
| Silver | patient_identity_bridge.py | 🔄 In Progress |
| Gold | All fact + dimension tables | ⬜ Week 3 |
| Orchestration | Airflow DAGs | ⬜ Week 3 |
| Quality | Great Expectations suites | ⬜ Week 3 |

---

## Related Project

[GhostKitchen](../ghostkitchen) — Real-Time Dark Kitchen Intelligence Platform (AWS Free Tier, Lambda Architecture, Data Vault + Star Schema)
