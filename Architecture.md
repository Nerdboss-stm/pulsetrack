# PulseTrack — Architecture Reference

## Overview
End-to-end wearable health analytics platform. Ingests from 6 sources (4 wearable device types, EHR clinical data, pharmacy prescriptions). Kappa Architecture with single streaming engine. Detects health anomalies. Serves clinical dashboards.

**Pattern:** Kappa Architecture (single streaming engine for everything)
**Modeling:** Snowflake Schema (Gold) — normalized hierarchical dimensions
**Cloud:** Azure Free Tier + Docker local development
**Cost:** $0

## Architecture Pattern: Kappa

### Why Kappa?
Wearable sensor data is NATURALLY APPEND-ONLY. A heart rate reading of 72 bpm at 2:34pm doesn't get "corrected" later — it's an immutable measurement. Unlike orders (which transition through states and may be cancelled/refunded), vital signs are simple append events.

**Single streaming pipeline handles:**
- Real-time processing: new sensor readings → Silver → Gold in ~30 seconds
- Historical backfill: replay events from Bronze through the SAME pipeline
- Late-arriving data: batch-synced readings (hours old) process through the SAME pipeline

**One codebase, one logic path.** No dual-pipeline inconsistency. No separate batch reconciliation.

### Why NOT Lambda?
Lambda makes sense when:
- Business events have complex state machines (order lifecycle)
- Financial reconciliation requires exact batch numbers
- Corrections/refunds change historical facts

PulseTrack doesn't have these issues. A sensor reading is a sensor reading — it doesn't change state. So the complexity of maintaining two pipelines (batch + streaming) isn't justified.

### Comparison with GhostKitchen (Lambda)
| Aspect | GhostKitchen (Lambda) | PulseTrack (Kappa) |
|--------|----------------------|-------------------|
| Data nature | Stateful (order lifecycle) | Append-only (sensor readings) |
| Corrections | Orders cancelled/refunded → need batch recomputation | Readings don't change |
| Backfill | Separate batch Spark job | Replay through same streaming pipeline |
| Code paths | 2 (streaming + batch) | 1 (streaming only) |
| Reconciliation | Daily batch overwrites streaming Gold | Not needed |
| Complexity | Higher (dual pipeline) | Lower (single pipeline) |
| When to use | Complex stateful events, financial data | Append-only events, IoT, logs |

**Interview talking point:** "I chose different architectures for each project based on the data characteristics. Lambda for stateful order lifecycles, Kappa for append-only sensor readings. Knowing WHEN to use each pattern is more important than memorizing what they are."

**PDF Reference:** Lambda (SD pages 27-29), Kappa (SD pages 29-32), comparison (SD page 32)

## Data Flow

```
KAPPA FLOW (single streaming engine handles everything):
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│ wearable_generator ──┐                                            │
│ pharmacy_generator ──┤──→ Kafka ──→ Spark Structured ──→ Bronze   │
│ device_registry ─────┘              Streaming             Delta   │
│                                        │                          │
│                                  SAME PIPELINE                    │
│                                        │                          │
│                                        ├──→ Silver (cleaned)      │
│                                        │      │                   │
│                                        │      ├── dedup           │
│                                        │      ├── normalize       │
│                                        │      ├── validate        │
│                                        │      └── SCD2 updates    │
│                                        │                          │
│                                        └──→ Gold (Snowflake)      │
│                                              │                    │
│                                              ├── fact tables      │
│                                              ├── dim tables       │
│                                              └── anomaly alerts   │
│                                                                    │
│ EHR batch files ──→ Airflow ──→ Bronze ──→ SAME PIPELINE picks   │
│ feedback CSVs ──→ Airflow ──→ Bronze     up new Bronze data      │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘

BATCH SOURCES JOIN THE STREAM:
Airflow writes EHR batches and CSVs to Bronze Delta tables.
The streaming pipeline reads from Bronze (using readStream on Delta).
So batch data flows through the SAME streaming transformations.

BACKFILL = REPLAY:
Stop pipeline → Reset Kafka offsets (or re-read Bronze from a past date)
→ Restart pipeline → Same code reprocesses everything → MERGE into Gold
→ No duplicates because MERGE is idempotent

SERVING:
Gold Delta Tables ──→ Apache Superset (dashboards)
Gold Delta Tables ──→ Cosmos DB (real-time patient API)
Anomaly alerts ──→ Azure Functions (email/SMS notifications)
```

## Component Details

### Apache Kafka (Docker — port 9093)
- **Role:** Event streaming backbone (same role as GhostKitchen, different port)
- **Topics:** sensor_readings (6 partitions), fitness_activities (2), pharmacy_events (2), device_registry (1)
- **Partition strategy:** sensor_readings keyed by device_id (6 partitions for higher throughput — this is the highest-volume source). Others lower volume → fewer partitions.
- **Kappa-specific:** Kafka also serves as the replayable log for backfills. Consumer offsets can be reset to replay history through the same pipeline.
- **PDF:** Event-Driven Architecture (SD pages 36-39), Partition planning (SD page 48)

### Apache Spark 3.5 Structured Streaming (Docker)
- **Role:** THE SINGLE PROCESSING ENGINE — Kappa's heart
- **Mode:** Continuous micro-batch (trigger every 30 seconds)
- **Reads from:** Kafka topics (for streaming sources) + Bronze Delta tables (for batch sources that Airflow lands there)
- **Writes to:** Silver Delta tables (with MERGE for dedup/SCD2) + Gold Delta tables (with MERGE for dimensional updates)
- **Why Spark not Flink?** (1) Backfills in Kappa require replaying large historical data — Spark handles multi-TB replays better, (2) Delta Lake MERGE integration is native in Spark, (3) same DataFrame API works for both streaming and batch mode
- **PDF:** Kappa Architecture (SD pages 29-32), Spark vs Flink (SD pages 124-127)

### Delta Lake on Azurite / Local Filesystem
- **Role:** ACID lakehouse storage
- **Why Delta Lake?** Kappa REQUIRES ACID MERGE operations. The streaming pipeline continuously upserts into Silver/Gold. Without ACID guarantees, concurrent reads/writes would corrupt data.
- **Why Azurite?** Local emulation of Azure Blob Storage. Same API — code works unchanged on real Azure.
- **Pragmatic note:** For local development, we use local filesystem (/tmp/pulsetrack-lakehouse/) instead of Azurite to avoid Azure SDK complexity. Same Delta Lake, different storage backend.
- **Layers:**
  - Bronze: /tmp/pulsetrack-lakehouse/bronze/{source}/ — raw, append-only
  - Silver: /tmp/pulsetrack-lakehouse/silver/{table}/ — cleaned, normalized, SCD2
  - Gold: /tmp/pulsetrack-lakehouse/gold/{table}/ — Snowflake Schema dimensional model
- **PDF:** Medallion (SD pages 32-35), Upsert Patterns (SD pages 43-47)

### Apache Airflow (Docker) — Maintenance Role
- **Role in Kappa:** Airflow does NOT orchestrate the main ETL (that's the continuous streaming pipeline). Airflow handles MAINTENANCE:
  - dag_ehr_batch_ingest: Daily — detect new EHR files, write to Bronze
  - dag_identity_resolution: Daily at 2AM — full identity resolution refresh
  - dag_data_quality_sweep: Every 6 hours — comprehensive quality checks
  - dag_compaction_maintenance: Daily — OPTIMIZE + Z-ORDER on hot Delta tables
  - dag_hipaa_audit: Weekly — full HIPAA compliance scan
  - dag_backfill_replay: Manual — reset offsets, replay through streaming pipeline
- **Key difference from GhostKitchen:** In Lambda, Airflow is the CONDUCTOR of the batch path. In Kappa, Airflow is the JANITOR — handling maintenance, not the core pipeline.
- **PDF:** Orchestration Patterns (SD pages 57-63), Hybrid orchestration (SD page 62)

### Anomaly Detection (Spark Streaming — stateful)
- **Role:** Real-time personalized health anomaly detection
- **How:** Per-patient rolling statistics (mean, std dev for each metric over 30 days). New readings compared against personal baseline. Deviation > 2σ = anomaly alert.
- **State:** Keyed by (patient_key, metric_name). Each key stores: running stats, circular buffer of recent values. Backed by RocksDB. TTL = 90 days for inactive patients.
- **Why personal baselines?** A runner with resting HR 52 at HR 85 is more concerning than a sedentary person at HR 85. Population-level thresholds miss personalized anomalies.
- **PDF:** Stateful Streaming (SD pages 49-51), State Stores (SD pages 49-50)

### Apache Superset (Docker)
- **Role:** Clinical analytics dashboards
- **Connected to:** Gold Delta tables (via DuckDB or direct read)
- **Dashboards:** Patient vital trends, anomaly alerts, medication timelines, population analytics
- **Why Superset (not Metabase)?** Different BI tool than GhostKitchen — shows you're not tied to one tool. Superset also has SQL Lab for ad-hoc queries.

### Azure Free Tier (optional cloud deployment)
- **Blob Storage:** Bronze/Silver/Gold (5GB free)
- **Cosmos DB:** Real-time patient lookup API (25GB free)
- **Azure Functions:** Alert notifications (1M executions free)
- **Event Hubs:** Cloud Kafka alternative (1M events free)
- **Azure Monitor:** Alerting
- **Terraform:** Infrastructure as code

## Late-Arriving Data Strategy (Kappa-specific)
**Kappa handles late data NATIVELY:**
- 30% of wearable readings arrive late (batch sync from devices)
- The streaming pipeline doesn't care — a 2-second-old event and a 2-hour-old event go through the SAME code
- MERGE into Silver/Gold handles idempotency — replayed data overwrites existing rows, no duplicates
- No DLQ needed (unlike GhostKitchen's Lambda approach)
- 48-hour watermark: events within 48h process normally. Events beyond 48h are flagged with late_flag=true but still processed.

**Comparison:**
| | GhostKitchen (Lambda) | PulseTrack (Kappa) |
|---|---|---|
| Late events | DLQ → nightly batch reconciliation | Same pipeline, no special handling |
| Correctness | Batch is the "truth" layer | Streaming IS the truth |
| DLQ needed? | Yes (events > 24h) | No |
| Complexity | Higher (separate reconciliation) | Lower (automatic) |

## Monitoring Strategy (Kappa-specific)
- **Streaming lag:** CRITICAL. In Kappa, the streaming pipeline IS the entire ETL. Lag > 5 min = stale Gold tables.
- **Checkpoint duration:** Must be < micro-batch interval (30s). If checkpoints take 25s+, pipeline falls behind.
- **State store size:** Per-patient anomaly state grows with users. Monitor RocksDB memory.
- **SCD2 chain integrity:** Custom check: no gaps/overlaps in effective_date ranges.
- **Identity resolution coverage:** What % of readings have resolved_patient_key? Target >95%.
- **Anomaly alert rate per firmware:** Spike in anomalies for one firmware version = device bug, not health crisis.

## HIPAA Compliance
- **De-identification:** Gold uses age_bracket (not exact age), city (not address), no names
- **Encryption:** Delta Lake files encrypted at rest
- **Access control:** data_engineer (all layers), clinical_analyst (Gold only, PII masked), researcher (Gold, fully de-identified)
- **Deletion:** Delta Lake DELETE across all layers → verification sweep → audit log
- **Retention conflict:** HIPAA requires 7-year retention. Deletion = de-identify (remove PII) but keep analytical record.
- **Audit:** Weekly dag_hipaa_audit scans for PII leakage

## Cost Architecture
- Local Docker: $0
- Azure Free Tier: Blob (5GB), Cosmos DB (25GB), Functions (1M req), Event Hubs (1M events)
- Production estimate (if scaled): ~$800/month for 100K users
  - Azure Blob: ~$100/month (50TB hot + warm)
  - Databricks/HDInsight: ~$400/month (streaming + batch)
  - Cosmos DB: ~$200/month (real-time lookups)
  - Azure Functions + Monitor: ~$100/month