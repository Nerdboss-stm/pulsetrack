# PulseTrack — Architecture One-Pager

A health-data lakehouse on AWS. Three independent sources, Kappa
streaming, Iceberg + Glue, star-schema gold, dbt + Snowflake consumers.

## Why this exists

To prove a single-engineer team can build a WHOOP-tier production data
platform: streaming-first, observable, cost-aware, and survivable in
incidents.

## Architecture

```
                    +--------------------+
                    |   WHOOP API        |  OAuth 2.0 + poll
                    |  (real wearables)  |
                    +---------+----------+
                              |
+-------------------+   +-----v-----+   +------------------+
| Synthetic vitals  |   |  Open FDA |   |   HAPI FHIR R4   |
| (physiology model)|   |   (drugs) |   |  (clinical EHR)  |
+--------+----------+   +-----+-----+   +--------+---------+
         |                    |                  |
         v                    v                  v
    +---------------------------------------------------+
    |  MSK Serverless  (SASL/IAM, sensor_readings, ...) |
    +-----------------------+---------------------------+
                            |
                            v Spark Structured Streaming on EMR 7.13
    +---------------------------------------------------+
    |  Bronze Iceberg  (Glue Catalog, append-only)      |  rid-partitioned
    |  pulsetrack_bronze_dev.sensor_readings            |
    +-----------------------+---------------------------+
                            | foreachBatch + MERGE
                            v
    +---------------------------------------------------+
    |  Silver Iceberg  (cleansed, watermark dedup)      |  + EHR, identity bridge
    |  pulsetrack_silver_dev.{sensor,ehr,identity_*}    |
    +-----------------------+---------------------------+
                            | streaming-skip-overwrite-snapshots
                            v
    +---------------------------------------------------+
    |  Gold Iceberg  (3 facts + 9 dims, star schema)    |  Glacierbase-managed
    |  pulsetrack_gold_dev.{fact_*,dim_*}               |
    +-----------------------+---------------------------+
                            |
                +-----------+-----------+
                v                       v
       +---------------+      +-----------------+
       | dbt project   |      | Snowflake views |
       | (Commons)     |----->| (analytics)     |
       +---------------+      +-----------------+
                                       |
                                       v
                          +--------------------------+
                          | Tableau / Mode / Hex     |
                          | + Monte Carlo monitors   |
                          | + AI-assisted engineering|
                          +--------------------------+

Operational plane: Prefect Cloud (7 deployments), Prometheus + Grafana,
                   CloudWatch + SNS, GX gates, DLQ, quarantine, postmortems
```

## What each layer does

- **Producers:** publish Avro to MSK Serverless via SASL/IAM. Three real
  sources (WHOOP, Open FDA, HAPI FHIR) + one synthetic for scale tests.
- **MSK:** partition by entity-key for ordering; pay-per-request.
- **Bronze:** Spark Structured Streaming, PERMISSIVE Avro decode, parse
  failures DLQ'd, append-only Iceberg snapshots.
- **Silver:** explode metrics map, range-check per metric, watermark
  + `dropDuplicatesWithinWatermark`, `MERGE INTO` via `foreachBatch`.
- **Identity:** 4-phase resolution (EHR → email → device → FDA), outputs
  `patient_key` (SHA-256 of canonical identifier). 93.5% link rate.
- **Gold:** streaming MERGE into 3 facts (fact_vital_reading,
  fact_vital_daily_summary, fact_lab_result) + 9 dims. Snowflake schema.
- **dbt + Snowflake:** Commons macros, snapshot SCD2, views for BI.
- **Prefect:** 7 flows orchestrating producers, streams, dbt, maintenance.
- **Observability:** Prometheus metrics, CloudWatch alarms, GX gates,
  Monte Carlo monitors, AI-assisted incident summaries.

## Key metrics (at scale-test target)

| Metric | Target | Source |
|---|---|---|
| Throughput (peak) | 25,000 rec/s producer | `docs/scale_test_capacity_plan.md` § 2 |
| End-to-end latency (Kafka → silver visible) p95 | < 60s | `docs/scale_test_results.md` § 3 |
| Identity resolution rate | ≥ 95% within 24h | `data_quality/identity_metrics.py` |
| Cost per million events | ~$1.00 (EMR + MSK + S3) | `docs/scale_test_capacity_plan.md` § 6 |
| SLO compliance | 99% of batches complete within trigger interval | `observability/monitors.py` |

## Three hardest engineering decisions

1. **Iceberg over Delta** (commits `0aaf0e1`, `1e03a98`). Vendor
   independence. Glue Catalog native. Partition evolution in-place.
   Snowflake reads it native. Tradeoff: smaller community, more sharp
   edges (e.g., the `streaming-skip-overwrite-snapshots` issue in
   `pulsetrack-study/PROMPT_4_REPORT.md` § 4.4). ADR: pending —
   `docs/adrs/0001-iceberg-over-delta.md`.

2. **Streaming-first hybrid, not pure Kappa** (architectural choice
   throughout). Wearable + pharmacy paths are streaming. EHR is daily
   batch. Identity bridge is batch. Same Spark engine, same transform
   code — `run_streaming()` / `run_batch()` differ only in trigger.
   The hybrid was the right call: forcing EHR through streaming would
   have meant Debezium-ifying HAPI FHIR for no benefit.

3. **Reversed-ID partitioning** (commit `901bf01`). S3 prefix-fanout to
   handle the midnight thundering-herd. Three strategies benchmarked on
   real S3: date-first (throttled), hash-bucket (uniform but opaque),
   reversed-ID (uniform AND grep-able). Picked reversed-ID for incident
   response ergonomics. See `lakehouse/partition_strategy.py` and
   `docs/s3_partitioning_analysis.md`.

## What's NOT on this page (intentionally)

This is one screen. Deep dives:
- `Architecture.md` — long-form architecture
- `DataModel.md` — schema + dimensional model
- `docs/PRODUCTION_RUNBOOK.md` — operational guide
- `pulsetrack-study/PROMPT_{1..9}_REPORT.md` — per-prompt deep dives
- `docs/war_stories.md` — STAR-formatted postmortems

---

*Use this as the prop for a screen interview. If you can talk through it
in 3 minutes, you've internalized the project. If you need 10 minutes,
practice.*

*Last updated: 2026-05-10.*
