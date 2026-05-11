# PulseTrack Architecture Classification

> **TL;DR**: PulseTrack is a **streaming-first hybrid lakehouse with strategic batch**.
> It is *not* pure Kappa, despite the marketing instinct to claim it is. Two of our
> six sources (EHR/FHIR, OpenFDA) are intrinsically request-response REST surfaces,
> and dbt marts are scheduled batch by design. Honest classification matters more
> than a tidy label.

---

## 1. A short history of streaming architectures

| Year | Author | Idea | Why it mattered |
|------|--------|------|-----------------|
| 2011 | Nathan Marz | **Lambda** | Two pipelines (batch + speed). Batch is the source of truth; speed gives fast-but-approximate. |
| 2014 | Jay Kreps | **Kappa** ("Questioning the Lambda Architecture") | One pipeline (the log). Reprocess by replaying. Removes the duplicate codebase. |
| 2016+ | Industry | **Hybrid** | Real shops land here. Pure Kappa breaks when sources aren't streamable (REST polls, file drops, vendor batch exports). Pure Lambda breaks the engineering team. |

Kreps' core argument was correct: **if your input is already a log, you don't need
a separate batch pipeline.** His counter-argument was also correct in retrospect:
**you only get a pure log if every upstream system speaks events.** Most don't.

The practical resolution most data platforms reach is the same one PulseTrack
reached independently: **use streaming where the data is naturally a stream;
use batch where it isn't; minimize duplicate logic between the two.**

---

## 2. PulseTrack classification, layer by layer

| Source | Velocity | Native shape | Ingestion mode | Classification |
|--------|----------|--------------|----------------|----------------|
| Wearable simulator (sensor) | continuous | event stream | Kafka → Spark structured streaming → bronze/silver/gold Iceberg | **Pure Kappa** |
| Pharmacy event simulator | continuous | event stream | Kafka → Spark structured streaming → bronze/silver/gold Iceberg | **Pure Kappa** |
| WHOOP REST API | semi-streaming | poll-and-snapshot | 15-min Prefect poll → Kafka producer → same Kappa path as wearables | **Kappa-ish** (poll-to-stream bridge) |
| EHR (FHIR REST poll) | daily | REST resource bundles | Prefect daily flow → S3 raw → Spark batch silver → Iceberg | **Micro-batch** |
| OpenFDA (REST poll) | daily | REST report endpoint | Prefect daily flow → S3 raw → Spark batch silver → Iceberg | **Micro-batch** |
| dbt Commons marts (Snowflake) | weekly | SQL transformation | Prefect Friday flow → `dbt build` → Snowflake | **Scheduled batch** |
| Patient identity bridge | daily | join across silver | Prefect daily flow → Spark batch → Iceberg | **Batch (Phase 2: streaming candidate)** |

Concretely:

- **Streaming queries** (4): `bronze-sensor-readings`, `silver-sensor-readings`,
  `bronze-pharmacy-events`, `silver-pharmacy-events` — all long-running Spark
  structured streaming jobs on EMR.
- **Batch flows** (5): EHR daily, pharmacy OpenFDA poll, identity bridge,
  dbt weekly, maintenance nightly.

The streaming path emits to the same Iceberg tables that batch silver/gold writes
read from. The lakehouse is the integration point. **Iceberg's snapshot isolation
and time travel are what make hybrid possible without Lambda's twin pipelines.**

---

## 3. Three reasons PulseTrack is *not* pure Kappa

### 3.1 EHR and OpenFDA aren't event streams natively

The FHIR specification is a REST resource model. A FHIR server is a paginated
search endpoint, not a Kafka topic. Forcing it into a "stream" means either:

1. Building a CDC bridge in front of every FHIR server we want to ingest from
   (operational nightmare across hospitals we don't control).
2. Polling on a schedule and *pretending* the response set is a "batch of
   events" — which is just batch ingestion with extra ceremony.

The same applies to OpenFDA: a public REST endpoint with a `since=` parameter
and a rate limit. Polling daily is the natural fit. Wrapping it in Kafka would
buy us nothing except a Kafka topic that produces one large burst per day.

### 3.2 dbt marts don't benefit from streaming

The Commons marts (`patient_cohort`, `drug_safety_summary`, weekly aggregates)
are explicitly *summarization* models. Their input is already at-rest in
Snowflake; their consumers (BI dashboards, weekly reports) operate on weekly
cadence. Continuous incremental materialization would:

- Burn Snowflake compute credits 24/7 for no business benefit.
- Add operational surface area (incremental state, late-arrival handling) that
  scheduled batch sidesteps for free.

dbt's incremental-materialization story exists for cases where it pays off.
Ours isn't one of them.

### 3.3 Always-on streaming for everything is wasteful

Even when a source *could* be streamed (e.g. identity bridge as a Flink job
joining Kafka topics), the cost calculus matters:

- An always-on Spark streaming query is a YARN executor pinned 24/7.
- A nightly Spark batch job for the same work runs ~10 minutes/day.
- Difference at dev scale: ~$15/day vs. ~$0.50/day per pipeline.

Streaming is justified when **latency** is a business requirement
(real-time anomaly detection on wearable vitals: yes) or when **the source
itself is a stream** (Kafka topics: yes). It is not justified by aesthetic
preference for a unified paradigm.

---

## 4. Classification label

> **PulseTrack is a streaming-first hybrid lakehouse with strategic batch.**

The components:

- **Streaming-first**: when in doubt, prefer streams. Real-time vitals and
  pharmacy events are the differentiating signal.
- **Hybrid**: streaming and batch coexist as first-class citizens, integrated
  through Iceberg tables.
- **Lakehouse**: open table format (Iceberg) on object storage (S3), governed
  by an open catalog (Glue), readable by multiple compute engines (Spark,
  Snowflake AUTO_REFRESH, future Trino/Athena).
- **Strategic batch**: batch is a deliberate choice for sources and use cases
  where it's the right fit, not a fallback for "we couldn't stream it."

This is precisely the architecture **WHOOP's published data platform** describes:
Kafka + Spark streaming for wearable telemetry, scheduled Prefect flows for
external integrations and marts, dbt for transformation, Iceberg-on-S3 as the
shared substrate. Cf. their engineering blog series on the Marquez → Iceberg
migration and the Prefect-replaces-cron migration.

---

## 5. Decision matrix: when do I choose streaming vs. batch for a new source?

```
                       ┌─────────────────────────────┐
                       │   New source to ingest      │
                       └──────────────┬──────────────┘
                                      │
              ┌───────────────────────┴───────────────────────┐
              │                                               │
       Is it a native                                  Is it REST /
       event stream?                                   file drop /
       (Kafka, MSK,                                    scheduled
       Kinesis, CDC log)                               export?
              │                                               │
            yes                                              yes
              │                                               │
              ▼                                               ▼
     ┌─────────────────┐                            ┌─────────────────┐
     │ Streaming path  │                            │ Is sub-hour     │
     │ (Spark struct.  │                            │ latency a       │
     │ streaming on    │                            │ business        │
     │ EMR + Iceberg)  │                            │ requirement?    │
     └─────────────────┘                            └────────┬────────┘
                                                             │
                                              ┌──────────────┴──────────────┐
                                              │                             │
                                            yes                            no
                                              │                             │
                                              ▼                             ▼
                                    ┌─────────────────┐         ┌─────────────────┐
                                    │ Poll-to-stream  │         │ Scheduled batch │
                                    │ bridge (Prefect │         │ (Prefect flow → │
                                    │ poll → Kafka)   │         │ Spark batch or  │
                                    │   "Kappa-ish"   │         │ dbt run)        │
                                    └─────────────────┘         └─────────────────┘
```

Concretely, three questions in order:

1. **Is the source already a log?** If yes → streaming. (Wearables, pharmacy.)
2. **Do consumers need fresh data within an hour?** If yes → poll-to-stream
   bridge, treat downstream as Kappa. (WHOOP API.)
3. **Otherwise** → scheduled batch. (EHR, OpenFDA, identity bridge, dbt marts.)

This matrix is the operational interpretation of "streaming-first hybrid."
The default is streaming; batch is an opt-out justified by source shape or
latency tolerance.

---

## 6. Three non-trivial case studies from this codebase

### 6.1 Silver sensor: streaming, not batch

Silver sensor (`transformations/bronze_to_silver/sensor_silver.py`) reads from
the bronze Iceberg table as a streaming source and writes silver as a streaming
sink. It could plausibly be a 15-minute micro-batch instead.

**Why we chose streaming**:
- The downstream gold vital readings table feeds a real-time anomaly detector
  (an explicit project goal: "elevated HR for >5 min triggers alert").
- Spark Iceberg streaming consumers handle the bronze APPEND snapshots
  natively (with `streaming-skip-overwrite-snapshots` to ignore maintenance
  compactions).
- The marginal cost of always-on silver streaming is ~$3/day, dwarfed by the
  cost of building a separate alerting pipeline.

**Why it was non-trivial**: the maintenance compaction jobs (Iceberg
`OPTIMIZE` / `expire_snapshots`) generate non-APPEND snapshots that would
ordinarily break a streaming consumer. We needed Iceberg 1.4+ and the
`streaming-skip-overwrite-snapshots` table property to make this work — see
commit `0cadc84`.

### 6.2 Weekly dbt vs. continuous: scheduled batch

The dbt Commons project (`dbt_project/`) builds Snowflake marts on a
**weekly Friday cadence** via the `dbt-weekly` Prefect deployment.

**Why not continuous**:
- The marts are summarization, not transactional state. Patient cohort
  membership doesn't change between Wednesday and Thursday in any way a
  consumer cares about.
- Snowflake compute is metered. A weekly full build is ~5 minutes of XS
  warehouse; a continuous incremental rebuild is hours per day.
- WHOOP's engineering blog explicitly documents the same Friday cadence
  (`feat: dbt project — WHOOP Commons, 100% docs, snapshot SCD2, weekly CI`,
  commit `23619a0`) — we adopted their pattern.

**Why it was non-trivial**: vital reading freshness is sub-second (streaming);
patient cohort freshness is sub-week (dbt). The same lakehouse serves both.
The seam between them is Iceberg's external-table integration with Snowflake
(`AUTO_REFRESH`).

### 6.3 FHIR file ingest vs. CDC: micro-batch

EHR data is FHIR resource bundles fetched from a hospital's FHIR server via
REST. We poll daily, write raw bundles to S3, and run Spark batch to project
silver.

**Why not CDC**:
- We don't control the source. We can't install Debezium on a hospital's EMR
  database. The only contract we have is the FHIR REST API.
- Hospitals publish updates on their own cadence — typically end-of-day
  aggregates. Polling more often than daily returns the same data.

**Why we considered streaming anyway**: a poll-to-Kafka bridge (like WHOOP)
would unify the codebase. We rejected it because:
- The poll volume is small (~10MB/day per hospital). The bridge buys nothing.
- The downstream silver projection is non-trivial Spark SQL with FHIR-specific
  unnesting. Running it as a streaming `foreachBatch` would still require
  the full Spark batch at trigger time — no streaming benefit.

This is the case study most often *wrongly* characterized as "we should
stream everything." We deliberately don't.

---

## 7. References

- Nathan Marz, *Big Data: Principles and Best Practices of Scalable Real-Time
  Data Systems* (2015) — Lambda original.
- Jay Kreps, "Questioning the Lambda Architecture" (O'Reilly Radar, 2014) —
  Kappa original.
- WHOOP engineering blog series — Prefect migration, Iceberg migration,
  Glacierbase migration framework. Our architecture closely tracks theirs.
- `/Users/nerdboss-stm/pulsetrack-cm/orchestration/README.md` — Prefect
  deployments + cadence table (canonical source for which sources run in
  which mode).
- `/Users/nerdboss-stm/pulsetrack-cm/docs/PRODUCTION_RUNBOOK.md` — operational
  view of the streaming layer.
- `/Users/nerdboss-stm/pulsetrack-cm/docs/adrs/ADR-005-streaming-first-hybrid.md`
  — the decision record formalizing this classification.
