# ADR-005: Streaming-First Hybrid Architecture (Not Pure Kappa)

## Status
Accepted

## Date
2026-05-10

## Context

PulseTrack ingests from six structurally different sources:

| Source | Native shape |
|--------|--------------|
| Wearable simulator | Event stream (Kafka producer) |
| Pharmacy event simulator | Event stream (Kafka producer) |
| WHOOP API | REST poll with `since=` cursor (semi-streaming) |
| EHR FHIR servers | REST resource bundles, daily updates |
| OpenFDA | REST report endpoint, rate-limited |
| dbt Commons marts | SQL transformation over already-at-rest data |

Three architectural archetypes were viable:

- **Pure Lambda** (Marz, 2011): twin batch + speed pipelines, batch as
  source-of-truth. Forces every dev to maintain two implementations.
- **Pure Kappa** (Kreps, 2014): one streaming pipeline, batch is
  reprocessing-by-replay. Requires all sources to be logs (or to look like logs).
- **Hybrid**: streaming where natural, batch where natural, integrated
  through a shared lakehouse. The pattern most production data platforms
  converge on.

The forces:

1. **Wearable vitals require streaming**: anomaly detection (elevated HR
   sustained >5 min) is a project requirement with sub-minute latency
   tolerance.
2. **EHR/OpenFDA can't be streamed without contorting reality**: they're
   REST APIs we don't control. A "stream" of them is just a polling loop
   with a Kafka topic in the middle, buying nothing.
3. **Marts don't benefit from continuous compute**: dbt Commons updates
   weekly. Continuous incremental refresh is pure cost.
4. **Cost is bounded**: dev target ~$200/mo, prod target ~$2k/mo. Always-on
   streaming for sources that don't need it would blow the budget.

## Decision

Adopt a **streaming-first hybrid** architecture:

- **Streaming** (Spark Structured Streaming on EMR, Kafka source, Iceberg
  sink): wearable vitals, pharmacy events, WHOOP-poll-to-Kafka bridge.
  Long-running queries, real-time bronze → silver → gold projection.
- **Scheduled batch** (Prefect → EMR steps or dbt subprocess): EHR daily,
  OpenFDA daily, identity bridge daily, dbt marts weekly, maintenance
  nightly.
- **Shared substrate**: Iceberg-on-S3 with Glue catalog. Streaming writes
  the same tables batch reads. Iceberg's snapshot isolation makes this
  safe.

The decision rule for new sources (also documented in
`docs/architecture_classification.md`):

1. Native event stream → streaming path.
2. REST + sub-hour latency requirement → poll-to-Kafka bridge → streaming.
3. Otherwise → scheduled batch.

## Consequences

**Positive**:
- Total compute cost is dramatically lower than pure Kappa (no always-on
  stream for daily/weekly sources).
- Operational complexity matches the data's actual velocity — slow-moving
  sources have simpler, cheaper pipelines.
- Streaming and batch share Iceberg tables, so there's no twin-pipeline
  duplication of business logic (silver projections, identity resolution).
- New engineers can reason about each source independently — the
  classification is honest, not aspirational.

**Negative**:
- Two operational paradigms to maintain: streaming queries (long-running,
  checkpoint-managed, watchdog-monitored) and Prefect flows
  (scheduled, retry-managed).
- Two failure modes per pipeline. The `streaming-monitor` Prefect flow
  exists specifically to bridge this — it watches the streaming layer
  and alerts via the same channels as flow failures.
- Mode selection is a judgment call, not a rule. New engineers need the
  decision matrix in `docs/architecture_classification.md` to choose
  correctly.

## Alternatives Considered

- **Pure Lambda**: rejected. Duplicate codebase across batch and speed
  pipelines. The integration point between the two is always painful
  ("which view do consumers query?"). Iceberg's existence eliminates the
  *need* for the speed pipeline to be separate from batch.
- **Pure Kappa**: rejected on two grounds. (1) EHR and OpenFDA aren't
  natively streamable; forcing them into Kafka is ceremony without benefit.
  (2) Always-on streaming for dbt marts and identity bridge would
  approximately double dev-scale compute cost for no business benefit.
- **Pure batch (nightly only)**: rejected. Wearable anomaly detection
  requires sub-minute latency. A nightly batch pipeline cannot meet that
  requirement.
- **Event sourcing with a single Kafka cluster as the source of truth**:
  rejected. Iceberg-on-S3 is a better source-of-truth than Kafka — it's
  cheaper for cold storage, queryable by multiple engines, and gives time
  travel without Kafka log compaction trickery.

## References

- `/Users/nerdboss-stm/pulsetrack-cm/docs/architecture_classification.md` —
  sibling doc with the layer-by-layer classification and decision matrix.
- `/Users/nerdboss-stm/pulsetrack-cm/orchestration/README.md` — Prefect
  deployments + cadence (canonical list of which sources run in which mode).
- `/Users/nerdboss-stm/pulsetrack-cm/streaming/bronze_ingestion.py` —
  streaming-mode entry point for wearable bronze.
- `/Users/nerdboss-stm/pulsetrack-cm/transformations/bronze_to_silver/ehr_silver.py`
  — batch-mode counterpart for EHR.
- Jay Kreps, "Questioning the Lambda Architecture" (O'Reilly Radar, 2014).
- WHOOP engineering blog — Prefect migration + Iceberg migration series
  (the architecture PulseTrack tracks).
- Related: ADR-001 (Iceberg), ADR-002 (EMR), ADR-003 (Avro).
