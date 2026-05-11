# ADR-008: Streaming vs Batch Selection per Layer, Gated by Cluster Capacity

## Status
**Accepted**

## Date
2026-05-11

## Context

ADR-005 established PulseTrack as a streaming-first hybrid: wearable
vitals stream from Kafka through bronze → silver → gold, while EHR /
OpenFDA / dbt marts run as scheduled batch. That decision is sound at
the source-classification level — it says *which* sources are streaming.
What it did not specify is what happens at *each layer* when the
deployed cluster cannot host three concurrent streaming queries.

During the 2026-05-11 10M-event scale test (`j-T5OF7WBI2I4V`, 2-core
m5.xlarge dev cluster), we attempted the canonical ADR-005 topology:
three concurrent Spark Structured Streaming queries — bronze from Kafka,
silver from bronze Iceberg, gold from silver Iceberg — all writing to
their respective sinks via `processingTime` triggers at 30s. The result
was reproducible **YARN scheduling starvation**:

```
2026-05-11 17:32:14 INFO YarnScheduler: Initial job has not accepted
  any resources; check your cluster UI to ensure that workers are
  registered and have sufficient resources
2026-05-11 17:32:29 INFO YarnScheduler: Initial job has not accepted
  any resources; check your cluster UI to ensure that workers are
  registered and have sufficient resources
[...repeats every 15 seconds...]
```

YARN ResourceManager UI confirmed the diagnosis: bronze had claimed the
single AM container + 2 executor containers; silver was waiting for AM
allocation in the ACCEPTED state; gold never reached ACCEPTED. With the
2-core cluster's ~6 task slots, bronze's executor count (2) + AM (1)
plus driver overhead consumed all but ~2 slots; silver's `spark.yarn.am.waitTime=600s`
expired before silver's AM slot was scheduled. Gold was similarly
starved.

The mechanism, in plain terms: Spark Structured Streaming holds executor
containers for the lifetime of the query. With three concurrent
long-running queries on a fixed-size YARN cluster, the total executor
demand (`spark.executor.instances` × 3 + 3 AMs + 3 drivers) must fit
inside the YARN-allocatable capacity. On a 2-core m5.xlarge (8 vCPU,
32 GB) cluster after YARN/HDFS reservations, you have approximately:
- 6.2 GB / executor × 4 executors = 24.8 GB allocatable
- 4.5 vCPU / executor × 4 executors = 18 vCPU

That envelope hosts **one** streaming query comfortably with 2 executors;
**two** queries marginally; **three** queries not at all.

We resolved this for the scale test by **serializing the layers** using
two Spark trigger modes already supported in the codebase:

```bash
# Bronze: drain Kafka in a single batch and exit (no long-running stream)
python streaming/bronze_ingestion.py \
    --trigger available_now \
    --format delta

# Silver: read bronze as a static dataset (not a stream)
python transformations/bronze_to_silver/sensor_silver.py \
    --mode batch \
    --format delta

# Gold: same — static read from silver
python transformations/silver_to_gold/fact_vital_reading.py \
    --mode batch \
    --format delta
```

`--trigger available_now` (Spark 3.3+) tells the streaming engine to
process all currently-available data in a single micro-batch and exit.
The query is still a streaming query (checkpoint-managed, schema-on-write,
foreachBatch sink) but it has a bounded lifetime. `--mode batch` in
silver and gold uses `spark.read` (static) instead of `spark.readStream`.

The scale test then ran each layer **sequentially** within the same EMR
step concurrency window: bronze finished in 9m 8s; silver started after
and finished in 6m 20s; gold ran 5 dim + 2 fact steps in parallel within
the remaining capacity.

This worked but moves "stream latency" out of the silver/gold layers in
dev. In production (4+ core cluster), all three layers run as live
streams concurrently; in dev they are batch-on-trigger, and silver/gold
freshness is bounded by the orchestration cadence (Prefect 5-minute
default) rather than the streaming `processingTime` trigger (30 seconds).

## Decision

PulseTrack uses **layer-streaming-status gated by cluster size**:

| Layer | 2-core dev cluster | 4+ core staging/prod cluster |
|---|---|---|
| **Bronze (Kafka → table)** | Streaming with `--trigger available_now` (bounded-lifetime stream, drains all available data and exits) | Streaming with `processingTime=30s` (continuous) |
| **Silver (bronze → silver)** | `--mode batch` (static read, Prefect-triggered) | Streaming with `processingTime=30s` (continuous) |
| **Gold facts (silver → gold)** | `--mode batch` (static read, Prefect-triggered) | Streaming with `processingTime=30s` (continuous) |
| **Gold dims** | `--mode batch` (always; no benefit to streaming dims) | `--mode batch` (always) |

Bronze is **always streaming-shaped** because Kafka offset commits + Avro
schema enforcement + `dropDuplicatesWithinWatermark` are only available
on the streaming write path. The difference between dev and prod is the
trigger mode: bounded (`available_now`) vs continuous (`processingTime`).

Silver and gold facts are **conditional**: streaming on prod where the
cluster can host three concurrent queries, batch-on-trigger on dev where
it can't. The orchestrator chooses by inspecting the EMR cluster's
configured core count at submit time.

The CLI knobs are already wired (this is a configuration decision, not
a code change):
- `--trigger available_now` / `--trigger processing` in
  `streaming/bronze_ingestion.py`
- `--mode batch` / `--mode streaming` in
  `transformations/bronze_to_silver/sensor_silver.py` and the
  gold fact transformations

## Consequences

**Positive:**
- Dev clusters ship at 2-core sizing (~$0.50/hr) with the full bronze →
  silver → gold pipeline exercising the same code paths as production.
  No "dev-only" code branches in the transformation logic.
- Production keeps the full streaming-first semantics from ADR-005: 30s
  end-to-end latency from Kafka to gold facts, no orchestration penalty.
- The decision rule is binary and inspectable: `core_count >= 4` →
  streaming; otherwise batch. Operators don't have to reason about
  Spark scheduling internals.
- Bronze's choice (`--trigger available_now` vs `processingTime`) preserves
  bronze's contract of exactly-once writes with Kafka offset commit,
  identically on both sides.

**Negative:**
- **Dev silver/gold freshness is bounded by Prefect cadence, not stream
  lag.** A Prefect flow polling every 5 minutes means silver in dev is
  up to 5 minutes stale even after bronze lands. This is acceptable for
  dev (no on-call paging on dev freshness) but means dev cannot validate
  the prod-shape F2/F4 SLOs (`silver_sensor_lag < 60s`, `gold_fact_freshness < 10min`).
- **Two operational modes per layer** to document and maintain. The
  on-call runbook for "silver is stale" branches on cluster size.
- **The 4-core threshold is empirical, not formally proven.** We know
  2-core fails and prod's 4-core succeeds; we have not validated 3-core
  m5.xlarge. The threshold may turn out to be 3, not 4.
- **Chaos drills are gated on streaming mode.** `scripts/chaos/kill_spark_app.py`
  is meaningful only against a live streaming query. On dev's
  batch-on-trigger silver/gold, the drill scripts run but only exercise
  a 6-minute batch job, not a multi-hour stateful stream.

**Neutral:**
- Throughput per layer is unchanged. Silver processed 32.3M rows in 380s
  (85k rec/s effective) in batch mode; streaming would process the same
  load in similar wall-clock at sufficient capacity.
- Cost per layer is unchanged. Cluster-hours are the unit of billing;
  streaming-mode doesn't change EMR compute price.

## Alternatives Considered

### Alternative 1: Scale dev up to 4 cores so all layers can stream
- **Pros:** Single operational mode. Dev exercises production-shape
  freshness SLOs. No batch/streaming branch.
- **Cons:** Doubles dev cluster cost (~$0.50/hr → ~$0.95/hr;
  ~$200/mo → ~$400/mo). The freshness validation has no business value
  in dev — no real consumers query dev silver/gold. We're paying for
  observability that nothing observes.

### Alternative 2: Prefect-triggered downstream layers in *all* environments
- **Pros:** Removes the streaming/batch branch. One mode everywhere.
- **Cons:** Adds 30 seconds to several minutes of latency in production
  for silver and gold, depending on Prefect schedule density. Wearable
  anomaly detection (the principal business case for streaming —
  elevated HR sustained >5min) has a sub-minute latency budget. Moving
  silver to batch costs us the latency budget and forces re-architecture
  of the anomaly explainer.

### Alternative 3: Continuous-mode bronze + always-rebuild gold
- **Pros:** Gold is always fresh; no streaming-stage coordination.
- **Cons:** Rebuilding `fact_vital_reading` (5M rows) on every 30-second
  trigger is ~$15/hr of unnecessary compute in steady state. This is
  the "continuous-rebuild" trap; ADR-005 explicitly rejected pure-Kappa
  partly to avoid it.

### Alternative 4: Use a separate small cluster per layer
- **Pros:** Each cluster sized for one streaming query; no contention.
- **Cons:** Three EMR clusters at $0.50-0.95/hr each = $1.50-2.85/hr.
  Approximately triples cost. Doesn't simplify operations — now you have
  three clusters to monitor and reconcile Iceberg snapshots across.

### Alternative 5: Move silver/gold to dbt incremental models on EMR-Serverless
- **Pros:** EMR-Serverless auto-scales per query; no fixed-size YARN
  contention. dbt incremental is well-trodden.
- **Cons:** Loses Spark Structured Streaming's checkpoint + exactly-once
  guarantees on silver. dbt incremental on Iceberg/Delta is "process all
  new rows since last run" which is semantically batch. Considered for
  Q3 follow-up but not for this ADR.

## Notes on Spark Configuration

For completeness, the Spark configs that bound the 2-core failure mode
(documented here so the next operator understands why "tune
`spark.executor.instances` up" doesn't fix it):

| Config | Default | 2-core dev value | 4-core prod value |
|---|---|---|---|
| `spark.executor.instances` | dynamic | 2 | 6 |
| `spark.executor.cores` | 1 | 2 | 4 |
| `spark.executor.memory` | 1g | 6g | 12g |
| `spark.yarn.am.waitTime` | 100s | **600s** (raised — see incident 13 in postmortem) | 100s |
| `spark.sql.streaming.minBatchesToRetain` | 100 | 50 | 100 |
| `spark.dynamicAllocation.enabled` | true | **false** (fixed-size for streaming) | true |

The 2-core configuration is tight but correct *for a single streaming
query*. Running three queries simultaneously requires either 3× the
executors (not available on 2 cores) or 3× the cluster.

## Related ADRs

- **ADR-005 (Streaming-first hybrid):** This ADR refines ADR-005 by
  specifying per-layer behavior when cluster capacity cannot host the
  full streaming topology. It does not contradict ADR-005 — production
  still runs the streaming-first topology end-to-end.
- **ADR-007 (Table format by cluster size):** Same gating dimension
  (cluster size); both ADRs reduce dev's resource demands. ADR-007
  covers the storage format; ADR-008 covers the compute pattern.
- **ADR-001 (Iceberg over Delta):** Iceberg's `streaming-skip-overwrite-snapshots`
  fix (commit `0cadc84`) is what makes the silver streaming over bronze
  case work in production; it's unused in dev's batch mode but remains
  the production code path.

## References

- Postmortem: `postmortems/2026-05-11_emr_cluster_bringup_13_incidents.md`,
  fix #18 ("Serialized concurrent streams via `--trigger available_now`
  + `--mode batch` to avoid YARN starvation on 2-core cluster").
- Scale test: `docs/scale_test_results.md`, §3 (latency not measured) and
  §10 (the actual end-to-end shape used in this run).
- Code: `streaming/bronze_ingestion.py` (the `--trigger` flag),
  `transformations/bronze_to_silver/sensor_silver.py` (the `--mode` flag),
  `transformations/silver_to_gold/*.py` (mirror `--mode` flag).
- Orchestrator: `scripts/run_scale_test.sh`, the per-script CLI args
  mapping (incident #7 in the postmortem).
- Spark docs: Structured Streaming Programming Guide, "Triggers" section,
  `Trigger.AvailableNow()` semantics.
- YARN docs: ResourceManager scheduling, AM container reservation.
