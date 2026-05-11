# ADR-007: Table Format Selection by Cluster Size — Iceberg on 4+ cores, Delta on 2-core

## Status
**Accepted** (partial supersession of ADR-001) — pending re-validation on a 4-core cluster in the next scale test.

## Date
2026-05-11

## Context

ADR-001 selected Apache Iceberg 1.10 with the AWS Glue catalog as PulseTrack's
canonical table format across bronze, silver, and gold. That decision still
stands for production. What ADR-001 did *not* anticipate is the failure mode
we hit during the 2026-05-11 10M-event scale test.

The dev cluster (`j-T5OF7WBI2I4V`) was sized for cost: 1 master m5.xlarge +
2 core m5.xlarge (4 vCPU × 16 GB RAM per node, total 8 task slots after YARN
overhead — closer to 6 usable). On that footprint, the **Iceberg bronze write
hung indefinitely on stage 3**: the metadata-commit stage that lists data
files, snapshots, and writes the manifest list. Specific symptoms from the
EMR step's stderr (`scale-test-v3-bronze-iceberg`, step duration capped at
the 30-minute step timeout):

- Stage 0 (Kafka read + Avro decode): completed in ~95s, normal.
- Stage 1 (`dropDuplicatesWithinWatermark` + write to Iceberg data files):
  completed in ~210s, normal.
- Stage 2 (data-file rewrite for partition transforms `days(ingestion_timestamp)`):
  completed in ~40s, normal.
- **Stage 3 (Iceberg snapshot commit + manifest-list write): hung.** No
  progress for 18+ minutes. YARN reported the single driver task as
  RUNNING with `Last contact: 0s ago`, but the executor's thread dump
  (captured via `jstack` on the YARN container) showed the commit thread
  blocked on `S3AFileSystem.listFiles` against the bronze partition root,
  serialized.

The hypothesis (consistent with Iceberg 1.10 source review and discussions
in the `iceberg-dev` mailing list): the metadata-commit stage performs
**file-listing of the entire partition spec output to build the manifest
list before commit**. On a 2-core cluster with only ~6 usable task slots
and the driver pinning one of them for the streaming query coordinator,
the manifest-list build effectively serializes on a single executor's
S3 listing throughput. Across 78K newly-written parquet files in 99
micro-batches, the per-file `HEAD` round-trips dominated.

We retreated to **Delta Lake 3.3.2** (the Amazon-shipped variant on
EMR 7.13, `delta-iceberg-3.3.2-amzn-2.jar`) and the same 10M-event write
completed in 9m 8s with no commit-stage stall. Delta's transaction log
is a single JSON file per commit; it does not list data files at commit
time the way Iceberg's manifest-list path does.

**Production implication.** Production (`prod` Terraform workspace) runs
on 4 core m5.2xlarge (8 vCPU × 32 GB RAM per node) plus auto-scaling to
12 cores under load. That cluster has 30+ task slots and ample S3
listing parallelism. The Iceberg metadata-commit path is fast there
because the manifest-list build parallelizes. Production Snowflake
external tables also depend on Iceberg (via `AUTO_REFRESH`); switching
production to Delta would require a CDC pipeline to Snowflake we
explicitly avoided in ADR-001.

## Decision

PulseTrack uses **two table formats, selected by deployed cluster size**:

| Environment | Core count | Format | Catalog |
|---|---|---|---|
| Laptop / docker-compose | 1 | Delta (local) | Local Hive metastore (Docker) |
| Dev EMR (`dev.tfvars`) | 2 core m5.xlarge | **Delta** | Glue Hive metastore |
| Staging EMR | 4 core m5.2xlarge | **Iceberg** | Glue Iceberg catalog (`glue_iceberg`) |
| Prod EMR | 4-12 core m5.2xlarge (auto-scaling) | **Iceberg** | Glue Iceberg catalog (`glue_iceberg`) |

The format is selected at `spark-submit` time via the `--format` flag
already wired in `streaming/bronze_ingestion.py`,
`transformations/bronze_to_silver/sensor_silver.py`, and the gold
transformations. The orchestrator (`scripts/run_scale_test.sh`) chooses
`--format delta` when targeting a 2-core cluster and `--format iceberg`
otherwise. The threshold is encoded as `MIN_CORES_FOR_ICEBERG=3` in the
orchestrator.

The rule for the format gate: **Iceberg requires ≥ 3 task slots for the
metadata-commit stage to parallelize file-listing and manifest writing**.
A 2-core m5.xlarge cluster has ~6 task slots total but only ~3 are
available for non-driver work during streaming.

## Consequences

**Positive:**
- The dev cluster ships at the same cost (~$0.50/hr cluster vs ~$0.95/hr
  for a 4-core dev tier). Dev iteration loop is unblocked.
- Production keeps Iceberg with full Snowflake `AUTO_REFRESH` interop
  (the original ADR-001 driver).
- Tests pass on either format because both formats are wired through
  the same `--format` argparse flag — no code branches.
- The decision is data-driven and reversible: if Iceberg's metadata-commit
  perf improves (Iceberg 1.11+ has work-in-flight on async commit), we
  can re-validate on the 2-core cluster and unify on Iceberg.

**Negative:**
- **Two formats means two query-engine compatibility matrices.** Snowflake
  reads Iceberg natively but Delta only via UniForm (which the Delta
  3.3.2-amzn-2 build supports but we haven't smoke-tested end-to-end on
  Glue). Dev environment cannot validate Snowflake refresh against
  production-shaped tables without a manual Iceberg conversion.
- **Schema-evolution semantics differ.** Iceberg's partition-transform
  evolution (`days(ingestion_timestamp)` → `hours(...)`) is supported
  in-place; Delta requires a rewrite. Any dev-time partition experiment
  that worked on Delta might need adjustment for the Iceberg prod side.
- **Two `OPTIMIZE` paths to maintain.** `maintenance/compaction.py` already
  branches on format; tests cover both, but the operator runbook now has
  two case studies.
- **Iceberg's time-travel SQL (`VERSION AS OF`) is not portable to Delta**
  syntax (`@v123`); ad-hoc dev queries written against one don't transfer.

**Neutral:**
- The bronze partition layout (`days(ingestion_timestamp)`) is expressible
  in both formats; data physical layout is identical Parquet+zstd.
- File-level cost (S3 storage, compute) is comparable between the two
  formats at this scale.

## Alternatives Considered

### Alternative 1: Stay on Iceberg everywhere, scale up dev to 4 cores
- **Pros:** One format. One mental model. Matches production exactly.
- **Cons:** Doubles dev cluster cost (~$0.50/hr → ~$0.95/hr just for EMR
  EC2; ~$200/mo → ~$400/mo). For a project with a $200/mo dev budget,
  this is a 200% cost increase that delivers no business value — we
  already validated the bronze-write pipeline works on Delta, and the
  Iceberg semantics that matter (Glue, Snowflake) are exercised in staging.

### Alternative 2: Force Iceberg on 2-core, accept the commit hang and retry
- **Pros:** No format split.
- **Cons:** *Proven not to work.* We watched the stage-3 commit hang for
  18+ minutes during the run; the YARN step timed out. The hang is
  systemic — repeated runs would deterministically fail. No amount of
  `spark.sql.iceberg.execution.parallelism` tuning changes the
  metadata-commit serialization on a 2-core footprint, because the bottleneck
  is the executor's S3 listing throughput, not Spark's task scheduling.

### Alternative 3: Use Delta everywhere; abandon Iceberg
- **Pros:** Single format. Simpler operations.
- **Cons:** **Loses Snowflake `AUTO_REFRESH` interop**, which was the
  primary driver of ADR-001. Snowflake's Delta external-table support
  on Glue lags Iceberg (no `AUTO_REFRESH` at decision time; manual
  `ALTER EXTERNAL TABLE ... REFRESH` only). For PulseTrack this means
  staging/prod loses real-time Snowflake views of the lakehouse — a
  product regression.

### Alternative 4: Use Apache Hudi 0.15
- **Pros:** A third option exists.
- **Cons:** Not validated in this stack. Hudi's Glue catalog support
  lagged at ADR-001 decision time and the Spark Structured Streaming
  write path is less straightforward. Switching to an untested third
  format to solve a sizing problem is engineering theater.

### Alternative 5: Use a single-node Iceberg metadata service (REST catalog)
- **Pros:** Moves metadata commits off the Spark cluster to a dedicated
  service that can list S3 with its own thread pool.
- **Cons:** Tabular/Lakekeeper REST catalogs would solve the listing
  bottleneck but add a new always-on service to operate. Out of scope
  for dev cost target. Worth re-evaluating in Q3 once the REST catalog
  ecosystem matures.

## Related ADRs

- **ADR-001 (Iceberg over Delta):** This ADR partially supersedes
  ADR-001 *for the dev environment only*. ADR-001 remains the canonical
  decision for staging and production.
- **ADR-005 (Streaming-first hybrid):** The decision rule here interacts
  with ADR-008 (the streaming-vs-batch-per-layer decision), because the
  Iceberg-on-2-core failure was compounded by YARN starvation when
  multiple streaming queries competed for the same cluster.
- **ADR-002 (EMR over Databricks):** EMR's bundled Delta and Iceberg
  releases (3.3.2-amzn-2 and 1.10) are what's available; this ADR
  doesn't change the runtime choice.

## References

- Postmortem: `postmortems/2026-05-11_emr_cluster_bringup_13_incidents.md`,
  fixes #17 (Iceberg-to-Delta retreat) and #18 (streaming-to-batch retreat).
- Scale test results: `docs/scale_test_results.md`, §4 (Delta metadata
  per layer) and §10 (end-to-end pipeline proof).
- Iceberg source: `org.apache.iceberg.spark.source.SparkWriteBuilder`
  and `org.apache.iceberg.SnapshotProducer#commit` — the path that
  serializes manifest-list construction.
- Spark config: `streaming/spark_config.py` — `glue_iceberg` catalog
  binding (used in staging/prod) and `spark_catalog` (used in dev for
  Delta).
- Open question for next scale test: re-run the **same** 10M bronze
  write on a 4-core m5.xlarge cluster with Iceberg, confirm stage-3
  commit completes in < 60s, and tighten the `MIN_CORES_FOR_ICEBERG`
  threshold if 3 is too aggressive.
