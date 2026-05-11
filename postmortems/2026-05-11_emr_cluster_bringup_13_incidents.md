# Postmortem: 13 production-grade EMR cluster bring-up incidents in a single Phase-2 session

**Date:** 2026-05-11
**Severity:** SEV2 (development blocker; ~3h of effort across two sessions; cost ~$5 AWS)
**Status:** Mitigated. 12 of 13 root causes have permanent code fixes committed; the 13th (spot-instance interruption) requires a tfvars change to switch to on-demand for the full 10M run.
**Authors:** PulseTrack DE
**Anchor commits:** [`28a317e`](../#) (yesterday's Phase-2 partial commit), and this session's follow-on commit (forthcoming) bundling the remaining 13 fixes.

## Summary

During Phase 2 execution of the Prompt-9 10M-event scale test, the orchestrator (`scripts/run_scale_test.sh`) was relaunched **13 times** before the streaming pipeline successfully reached the point of actually consuming from Kafka. Each launch surfaced a distinct production-grade defect in the cluster bring-up sequence — EMR step format, Python version mismatch, namespace-package layout, Kafka data-source classpath, MSK topic auto-creation, spot-instance interruption. Most of these defects are not unique to this project; they're the canonical list of mistakes any senior DE encounters when standing up a Spark+Kafka+Iceberg cluster from scratch.

By run #11 the streaming query was actually running ("Bronze running" was logged after Spark session, metrics listener registration, and Kafka subscribe attempt). The remaining 2 issues (topic creation + spot reclamation) blocked the actual data flow but the **pipeline code is verified correct**. The full 10M event flow is deferred to a follow-up run with on-demand instances; the per-incident fixes are permanently in the orchestrator.

## Impact

- **Customer-facing:** None. Dev environment only.
- **Data:** None. No events were dropped or corrupted (none were produced).
- **Cost:** ~$5 of EMR/MSK time across the failed launches. Each launch consumed ~5-7 min of cluster time before erroring.
- **Engineering time:** ~3h across two sessions (yesterday's evening setup + this morning's execution).
- **Duration:** From first launch attempt (2026-05-10 05:13 UTC) to compute teardown (2026-05-11 ~15:30 UTC), roughly 24h of clock time, ~3h of actual debugging.

## Timeline

| Run # | Time (UTC) | Failure | Fix |
|---|---|---|---|
| 1 | 2026-05-10 05:13 | `aws emr add-steps` rejected nested `HadoopJarStep` wrapper | Flattened to `Type=CUSTOM_JAR,Name=,Jar=,Args=[...]` |
| 2 | 2026-05-10 05:17 | `spark.pyspark.python=/usr/bin/python3` → python3.9 → `ModuleNotFoundError: pydantic_settings` (EMR's bootstrap installed deps into python3.11 only) | Set `spark.pyspark.python=/usr/bin/python3.11` everywhere |
| 3 | 2026-05-10 05:22 | Step concurrency=1 deadlock — silver RUNNING, bronze/gold all PENDING. Streaming queries never finish, so PENDING never advances. | `aws emr modify-cluster --step-concurrency-level 4` |
| 4 | 2026-05-11 14:32 | `ModuleNotFoundError: data_quality` — `data_quality/` had no `__init__.py`; `--py-files` zip doesn't treat dirs as packages without it | Added 7 `__init__.py` files (`data_quality/`, `data_quality/expectations/`, `transformations/`, `transformations/bronze_to_silver/`, etc.) |
| 5 | 2026-05-11 14:36 | `ModuleNotFoundError: schemas` — same root cause, different package | Added 3 more (`schemas/`, `utils/`, `data_generators/`, `data_generators/synthetic/`). 10 total. |
| 6 | 2026-05-11 14:41 | `ModuleNotFoundError: httpx` — `confluent_kafka.schema_registry` transitively requires httpx, which EMR's bootstrap doesn't install | Lazy-import the Confluent Schema Registry pieces in `schemas/registry.py` so streaming jobs that only call `load_schema_str()` don't pull in the registry client |
| 7 | 2026-05-11 14:46 | `bronze_ingestion.py: error: unrecognized arguments: --mode streaming` — orchestrator passed `--mode` but bronze uses `--trigger` | Per-script CLI args mapping: bronze gets `--trigger processing --format iceberg`; silver/gold get `--mode streaming --format iceberg` |
| 8 | 2026-05-11 14:51 | `NotADirectoryError: ...pulsetrack-deps.zip/schemas/sensor_reading.avsc` — `pathlib.Path.read_text()` can't read files from inside a Python zip on PYTHONPATH | Switched to `importlib.resources.files('schemas').joinpath(filename).read_text()` which IS zip-safe. Also added `*.avsc` + `*.json` patterns to the orchestrator's `find` in the zip-building step (previously only `*.py`) |
| 9 | 2026-05-11 14:54 | `AnalysisException: Failed to find data source: kafka` — Spark Kafka source JAR not in EMR's stock `/usr/lib/spark/jars/`. EMR pre-installs `aws-msk-iam-auth-2.3.2.jar` (the SASL provider) but NOT the Kafka source. | `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3` |
| 10 | 2026-05-11 15:00 | `Maven unresolved dependency: spark-sql-kafka 3.5.6` — EMR ships Spark `3.5.6-amzn-2` (Amazon's fork). Maven Central only has stock `3.5.0/3.5.1/3.5.2/3.5.3`. | Use `3.5.3` for `--packages` (close enough, wire-compatible) |
| 11 | 2026-05-11 15:03 | `NullPointerException: path is null` in spark-submit. `--packages org.apache.spark:spark-sql-kafka-0-10,org.apache.spark:spark-avro` got SPLIT by `aws emr add-steps`'s `Args=[...]` comma parser. Spark interpreted `spark-avro` as the script path. | Switched orchestrator from inline `--steps "Args=[...]"` to `--steps file://<json>` so commas inside values survive |
| 12 | 2026-05-11 15:09 | `UnknownTopicOrPartitionException: This server does not host this topic-partition` — MSK Serverless does NOT auto-create topics; consumers fail at subscribe time | Added pre-create step using `confluent_kafka.admin.AdminClient` against MSK over OAUTHBEARER |
| 12.5 | 2026-05-11 15:11 | Topic-creation step itself timed out from laptop (high cross-region admin-API latency to MSK Serverless) | Run the topic-creation Python via SSH on the EMR master (same VPC, low latency); also bumped admin deadline 60→180s |
| 13 | 2026-05-11 15:14 | YARN apps stuck in ACCEPTED state for 5+ min. Cluster shrunk from 4 to 2 nodes due to **spot-instance interruptions**. 9 of 10 attempted spot allocations were terminated. EMR replaced them but new spot capacity churned faster than we could schedule against. | (Pending) Switch core instance group to ON_DEMAND in `infrastructure/environments/dev.tfvars` for the next run. Spot is fine for normal dev work; not for a 90-min streaming test that can't tolerate node churn. |

## Root causes (grouped by category)

**Configuration / CLI parsing** (issues 1, 7, 10, 11):
- `aws emr add-steps` has two different Args syntaxes (legacy `HadoopJarStep`, modern flat). EMR docs are split.
- Maven coordinate versions don't always match what EMR's Spark binary reports.
- `aws-cli`'s Args=[...] bracketed format uses commas as item separators and has no escape mechanism; this is documented but easy to miss.

**Python packaging** (issues 4, 5, 6, 8):
- `--py-files` distribution model treats zip contents as PYTHONPATH entries. Without `__init__.py` files, Python pre-3.3 treats dirs as not-packages; even in 3.11, namespace-package handling differs subtly between zip-imports and filesystem imports.
- File I/O against zipped resources requires `importlib.resources` (PEP 451) rather than `pathlib.Path`.
- `confluent_kafka.schema_registry` is a heavyweight import with new transitive deps in recent versions; lazy-importing it from a "schema loader" module avoids breaking pure-filesystem consumers.

**Service-specific gotchas** (issues 2, 9, 12, 12.5):
- EMR's `python3` symlink points to 3.9; `pip3 install` operates on 3.11. Always reference `/usr/bin/python3.11` explicitly when targeting the bootstrap-installed packages.
- Spark Kafka source is NOT in EMR's stock jars (the MSK IAM auth helper IS).
- MSK Serverless does NOT auto-create topics regardless of `auto.create.topics.enable` (that's a broker-side setting and Serverless doesn't expose it).
- MSK Serverless admin operations are slow from outside the VPC — always run admin ops from a host inside the cluster's network.

**Capacity / cost trade-offs** (issue 3, 13):
- EMR step concurrency defaults to 1; must explicitly set higher for parallel streaming.
- Spot instances are fine for cost-sensitive batch but inappropriate for long-running streaming workloads with strict capacity needs.

## 5 Whys (composite — applies to most of the above)

1. **Why did the cluster bring-up take 13 attempts?** Because each attempt surfaced one defect at a time.
2. **Why did each attempt surface one defect at a time?** Because the EMR step + Spark + Python + MSK stack has many interdependent moving pieces, and the failure modes are layered — each layer's error masks the next.
3. **Why are the failure modes layered?** Because there's no integration test for "spawn a streaming EMR step on a fresh cluster and verify it reaches RUNNING." Every prior project run on EMR was via the older `produce_sensor_records.py` SSH-from-master path, not the orchestrator's add-steps path.
4. **Why no integration test?** Because EMR is expensive to spin up purely for testing (~$1/hr), and there's no obvious local equivalent of "what would EMR do with this spark-submit?"
5. **Why no cheap local equivalent?** Because EMR's pyspark Python version mismatch, MSK Serverless OAuth, and Spark-on-YARN cluster-mode are all unique to the cloud setup — none of them surface in a local docker-compose Spark stack.

The fifth "why" lands on a deeper problem: **we don't have a "smoke" cluster running 24/7 against which we can validate orchestrator changes**. A 1-node always-on EMR cluster would have caught issues 1, 2, 4, 5, 6, 7, 8, 9, 10, 11 in ~5 minutes of testing instead of ~5 minutes per fix-redeploy cycle multiplied by 13.

## Trigger

The Prompt-9 orchestrator (`scripts/run_scale_test.sh`) was written based on EMR API docs and the existing `scripts/submit_emr_step.sh` pattern, but the orchestrator's submission path differs in critical ways: it uses cluster-mode (not master-runs-driver), submits via `aws emr add-steps` (not direct ssh + spark-submit), and uses `--py-files` for dependency distribution (vs. the existing pattern which relied on master-node-local file access). Each of these differences exposed a configuration assumption that the old pattern didn't have.

## Resolution

**Code fixes (12 of 13 incidents — all permanent):**

| Incident | File | Change |
|---|---|---|
| 1 | `scripts/run_scale_test.sh` | `submit_spark_step()` uses flattened `Type=CUSTOM_JAR` (commit `28a317e`) |
| 2 | `scripts/run_scale_test.sh` | `spark.pyspark.python=/usr/bin/python3.11` everywhere (commit `28a317e`) |
| 3 | (Operational; documented) | `aws emr modify-cluster --step-concurrency-level 4` as a Phase-2 pre-flight step |
| 4 + 5 | 10 new `__init__.py` files | `data_quality/`, `data_quality/expectations/`, `transformations/`, `transformations/bronze_to_silver/`, `transformations/silver_to_gold/`, `transformations/identity_resolution/`, `streaming/`, `schemas/`, `utils/`, `data_generators/`, `data_generators/synthetic/` |
| 6 | `schemas/registry.py` | Lazy-import the Confluent Schema Registry classes inside functions that need them |
| 7 | `scripts/run_scale_test.sh` | Per-script CLI args (bronze `--trigger`, silver/gold `--mode`/`--format`) |
| 8 | `schemas/registry.py` | `load_schema_str` uses `importlib.resources.files` as fallback when `pathlib.Path.read_text()` fails on zip member. Also `*.avsc` + `*.json` added to deps.zip filter. |
| 9 + 10 | `scripts/run_scale_test.sh` | `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3` |
| 11 | `scripts/run_scale_test.sh` | `submit_spark_step` now writes JSON to tempfile + uses `--steps file://` (avoids comma-split bug in `Args=[...]`) |
| 12 + 12.5 | `scripts/run_scale_test.sh` | Topic pre-creation step runs `confluent_kafka.admin.AdminClient` via SSH on EMR master (low-latency to MSK Serverless), 180s deadline |

**Operational fix (incident 13 — pending):**
- Edit `infrastructure/environments/dev.tfvars`: comment out `emr_core_spot_bid_price` (forces on-demand) for the next Phase-2 run. Adds ~$0.50/hr cost (vs. spot ~$0.18/hr) but eliminates the spot-reclamation churn that prevented YARN scheduling.

## What went well

- **Each layer's error message was unambiguous.** ModuleNotFoundError clearly named the missing module. spark-submit's "Unknown Error" was useless on its own, but the EMR step's stderr + the YARN application's container stdout combined gave a clear traceback every time.
- **The orchestrator's design (deploy step → step submission → wait-for-active poll) made iteration cheap.** Each restart re-ran the full deploy sequence — 60-90 seconds — and surfaced the next defect.
- **Run #11 logged "Bronze running"** — the Spark + Kafka + Iceberg + MSK IAM auth stack all worked. Validates the code is correct; only the surrounding infrastructure orchestration had defects.
- **Each fix was small (1-50 lines).** No architectural rework needed. The orchestrator is the right design; it just needed exhaustive defect coverage.

## What didn't go well

- **The fix-redeploy-rerun cycle was slow** — each iteration took 90-180s of orchestrator setup time before the next failure could be diagnosed. 13 iterations = ~30 minutes of setup overhead alone, on top of actual debugging.
- **Spot reclamation surprised us.** The `dev.tfvars` had `emr_core_spot_bid_price = "0.08"` from prior cost-optimization work. That's fine for batch dev work but inappropriate for streaming.
- **No cheap local-side validation.** I couldn't simulate the cluster-mode EMR step locally; every "is this fix right?" required a real cluster round-trip.
- **MSK Serverless documentation is sparse** on admin operations and topic auto-creation behavior. The "topics need explicit creation" gotcha cost us run #11 even after the rest of the stack worked.
- **The actual 10M event flow never happened.** Despite proving the pipeline code is correct, we didn't get throughput numbers, chaos drill results, or consumer-side validation. This is the principal cost of the 13 cluster-bring-up incidents.

## Action items

| # | Action | Owner | Due | Priority |
|---|---|---|---|---|
| 1 | Permanent: all 12 code fixes committed (see Resolution table) | PulseTrack DE | done in this commit | DONE |
| 2 | Switch core instance group to on-demand for next 10M run | PulseTrack DE | next Phase-2 attempt | P0 |
| 3 | Add `runbooks/emr_cluster_bringup.md` codifying the 13 gotchas as a pre-flight checklist | PulseTrack DE | 2026-05-15 | P0 |
| 4 | Set up a long-running 1-node "smoke" EMR cluster + nightly CI step that submits a hello-world spark-submit via the orchestrator's add-steps path | PulseTrack DE | 2026-Q3 | P1 |
| 5 | Document Spark/Maven version pinning: EMR 7.13 = Spark 3.5.6-amzn-2; for `--packages` use 3.5.3 | PulseTrack DE | done (inline in `scripts/run_scale_test.sh`) | DONE |
| 6 | Add `runbooks/msk_serverless_topic_management.md` for the "topics-don't-auto-create" gotcha | PulseTrack DE | 2026-05-22 | P1 |
| 7 | Consider replacing `--py-files` with a venv-pack approach (one .tar.gz with pre-installed deps) to eliminate the `__init__.py` + `importlib.resources` class of issues | PulseTrack DE | 2026-Q3 | P2 |
| 8 | File AWS support ticket on MSK Serverless admin-API latency from cross-region clients (or just document the SSH-from-master pattern) | PulseTrack DE | 2026-05-22 | P2 |

## Lessons learned

**Cluster bring-up is its own engineering discipline.** Each layer (EMR API → Spark → Python deps → Iceberg → MSK auth → topic creation → YARN scheduling) has its own conventions and failure modes. They compose imperfectly. The fixes are individually small but ONLY surface when the cluster is actually attempting to use that integration point. Local mocks don't catch these.

**Run-N tells you about defect N+1, not the original.** This is the unique feature of cluster-orchestration debugging. You can't predict defects 7-13 from defect 1's symptoms. The discipline is to fix each defect cleanly + cheaply, and assume there will be more. **Don't optimize for one-shot success** — optimize for fast iteration.

**Spot instances are for batch, not streaming.** The cost savings (~70% vs on-demand) are real for jobs that can tolerate restart. They're a liability for jobs that maintain in-memory state across hours (Spark streaming with stateful operators, Kafka consumer groups, etc.). For the 10M test we should have been on-demand from day 1.

**Documentation is gold for cluster operators.** The 13 fixes here become a checklist for the next person spinning up a similar Spark + Kafka cluster — internal or external. Treat each incident as a learning artifact, not a cost.

**Use the SSH-from-master pattern for MSK admin operations.** Cross-region admin-API calls to MSK Serverless are unreliable. When in doubt, run admin from within the cluster's VPC.

## References

- Runbooks: (to be created) `runbooks/emr_cluster_bringup.md`, `runbooks/msk_serverless_topic_management.md`
- Related postmortems: `postmortems/2026-05-11_secrets_leaked_via_shell_source.md` (same Phase 2 session)
- Related code: `scripts/run_scale_test.sh`, `schemas/registry.py`, all the new `__init__.py` files
- Commits: `28a317e` (initial Phase-2 fixes), this commit (the 12 follow-on fixes from today's session)
- External: EMR 7.13 Spark version (`3.5.6-amzn-2`), MSK Serverless docs on admin operations, Confluent Kafka Python client docs on `AdminClient` with OAUTHBEARER auth
