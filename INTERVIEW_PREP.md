# PulseTrack — Interview Prep (Senior DE)

This is the repo-root interview prep doc. It is the WHOOP DE II/III
elevator-pitch + Q&A bank built from real PulseTrack work.

The companion meta-doc (in `/Users/nerdboss-stm/pulsetrack-study/INTERVIEW_PREP.md`)
indexes the per-prompt reports for deeper drill-downs. This file is the
self-contained interview-prep navigator.

## Elevator pitch (60 seconds)

> "PulseTrack is a streaming health-data lakehouse on AWS. It ingests
> wearable sensor readings from MSK Serverless via the WHOOP API and
> synthetic generators, runs Kappa-architecture Spark Structured
> Streaming through bronze/silver/gold on S3 with Iceberg + Glue
> catalog, and lands a star-schema warehouse for downstream ML and
> analytics. The pipeline includes a WHOOP-style Glacierbase migration
> framework with DynamoDB-backed concurrency lock, reversed-ID S3
> partitioning to mitigate the midnight thundering-herd, and identity
> bridging across three independent data sources (wearable, EHR FHIR,
> pharmacy CDC). End-to-end verified continuous on EMR 7.13 / Iceberg
> 1.10 — bronze, silver, and gold all running concurrently as YARN
> applications, with the producer feeding MSK from inside the VPC."

If you can deliver that in 60 seconds with no notes, you're prepared.

## Architecture in one sentence per layer

- **Producers:** Avro to MSK Serverless (SASL/IAM); 3 sources (WHOOP API,
  Open FDA, HAPI FHIR) + synthetic for scale tests.
- **Bronze:** Spark Structured Streaming, PERMISSIVE Avro decode, parse
  failures DLQ'd to a Delta + Kafka topic, append-only Iceberg snapshots.
- **Silver:** explode-and-validate, watermark + dropDuplicatesWithinWatermark,
  foreachBatch MERGE INTO, blocking GX gate.
- **Identity bridge:** 4-phase resolution (EHR → email → device → FDA),
  outputs `patient_key` (SHA-256), 93.5% link rate.
- **Gold:** streaming MERGE into 3 facts + 9 dims, star schema, Glacierbase-
  managed schemas.
- **dbt + Snowflake:** Commons macros, snapshot SCD2, weekly CI, analytical
  views for BI.
- **Orchestration:** 7 Prefect Cloud deployments (commits `02e5a2b`,
  `781c409`).
- **Observability:** Prometheus + StreamingQueryListener + CloudWatch +
  Monte Carlo monitors + AI-assisted incident summaries.

---

## Senior DE Q&A (added Prompt 9)

What follows is ~50 Q&A across 6 categories, written from PulseTrack
work. Each answer cites specific files, commits, and reports. Read out
loud at least once to practice the cadence.

### Category 1 — Incident response

**Q1.1 — Walk me through how you handled the silver cold-start hang.**

A: Mid-Prompt-4. After wiring silver to read from the bronze Iceberg
table, the streaming query showed `RUNNING` in the Spark UI but
`Input rate = 0` for 40 minutes. I started with the StreamingQueryListener
(`utils/streaming.py`, `register_metrics_listener`) which confirmed
`numInputRows = 0` per batch. Then I checked the S3 checkpoint:
`aws s3 ls s3://pulsetrack-lakehouse-dev/checkpoints/silver_sensor/`
showed `offsets/0` written but `commits/0` missing — classic stuck batch.
Wiping the checkpoint restarted the query cleanly. The deeper fix
came in commit `0cadc84` where gold opts into
`streaming-skip-overwrite-snapshots=true` because silver's MERGE
creates overwrite snapshots that the Iceberg streaming source rejects
by default. See `pulsetrack-study/PROMPT_4_REPORT.md` § 3.12 and
`docs/war_stories.md` story 1.

**Q1.2 — A Kafka consumer group is showing 500K lag and growing. Walk me
through your triage in the first 5 minutes.**

A: Open `runbooks/kafka_consumer_lag.md`. The 30-second triage is three
commands: list consumer groups, describe the lagging one to confirm
which partitions are behind, then `yarn application -list -appStates
RUNNING` to confirm the consumer is alive. Three flavors map to three
fixes: dead consumer (executor crashed → restart), alive but slow
(parallelism insufficient → scale up `maxOffsetsPerTrigger` and add
executors), or producer outran consumer transiently (wait). Cross-check
with Prometheus `consumer_lag{layer="bronze"}` for the trend slope.
If lag is monotonic and growing, that's SEV2.

**Q1.3 — At 2am you get paged: `pulsetrack-dev-s3-slowdown` ACTIVE.
What's your first move?**

A: Open `runbooks/s3_503_throttling.md`. First confirm with CloudWatch
`AWS/S3 5xxErrors`. Then ID the hot prefix: `aws s3 ls
s3://${BUCKET}/bronze/sensor_readings/ | head -20`. With reversed-ID
partitioning we've laid out `rid=<reversed_device_id>/...`, so a hot
prefix usually means one device is overrepresented. If we see real
503s, the immediate action is reduce `maxOffsetsPerTrigger` to slow the
write rate (gives S3 time to auto-split the prefix). The long fix is
on the producer side (rebalance partition keys) or via Iceberg
compaction. Postmortem in `postmortems/` template.

**Q1.4 — Tell me about a non-obvious bug you found.**

A: The em-dash that broke EMR Terraform. Commit `5e08bb0`. I had
written an SG description with an em-dash (—, U+2014). AWS rejected
it with the generic `InvalidParameterValue: Description contains
invalid characters`. The fix was 30 seconds (replace with hyphen) but
the diagnosis took 25 minutes because the error message points nowhere.
The lesson: macOS smart-substitutions silently swap `--` for `—` and
`"` for `"`; disable them in System Settings if you write infra code.
See `docs/war_stories.md` story 2.

**Q1.5 — How do you write a postmortem so it's actually useful?**

A: I use the template in `postmortems/_template.md`. Structure:
(1) one-paragraph summary; (2) timeline with timestamps and what
happened at each point; (3) root cause — not the proximate cause but
the chain back to "what would have prevented this entirely"; (4)
impact (users affected, data lost, hours of degradation); (5) action
items with explicit owners and dates. The "leaked WHOOP credential"
postmortem (`postmortems/2026-05-09_whoop_secret_in_git.md`, referenced
in `docs/scale_test_runbook.md` § "T-25 minutes") is the canonical
example. The action items there shipped as commits `4a39930` (Secrets
Manager migration) and `62a8514` (gitleaks + trufflehog).

### Category 2 — System design tradeoffs

**Q2.1 — Iceberg vs. Delta. Why did you choose Iceberg?**

A: Three reasons, in order: catalog independence (Glue native — Delta
needed Databricks until very recently), partition evolution in-place
(Iceberg supports `ALTER TABLE ADD PARTITION FIELD`; Delta required
rewrite as of 2024), and broader ecosystem support (Snowflake reads
Iceberg native, Athena does too). The honest counterweight is that
Delta is the better choice if you're committed to Databricks-as-platform.
PulseTrack runs on EMR + Glue + S3 + Snowflake; Iceberg fits cleanly.
The codebase preserves Delta for local-dev (`--format delta` flag in
the spark-submit invocations) so we can swap back if needed. See
`pulsetrack-study/PROMPT_4_REPORT.md` § 2.1 and
`lakehouse/format_writer.py` for the abstraction.

**Q2.2 — Streaming vs. batch for the EHR path. Why batch?**

A: The HAPI FHIR public server publishes daily clinical bundles. They
update on a daily cadence, not continuously. Forcing them through
streaming would require: (a) a Debezium-like CDC layer to detect
changes, (b) a producer to publish to Kafka, (c) state to track what
we've already seen. For zero business benefit — analytics consumers
update overnight anyway. Daily batch through the same Spark engine
(`run_batch()` in `ehr_silver.py`) gives us replay safety, idempotency
via MERGE, and no extra infrastructure. Kappa lets one engine handle
both — different trigger, same code.

**Q2.3 — Kappa vs. Lambda. Why one engine?**

A: Lambda (two pipelines: stream-for-realtime, batch-for-correctness)
gives you flexibility but doubles maintenance. Two transform codebases,
two CIs, two sets of bugs, two sets of monitors. Kappa says "use one
engine; choose the trigger." Spark Structured Streaming with `trigger=
ProcessingTime("30s")` is streaming; with `trigger=Once()` it's batch.
The transform code is identical. For PulseTrack's scale (10M-event
test target, ~25K rec/s burst), one Spark cluster handles both fine.
At 10x our scale you might split.

**Q2.4 — MSK Serverless vs. provisioned MSK. Why serverless?**

A: PulseTrack is a small project. We need ~7 MB/s burst, not 200. Pay-per-
request + GB-stored makes the dev environment essentially free during
idle (which is most of the time). SASL/IAM auth integrates with EMR
EC2 instance profiles, so no password rotation. Tradeoff vs provisioned:
serverless caps per-topic at ~200 MB/s, no JMX exposure for deeper
monitoring, less granular control. At WHOOP's scale (millions of
users, sustained high write rate), provisioned MSK or self-managed
Kafka makes more sense.

**Q2.5 — Why MERGE INTO via foreachBatch instead of Iceberg's native
streaming sink?**

A: Iceberg's default streaming sink is INSERT only. If Kafka replays
a record (which it will — Kafka is at-least-once), the default sink
creates a duplicate row in Iceberg. We want idempotency on the grain
key. `foreachBatch` lets us call `MERGE INTO` per batch:
`WHEN NOT MATCHED THEN INSERT WHEN MATCHED THEN UPDATE`. This is
idempotent on `(reading_id, metric_name)` in silver and on the fact's
grain in gold. Spark guarantees the same `batch_id` on retry, so the
MERGE is deterministic.

**Q2.6 — Why `streaming-skip-overwrite-snapshots=true` for gold?**

A: Silver's MERGE creates Iceberg `overwrite` snapshots even when only
INSERTs happen — Iceberg classifies a write as overwrite if it MIGHT
rewrite data files (for compaction or row-level deletes). Iceberg's
streaming source rejects overwrite snapshots by default. We opt in to
skipping them for gold because (a) silver UPDATEs are extremely rare in
our pipeline today (watermark dedup catches replays before they hit
MERGE), and (b) being unblocked is worth more than the theoretical
correctness for a case that doesn't happen. Documented trade-off in
commit `0cadc84` and `pulsetrack-study/PROMPT_4_REPORT.md` § 4.4.

**Q2.7 — Reversed-ID vs. hash-bucket partitioning. Why reversed-ID?**

A: Both achieve uniform write distribution across S3 prefix space.
Hash-bucket (`bucket(N, device_id)`) produces opaque paths like
`data/00009/file.parquet`. Reversed-ID produces grep-able paths like
`data/rid=2143abc/file.parquet`. In a 3am incident when I need to find
"which device's data is in this file," I would much rather reverse a
hex prefix than reverse-engineer a hash. The benchmark on real S3 (1M
records, 1000 devices, three strategies) showed both within 1% on
throughput. Reversed-ID won on operational ergonomics. See
`lakehouse/partition_strategy.py`, `docs/s3_partitioning_analysis.md`,
and `docs/war_stories.md` story 3.

**Q2.8 — SHA-256 vs. hash-bucket for identity surrogate key.**

A: We use SHA-256 to derive `patient_key` from the canonical identifier
(EHR patient_email or MRN). Why: deterministic (same input always
maps to same key), collision-free at our scale (256-bit), trivially
comparable across systems. WHOOP's published Glacierbase reference
uses MD5 historically; we use SHA-256 for the longer collision-resistance
window. Tradeoff: larger key (32 vs 16 bytes), marginally slower compute
(negligible at our scale). The `sha256_key` macro lives in
`dbt_project/macros/commons/`.

**Q2.9 — Why dbt for gold-marts but Spark for silver?**

A: Spark is the right tool for heavy lifting: Kafka decode, MERGE
across millions of rows, joins with billions of bridge entries. dbt
is the right tool for analytical "last mile": derived columns, weekly
aggregations, accepted-values tests. The split:
- Spark owns bronze → silver → gold-facts (source-of-truth star schema)
- dbt owns derived marts + views on top of gold

Adding a derived column used to take 1-2 hours (edit Python, CI, EMR).
Now it takes 10 minutes (edit SQL, dbt run local, PR, merge). See
commit `23619a0` and `docs/war_stories.md` story 7.

**Q2.10 — Why DynamoDB for the Glacierbase migration lock instead of
Postgres advisory lock?**

A: Three reasons. (1) AWS-native — we have DynamoDB in IAM scope
already; adding a Postgres just for this is dead-weight. (2) Pay-per-
request — idle cost is $0. (3) TTL feature auto-reaps stale locks if
the runner crashes mid-migration without explicitly releasing. The lock
implementation in `migrations/lock.py` uses `attribute_not_exists OR
expires_at < :now` for the acquire, keyed-on-holder-identity DeleteItem
for the release. Reserved-keyword `catalog` had to be aliased via
`ExpressionAttributeNames` (commit `8028edb`); see
`docs/war_stories.md` story 4.

### Category 3 — Data modeling

**Q3.1 — Why SCD2 for `dim_device`? Why not SCD1?**

A: Firmware version is operationally important. When a fitness band's
HR sensor starts reporting outlier values, the first hypothesis is "did
firmware roll forward yesterday?" SCD2 keeps history: `effective_from`,
`effective_to`, `is_current`. We can correlate readings to firmware
versions and detect bad rollouts. SCD1 would overwrite the firmware
version and lose the correlation. See
`transformations/silver_to_gold/dim_device.py` (commit `6187196`).

**Q3.2 — How do you handle late-arriving facts in a daily-summary
table?**

A: Two mechanisms. (1) Watermark sets the bound: events later than
watermark are dropped. We use 10 minutes for streaming dedup and 7
days for late-arrival in batch-recompute mode. (2) When a late event
arrives within the late-arrival window, we recompute the touched slice.
For `fact_vital_daily_summary`, that means recomputing the (patient,
device, date, metric) aggregate. The pattern is in
`transformations/silver_to_gold/fact_vital_daily_summary.py`: identify
touched (patient, date) tuples in this batch, recompute their
aggregates, MERGE INTO. Idempotent on grain key.

**Q3.3 — Why a junk dimension for `dim_metric`?**

A: We have ~15 metric attributes (unit, lower_bound, upper_bound,
display_name, category) that are low-cardinality and frequently joined.
A junk dimension packs them into one dim row per metric. The
alternative — duplicating the attributes on every fact row — is
storage-wasteful AND introduces inconsistency risk (what if the unit
for HR changes from bpm to Hz?). Junk dim gives us one place to
maintain. See `transformations/silver_to_gold/dim_metric.py`.

**Q3.4 — Why mask PII at the gold layer, not bronze?**

A: Bronze must support replay and incident investigation. If a producer
ships a malformed patient_email, we need the raw value to debug. Gold
is the published API; consumers (analysts, ML) don't need PII. The
`dim_patient` transform in `transformations/silver_to_gold/dim_patient.py`
masks PII into hashes + age-bucketed demographics before publishing.
This way, the audit trail is preserved in bronze (access-controlled)
while gold is safe for broader consumption.

**Q3.5 — Why a SHA-256 of canonical identifier as the surrogate key
instead of a database-generated sequence?**

A: The hash is deterministic across systems — when we re-process from
bronze, the same patient gets the same key. A database sequence would
require coordination (which warehouse generated it? do they collide?).
Across the three independent sources (wearable, EHR, pharmacy), we
need the patient_key to be the same regardless of which source you
arrived from. SHA-256 of patient_email achieves this without any
coordination service.

**Q3.6 — How do you handle the case where the same patient has two
different emails (e.g., personal + work)?**

A: This is the transitive linkage problem. The identity bridge
materializes `(patient_key, identifier_type, identifier_value)`
rows. If we know `device_account_id ↔ work_email` from device
registration AND `personal_email ↔ patient_key` from EHR, we can
link device → patient via the work_email row. The bridge's phase 2
does this transitive resolution
(`transformations/identity_resolution/patient_identity_bridge.py`).
Observed link rate is 93.5%; the remaining 6.5% are tracked as
`link_status=pending` for manual review.

**Q3.7 — Walk me through your star schema.**

A: 9 dimensions + 3 facts:
- **Dimensions:** dim_patient (PII-masked), dim_device (SCD2), dim_metric
  (junk), dim_date, dim_time, dim_condition + dim_condition_category,
  dim_medication + dim_drug_class.
- **Facts:** fact_vital_reading (grain = reading × metric), fact_vital_daily_summary
  (grain = patient × device × date × metric), fact_lab_result (grain =
  lab_observation_id).
- The bridge: patient_identity_bridge resolves account_id / mrn / email /
  fda_report_id to patient_key.

See `DataModel.md` for the full ERD. Tests in `tests/test_*.py` cover
FK integrity (34 tests just on dimensional integrity).

**Q3.8 — Why not just have one fact table?**

A: Grain dictates structure. `fact_vital_reading` is grain=reading; we
need this for ML feature stores ("give me the last 1000 HR readings
for patient X"). `fact_vital_daily_summary` is grain=daily; we need
this for trend analytics ("show me weekly HR averages"). Aggregating
from reading to daily in every query would be wasteful. Two facts =
two grains = two access patterns.

**Q3.9 — How do you handle schema evolution at the bronze layer?**

A: Bronze accepts schema-merge-on-write via Iceberg's `mergeSchema=true`.
When the Avro schema gains a new optional field, bronze adds the column
automatically. This is by design — bronze is "raw, data is the contract."
The enforcement happens at the bronze → silver boundary, where
`sensor_silver.py`'s explicit `select(...)` projection chooses which
fields propagate. Schema-merge-on-write is documented in
`docs/PRODUCTION_RUNBOOK.md` § 3.0 as part of the Glacierbase carve-out.

**Q3.10 — What's the Iceberg snapshot expiration policy?**

A: 7-day window. After 7 days, snapshots are eligible for removal via
`OPTIMIZE TABLE ... PURGE`. We run this nightly via the maintenance
pipeline (`orchestration/flows/maintenance_pipeline.py`). The trade-off:
shorter window saves storage, longer window keeps more history for
time-travel debugging. 7 days was the WHOOP-aligned choice.

### Category 4 — Operational depth

**Q4.1 — How do you debug a Spark streaming query that's lagging?**

A: From `runbooks/kafka_consumer_lag.md`: (1) Check YARN to confirm the
app is RUNNING. (2) Open Spark UI → Structured Streaming tab. Compare
Input rate to Processing rate. If processing < input, you have lag.
(3) Check batch duration trend. If duration is growing, the per-batch
cost is increasing (state store growing? S3 503s? executor going slow?).
(4) Check Prometheus `pt_processing_latency_seconds` histogram. (5)
Check S3 5xxErrors — a slowdown cascade looks like flat throughput but
rising latency.

**Q4.2 — How does graceful shutdown work in the streaming jobs?**

A: `utils/streaming.setup_graceful_shutdown()` registers SIGTERM and
SIGINT handlers. On receiving the signal, the handler calls
`stream.stop()` which waits for the current micro-batch to finish, then
flushes the checkpoint, then exits. This means we don't lose in-flight
batches when EMR auto-terminates or we Ctrl-C locally. See commits
`06770a8` and `ef1b55a`. Wired into all streaming jobs via the bronze
ingestion entry point.

**Q4.3 — What's the maintenance pipeline doing?**

A: `maintenance/compaction.py` runs nightly: OPTIMIZE on each Iceberg
table (compact small files), Z-ORDER on the query-pattern columns
(`patient_key`, `metric_name`), VACUUM with 168h retention (removes
files outside the snapshot window), and sets TBLPROPERTIES for
`auto-compact`, `optimize-write`, `delete-file-retention`. 21 tables
total. Scheduled daily via Prefect (`maintenance_pipeline` flow). See
commit `b6dc506` for the initial implementation.

**Q4.4 — What's exactly-once in this pipeline?**

A: End-to-end exactly-once on grain keys within the watermark window;
at-least-once outside. The chain: Kafka delivers at-least-once. Bronze
is append-only (no dedup; let silver handle it). Silver's
`dropDuplicatesWithinWatermark(["reading_id", "metric_name"])` provides
bounded-state dedup within the 10-minute watermark. Silver's
`foreachBatch MERGE INTO` is idempotent on grain. Gold's MERGE is
idempotent on grain. So a replay within 10 minutes produces no
duplicate rows; a replay 11+ minutes later might.

**Q4.5 — How do you handle the case where the EMR cluster crashes
mid-batch?**

A: Spark Structured Streaming checkpointing handles it. Each batch
writes `offsets/N` before processing, `commits/N` after success. On
restart: Spark reads the last `commits/N`, fast-forwards to the
corresponding Kafka offsets, replays from there. The `foreachBatch`
MERGE is idempotent on grain, so re-processing the same batch produces
the same result. EMR's `2h auto-terminate` (commit `3a9a339`) is the
worst case; we lose ~7 minutes of orphan-batch reprocessing.

**Q4.6 — How does the budget alert work?**

A: AWS Budget at $40/month for the dev account, alert at 80% via SNS
to the operator's email. When the alert fires, the runbook
(`docs/PRODUCTION_RUNBOOK.md`) says "run `infrastructure/teardown-compute.sh`"
which destroys EMR + MSK but keeps S3 + Glue. Cost drops from $1.30/hr
to ~$0.01/hr. Next morning's `terraform apply` brings the compute
back; streaming queries resume from checkpoint. See commits `963692c`
and `a286cda`. We've never hit the alert in practice but the muscle
memory matters.

**Q4.7 — Tell me about your observability stack.**

A: Three layers. (1) Application metrics via Prometheus: `pt_records_processed_total`,
`pt_records_failed_total`, `pt_processing_latency_seconds` histogram,
`pt_consumer_lag` gauge. Scraped by per-driver HTTP servers on ports
8001/8002/8003. (2) Spark-native via StreamingQueryListener emitting
per-batch JSON (`utils/streaming.register_metrics_listener`, commit
`6d896ec`). (3) AWS-native: CloudWatch alarms on S3 5xxErrors, EMR
step failures, SNS for paging. Plus the Grafana dashboard
(`monitoring/grafana/dashboards/pulsetrack.json`) with 7 panels covering
throughput, errors, lag, latency p50/p95/p99, active queries, quarantine,
DLQ.

**Q4.8 — What runs the Monte Carlo monitors?**

A: `observability/monitors.py` defines freshness, volume, schema, and
distribution checks for the gold tables. Schedule is Prefect-driven
(daily). The freshness check looks at the latest Iceberg snapshot
timestamp; alerts if older than the SLA (15min for streaming tables,
24h for daily-batch). Volume check compares row count to a 7-day
rolling baseline (3-sigma deviation triggers a warn). Schema check
compares column list to the registered schema. Output goes to the
Slack channel via the existing alerting hook (commits `c5ea199`).

### Category 5 — Behavioral / leadership

**Q5.1 — Tell me about a time you mentored a junior.**

A: At PulseTrack-scale this is hypothetical, but the artifacts that
would mentor a junior exist: `docs/onboarding_new_de.md` (the day-by-day
ramp), `docs/first_pipeline_tutorial.md` (the file-by-file walk), the
`tests/conftest.py` fixtures (so they don't have to learn the SparkSession
boilerplate). The mentor's job is removing friction from the things the
junior doesn't need to learn yet, so they can focus on the things they
do. Real mentoring looks like 1:1s, code review with substantive
feedback, and pairing on the hard stuff. Documenting all of this
is the foundation.

**Q5.2 — When did you push back on a senior's design?**

A: Hypothetically, the choice to dual-write Delta + Iceberg during the
modernization (commit `1e03a98`). A simpler approach would have been
"cut over to Iceberg, drop Delta." But that locks us in and breaks the
local-dev path. I argued for the `FormatWriter` abstraction
(`lakehouse/format_writer.py`) that supports both, with `--format
iceberg` as the production flag. The cost is a few hundred extra lines
of code; the benefit is reversibility AND a clean local-dev story.
This was the right call — I've used the Delta path during local
testing multiple times.

**Q5.3 — Tell me about a time you said no.**

A: Hypothetically: an analyst wanting bronze tables exposed in Snowflake.
The ask was reasonable on the surface (more data, less waiting for the
pipeline). But bronze is the raw debug layer; exposing it would have
created downstream consumers of unvalidated, undeduped data. The right
"no" was "no, but here's what we can do instead": stand up the gold
star schema in Snowflake (we did this in commit `c5ea199`), add the
dbt project for analyst-friendly transformations (commit `23619a0`),
and write the Snowflake views (`snowflake/models/vw_*.sql`). The
analyst got more access AND the data is trustworthy. Win-win, but it
required saying no first.

**Q5.4 — Tell me about a time you made the wrong choice.**

A: Initially I set `streaming-skip-overwrite-snapshots=true` for gold
without thinking through the silver-UPDATE case. The pipeline was
unblocked. The build was green. Ship it. A few hours later I realized:
healthcare data corrections are a real case. With the flag at `true`,
gold misses corrections. The right move was to document the trade-off
explicitly, add a follow-up reconciliation backlog item, and accept
the risk consciously rather than implicitly. Commit `0cadc84` documents
this. See `docs/war_stories.md` story 6.

**Q5.5 — How do you handle disagreement on technical decisions?**

A: Write the design down. Ambiguity in conversation is much worse than
ambiguity in a doc. The Glacierbase carve-out — what's in scope, what's
not — is documented in `docs/PRODUCTION_RUNBOOK.md` § 3.0. If someone
disagrees, they can edit the doc and we have a concrete artifact to
discuss. Verbal disagreements drift; written ones converge.

**Q5.6 — Tell me about a time you had to learn something new quickly.**

A: The WHOOP-aligned Glacierbase implementation. I had read WHOOP's
public blog on their migration framework. Two days later I had a working
Python implementation with: WHOOP-style headers, per-catalog YAML
config, DynamoDB lock, Go-template variable substitution, SHA-256
file-hash immutability. Commit `0aaf0e1`. The trick was breaking down
the blog into "five concrete things to implement" and shipping each
incrementally with tests. See `pulsetrack-study/PROMPT_4_REPORT.md` §
2.8.

**Q5.7 — Tell me about a time you saw a problem before it was a
problem.**

A: The leaked WHOOP credential. The exploration agent flagged it during
Prompt 9 routine repository scanning. The credential had been in public
git history for weeks. We rotated immediately, migrated to Secrets
Manager (commit `4a39930`), and added pre-commit + CI gates (commit
`62a8514`). The lesson: scan adversarially. Don't just ask "what should
I build next"; ask "what's wrong with what I already have." See
`docs/war_stories.md` story 5.

**Q5.8 — How do you prioritize technical debt vs new features?**

A: Tech debt has a half-life: it gets worse if you don't address it.
But not all tech debt is equal — debt that's blocking is critical,
debt that's annoying is tolerable. The way I frame it: "what's the
cost of NOT fixing this in the next quarter?" The silver gate refactor
(commit `78ddfea` — dropping the absolute `event_timestamp` window)
was tech debt that became critical when the WHOOP 240-day backfill
hit. The em-dash bug was annoying; the actual fix was 30 seconds. I'd
budget ~20% of capacity to addressing debt proactively.

**Q5.9 — What does "ownership" mean to you?**

A: When something in your area breaks at 3am, you're the one who fixes
it — and you make sure the next 3am incident is something different,
not the same one. Ownership is the chain from "I shipped this" to "I
own the runbook" to "I wrote the postmortem" to "I made the fix that
prevented recurrence." The artifacts that mark ownership at PulseTrack:
the runbooks I wrote (`runbooks/`), the postmortems I'd write
(`postmortems/`), the production runbook (`docs/PRODUCTION_RUNBOOK.md`).

**Q5.10 — Tell me about a time you had to make a tradeoff between
shipping fast and shipping right.**

A: The cloud migration. I could have spent two more weeks gold-plating
the Terraform modules with full multi-environment, multi-region,
private-subnet, NAT-gateway plumbing. Instead I shipped MSK on public
subnets (commit `acab385`), dropped NAT gateway entirely (commit
`c3a4575`) — saving $30/month — and accepted the trade-off that
production-grade hardening is Phase 3 work. For a dev environment, the
right call. For prod, no. The runbook is explicit about which is which.
See `infrastructure/main.tf` and `docs/PRODUCTION_RUNBOOK.md` § 8.

### Category 6 — WHOOP-specific

**Q6.1 — How does PulseTrack align with WHOOP's published architecture?**

A: Five explicit alignment points:
1. **Glacierbase migration framework** — WHOOP-style headers,
   per-catalog YAML, DynamoDB lock, Go-template variable substitution,
   SHA-256 file-hash immutability. Commit `0aaf0e1` and `1e03a98`.
2. **Reversed-ID S3 partitioning** — directly inspired by WHOOP's blog
   post on prefix-throttling. Commit `901bf01`,
   `docs/s3_partitioning_analysis.md`.
3. **Iceberg + Glue Catalog** — WHOOP's lakehouse stack.
4. **dbt Commons macros + 100% docs + snapshot SCD2** — WHOOP's dbt
   pattern. Commit `23619a0`.
5. **Prefect Cloud orchestration** — WHOOP migrated to Prefect; we
   match. Commit `02e5a2b`.

This isn't "I copied their work." It's "I read their public engineering
posts, recognized the patterns, and reimplemented them with my own
twist." Each implementation has decisions of its own (e.g., reversed-ID
over hash-bucket on operational grounds, SHA-256 over MD5 on collision
grounds).

**Q6.2 — What would you do differently at WHOOP's scale (10M users)?**

A: A few things change. (1) MSK Serverless caps at ~200 MB/s per cluster;
at 10M users you'd hit that. Move to provisioned MSK or a Kafka-on-K8s
deployment. (2) Per-driver Prometheus HTTP servers don't scale to
hundreds of drivers; move to a sidecar pattern with the metrics pushed
to a central aggregator (e.g., VictoriaMetrics or Cortex). (3) Spark
on EMR with dynamic allocation works to ~100 executors comfortably;
beyond that, Spark-on-K8s gives better resource utilization and
faster recovery. (4) Iceberg snapshot expiration at 7 days is too long
at scale — snapshots accumulate fast. Tune to 24-48h. (5) Identity
bridge at scale needs incremental refresh, not full recompute. The
current pattern works at 50K users; at 10M you'd need delta-only.

**Q6.3 — How would PulseTrack's identity bridge handle WHOOP's
multi-tenant cross-source identity?**

A: WHOOP has at least three identity sources: their app users, partner
integrations (CarePlus, Strava, etc.), and healthcare partners (HRSA,
hospital systems). The 4-phase pattern in
`transformations/identity_resolution/patient_identity_bridge.py` extends
naturally: each new source adds a phase. The hard part is the canonical
identifier choice. We use `patient_email` because it's stable across
EHR vs device-registration. WHOOP would likely use `whoop_user_id` as
the primary, with email/mrn/partner_id as secondary links. The bridge
output schema is the same: `(canonical_key, identifier_type,
identifier_value, link_status, link_method)`.

**Q6.4 — How does PulseTrack's observability compare to WHOOP's
typical stack?**

A: PulseTrack uses Prometheus + Grafana for application metrics,
CloudWatch + SNS for AWS-level, Monte Carlo monitors
(`observability/monitors.py`) for data quality. WHOOP has talked about
similar stacks publicly. The differences are scale-driven: WHOOP
probably has Datadog or a similar SaaS for log + metric unification
where we have separate stacks. Our Prometheus + per-driver port
pattern works at ~5 streaming queries; WHOOP would need a more
centralized aggregator.

**Q6.5 — How does Glacierbase compare to dbt for schema management?**

A: They overlap but serve different needs. Glacierbase manages
imperative, deterministic schema migrations for Iceberg / Delta tables
in the warehouse — partition evolution, ALTER COLUMN, CREATE TABLE.
dbt manages declarative, idempotent SQL transformations on top of
already-existing tables. WHOOP uses both. The split is clean: Glacierbase
for "structural changes that require migration ledger discipline," dbt
for "model logic that's idempotent and rebuildable from source."

**Q6.6 — Why is healthcare data harder than fitness data?**

A: Three reasons. (1) Corrections are real: a lab result can be
re-issued days later with a corrected value. Fitness data is mostly
append-only. (2) Compliance: HIPAA, audit trails, BAAs with cloud
providers. The PulseTrack identity bridge masks PII at the gold layer
because of this. (3) Identity resolution: a patient might have a
patient_email, an MRN, an FDA report ID, and a device_account_id —
four identifiers across three systems. The bridge has to resolve all
four. Fitness data typically has one identifier (the device).

**Q6.7 — What's the hardest thing about building this kind of system?**

A: Operational discipline. The code is the easy part — Spark + Iceberg
+ Kafka + dbt are well-documented; you can copy patterns. The hard
part is everything around the code: cost management (budget alerts,
spot instances, auto-terminate), runbooks for every paging condition,
postmortems that actually capture lessons, schema migrations that
don't break consumers, security gates (gitleaks, trufflehog) that
catch credentials before they ship. The artifacts that mark a
production-grade system aren't the transforms; they're
`runbooks/`, `postmortems/`, `docs/PRODUCTION_RUNBOOK.md`,
`pt_secrets/`, `.gitleaks.toml`. PulseTrack has all of them.
That's what separates portfolio-grade from production-grade.

---

## Per-prompt deep-dive index

The companion meta-doc at
`/Users/nerdboss-stm/pulsetrack-study/INTERVIEW_PREP.md` indexes:

- `PROMPT_1_REPORT.md` — foundational pipeline (medallion, Spark streaming,
  Kafka, Delta, Glue, terraform infra)
- `PROMPT_2_REPORT.md` — WHOOP API + pharmacy/FDA + identity bridge expansion
- `PROMPT_3_REPORT.md` — streaming observability + cloud-bring-up hardening
- `PROMPT_4_REPORT.md` — Iceberg + Glacierbase migrations
- `PROMPT_5_REPORT.md` — reversed-ID S3 partitioning + benchmarks
- `PROMPT_6_REPORT.md` — dbt project + Commons macros
- `PROMPT_7_REPORT.md` — Prefect Cloud orchestration
- `PROMPT_8_REPORT.md` — Snowflake + Monte Carlo + AI-assisted engineering
- `PROMPT_9_REPORT.md` — scale test + capacity plan + production hardening

For interview prep, the Q&A bank above is sufficient. Use the prompt
reports when you want depth on one specific topic.

---

## Final pre-interview checklist

1. Practice the elevator pitch out loud, 3× without notes.
2. Pick 2–3 stories from `docs/war_stories.md` you can tell concisely.
3. Have specific commit SHAs cached: `0aaf0e1` (Iceberg/Glacierbase),
   `1e03a98` (modernization), `0cadc84` (streaming fixes), `901bf01`
   (partition strategy), `02e5a2b` (Prefect), `23619a0` (dbt),
   `c5ea199` (Snowflake + Monte Carlo), `4a39930` (Secrets Manager),
   `62a8514` (gitleaks + trufflehog).
4. Be able to draw the architecture from memory (use
   `docs/architecture_one_pager.md` as the prop).
5. Know which file holds what (use the `docs/onboarding_new_de.md` Day 2
   table as the map).
6. Have one technical opinion ready that you'd push back on, with
   reasoning.

You are prepared.

---

*Last updated: 2026-05-10. Companion to
`/Users/nerdboss-stm/pulsetrack-study/INTERVIEW_PREP.md`.*
