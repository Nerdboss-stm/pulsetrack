# War Stories — PulseTrack Engineering

Eight stories from the PulseTrack build, each in STAR format (Situation,
Task, Action, Result, What I Learned). Use these in behavioral interviews
when asked "tell me about a time when..."

Each story is real. Commits referenced are real commits in this repository.

---

## 1. The night silver hung on cold start

**Situation.** Mid-Prompt-4 (the Iceberg + Glacierbase migration). I had
just finished wiring the silver streaming source to read from the bronze
Iceberg table. EMR was running, MSK was producing, bronze was writing.
I started the silver job. It... did nothing. No errors. No log output
past "starting query." Spark UI showed the streaming query as `RUNNING`
but the `Input rate` and `Processing rate` were both zero. For 40 minutes.

**Task.** Diagnose why silver was hung, then fix it without losing the
bronze data already in flight.

**Action.**

I started with the StreamingQueryListener (`utils/streaming.py`,
`register_metrics_listener`). It emits per-batch JSON. The listener
showed `numInputRows = 0` for every batch — silver was being told there
was nothing to read.

I then checked the S3 checkpoint:
```bash
aws s3 ls s3://pulsetrack-lakehouse-dev/checkpoints/silver_sensor/
```

I saw `offsets/0` had been written but `commits/0` had not. Batch 0
was in flight but never completing. The classic "is the source empty
or is the sink blocked?" situation.

I dug into the Iceberg streaming source docs and found the snapshot-
classification logic. Iceberg classifies every write as `append`,
`overwrite`, `delete`, or `replace`. The streaming source has a flag
`streaming-skip-overwrite-snapshots` defaulting to `false`. So if the
source table has any overwrite snapshots, the streaming source rejects
them.

I checked the bronze table's snapshots:
```python
spark.read.format("iceberg").load("...bronze.sensor_readings.snapshots").show()
```

Every bronze snapshot was `append`. So bronze wasn't the issue.

I re-read the silver streaming setup. The watermark + dropDuplicatesWithinWatermark
state was being persisted in the checkpoint. Wiping the checkpoint and
restarting the query immediately produced output.

Root cause: the *first* batch had hit a metadata-discovery race where Spark
wrote `offsets/0` but the consumer-side metadata fetch failed, and the
retry didn't re-trigger the source. Wiping the checkpoint forced the
clean re-init.

The deeper fix came in commit `0cadc84`: gold (which reads silver, where
silver DOES create overwrite snapshots due to MERGE INTO) needed
`streaming-skip-overwrite-snapshots=true` set explicitly. I documented
the trade-off (gold won't see silver UPDATEs, only INSERTs) in the
commit message and runbook.

**Result.** Silver was processing within 5 minutes of the checkpoint wipe.
The gold opt-in fix shipped same-day. Documented in
`pulsetrack-study/PROMPT_4_REPORT.md` § 3.12 and § 4.4. No data loss.

**What I learned.** When a streaming query "hangs" with no errors, the
problem is almost always either (a) the source is empty and Spark
isn't telling you, or (b) the sink is in a state Spark doesn't expect.
Check the checkpoint directly. The Spark UI lies by silence; the
filesystem doesn't.

Also: read the Iceberg streaming source source code, not just the docs.
The flag I needed (`streaming-skip-overwrite-snapshots`) is documented
but its semantics are subtle.

---

## 2. The em-dash that brought down EMR

**Situation.** Early in the cloud migration (Prompt 3-ish). I had just
finished the Terraform module for the EMR security group. The Terraform
`apply` was failing at the SG step with a cryptic AWS error:
`InvalidParameterValue: Description contains invalid characters`.

The SG description I had written was `"PulseTrack EMR — internal traffic"`.

**Task.** Figure out why AWS hated my SG description, fix it, document
the lesson so the next engineer didn't trip on it.

**Action.**

I started by checking the AWS docs for SG description constraints. They
say "ASCII characters." I had used an em-dash (—), which is U+2014, not
ASCII. AWS rejected it silently with the generic
`InvalidParameterValue`.

I fixed the SG description: `"PulseTrack EMR - internal traffic"` (plain
hyphen instead of em-dash). Terraform applied cleanly.

But this was the second time I had been bitten by smart-quote /
em-dash issues in the codebase (the first was a Markdown file
that broke a downstream parser). I wrote a `.gitleaks.toml` rule that
catches non-ASCII in Terraform files specifically — not as a security
rule but as a "you'll regret this" rule.

The fix shipped as commit `5e08bb0`:
> fix(infra): EC2 SG description rejects non-ASCII (em-dash); use plain
> hyphen

**Result.** Terraform applied. EMR cluster came up. I added the lesson to
my personal "things AWS hates" note. Total time to diagnose: 25 minutes.
Total time to fix: 30 seconds.

**What I learned.** AWS error messages are often generic where they
should be specific. When you see `InvalidParameterValue` on an SG, check:
non-ASCII characters, special characters that look ASCII but aren't
(smart quotes, em-dashes), trailing whitespace, length limits.

Also: macOS's "smart substitutions" in any text input will silently
swap `--` for `—` and `"` for `"`. Disable them in System Settings >
Keyboard > Text Input. I've never regretted this.

---

## 3. Reversed-ID partitioning vs. S3 throttling

**Situation.** Prompt 5. The capacity plan for the 10M-event scale test
projected that bronze would write ~5,500 events/s during the burst. S3's
per-prefix request rate limit is 3,500 PUT/s. With date-first partitioning
(`dt=2026-05-10/...`), every write at midnight would hammer the new
date prefix. We'd be throttled within minutes.

**Task.** Choose a partitioning strategy that distributes writes across
many prefixes. Implement it. Verify it on real S3, not just paper.

**Action.**

I read WHOOP's published blog post on S3 partitioning. They use reversed-ID:
take your `device_id`, reverse the string, partition by that. Because
device IDs have entropy in the low-order bits, reversing them spreads
across the prefix space.

I considered three alternatives:
1. **Hash-bucket partition:** `bucket(N, device_id)`. Iceberg-native.
   Uniform distribution.
2. **Reversed-ID:** materialize a `rid` column, partition by it. Uniform
   distribution AND grep-able.
3. **Date-first:** what we had. Throttled at scale.

Both (1) and (2) achieve uniform distribution. The difference: with
bucket(), the S3 path is `data/00009/file.parquet` — opaque. With
reversed-ID, the path is `data/rid=2143_abc/file.parquet` — human-grep-able.

In an incident at 3am, when I need to find "which device's data went
into this file?", I would much rather grep a hex prefix than reverse-
engineer a hash. I picked reversed-ID.

I implemented it as a strategy abstraction in `lakehouse/partition_strategy.py`
that supports all three strategies, so we could compare. The Iceberg
table is created with the chosen strategy's partition spec.

Then I ran the actual benchmark on real S3. 1M records, 1000 devices,
three strategies. Wrote the results doc:
`docs/s3_partitioning_analysis.md`. Reversed-ID and hash-bucket were
within 1% on throughput. Date-first was 60× slower under burst.

The fix shipped as commit `901bf01`:
> feat: reversed-ID S3 partitioning with real S3 benchmarks

**Result.** The 10M scale test hit 25,000 rec/s peak with zero S3 503s.
Postmortem-worthy artifact: `docs/s3_partitioning_analysis.md`. Future-
proofed for 100M events.

**What I learned.** Read the public engineering blogs of the company
you'd want to work at. WHOOP published their solution; reimplementing it
is one of the fastest ways to align with their patterns.

Also: when two solutions look equivalent on the engineering benchmark,
choose the one with better operational ergonomics. Grep-ability mattered
exactly once (in a hypothetical 3am incident), and that one time was
worth the small implementation overhead.

---

## 4. The DynamoDB reserved keyword

**Situation.** Mid-Prompt-4. I had just shipped the first cut of the
Glacierbase migration framework, including the DynamoDB-backed lock
(`migrations/lock.py`). I ran the integration test. It threw:
`ValidationException: Invalid attribute name 'catalog'. Attribute names
cannot be reserved keywords.`

`catalog` is a reserved keyword in DynamoDB. Of course it is.

**Task.** Fix the lock without breaking the API contract or losing the
"one row per catalog" semantics.

**Action.**

DynamoDB's `ExpressionAttributeNames` is the workaround: you write
`#catalog` in the expression, and pass `{'#catalog': 'catalog'}` in the
attribute names map. DynamoDB resolves the substitution and ignores
the reserved-word check.

I updated the lock's PutItem and DeleteItem calls:

```python
# Before
table.put_item(
    Item={'catalog': catalog, ...},
    ConditionExpression='attribute_not_exists(catalog)'
)

# After
table.put_item(
    Item={'catalog': catalog, ...},
    ConditionExpression='attribute_not_exists(#c)',
    ExpressionAttributeNames={'#c': 'catalog'}
)
```

I added a regression test (`tests/test_migrations_lock.py`) that
specifically exercises the reserved-word path: tries to acquire a lock
on `catalog='glue_iceberg'`, asserts no exception.

I also did a sweep of the other column names I had picked (`status`,
`name`, etc.) against the DynamoDB reserved words list (it's a long
list). Found two more: I had used `name` for the migration name, and
`status` for the apply result. Both needed the same treatment.

The fix shipped as commit `8028edb`:
> fix(iceberg,migrations): close 3 gaps from prompt 4

**Result.** Lock acquires and releases cleanly. Three reserved-word bugs
fixed. Test coverage on the lock module went from 60% to 92%. Documented
in `pulsetrack-study/PROMPT_4_REPORT.md` § 3.8.

**What I learned.** Every NoSQL database has reserved words. Don't assume
your column names are safe. Check the docs (DynamoDB has an explicit
reserved word list).

Also: when you find one instance of a class of bug, sweep for the others.
The third reserved-word case wouldn't have shown up until much later
because that code path wasn't covered by the integration test. Five
minutes of grep saved a future incident.

---

## 5. The leaked WHOOP credential

**Situation.** Mid-Prompt-9 (the production-hardening / scale-test
prompt). I had set up the AI-assisted exploration agent to scan the repo
for risks. It came back with: "your git history contains
`.env` with `WHOOP_CLIENT_ID` and `WHOOP_CLIENT_SECRET`. These are real
credentials for a real WHOOP account."

I checked. They were real. The first commit that introduced them was
weeks back. They had been in public git history (the repo was public on
my GitHub) for weeks.

**Task.** Rotate the credentials, scrub them from history (or accept
that they're rotatable), document the incident, harden the workflow so
this can't happen again.

**Action.**

Step 1: I rotated the WHOOP credentials immediately. Generated a new
client_secret in the WHOOP developer dashboard. Revoked the old one.

Step 2: I created a Secrets Manager migration so credentials live in AWS
Secrets Manager, not `.env`. Commit `4a39930`:
> feat(secrets): AWS Secrets Manager migration — TF module + Python
> facade + bootstrap

The `pt_secrets/` facade exposes a small Python API. Code reads
`pt_secrets.whoop_client_id()`, not `os.environ['WHOOP_CLIENT_ID']`.
The facade reads from Secrets Manager in cloud mode and from `.env` in
local-dev mode (with a noisy warning if the `.env` is being used in
cloud mode by accident).

Step 3: I added a `gitleaks` pre-commit hook AND a CI gate with
`trufflehog` (commit `62a8514`):
> chore(security): pre-commit gitleaks + CI trufflehog gate

This catches credentials BEFORE they get committed, and double-checks
in CI in case a developer disables pre-commit (`--no-verify`).

Step 4: I wrote the postmortem:
`postmortems/2026-05-09_whoop_secret_in_git.md` (referenced in
`docs/scale_test_runbook.md` § "T-25 minutes"). Includes the timeline,
the credentials affected, the rotation done, and the workflow changes
to prevent recurrence.

I did NOT try to scrub git history. The credential was already in the
wild; rotating is the only real fix. Scrubbing creates a false sense
of security.

**Result.** New credentials in Secrets Manager. Old credentials revoked.
Pre-commit + CI gates prevent recurrence. Process documented.

**What I learned.** Git history is forever. Don't ever commit credentials,
even "temporarily." If you DO, the right response is rotate-first,
postmortem-second, don't-try-to-scrub-history-third.

Also: AWS Secrets Manager pays for itself in stories like this. The
$0.40/secret/month was trivially worth the one rotation we needed.

Also: an AI exploration agent (which I had set up for productivity)
caught a security bug I had missed. The agents are useful adversarially:
ask them to find what's wrong with your codebase, not just what to
build next.

---

## 6. The Iceberg overwrite snapshot near-miss

**Situation.** Prompt 4 end-game. I was running the gold streaming
queries (fact_vital_reading from silver). It threw:
`UnsupportedOperationException: Found overwrite snapshot in streaming
source — set 'streaming-skip-overwrite-snapshots' to true if you want
to skip these snapshots`.

I had two paths:
1. Set the flag to `true` and accept that gold won't see silver UPDATEs
2. Refactor silver to never produce overwrite snapshots (e.g., switch
   MERGE to INSERT-only and dedup downstream)

I picked path 1 without thinking too hard. The streaming pipeline was
unblocked. The build was green. Ship it.

A few hours later, working on a separate change, I realized: what if
silver UPDATEs are a real case? E.g., the watermark dedup re-emits
a corrected reading because the first emission was malformed. With
the flag at `true`, gold misses the correction.

**Task.** Reason about the actual case for silver UPDATEs in our
pipeline. Decide if the trade-off is acceptable. Document the choice.

**Action.**

I sat down and wrote out the cases where silver MERGE would generate
an UPDATE (not just an INSERT):
1. **Replay within watermark:** Kafka replays an event we already
   processed. Dedup catches it; the MERGE finds an existing row and
   either skips (WHEN MATCHED THEN DO NOTHING) or replaces (WHEN
   MATCHED THEN UPDATE).
2. **Quality re-classification:** a row was previously `is_valid=false`
   and a later batch corrects the data. Unlikely with our pipeline
   (each reading is immutable once received), but possible.
3. **Late-arriving corrections:** the same reading is published with a
   corrected value (e.g., a producer bug fix re-publishes a window).
   Not part of our current product but a real concern for healthcare
   data where corrections are common.

For case 1, the MERGE is logically a no-op; gold doesn't need to see it.
For case 2, the volume is essentially zero in our pipeline today.
For case 3, this would be a real problem.

The acceptable mitigation: document the limitation, build a
reconciliation job that compares silver and gold periodically, and
revisit if case 3 becomes a real pattern.

I wrote it all up in `pulsetrack-study/PROMPT_4_REPORT.md` § 4.4. The
commit shipped (`0cadc84`):
> fix(streaming): gold opts into streaming-skip-overwrite-snapshots;
> doc fix

I added a follow-up backlog item: "build silver-to-gold reconciliation
job for healthcare-correction case."

**Result.** Gold pipeline unblocked. Documented trade-off. No data
loss because case 3 isn't happening today; we have a plan if it
starts to.

**What I learned.** When a quick fix unblocks you, take 30 minutes to
reason about what the fix actually means. The cost is small; the cost
of NOT doing this (silently breaking healthcare data quality) is huge.

Also: write down the trade-off, even when nobody asks. The postmortem
of a problem you avoid is worth more than the postmortem of a problem
you have.

---

## 7. Choosing dbt over more Spark

**Situation.** Prompt 6. I had the gold layer working as Spark
transforms (one Spark file per fact and dim, e.g.
`transformations/silver_to_gold/fact_vital_reading.py`). The analytics
team (a hypothetical analyst named, charitably, "future me") was going
to want to add derived columns weekly: "anomaly flags", "time of
day buckets", "rolling 7-day averages." Adding these to Spark transforms
meant writing Python, deploying via EMR, running through CI.

**Task.** Decide whether to keep extending Spark for gold-layer
analytics OR introduce dbt for the "last mile" SQL transformations.

**Action.**

I framed the decision honestly. Spark for analytics has real strengths:
- Same stack the engineering team owns
- No new tooling
- Tested heavily in the existing CI

But it has costs:
- Adding a column means a Python file, a CI run, an EMR step
- Analysts don't read Python well
- Iteration is slow (15-min round trip via EMR)

dbt has different strengths:
- SQL-native (analyst-friendly)
- Fast iteration (DuckDB local + Snowflake cloud)
- Built-in testing, docs, snapshots
- WHOOP's published pattern (Commons macros, snapshot SCD2)

And different costs:
- New tooling
- Requires a Snowflake catalog (we needed one anyway)
- Splits the codebase

I chose dbt for the analytical layer, kept Spark for ingestion +
silver. The split:
- **Spark:** bronze → silver, identity bridge, gold facts (the canonical
  star schema)
- **dbt:** views and marts ON TOP OF the gold facts, derived columns,
  aggregations, snapshots

I implemented it as commit `23619a0`:
> feat: dbt project — WHOOP Commons, 100% docs, snapshot SCD2, weekly CI

The dbt project (`dbt_project/`) has staging, intermediate, marts.
100% column-level documentation as a discipline. Commons macros in
`dbt_project/macros/commons/` (sha256_key, safe_divide, etc.).
A weekly CI run that exercises the full dbt build against DuckDB
fixtures.

**Result.** Adding a new analytical column now takes 10 minutes
(edit SQL, run dbt locally, PR, merge, auto-deploy). The same change
in Spark used to take 1-2 hours. The analytics velocity improved
without any compromise on engineering rigor (the Spark transforms
are still the source of truth for gold).

**What I learned.** Pick the right tool for the right layer.
Spark for "heavy lifting" (Kafka decode, MERGE, large joins).
dbt for "last mile" SQL (derived columns, aggregations,
materialized views). Forcing one to do the other's job is what
creates the painful incidents.

Also: when introducing new tooling, do it in a clearly-bounded layer.
The Spark / dbt split is clean because nothing in dbt depends on
anything in Spark *except* the gold tables. Easy to reason about,
easy to debug, easy to teach a new hire.

---

## 8. Migrating from Makefile to Prefect

**Situation.** Late in the build (Prompt 8-ish). The Makefile had
become the de-facto orchestrator. `make stream-bronze`, `make stream-silver`,
`make identity`, `make batch-gold`, `make compact`. It worked, but:
- No schedules (cron in shell wrappers, scattered)
- No retries
- No alerting on failure
- No dependency graph (you had to know that `batch-gold` needs `identity`
  to run first)
- WHOOP had publicly migrated to Prefect Cloud; their patterns were what
  I wanted to align with

**Task.** Migrate orchestration to Prefect Cloud. Keep the Makefile
working for local dev. Preserve all 7 production schedules.

**Action.**

I designed the Prefect flows in `orchestration/flows/`:
- `daily_ehr_pipeline.py` — daily HAPI FHIR fetch → batch silver
- `daily_pharmacy_pipeline.py` — daily Open FDA + pharmacy silver
- `whoop_poll_pipeline.py` — 30-min poll of WHOOP API
- `streaming_monitor.py` — health check + restart of streaming queries
- `maintenance_pipeline.py` — OPTIMIZE / Z-ORDER / VACUUM (weekly)
- `dbt_pipeline.py` — dbt seed / run / test (daily after gold)
- `full_refresh.py` — emergency rebuild (manual trigger)

Each flow uses Prefect tasks for retries (`retries=3, retry_delay_seconds=60`),
caching, and structured logging. Tasks compose; flows orchestrate.

The hardest part: the dbt task. dbt commands need a working directory and
respect environment variables. I built it so the task can either be
called with an explicit `project_dir` OR resolve to a sensible default
from the environment. This took one extra commit (`781c409`) to fix
when I realized the test fixtures weren't passing `project_dir` and
were getting `None`:
> fix(orchestration): dbt tasks accept project_dir=None and resolve to
> env default

The Makefile stayed — it's still useful for local dev where Prefect
Cloud is overkill. The flows can be invoked locally too (`python -m
orchestration.flows.daily_ehr_pipeline`) so the dev loop is preserved.

The migration shipped as commit `02e5a2b`:
> feat: Prefect Cloud orchestration — matches WHOOP's Prefect migration

**Result.** 7 deployments in Prefect Cloud with schedules, retries,
alerting. The Makefile still works for local dev. Operational visibility
went from "check Grafana hourly" to "Prefect Cloud dashboard + Slack
alerts on failure."

**What I learned.** Don't throw away the old tool when introducing the
new one — provide both during the migration. The Makefile + Prefect
co-existence let me move incrementally. If I had ripped out the Makefile
day 1, the local dev loop would have broken and I would have lost a
week.

Also: WHOOP's choice of Prefect Cloud was a meaningful signal. They
made the call after evaluating Airflow and Dagster. Following their
choice meant I didn't have to redo their evaluation. Industry alignment
saves real time.

Also: write the test that exercises the "I forgot to pass the optional
arg" case. The `project_dir=None` fix was an afternoon I could have
saved with one more test.

---

## How to use these stories

In an interview:
1. **Listen for the question.** Don't pre-cache a story and force-fit
   it to whatever they asked.
2. **State the situation in two sentences.** Don't ramble.
3. **Make the action concrete.** Specific commands, specific files,
   specific decisions. Don't generalize.
4. **Quantify the result.** "5 minutes vs 1 hour", "no data loss",
   "60× faster." Numbers are credibility.
5. **End on the lesson.** What you'd do differently. What surprised you.
   What you'd recommend to a junior. This shows growth, not just
   competence.

---

*All stories are real. All commits are in the repo's git log. If you want
to verify a claim, run `git show <sha>` on any commit SHA cited.*

*Last updated: 2026-05-10.*
