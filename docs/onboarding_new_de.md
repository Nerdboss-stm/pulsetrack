# Your First Week as a PulseTrack DE

Welcome. You are about to ramp on a streaming health-data lakehouse that
ingests three independent data sources, runs Spark Structured Streaming
through a medallion architecture on Iceberg + Glue, and lands a star-schema
warehouse for analytics + ML.

The pipeline is real. The production runbook in `docs/PRODUCTION_RUNBOOK.md`
is real. The incidents documented in `postmortems/` are real. This is not
a toy.

Your first week is structured to get you from "I just got laptop access"
to "I shipped a small change to production" in 5 working days. We have
done this with three previous hires; the timing is achievable but tight.

**The rule:** ask questions. Slack `#data-platform` for technical, your
buddy for "is this a stupid question" filtering. Both are encouraged.

---

## Pre-arrival checklist (the week before Day 1)

Your buddy will email you with:
- A laptop with admin access
- An invite to the GitHub org with `pulsetrack` repo access
- An AWS SSO account in the dev org with the `pulsetrack-de` role
- A Snowflake user with the `de_ramp` role (read-only on dev)
- A Slack invite to `#data-platform` and `#oncall-de`
- A 1:1 calendar block with your manager for Day 1 morning
- A Notion/wiki link to this document

**Before Day 1, please complete:**

1. **Install the prerequisites** (use Homebrew, this is a Mac-first shop):
   ```bash
   brew install python@3.11 git pre-commit terraform awscli jq
   brew install --cask docker
   brew install --cask snowflake
   ```
2. **Set up SSH for GitHub** if you haven't already:
   `https://docs.github.com/en/authentication/connecting-to-github-with-ssh`
3. **Test your AWS access:** `aws sts get-caller-identity` should return
   your IAM identity in the dev account.
4. **Skim the README at the repo root.** Don't try to understand
   everything — just see the shape.

If any of the above fails, message your buddy by EOD on the day before
your start. We'd rather fix Day 0 problems on Day 0.

---

## Day 1 — Read, clone, install

**Morning (9am – 12pm)**

- 9:00am: 1:1 with manager (30 min, agenda: introduction, expectations,
  questions)
- 9:30am: 1:1 with your buddy (60 min, agenda: tour of Slack, calendar,
  expectations for buddy relationship)
- 10:30am: Read this document fully. It will take ~45 minutes if you
  read carefully.

**Afternoon (1pm – 5pm)**

```bash
# 1. Clone the repo
cd ~
git clone git@github.com:Nerdboss-stm/pulsetrack.git pulsetrack-cm
cd pulsetrack-cm

# 2. Install Python deps + pre-commit
make setup
pre-commit install

# 3. Verify the pre-commit hooks run
git commit --allow-empty -m "test: verify pre-commit works"
# You should see: black, ruff, gitleaks, ... all pass
```

Then read these files, in order. Don't try to understand line-by-line; we'll
do that in week 2. Right now we are building a mental map.

1. `README.md` — what the system is
2. `Architecture.md` — the high-level architecture
3. `DataModel.md` — the bronze / silver / gold data model
4. `docs/PRODUCTION_RUNBOOK.md` — what an operator does
5. `docs/de_career_ladder.md` — where you fit on the ladder

**Done for the day** when you can describe in one paragraph: "the wearable
data path goes from producer → Kafka → bronze on Iceberg → silver via
Spark Structured Streaming → gold star schema → dbt marts → Snowflake."

---

## Day 2 — Walk the wearable pipeline file by file

Today you walk the single most important data path in the system: a wearable
record from sensor to fact table.

Open each file. Read the docstring. Read the imports. Skim the body. Don't
attempt to memorize.

**The journey:**

| Step | File | What it does |
|------|------|--------------|
| 1. Producer | `data_generators/wearable_generator.py` | Generates physiologically-realistic readings, Avro-encodes them, publishes to Kafka |
| 2. Schema | `schemas/sensor_reading.avsc` | The Avro schema; the contract between producer and Kafka |
| 3. Schema helpers | `schemas/registry.py` | Schema Registry client (register, get_serializer) |
| 4. Bronze ingest | `streaming/bronze_ingestion.py` | Consumes Kafka, decodes Avro, writes Iceberg bronze, DLQs failures |
| 5. DLQ | `streaming/dlq.py` | Where parse failures go (Delta + Kafka topic) |
| 6. Silver | `transformations/bronze_to_silver/sensor_silver.py` | Explodes metrics map, applies range checks, watermark dedup, MERGE INTO silver |
| 7. Identity bridge | `transformations/identity_resolution/patient_identity_bridge.py` | 4-phase identity resolution; outputs `patient_key` |
| 8. dim_metric | `transformations/silver_to_gold/dim_metric.py` | Junk dimension for metric attributes |
| 9. dim_patient | `transformations/silver_to_gold/dim_patient.py` | PII-masked patient dimension |
| 10. dim_device | `transformations/silver_to_gold/dim_device.py` | SCD2 device dimension |
| 11. Fact | `transformations/silver_to_gold/fact_vital_reading.py` | Atomic per-reading fact, streaming MERGE |
| 12. GX gate | `data_quality/expectations/silver_sensor_suite.py` | The quality contract for silver |

**Exercise:** at the end of the day, write a 1-page doc to yourself (in
your private notes, not the repo) titled "How a heart rate reading becomes
a row in fact_vital_reading." Include file names and 1 sentence per file.

**Why this matters:** you cannot debug what you don't know exists. The
fastest way to ramp on this codebase is to know where each piece of logic
lives.

---

## Day 3 — Run the dbt project against local fixtures

Today you actually run code.

The dbt project is in `dbt_project/`. It uses DuckDB locally and Snowflake
in prod. You'll use DuckDB.

```bash
cd dbt_project

# 1. Install dbt deps
dbt deps --profiles-dir .

# 2. Run dbt against the local DuckDB profile
dbt seed --profiles-dir . --target local
dbt run --profiles-dir . --target local
dbt test --profiles-dir . --target local
```

What just happened?
- `dbt seed` loaded CSV fixtures from `dbt_project/seeds/` into DuckDB
- `dbt run` built every staging, intermediate, and mart model in the right
  order (the DAG)
- `dbt test` ran the schema tests and generic tests defined in `_sources.yml`
  and `_core.yml`

**Read these:**

1. `dbt_project/models/_sources.yml` — the source contracts
2. `dbt_project/models/staging/stg_sensor_readings.sql` — your first staging
   model. It's just a SELECT from a source with some renaming. That's it.
3. `dbt_project/models/marts/core/fact_vital_reading.sql` — the equivalent
   gold fact in dbt
4. `dbt_project/macros/commons/` — the WHOOP-style commons macros
   (`sha256_key`, `safe_divide`, etc.)

**Exercise:** explore the rendered SQL by opening `dbt_project/target/run/`.
The compiled SQL is real SQL you can paste into DuckDB and run.

---

## Day 4 — Shadow on-call

Today you read three runbooks and walk through a hypothetical incident with
your buddy.

**Read in order:**

1. `runbooks/kafka_consumer_lag.md` — the most common page
2. `runbooks/s3_503_throttling.md` — the highest-stakes page (data loss risk
   if mishandled)
3. `runbooks/emr_step_failure.md` — the most-likely-to-be-flaky page

Each runbook follows the same shape:
- Severity ladder (SEV3 / SEV2 / SEV1)
- TL;DR (30-second triage)
- Symptoms (what triggered the page)
- Diagnosis (commands to run first)
- Remediation (what to do)
- Postmortem template

**Hypothetical incident with your buddy:**

> "It's 2am. PagerDuty wakes you up: `pulsetrack-dev-bronze-stream-lag`
> ACTIVE. Walk me through what you do."

Don't pretend; actually open the runbook on your laptop and read aloud.
Your buddy will play the role of "the senior engineer you'd call for help."
This is a practice fire drill.

**Read this list of postmortems** (the 4 most-cited):

- The silver streaming cold-start hang (referenced in
  `pulsetrack-study/PROMPT_4_REPORT.md` § 3.12)
- The em-dash that brought down EMR (commit `5e08bb0`)
- The DynamoDB reserved keyword (commit `8028edb`)
- The leaked WHOOP credential (referenced in
  `docs/scale_test_runbook.md` § "T-25 minutes")

You are NOT on-call yet. You will not be alone on-call for at least 3
months. Today is shadow.

---

## Day 5 — Ship something tiny

Today you ship a real PR.

**Pick one of these starter tasks** (all are real, all are small, all have
been pre-vetted as good first issues):

### Option A — Add a column to `dim_metric`

The `dim_metric` table in `transformations/silver_to_gold/dim_metric.py`
is a junk dimension for metric attributes. It's missing a
`measurement_category` column that groups metrics into "vitals",
"activity", "sleep", "respiration".

**Steps:**
1. Branch off `main`: `git checkout -b yourname/dim-metric-measurement-category`
2. Read `transformations/silver_to_gold/dim_metric.py` end-to-end
3. Read `tests/test_dim_metric.py` end-to-end
4. Add the new column. The mapping is your call (use your judgment for what
   each metric belongs to).
5. Update the test
6. Update `dbt_project/models/marts/core/dim_metric.sql` to match
7. Run `make test` and verify the existing 82 tests still pass
8. Commit and push, open a PR
9. Tag your buddy and one reviewer from `CODEOWNERS`

### Option B — Extend a dbt test

The dbt project has schema tests, but `fact_vital_reading.sql` doesn't have
a test that `value` is non-negative for heart_rate metrics (HR should never
be < 30 for living humans; <30 is a quality issue).

**Steps:**
1. Branch off `main`
2. Read `dbt_project/models/marts/core/_core.yml`
3. Add a `dbt_utils.expression_is_true` test that for `metric_name = 'heart_rate'`,
   `value >= 30`
4. Run `dbt test --profiles-dir . --target local --select fact_vital_reading`
5. Commit and push, open a PR

### Option C — Add a quality-gate metric

The Great Expectations suite in `data_quality/expectations/silver_sensor_suite.py`
has range checks for HR and SpO2, but no check for `spo2 <= 100`. SpO2 above
100 is physically impossible.

**Steps:**
1. Branch off `main`
2. Read `data_quality/expectations/silver_sensor_suite.py`
3. Add the expectation
4. Run `make quality` and verify the suite still runs clean against the
   existing test data

**Whichever you pick, the goal is to:**
- Read enough code to know what you're changing
- Make a focused, minimal change
- Get the tests green
- Submit a clean PR

Your buddy will review. Don't be surprised if they ask you to
make small changes. That's the system working.

---

## Common errors and their fixes

### `make setup` fails with "JAVA_HOME not set"

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

Add to your `~/.zshrc`. Spark needs Java 17.

### `pip install` fails on `confluent-kafka`

You need librdkafka. On Mac:
```bash
brew install librdkafka
export CPPFLAGS="-I$(brew --prefix librdkafka)/include"
export LDFLAGS="-L$(brew --prefix librdkafka)/lib"
pip install confluent-kafka
```

### Pre-commit's gitleaks hook keeps failing

```bash
# Check what it found
pre-commit run gitleaks --all-files
```

If it's a false positive (test fixture with a fake-looking token), add an
exclusion to `.gitleaks.toml`. If it's real, rotate the credential
immediately and DO NOT commit.

### `dbt run` fails with "No matching profiles found"

The profiles directory is `./` (dbt_project), not `~/.dbt`. Use:
```bash
dbt run --profiles-dir .
```

### Spark UI shows "Source: Iceberg, OffsetMin=0, OffsetMax=null"

This means Spark can't find the Iceberg source. Usually:
- Iceberg JAR is missing from the classpath
- The catalog config in `spark.sql.catalog.glue_iceberg.*` is wrong
- The bronze table doesn't exist yet (run `make stream-bronze` first)

### Test fails with "Path '/tmp/pulsetrack-lakehouse' does not exist"

The tests in `tests/test_gold.py` are skipped unless you've run a full
pipeline locally. This is expected. From `make test`:
```
SKIPPED [33] test_gold.py::test_xxx — populated by pipeline run
```

### GX schema mismatch on silver

The most common cause: you added a column to the upstream transform but
didn't update the `prepare_for_validation()` function in the matching GX
suite. The fix:
```python
# In data_quality/expectations/silver_sensor_suite.py
def prepare_for_validation(df):
    return df.select("...", "your_new_column")
```

### dbt `ref` vs `source` confusion

- `{{ ref('stg_sensor_readings') }}` — references another dbt model in
  the same project. Use this for everything in `models/`.
- `{{ source('iceberg', 'sensor_readings') }}` — references an upstream
  table managed outside dbt (e.g., the silver Iceberg table).
- Use `source` only for the staging layer's entry points.

---

## Who to ask about what

We don't have a strict CODEOWNERS yet because this is a small team, but
here's the de-facto ownership:

| Topic | Primary | Backup |
|-------|---------|--------|
| Spark streaming | Senior DE | Staff |
| Iceberg / Delta | Senior DE | Staff |
| Glacierbase migrations | Staff DE | Senior |
| Terraform / infra | Staff DE | Senior |
| dbt project | DE II | Senior |
| Quality gates / GX | DE II | Senior |
| WHOOP API connector | DE II | Senior |
| Identity bridge | Senior DE | Staff |
| Observability / Prometheus | DE II | Senior |
| Snowflake views | Senior DE | DE II |
| Prefect orchestration | DE II | Senior |

Ask in `#data-platform` first; tag the primary if no one responds in 30 min.

---

## How to read the Spark UI

The Spark UI is at `http://<EMR_MASTER_DNS>:18080` (history server) or
`http://<EMR_MASTER_DNS>:4040` (live, while a query runs).

The UI has 7 tabs that matter:

### Jobs
A list of Spark jobs (one per action). For Structured Streaming, the
"job" you care about is the `foreachBatch` execution.
- **Duration:** how long the batch took. If this grows over time, you have
  a problem.
- **Stages:** number of stages in the batch. More stages = more shuffles.

### Stages
Stage details. For each stage:
- **Tasks:** parallelism within the stage. If you see "Tasks: 1", you
  have a shuffle skew problem.
- **Shuffle Read/Write:** how much data moves between executors. Big
  shuffle = expensive.
- **Duration:** task-level duration. Look for outliers (one task taking 10x
  the others = skew).

### Storage
What's cached. For streaming, usually empty. If you cache a DataFrame in
a transform, it shows here.

### Environment
Spark conf. Useful when you're not sure if a setting (e.g., `spark.sql.catalog.glue_iceberg.warehouse`)
is actually applied.

### Executors
Per-executor stats. Look at:
- **Cores:** vs. configured
- **Memory:** RSS vs. configured heap
- **Task Time:** unbalanced executors are a smell

### SQL / DataFrame
For each Spark SQL query (including foreachBatch's MERGE INTO), shows:
- The physical plan
- Time per node
- Whether codegen was used
- **The "Whole Stage CodeGen" boxes are what you want to see.** If a
  predicate is OUTSIDE a codegen box, that's a hot path that can be
  optimized.

### Structured Streaming
Streaming-specific tab. Per query:
- **Input rate:** records/sec from source (Kafka)
- **Processing rate:** records/sec processed
- **Batch duration:** wall-clock time per batch
- **Operation duration:** breakdown (addBatch, walCommit, etc.)
- **Latency:** trigger-to-completion

If `processing_rate < input_rate`, you have lag and the query is falling
behind. This is the page condition.

**Practice exercise:** while a streaming query is running, open the
Structured Streaming tab. Watch the input rate vs processing rate. Add
artificial backpressure by sleeping in a transform. Watch the gap grow.
Remove the sleep. Watch it close.

---

## End of week 1

By Friday afternoon, you should be able to say:
- "I can clone, install, test, and ship a one-line PR"
- "I know where every layer of the pipeline lives in the repo"
- "I can run the dbt project locally"
- "I can read a runbook and walk a fellow engineer through what I'd do"
- "I shipped a small PR and it merged"

If any of those is "not yet", talk to your buddy about extending the ramp
one more week. There is no penalty. Ramp is a one-time investment; rushing
it hurts everyone.

**Week 2 starts with:** owning a pipeline area. Your buddy will assign you
one. From here it's real work.

Welcome.

---

*Maintained by the senior DE team. Last revised: 2026-05-10. If anything
in this doc is out of date, file a PR — onboarding docs are everyone's
problem.*
