# Your First Pipeline — A Junior DE Tutorial

This tutorial walks you through building the simplest possible source →
bronze → silver → gold pipeline as a learning exercise. By the end, you
should understand every layer of PulseTrack well enough to debug it.

**Audience:** DE I or DE II who has finished the
`docs/onboarding_new_de.md` week-1 ramp and is ready to write code that
matters.

**Estimated time:** 1.5 days (one to read + run, half a day to modify).

**Prerequisites:**
- `make setup` succeeded
- `make test` passes (82 tests green)
- Docker is running (`docker-compose up -d`)

---

## The pipeline you're going to build

We are not going to build a new pipeline from scratch — that would be
pretending. Instead, we are going to walk the existing wearable
pipeline as if you were the DE who designed it, step by step, and at the
end you'll make one targeted modification.

The path:

```
data_generators/wearable_generator.py        (the producer)
    │  Avro-encoded records
    ▼
Kafka topic: sensor_readings                 (the bus)
    │
    ▼
streaming/bronze_ingestion.py                (Kafka → bronze)
    │  Iceberg / Delta
    ▼
pulsetrack_bronze_dev.sensor_readings        (the bronze table)
    │
    ▼
transformations/bronze_to_silver/sensor_silver.py    (bronze → silver)
    │  watermark + dedup + MERGE INTO
    ▼
pulsetrack_silver_dev.sensor_readings        (the silver table)
    │
    ▼
transformations/silver_to_gold/fact_vital_reading.py (silver → gold fact)
    │  streaming MERGE
    ▼
pulsetrack_gold_dev.fact_vital_reading       (the gold fact)
    │
    ▼
dbt_project/models/marts/core/fact_vital_reading.sql (dbt mart)
    │
    ▼
Snowflake view: vw_vital_trends              (the consumer)
```

---

## Step 1 — The producer

**File:** `data_generators/wearable_generator.py`

**What it does:**
- Loops over a set of synthetic patients
- For each patient, calls the physiological vitals model
  (`data_generators/vitals_model.py`) to get an HR, SpO2, HRV, etc.
- Avro-encodes the record using the schema in `schemas/sensor_reading.avsc`
- Publishes to the Kafka topic `sensor_readings`

**Why it's structured this way:**
- **Avro over JSON:** schema is enforced at the producer. If you try to
  publish a record with a wrong type, you'll get an error before it hits
  Kafka. JSON would let the error propagate downstream.
- **Schema Registry:** the schema is registered centrally so consumers
  can discover it without coordination. The wire prefix
  (1 magic byte + 4 schema-id bytes) lets a consumer figure out which
  schema version to deserialize against.
- **Continuous mode:** `--mode continuous` produces forever at a steady
  rate. This is what drives the streaming pipeline. The one-shot mode
  is only for backfills and tests.

**The contract:**
- Topic: `sensor_readings` (Kafka)
- Format: Avro, Schema Registry id-prefixed
- Schema: `schemas/sensor_reading.avsc`
- Key: `device_id` (so all readings from one device hash to the same partition)

**Run it locally:**

```bash
make generate-vitals
# Or directly:
python -m data_generators.wearable_generator --mode continuous --rate 10
```

**Try it yourself:**
- Stop the producer, change the rate to 100/s, restart. Watch your Kafka
  console fill faster.
- Open `localhost:8081/subjects` in your browser. You should see
  `sensor_readings-value` registered. Click in to see the schema.

---

## Step 2 — Kafka + Schema Registry

**Files:**
- `docker-compose.yml` — defines the local Kafka + ZK + SR stack
- `schemas/registry.py` — Python helpers for SR
- `schemas/sensor_reading.avsc` — the wire schema

**What it does:**
- Kafka stores the records, partitioned by `device_id`
- Schema Registry serves the Avro schema by id

**Why it's structured this way:**
- **Kafka decouples the producer rate from the consumer rate.** If silver
  goes down for an hour, the producer keeps producing; silver catches up
  when it comes back. Without Kafka, you'd block the producer or drop data.
- **Partition by device_id:** ordering matters per-device but not across
  devices. So we get parallelism (across devices) without losing per-device
  ordering (e.g., the morning HR comes before the noon HR for one device).

**The contract:**
- At-least-once delivery (Kafka can replay)
- Per-partition ordering preserved
- Schemas are forward + backward compatible (Schema Registry enforces this)

**Verify it's running:**
```bash
# Topic list
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Tail messages
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic sensor_readings --max-messages 1 --from-beginning
```

You'll see bytes (because Avro), not JSON. That's expected.

---

## Step 3 — Bronze ingestion

**File:** `streaming/bronze_ingestion.py`

**What it does:**
- `readStream.format("kafka")` — Spark Structured Streaming source
- For each row: decode the Avro wire prefix, deserialize against the
  schema, project `(kafka_offset, decoded.*, is_parseable, source_type, rid)`
- Write to Iceberg bronze table via `writeStream.format("iceberg").outputMode("append")`
- On parse failure: route to DLQ (`streaming/dlq.py` — Delta + Kafka topic)

**Why it's structured this way:**
- **PERMISSIVE Avro decode:** a bad record doesn't kill the batch; it gets
  `is_parseable=false` and lands in DLQ. The pipeline keeps going.
  Alternative: `FAIL_ON_ERROR` would stop the whole batch on one bad record.
  We chose PERMISSIVE because at scale, bad records are inevitable; you
  optimize for liveness.
- **`rid` column:** reversed device_id. This is the primary partition for
  S3 to distribute writes across prefix slots. See
  `lakehouse/partition_strategy.py` and the WHOOP S3 partitioning blog post
  for the deep dive.
- **append-only:** bronze is immutable. We never UPDATE or DELETE bronze
  rows. All cleansing happens at the silver step. This is the "data is
  the contract" principle for raw ingestion.

**The contract:**
- Input: Kafka topic `sensor_readings`
- Output: Iceberg table `pulsetrack_bronze_dev.sensor_readings`
- Schema: see `_decode_envelope` in `bronze_ingestion.py`
- Guarantees: at-least-once (Kafka offsets checkpointed); parse failures DLQ'd

**Run it locally:**
```bash
make stream-bronze
```

This will run forever (until you Ctrl-C). The streaming query is named
`bronze_sensor` (look in `streaming/bronze_ingestion.py`).

**Verify it's working:**
```bash
# In a fresh terminal, count records
python query_ehr_bronze.py
# Or directly:
spark-sql -e "SELECT COUNT(*) FROM pulsetrack_bronze_dev.sensor_readings"
```

---

## Step 4 — Silver

**File:** `transformations/bronze_to_silver/sensor_silver.py`

**What it does:**
- `readStream.format("iceberg")` from bronze
- Explode the metrics map: one bronze row with `{hr:75, spo2:97, hrv:45}`
  becomes three silver rows, one per metric
- Apply per-metric range checks: `is_valid = (hr BETWEEN 30 AND 220)`,
  etc. Invalid rows quarantine.
- Apply watermark: `withWatermark("event_timestamp", "10 minutes")`
- Dedup: `dropDuplicatesWithinWatermark(["reading_id", "metric_name"])`
- `foreachBatch`: for each micro-batch, run `MERGE INTO silver ... WHEN NOT
  MATCHED THEN INSERT WHEN MATCHED THEN UPDATE`

**Why it's structured this way:**
- **Explode metrics to one row per (reading, metric):** silver is the
  business-rule layer. Range checks are per-metric. Quarantine is
  per-metric. Aggregations downstream are per-metric. Exploding here
  makes everything else easy.
- **Watermark for state bound:** Kafka can replay. Watermark
  + dropDuplicatesWithinWatermark gives bounded-state deduplication.
  Without a watermark, the dedup state grows unbounded.
- **MERGE INTO via foreachBatch:** Iceberg's append-only writer would
  insert duplicates if Kafka replayed. MERGE is idempotent on the
  grain key (reading_id, metric_name).
- **GX gate inside foreachBatch:** if the silver suite fails, the
  MERGE doesn't happen and the batch retries.

**The contract:**
- Input: bronze Iceberg table
- Output: silver Iceberg table `pulsetrack_silver_dev.sensor_readings`,
  grain = (reading_id, metric_name)
- Watermark: 10 minutes
- Guarantees: exactly-once on grain key within watermark window, at-least-once
  outside

**Run it locally:**
```bash
make stream-silver
```

---

## Step 5 — Gold (fact)

**File:** `transformations/silver_to_gold/fact_vital_reading.py`

**What it does:**
- `readStream.format("iceberg").option("streaming-skip-overwrite-snapshots", "true")`
  from silver
- Join with `dim_metric` and `patient_identity_bridge`
- `MERGE INTO fact_vital_reading` on grain key (patient_key, device_key,
  date_key, time_key, metric_key)

**Why it's structured this way:**
- **`streaming-skip-overwrite-snapshots=true`:** silver's MERGE creates
  `overwrite` snapshots (Iceberg classifies MERGE as overwrite even if
  no rows were updated). Iceberg's streaming source rejects overwrite
  snapshots by default. We opt in to skipping them. The tradeoff: if
  silver UPDATEs an existing row, gold won't see the update — only new
  rows arrive. For our use case (fact_vital_reading is append-mostly),
  this is acceptable. See `pulsetrack-study/PROMPT_4_REPORT.md` § 4.4.
- **Join with dim_patient via patient_identity_bridge:** the identity
  bridge resolves `account_id` to `patient_key`. Without this join,
  the fact has no patient. The bridge is materialized as a silver
  table (see step 6).
- **MERGE not INSERT:** lets us re-run the gold transform without
  duplicate facts.

**The contract:**
- Input: silver, dim_metric, dim_patient (via patient_identity_bridge)
- Output: `pulsetrack_gold_dev.fact_vital_reading`
- Grain: (patient_key, device_key, date_key, time_key, metric_key)
- Guarantees: idempotent on grain

---

## Step 6 — dbt mart

**File:** `dbt_project/models/marts/core/fact_vital_reading.sql`

**What it does:**
- A pure SQL transformation of `{{ source('iceberg', 'fact_vital_reading') }}`
  (the gold fact)
- Adds derived columns: `is_anomaly`, `hour_of_day`, etc.
- Adds Commons macros: `{{ sha256_key(['account_id','metric_name']) }}`
- Tagged for selection: `{{ config(tags=['core','daily','vital']) }}`

**Why it's structured this way:**
- **dbt for analytical transformations:** Spark is the right tool for
  ingestion + heavy transformations. dbt is the right tool for the
  "last mile" analytical work where SQL is more legible and the team is
  often analysts, not engineers. See commit `23619a0`.
- **Macros in commons:** WHOOP's pattern. Shared logic (SHA-256 key
  generation, SCD2 merge, safe_divide) lives in one place. Models
  reference them. Consistency across 100+ models.
- **Snapshot SCD2:** dbt's `snapshot` materialization handles the
  pattern. We use it for `dim_medication` in the dbt project.

**The contract:**
- Input: gold Iceberg table (via Snowflake external table)
- Output: dbt model materialized as a Snowflake view (or table, depending
  on config)
- Tests: in `_core.yml` (not_null, accepted_values, expression_is_true)

**Run it locally:**
```bash
cd dbt_project
dbt run --profiles-dir . --target local --select fact_vital_reading
```

---

## Step 7 — Snowflake view

**File:** `snowflake/models/vw_vital_trends.sql`

**What it does:**
- Reads the dbt mart `fact_vital_reading`
- Aggregates by patient + week, computing trend deltas
- Exposed to BI tools (Tableau, Mode, Hex)

**Why it's structured this way:**
- **Views in Snowflake, not dbt:** the SQL is analyst-owned, evolving
  weekly. View redefinition is cheap; we don't need dbt's full
  materialization machinery for a SELECT.
- **Aggregation here, not at gold:** gold is grain = per-reading. The
  consumer wants weekly. Doing the aggregation at the view layer keeps
  the gold table general-purpose.

---

## Why we made it this way (the architecture summary)

Read this section after you've walked steps 1-7. It will make more sense.

**The medallion pattern** (bronze → silver → gold) gives us three benefits:
1. **Decouple semantics.** Bronze is "raw, immutable, debuggable." Silver
   is "cleansed, business rules applied." Gold is "analytical, star schema."
   Each layer can evolve independently.
2. **Debuggability.** When a number looks wrong, you can walk back through
   the layers. Silver vs. bronze: cleansing issue. Gold vs. silver: model
   issue. Snowflake view vs. gold: aggregation issue.
3. **Reusability.** Multiple gold facts can read from the same silver. We
   have `fact_vital_reading` (per-reading) AND `fact_vital_daily_summary`
   (per-day) both reading from `silver/sensor_readings`.

**Streaming vs batch in the same engine** (Kappa) gives us:
- One codebase. The transform logic in `sensor_silver.py` has both a
  `run_streaming()` and `run_batch()` entry. They share the transform.
- Replayability. The batch mode is just streaming with `trigger=once`.
- Operational simplicity. One framework, one set of monitoring, one set
  of dependencies.

**Iceberg over Delta** (or Hive):
- Catalog-independent (we use Glue; you could swap to Hive without
  rewriting data)
- Partition evolution in-place
- Time travel
- Atomic snapshots
- See `pulsetrack-study/PROMPT_4_REPORT.md` § 2.1 for the deep dive

---

## Try it yourself — modifications

Now that you've walked the pipeline, modify it. Pick one:

### Modification A — Add a metric

The producer emits `hr`, `spo2`, `hrv`, `skin_temp`, `bp_systolic`,
`bp_diastolic`, `steps`, `sleep_stage`. Add a new metric: `respiration_rate`.

Steps:
1. Add the field to `data_generators/vitals_model.py` (use a sensible
   physiological model — adult resting respiration is 12-20/min)
2. Add the field to `schemas/sensor_reading.avsc`
3. Register the new schema with Schema Registry (the producer
   `make generate-vitals` will do this on next start)
4. Update `bronze_ingestion.py` if you need explicit schema projection
   (it auto-derives, so usually no change needed)
5. Update `sensor_silver.py`'s metric list and range check
6. Update `dim_metric.py` to add a `respiration_rate` row with attributes
7. Update `data_quality/expectations/silver_sensor_suite.py` for the new
   range
8. Run end-to-end. Verify rows land in silver with `metric_name='respiration_rate'`
   and pass the GX gate.

### Modification B — Add a quality test

Pick a silver rule that's currently not enforced. Example: "no two readings
for the same device within 100ms" (would indicate a duplicate / replay).

Steps:
1. Write a `pytest` test in `tests/test_sensor_silver.py` that asserts
   the rule
2. Run it; it should fail (because the rule isn't enforced)
3. Update `sensor_silver.py` to enforce the rule (add a filter or quarantine
   route)
4. Run the test again; it should pass

### Modification C — Add a gold fact

Add a new fact: `fact_vital_anomaly`. Grain: (patient_key, date_key,
metric_key). Captures one row per anomaly event (a reading outside the
physiological range).

Steps:
1. Create `transformations/silver_to_gold/fact_vital_anomaly.py`
2. Read from silver, filter `is_valid=false`, MERGE into the new fact
3. Create the schema migration: `migrations/versions/V006__add_fact_vital_anomaly.sql`
4. Run `python migrations/cli.py --catalog glue_iceberg run`
5. Test it: `pytest tests/test_fact_vital_anomaly.py`

---

## How to add a test

PulseTrack uses pytest with Spark fixtures.

**Pattern for a transform test:**

```python
# tests/test_sensor_silver.py
def test_silver_explodes_metrics(spark, tmp_lakehouse):
    # Arrange: build a bronze DataFrame
    bronze_df = spark.createDataFrame([
        Row(
            reading_id="r1",
            device_id="d1",
            event_timestamp=datetime(2026, 5, 10, 12, 0, 0),
            decoded=Row(metrics={"hr": 75.0, "spo2": 97.0})
        )
    ])

    # Act: run the transform
    silver_df = sensor_silver.transform_bronze_to_silver(bronze_df)

    # Assert: two rows, one per metric
    assert silver_df.count() == 2
    assert {r.metric_name for r in silver_df.collect()} == {"hr", "spo2"}
```

The `spark` and `tmp_lakehouse` fixtures are in `tests/conftest.py`. The
`spark` fixture is session-scoped (one SparkSession per test run, fast).
The `tmp_lakehouse` fixture is per-test (clean state).

**Run it:**
```bash
make test
# Or just one file
pytest tests/test_sensor_silver.py -v
```

**Coverage:**
```bash
pytest --cov=transformations --cov-report=term-missing
```

We have an `--cov-fail-under=70` in CI, so coverage must stay ≥ 70%.

---

## How to run pre-commit

Pre-commit runs automatically on `git commit`. To run it manually:

```bash
pre-commit run --all-files
```

The hooks in `.pre-commit-config.yaml`:
- **black** — Python formatter. No options; one style.
- **ruff** — Python linter. Configured in `pyproject.toml`.
- **gitleaks** — secret scanner. Configured in `.gitleaks.toml`. Will
  block commits that look like they contain credentials.
- **end-of-file-fixer** — every file ends with a newline.
- **trailing-whitespace** — no trailing whitespace.

If a hook fails, the commit aborts. Fix the issue and `git commit` again.

**For a real secret leak:** if gitleaks flags a real secret, do NOT just
add it to the exclusion. Rotate the credential immediately. See
`postmortems/2026-05-09_whoop_secret_in_git.md` for the precedent.

---

## When your CI fails, here's the order of operations

You pushed; the GitHub Action turned red. Don't panic. Walk down this list:

1. **Reproduce locally first.** CI runs `make lint test` plus security
   scans. If you run those locally and they pass, CI should pass — if not,
   it's an env difference and you should escalate.
2. **Check black + ruff:**
   ```bash
   black .
   ruff check . --fix
   ```
   This is the most common CI failure. If you forgot pre-commit, you
   missed this.
3. **Check gitleaks:**
   ```bash
   pre-commit run gitleaks --all-files
   ```
4. **Check the workflow logs.** Click the red X in your PR. The failing
   step will be highlighted. Read the output, not the summary.
5. **Common gotchas:**
   - Tests pass locally but fail in CI: probably a timezone or filesystem
     difference. Look for hardcoded paths or `datetime.now()` in your test.
   - Coverage drops below 70%: you added code but not tests. Add tests
     OR justify the gap with a comment.
   - Security scan fails: read the bandit output. If it's a false positive,
     add a `# nosec` comment with a justification.
6. **If you can't reproduce:** ask in `#data-platform`. Don't just
   keep pushing commits to try to fix CI; you'll waste a CI run each time.

---

## Closing

You have now walked the entire wearable pipeline, run it locally, and (if
you completed a modification) shipped a change to it. You understand:
- Why each layer exists
- The contracts between layers
- How to add a feature
- How to debug a failure
- How to run the test + lint loop

This is the foundation. Everything else — Iceberg internals, Glacierbase
migrations, identity resolution, Snowflake consumers — builds on this.

The senior engineers in your team have spent years getting these patterns
right. Don't be afraid to ask "why is this structured this way?"; the
answer is almost always interesting.

Next up: `docs/onboarding_new_de.md` Day 4 had you read three runbooks.
By month 2 you should be able to write a runbook for any pipeline you own.
That's the next milestone.

---

*Last updated: 2026-05-10. Pairs with `docs/onboarding_new_de.md`.*
