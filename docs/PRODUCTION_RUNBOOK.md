# PulseTrack — Production Runbook (Cloud / EMR + MSK + Iceberg)

This runbook is the operator's guide for the cloud deployment. If you're new
to the system, read it linearly. If you're paged, jump to the symptom in
"Common failures" and follow the remediation.

## 1. Architecture at a glance

```
producer (Python, OAUTHBEARER)  ──▶  MSK Serverless (SASL_SSL/IAM)
                                      │ topic: sensor_readings (Avro)
                                      ▼
Spark Structured Streaming on EMR    ──▶  Bronze Iceberg (Glue Catalog)
streaming/bronze_ingestion.py             pulsetrack_bronze_dev.sensor_readings
foreachBatch + checkpoint                 hidden partition: days(ingestion_timestamp)
                                      │
                                      ▼ readStream.format("iceberg")
                                          (or readStream Delta for legacy path)
Spark Structured Streaming on EMR    ──▶  Silver Iceberg (Glue Catalog)
transformations/bronze_to_silver/         pulsetrack_silver_dev.sensor_readings (+ EHR + bridge)
sensor_silver.run_streaming               foreachBatch MERGE INTO + watermark dedup
watermark + dropDuplicatesWithinWatermark hidden partition: days(event_timestamp), bucket(16, account)
                                          state store on S3 checkpoint
                                      │
                                      ▼  see § "Iceberg streaming gold limitation"
Spark batch (or streaming-with-caveat) ──▶  Gold Iceberg (Glue Catalog)
transformations/silver_to_gold/           pulsetrack_gold_dev.* (3 facts + 9 dims)
fact_vital_daily_summary, fact_vital_reading,
fact_lab_result, dim_*

Glacierbase migration framework      ──▶  schema_migrations Iceberg ledger
migrations/cli.py {validate|run|...}      pulsetrack_gold_dev.schema_migrations
```

Twenty production tables register in Glue Catalog (1 bronze + 6 silver + 13
gold) plus the schema_migrations ledger.

## 2. Daily operations

### 2.1 Running the pipeline end-to-end

The `--format iceberg` flag is the production path. The `--format delta`
flag is preserved for the local dev loop and for callers that haven't
migrated their downstream consumers.

```bash
# 1. Producer (locally, with PT_AWS_ENV=dev pulsetrack profile)
python3 scripts/produce_sensor_records.py \
    --brokers boot-XXXX.kafka-serverless.us-east-1.amazonaws.com:9098 \
    --topic   sensor_readings \
    --count   10000

# 2. Bronze (streaming, on EMR — exits when caught up via available_now)
/usr/lib/spark/bin/spark-submit \
    --master yarn --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,\
               org.apache.spark:spark-avro_2.12:3.5.6,\
               software.amazon.msk:aws-msk-iam-auth:2.2.0 \
    --jars file:///usr/share/aws/delta/lib/delta-spark_2.12-3.3.0-amzn-0.jar,\
           file:///usr/share/aws/delta/lib/delta-storage-3.3.0-amzn-0.jar \
    streaming/bronze_ingestion.py --trigger processing --format iceberg

# 3. Silver (streaming, on EMR — continuous)
/usr/lib/spark/bin/spark-submit ... --jars ... \
    transformations/bronze_to_silver/sensor_silver.py \
    --mode streaming --trigger processing --format iceberg

# 4. EHR silver (batch — drives off local JSON, not Kafka)
/usr/lib/spark/bin/spark-submit ... --jars ... \
    transformations/bronze_to_silver/ehr_silver.py --format iceberg

# 5. Identity bridge
/usr/lib/spark/bin/spark-submit ... --jars ... \
    transformations/identity_resolution/patient_identity_bridge.py --format iceberg

# 6. Gold dims (parents first, then children)
/usr/lib/spark/bin/spark-submit ... --jars ... \
    transformations/silver_to_gold/dim_date.py --format iceberg
# repeat for: dim_time, dim_condition_category, dim_drug_class
# then:       dim_condition, dim_medication
# then:       dim_metric, dim_device, dim_patient

# 7. Gold facts (STREAMING on EMR 7.13.0 / Iceberg 1.10 — full MERGE)
/usr/lib/spark/bin/spark-submit ... --jars ... \
    transformations/silver_to_gold/fact_vital_daily_summary.py \
    --mode streaming --trigger processing --format iceberg
# repeat for: fact_vital_reading, fact_lab_result
```

Required env vars on EMR for every transform (the spark-submit invocation
should `export` these):

```
PT_LAKEHOUSE_BASE=s3://pulsetrack-lakehouse-<env>-<suffix>
PT_ENVIRONMENT=cloud                    # local|cloud — selects Spark config path
PT_AWS_ENV=dev                          # dev|staging|prod — Glue DB suffix
PT_SPARK_MASTER=yarn
PT_GLUE_ICEBERG_WAREHOUSE=s3://pulsetrack-lakehouse-<env>-<suffix>/iceberg/warehouse
PT_KAFKA_BOOTSTRAP=<msk bootstrap>
PT_KAFKA_SECURITY_PROTOCOL=SASL_SSL     # SASL_SSL|PLAINTEXT
PT_KAFKA_TOPIC_SENSOR=sensor_readings
```

### 2.2 Verifying tables landed in Glue

```bash
for db in pulsetrack_bronze_dev pulsetrack_silver_dev pulsetrack_gold_dev; do
  aws glue get-tables --database-name $db \
      --query 'TableList[].{Name:Name,Format:Parameters.table_type}' \
      --output table
done
```

Expected steady-state: 1 + 6 + 13 = 20 production tables, all `Format=ICEBERG`.

### 2.3 Querying gold tables

```python
spark.read.table("glue_iceberg.pulsetrack_gold_dev.fact_vital_daily_summary").show()
spark.read.table(
    "glue_iceberg.pulsetrack_gold_dev.fact_vital_daily_summary"
).join(
    spark.read.table("glue_iceberg.pulsetrack_gold_dev.dim_metric"),
    "metric_key",
).show()
```

## 3. Schema evolution (Glacierbase migrations)

### 3.0 What Glacierbase is — and what it deliberately is not

Glacierbase manages **declarative, deterministic schemas** for our
high-value analytical and model-training datasets. The framework is
intentionally scoped:

**In scope (Glacierbase-managed)**

| Layer | Scope | Why |
|---|---|---|
| Silver Iceberg | `pulsetrack_silver_dev.*` (sensor_silver, EHR silver, identity bridge outputs) | Curated, business-rule-cleaned tables consumed by downstream gold + analytics. Schema must be reviewed, hashed, peer-reviewed. |
| Gold Iceberg | `pulsetrack_gold_dev.*` (3 facts + 9 dims) | The "published API" of the lakehouse. Star-schema tables used by ML training and BI. Schema changes must be auditable. |

**Out of scope (carve-out — managed outside Glacierbase)**

| Path | Why it's carved out |
|---|---|
| **Bronze append-only ingestion** (`pulsetrack_bronze_dev.sensor_readings`) | Bronze uses Iceberg's [schema-merge-on-write](https://iceberg.apache.org/docs/latest/spark-writes/#writing-with-sql) (`mergeSchema=true`) to absorb controlled producer-side schema drift. Forcing every Avro field rename through a migration would gate the streaming pipeline on a PR — wrong tradeoff for raw-event data with downstream cleansing. |
| **CDC streams from external OLTP** (none today; provisioned for future Postgres / EHR sources) | CDC tools (Debezium / DMS / Glue streaming) re-emit upstream schema as-is. Trying to lock that with hashed SQL fights the source-of-truth. We let CDC tables drift and do schema enforcement at the silver step where business rules apply. |
| **DLQ tables** (`s3://lakehouse/dlq/...`) | Operationally short-lived; format chosen to match dead-letter records' raw shape. |
| **Quarantine paths** (`s3://lakehouse/quarantine/...`) | Same reasoning — informative, not contractual. |
| **Checkpoint state** (`s3://lakehouse/checkpoints/...`) | Spark-managed; not a "table" in the warehouse sense. |

**Why this matters operationally:** if you see schema drift in
`pulsetrack_bronze_dev.sensor_readings`, it is *not* a Glacierbase
violation — it is the contract. Schema enforcement happens at the
bronze→silver boundary in `transformations/bronze_to_silver/sensor_silver.py`,
where the explicit `select(...)` projection drops or fails on unknown
fields per business rule. Silver and gold are the layers where
"add a column" must go through `migrations/cli.py create`.

This carve-out matches WHOOP's published Glacierbase scope: the
framework focuses on the high-value analytical and model-training
datasets, deliberately leaving raw ingestion and CDC alone.

### 3.1 Authoring a new migration

```bash
# Scaffolds versions/V005__<name>.sql + V005__<name>__down.sql
# with WHOOP-style headers pre-filled.
python3 migrations/cli.py --catalog glue_iceberg create \
    --name "add_glucose_column" \
    --description "Add fasting_glucose column to fact_vital_reading" \
    --author "PulseTrack Data Platform"
```

The generated file uses WHOOP Glacierbase header conventions:

```sql
-- MIGRATION_DESCRIPTION: Add fasting_glucose column to fact_vital_reading
-- MIGRATION_AUTHOR: PulseTrack Data Platform
-- depends_on: V001, V003

-- Reference variables from migrations/catalogs/glue_iceberg.yaml using
-- Go-template syntax (matches WHOOP):
ALTER TABLE
  {{ .variables.iceberg.catalog }}.{{ .variables.glue.database.gold }}.fact_vital_reading
  ADD COLUMN fasting_glucose DOUBLE;
```

Two substitution layers, applied in order:
1. **Go-template** `{{ .variables.X.Y.Z }}` → resolved from the catalog
   YAML's `variables` block. Use this for *catalog-scoped* values
   (catalog name, database name, bucket-size for hidden partitioning).
2. **Env var** `${VAR}` and `${VAR:-default}` → resolved from process
   environment. Use this only for genuinely env-specific runtime
   plumbing.

### 3.2 Validating + dry-running locally before the PR

```bash
python3 migrations/cli.py --catalog glue_iceberg validate    # hash, conflicts, deps, variable resolution
python3 migrations/cli.py --catalog glue_iceberg dry-run     # prints would-execute SQL, no writes
python3 migrations/cli.py --catalog glue_iceberg pending     # show what would run next
```

CI runs `validate` on every PR via `.github/workflows/migration-check.yml`. PRs
that introduce conflicts (two unapplied migrations writing to the same
table) or break the dependency graph fail validation.

### 3.3 Applying migrations on the cluster

Use EMR's spark-submit (NOT `python3` directly — see § "Known traps"):

```bash
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode client \
    --jars file:///usr/share/aws/delta/lib/delta-spark_2.12-3.3.0-amzn-0.jar,\
           file:///usr/share/aws/delta/lib/delta-storage-3.3.0-amzn-0.jar \
    migrations/cli.py --catalog glue_iceberg run
```

The state ledger is `glue_iceberg.<gold_db>.schema_migrations` (table FQN
configured per-catalog in `migrations/catalogs/<catalog>.yaml` under
`state.table`).

### 3.4 Rolling back

```bash
python3 migrations/cli.py --catalog glue_iceberg rollback --version V003
```

Rollback executes `versions/V003__<name>__down.sql` if present. The state
row is NOT deleted — it's marked `rolled_back_at`. Re-running `run` will
re-apply.

### 3.5 SHA-256 immutability

Once a migration is applied, its file is hashed and stored. If anyone edits
the file afterward, `run` and `validate` will both fail loudly with a
"migration tampering detected" error. The right way to fix a bad migration
is a NEW versioned migration, not editing the old one.

### 3.6 Per-catalog YAML config (`migrations/catalogs/<name>.yaml`)

Each catalog has its own configuration file that pins:

- `migrationExecutor.conf.sparkConf` — the exact Spark + Iceberg + Glue
  catalog wiring used to execute SQL. Migrations cannot drift from the
  runtime they were authored against.
- `dependencies` — Maven coordinates for Iceberg / Delta / Glue
  catalog jars that get added via `--packages`.
- `variables` — the catalog-scoped values referenced by Go-template
  syntax in migration SQL (`{{ .variables.iceberg.catalog }}` etc).
- `state.table` — fully-qualified name of the migration ledger table.
- `lock` — DynamoDB-backed concurrency lock (see § 3.7).

Adding a new catalog (e.g., `glue_delta` for a parallel Delta layer) is a
matter of adding `migrations/catalogs/glue_delta.yaml` and pointing the
CLI at it via `--catalog glue_delta`.

### 3.7 DynamoDB concurrency lock

Concurrent invocations of `migrations/cli.py run` on the same catalog are
prevented by a DynamoDB-backed lock (matches WHOOP Glacierbase). Lock
table is provisioned by Terraform:

```
DynamoDB table:    pulsetrack-dev-glacierbase-locks
Hash key:          catalog (string)
TTL attribute:     expires_at (Unix epoch seconds)
```

Acquisition is a conditional `PutItem` with
`attribute_not_exists(catalog) OR expires_at < :now`. A stale lock (e.g.
crashed runner) auto-expires after `lock.lease_seconds` (default 600s)
and the next runner reclaims it.

If you see `LockAcquisitionError: catalog 'glue_iceberg' is held by …`,
either wait for the other run to finish or — if it's a known-dead
process — verify in the DynamoDB console and `DeleteItem` manually with
`catalog = "glue_iceberg"`.

## 4. Known limitations

### 4.1 Iceberg streaming gold from Iceberg silver (resolved on EMR 7.13.0)

Silver's `foreachBatch` MERGE produces Iceberg "overwrite" snapshots.
On older Iceberg (1.5.0, shipped with EMR 7.2.0), the streaming source
rejected these by default:

```
java.lang.IllegalStateException: Cannot process overwrite snapshot: <id>,
to ignore overwrites, set streaming-skip-overwrite-snapshots=true
```

**EMR 7.13.0 ships Iceberg 1.10.0**, where the streaming source handles
overwrite snapshots cleanly. The gold transforms (`fact_vital_daily_summary`,
`fact_vital_reading`, `fact_lab_result`) now run streaming with the full
MERGE INTO path — no `streaming-skip-overwrite-snapshots` workaround
needed, no retract loss.

For continuous streaming end-to-end:

```bash
# Producer in continuous mode
python3 scripts/produce_sensor_records.py \
    --brokers boot-XXXX.kafka-serverless.us-east-1.amazonaws.com:9098 \
    --topic   sensor_readings \
    --continuous --interval-seconds 5

# Bronze + silver + gold all on default `--trigger processing` (continuous)
/usr/lib/spark/bin/spark-submit ... streaming/bronze_ingestion.py
/usr/lib/spark/bin/spark-submit ... transformations/bronze_to_silver/sensor_silver.py --mode streaming
/usr/lib/spark/bin/spark-submit ... transformations/silver_to_gold/fact_vital_daily_summary.py --mode streaming
```

The Delta path retains full retract correctness via Delta's CDF (Change
Data Feed) and is preserved for the local dev loop.

### 4.2 `python3 migrations/cli.py` vs `/usr/lib/spark/bin/spark-submit migrations/cli.py`

The bootstrap installs `delta-spark` with `--no-deps` to avoid pulling
PyPI's `pyspark` (which clobbers EMR's bundled `spark-submit`). However,
`/usr/local/bin/spark-submit` may still exist if anyone re-installs
`pyspark` on the master. **Always invoke** `/usr/lib/spark/bin/spark-submit`
explicitly for migrations. Calling `python3 migrations/cli.py` uses
whichever pyspark is on PATH and may be missing the Iceberg jars.

### 4.3 First-run cold start

Iceberg tables created by migrations exist as empty. Some transforms
(e.g., `patient_identity_bridge`) require silver tables to be populated
before they can read them. If `run_identity_bridge` errors with
`TABLE_OR_VIEW_NOT_FOUND` on `pulsetrack_silver_dev.ehr_conditions`, run
`ehr_silver.py --format iceberg` first.

## 5. Common failures + remediation

| Symptom | Likely cause | Fix |
|---|---|---|
| `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` | Using `python3` instead of `/usr/lib/spark/bin/spark-submit` | Use the EMR spark-submit explicitly |
| `[DELTA_SCHEMA_NOT_SET]` or `[PATH_NOT_FOUND]` reading bronze in silver | Silver was run with `--format delta` against an Iceberg bronze (or vice versa) | Make all layers in a chain use the same format |
| `ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog` | Delta jars not on classpath; bootstrap symlink failed | Re-run bootstrap or pass `--jars file:///usr/share/aws/delta/lib/delta-spark_2.12-3.3.0-amzn-0.jar,delta-storage*` |
| `LockAcquisitionError: catalog 'glue_iceberg' is held by …` | Concurrent invocation of `migrations/cli.py run` on the same catalog | Wait for the other run, or — if a known-dead process — `DeleteItem` from `pulsetrack-dev-glacierbase-locks` with `catalog = "glue_iceberg"` |
| MSK SASL handshake hangs | `confluent_kafka.AdminClient` doesn't pump `oauth_cb`; or Kafka SG denies ingress | (a) Call `admin.poll(0.5)` in a loop after construction (already in `verify_msk_iam.py` and the producer); (b) confirm Kafka SG allows 9098 from EMR master SG |
| `[PARSE_SYNTAX_ERROR] near 'WRITE'` during `migrations/cli.py run` | Iceberg `WRITE ORDERED BY` requires the Iceberg SQL extension to be loaded | Confirm `spark.sql.extensions` includes `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` (note the trailing **s**) |
| Migration runner collapses two `;`-terminated statements into one | Apostrophe in a SQL comment toggles in-string state | Splitter is comment-aware now (`migrations/runner.py:split_statements`); verify your `migrations/runner.py` is at HEAD |
| `Cannot process overwrite snapshot` in gold streaming | See § 4.1 | Use `--mode batch --format iceberg` or accept `skip-overwrite-snapshots` |

## 6. Recovery procedures

### 6.1 Lost a checkpoint

```bash
# Wipe the affected checkpoint subtree and the partial Iceberg snapshots.
aws s3 rm s3://pulsetrack-lakehouse-dev-XXXX/checkpoints/silver_sensors --recursive
# Then re-run the silver streaming job — it'll start over from earliest.
```

Iceberg snapshots that were already committed are still there; they'll be
GC'd by the next `expire_snapshots` call (`writer.vacuum(retention_hours=168)`).

### 6.2 Bad migration applied

Treat as immutable history. Author a forward-correcting V<n+1> migration
that undoes the damage. Don't edit the bad migration in place — the
SHA-256 check will block it on every node.

### 6.3 Producer ran with bad data

The bronze GX gate is informative-only — bad data lands in bronze. The
silver GX gate is blocking — bad data is quarantined to
`s3://lakehouse/quarantine/`. Inspect the quarantine path and re-publish
fixed records via the producer.

## 7. Cost controls

* `infrastructure/teardown-compute.sh` destroys EMR + MSK only, leaves S3
  data + Glue + IAM + VPC. Bronze/silver/gold tables persist; the next
  `terraform apply` re-creates EMR + MSK against the existing data.
* The CloudWatch alarm `pulsetrack-dev-emr-apps-failed` notifies the SNS
  topic on any EMR app failure. The Budget alert fires at 80% of monthly
  spend.

## 8. Where things live

| Concern | Path |
|---|---|
| Format abstraction | `lakehouse/format_writer.py`, `lakehouse/__init__.py` |
| Migration framework | `migrations/{cli,runner,state,validator}.py` |
| Migration SQL | `migrations/versions/V<NNN>__<name>.sql` |
| Streaming bronze | `streaming/bronze_ingestion.py` |
| Silver transforms | `transformations/bronze_to_silver/` |
| Gold transforms | `transformations/silver_to_gold/` |
| Identity bridge | `transformations/identity_resolution/patient_identity_bridge.py` |
| DLQ handler | `streaming/dlq.py` |
| Cloud verification scripts | `scripts/{smoke_test_cloud,verify_msk_iam,produce_sensor_records,query_gold}.py` |
| CI checks | `.github/workflows/migration-check.yml` |
| Unit tests (no Spark) | `tests/test_migrations_runner.py`, `tests/test_format_writer.py` |
