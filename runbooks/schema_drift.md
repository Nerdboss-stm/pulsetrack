# Runbook: Schema drift between producer and decoder/silver

**Severity ladder:**
- SEV3: schema-registry version bump observed; `is_valid` rate dips < 1 %
- SEV2: silver `is_valid` rate drops > 5 %, OR a new `metric_name` value fails `KNOWN_METRICS` (`gxe.ExpectColumnValuesToBeInSet`)
- SEV1: bronze decoder is dropping fields silently (`decoded.<field>` reads null when raw bytes contain the value) — silent data loss

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

The drift this runbook covers: a producer adds a field (or a new `metric_name` value), the Schema Registry accepts under `BACKWARD` compatibility, the new record bytes flow through Kafka — but one of three downstream consumers silently breaks:

1. **Bronze decoder** (`streaming/bronze_ingestion.py::_decode_envelope`) loads `schemas/sensor_reading.avsc` from local disk via `schemas/registry.py::load_schema_str`. If the producer schema and the local `.avsc` diverge, `from_avro(..., {"mode": "PERMISSIVE"})` will *parse* the wire bytes against the decoder's schema and silently drop the new field.
2. **Silver suite** (`data_quality/expectations/silver_sensor_suite.py::KNOWN_METRICS`) rejects new `metric_name` values — see `runbooks/gx_failure_drains_batch.md`.
3. **`dim_metric` SCD2** (`transformations/silver_to_gold/dim_metric.py::METRIC_SEED`) is a static seed; new metrics don't get a `metric_key` row, so gold-fact joins lose the metric.

## TL;DR (30-second triage)

```bash
# 1. What schema versions does the registry have for the sensor topic?
curl -s $PT_SCHEMA_REGISTRY_URL/subjects/${PT_KAFKA_TOPIC_SENSOR:-sensor_readings}-value/versions

# 2. Diff the latest registry schema vs. the .avsc on the decoder side
curl -s $PT_SCHEMA_REGISTRY_URL/subjects/${PT_KAFKA_TOPIC_SENSOR:-sensor_readings}-value/versions/latest | jq -r .schema > /tmp/registry.avsc
diff <(jq -S . /tmp/registry.avsc) <(jq -S . schemas/sensor_reading.avsc)

# 3. Are there metric_name values in silver that aren't in KNOWN_METRICS?
spark-sql -e "
  SELECT DISTINCT metric_name FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings
  WHERE metric_name NOT IN (
    'heart_rate_bpm','spo2_pct','steps_since_last','skin_temp_celsius',
    'hrv_ms','respiration_rate','sleep_stage','blood_glucose_mgdl',
    'bp_systolic_mmhg','bp_diastolic_mmhg'
  )
"
```

## Symptoms

- Schema Registry has a new version for `sensor_readings-value` or `pharmacy_events-value` subject
- `records_failed{layer="silver",reason="quality_gate"}` rising (see `gx_failure_drains_batch.md`)
- New `metric_name` appears in bronze `decoded.metrics` map but doesn't make it into silver (`is_valid` false because no `METRIC_RANGES` entry → silent loss via quarantine)
- Decoder DLQ (`streaming/dlq.py`) shows no traffic but downstream "fields missing" alarms fire — classic PERMISSIVE-mode drop
- `dim_metric` row count flat while sensor row count grows; gold facts show NULL `metric_key`

## Diagnosis

### 1. Schema Registry: list all versions for the affected subject

```bash
SUBJECT=${PT_KAFKA_TOPIC_SENSOR:-sensor_readings}-value
curl -s $PT_SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions
# → [1, 2, 3, ...]

# Diff the two latest versions
PREV=$(curl -s $PT_SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions | jq '.[-2]')
LATEST=$(curl -s $PT_SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions | jq '.[-1]')

curl -s $PT_SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions/$PREV | jq -r .schema > /tmp/prev.avsc
curl -s $PT_SCHEMA_REGISTRY_URL/subjects/$SUBJECT/versions/$LATEST | jq -r .schema > /tmp/latest.avsc

diff <(jq -S .fields /tmp/prev.avsc) <(jq -S .fields /tmp/latest.avsc)
```

Any added field with a `default` value is `BACKWARD`-compatible (old consumers ignore it). Any added field WITHOUT a default, or any removed field with no default — those break BACKWARD compat and the registry should have rejected the publish.

### 2. Verify the decoder's local `.avsc` matches the registry

`streaming/bronze_ingestion.py` does NOT fetch the schema from the registry at runtime — it reads `schemas/sensor_reading.avsc` from disk:

```python
schema_str = load_schema_str(SCHEMA_FILE)  # SCHEMA_FILE = "sensor_reading.avsc"
```

This is the most common silent failure: producer pushed v3 to the registry, but the EMR-side jar/deployment still has the v2 `.avsc` baked in. PERMISSIVE mode silently drops the v3-only fields.

```bash
# On the EMR master
ssh hadoop@$MASTER_DNS "cat /opt/pulsetrack/schemas/sensor_reading.avsc" | jq -S .fields > /tmp/emr.avsc
diff /tmp/latest.avsc /tmp/emr.avsc
```

### 3. Compare `KNOWN_METRICS` to actually-arriving metric names

```bash
spark-sql -e "
  WITH known AS (
    SELECT explode(array(
      'heart_rate_bpm','spo2_pct','steps_since_last','skin_temp_celsius',
      'hrv_ms','respiration_rate','sleep_stage','blood_glucose_mgdl',
      'bp_systolic_mmhg','bp_diastolic_mmhg'
    )) AS m
  ),
  seen AS (
    SELECT DISTINCT metric_name AS m
    FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings
    WHERE ingestion_timestamp > current_timestamp() - INTERVAL 1 HOUR
  )
  SELECT seen.m FROM seen LEFT JOIN known ON seen.m=known.m WHERE known.m IS NULL
"
```

Any row returned is a metric the producer is sending that the silver suite doesn't know about.

### 4. Inspect `dim_metric` coverage

```bash
spark-sql -e "
  SELECT
    (SELECT COUNT(DISTINCT metric_name) FROM ${PT_GLUE_DB_GOLD:-pulsetrack_gold_dev}.dim_metric) AS dim_metric_metrics,
    (SELECT COUNT(DISTINCT metric_name) FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings) AS silver_metrics
"
```

If `silver_metrics > dim_metric_metrics`, you have orphaned metrics in silver that won't join to `dim_metric` in gold fact tables.

### 5. Run the schema observability monitor

```bash
python -m observability.cli --table glue_iceberg.${PT_GLUE_DB_BRONZE:-pulsetrack_bronze_dev}.sensor_readings --check schema
```

`observability/monitors.py::check_schema` returns `status='warn'` for added columns and `status='error'` for removed/type-changed columns. The detail field names the diff.

## Recovery (ranked fastest first)

### Case A: Update the decoder-side `.avsc`, redeploy the streaming app

If the producer pushed a backward-compatible change (new field with `default`), the fix is trivial: get the local `.avsc` in sync with the registry, redeploy.

```bash
# Pull the latest schema from the registry into the repo
curl -s $PT_SCHEMA_REGISTRY_URL/subjects/${PT_KAFKA_TOPIC_SENSOR:-sensor_readings}-value/versions/latest \
    | jq -r .schema | jq . > schemas/sensor_reading.avsc

# Update the Bronze DDL if the new field needs to land in the Iceberg
# bronze table (see BRONZE_SENSOR_DDL in streaming/bronze_ingestion.py;
# its decoded struct mirrors the .avsc shape).
$EDITOR streaming/bronze_ingestion.py
#   → add the new field to the STRUCT< ... > definition in BRONZE_SENSOR_DDL

# Tests
pytest tests/ -k "schema or bronze_ingestion" -x

# Deploy
git commit -am "schemas: bump sensor_reading.avsc to registry vN"
bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py
```

For an existing Iceberg table that has data, you'll need an `ALTER TABLE ADD COLUMN` to evolve it before the new field will be persisted — Iceberg supports this in place:

```sql
ALTER TABLE glue_iceberg.pulsetrack_bronze_dev.sensor_readings
    ADD COLUMN decoded.<new_field> <type>;
```

### Case B: Add the new metric to `KNOWN_METRICS` + `METRIC_RANGES` + `dim_metric`

Most schema-drift pages are about a new `metric_name` value (e.g. producer rolled out `respiration_rate_v2`). Three files in one change:

```python
# 1. data_quality/expectations/silver_sensor_suite.py
KNOWN_METRICS = [..., "respiration_rate_v2"]

# 2. transformations/bronze_to_silver/sensor_silver.py
METRIC_RANGES["respiration_rate_v2"] = (4, 60)
```

```python
# 3. transformations/silver_to_gold/dim_metric.py — append to METRIC_SEED
METRIC_SEED = [
    ...,
    ("respiration_rate_v2", "breaths/min", 8.0, 30.0, "sleep_ring"),
]
```

```bash
# Validate
pytest tests/test_quality_gates.py tests/test_sensor_silver.py tests/test_dim_device.py -x

# Re-seed dim_metric (it's overwrite-on-each-run by design)
spark-submit transformations/silver_to_gold/dim_metric.py --format $PT_LAKEHOUSE_FORMAT
```

### Case C: Evolve `dim_metric` without overwriting (production)

`dim_metric.py::main` calls `writer.overwrite(df)` — fine for dev where the seed is the source of truth, dangerous in prod if downstream facts have already joined to the existing `metric_key` values. Hash collisions are theoretical (the key is `abs(hash(concat_ws("|", metric_name, device_type)))`), but if downstream joins are pinned to specific `metric_key`s an overwrite will re-assign keys for unchanged metrics.

Two safer options:

```sql
-- Option 1: INSERT INTO (additive only, preserves existing keys)
INSERT INTO glue_iceberg.pulsetrack_gold_dev.dim_metric
SELECT
    abs(hash(concat_ws('|', 'respiration_rate_v2', 'sleep_ring'))) AS metric_key,
    'respiration_rate_v2' AS metric_name,
    'breaths/min'         AS unit,
    8.0                   AS normal_low,
    30.0                  AS normal_high,
    'sleep_ring'          AS device_type;
```

```sql
-- Option 2: re-seed AND verify keys haven't moved
WITH new_seed AS (
    SELECT abs(hash(concat_ws('|', metric_name, device_type))) AS metric_key, *
    FROM (VALUES
        ('respiration_rate_v2', 'breaths/min', 8.0, 30.0, 'sleep_ring'),
        ...
    ) v(metric_name, unit, normal_low, normal_high, device_type)
)
SELECT n.metric_key, d.metric_key, n.metric_name, n.device_type
FROM new_seed n
JOIN glue_iceberg.pulsetrack_gold_dev.dim_metric d
  ON n.metric_name = d.metric_name AND n.device_type = d.device_type
WHERE n.metric_key != d.metric_key;
-- → must be empty before running the overwrite
```

### Case D: Retro-process bronze affected partitions

After fixing the decoder schema (Case A), older bronze rows from before the deploy have the new field as NULL. To backfill from the raw Avro bytes:

```bash
# raw_avro_bytes is preserved on every bronze row — that's the whole point
# of the design. Replay bronze→silver for the affected hours.
spark-submit transformations/bronze_to_silver/sensor_silver.py \
    --mode batch --format $PT_LAKEHOUSE_FORMAT
```

For a partial replay (just one day):

```python
# scripts/replay_bronze_window.py
from pyspark.sql.avro.functions import from_avro
bronze = spark.read.format("delta").load(settings.bronze_sensor) \
              .filter("ingestion_date = '2026-05-09'")
schema_str = open("schemas/sensor_reading.avsc").read()
redecoded = bronze.select(
    "*",
    from_avro(F.expr("substring(raw_avro_bytes, 6, length(raw_avro_bytes) - 5)"),
              schema_str, {"mode": "PERMISSIVE"}).alias("re_decoded")
)
# Now project re_decoded.<new_field> into silver via the standard transform
```

## Verification

1. **Decoder + registry are in sync:**
   ```bash
   diff <(curl -s $PT_SCHEMA_REGISTRY_URL/subjects/sensor_readings-value/versions/latest | jq -rS .schema | jq -S .fields) \
        <(jq -S .fields schemas/sensor_reading.avsc)
   # Expect: no diff
   ```
2. **All seen metrics are in `KNOWN_METRICS`:** re-run diagnosis step 3, expect zero rows.
3. **`dim_metric` covers every silver metric:**
   ```bash
   spark-sql -e "
     SELECT m FROM (
       SELECT DISTINCT metric_name AS m FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings
     ) s
     LEFT JOIN ${PT_GLUE_DB_GOLD:-pulsetrack_gold_dev}.dim_metric d ON s.m = d.metric_name
     WHERE d.metric_name IS NULL
   "
   # Expect: zero rows
   ```
4. **Schema monitor green:**
   ```bash
   python -m observability.cli --table glue_iceberg.${PT_GLUE_DB_BRONZE:-pulsetrack_bronze_dev}.sensor_readings --check schema
   ```

## Prevention

- **Pin the registry compatibility level to `BACKWARD` (or stricter `BACKWARD_TRANSITIVE`)** for both `sensor_readings-value` and `pharmacy_events-value`:
   ```bash
   curl -X PUT $PT_SCHEMA_REGISTRY_URL/config/sensor_readings-value \
       -d '{"compatibility": "BACKWARD_TRANSITIVE"}' \
       -H "Content-Type: application/vnd.schemaregistry.v1+json"
   ```
- **CI test:** `tests/conftest.py` should diff `schemas/sensor_reading.avsc` against a checked-in `schemas/sensor_reading.expected.avsc`, OR (better) against the registry's `latest` version at build time. The check should fail loudly on drift.
- **Fetch-from-registry at startup** (deferred work): rather than `load_schema_str("sensor_reading.avsc")`, have `streaming/bronze_ingestion.py` fetch via `schemas/registry.py::get_schema_registry_client().get_latest_version(...)`. Eliminates the entire class of "decoder is stale" bugs. The trade-off is a startup dependency on the registry being reachable.
- **`KNOWN_METRICS`/`METRIC_RANGES`/`METRIC_SEED` invariant:** add a test that asserts these three sets agree (modulo `device_type` cross-product). Currently you have to grep three files when adding a metric.
- **Schema-evolution checklist** in the producer-team's PR template: any producer change to a field set requires a paired PR updating `schemas/*.avsc` + `KNOWN_METRICS` + `METRIC_RANGES` + `METRIC_SEED`.

## Related postmortems

- *(none yet)*

## Related runbooks

- `runbooks/gx_failure_drains_batch.md` — the `KNOWN_METRICS` rejection is the most-common downstream of schema drift
- `runbooks/identity_unresolved_spike.md` — if the dropped field is `patient_email` or `user_device_account_id`, identity bridge collapses
- `runbooks/kafka_consumer_lag.md` — schema-registry outages will manifest as bronze decoder slowdowns
