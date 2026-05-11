# Runbook: GX validation failure drains the whole microbatch

**Severity ladder:**
- SEV3: a single batch fails the gate; quarantine row-count for layer < 1 % of batch
- SEV2: > 3 consecutive batches fail the same expectation OR quarantine rate > 5 % over a 15-min window
- SEV1: GX gate has been failing > 30 min AND silver `sensor_readings` MERGE has not advanced (downstream freshness alarm firing)

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

The pathology this runbook covers: the silver gate in `transformations/bronze_to_silver/sensor_silver.py::_process_batch` calls `gx_validate(...)` over the *whole* `valid` DataFrame for the microbatch. One row that violates an expectation (e.g. a single `metric_name` not in `KNOWN_METRICS`) flips `gate_pass=False`, the `writer.merge(valid, ...)` is **skipped for the entire batch**, and 50k otherwise-good rows fail to propagate to silver. The invalid subset is quarantined separately, but the rest of the batch is effectively stuck behind the offending row until the offset is reprocessed.

## TL;DR (30-second triage)

```bash
# 1. Has the silver query advanced? (compare ingestion_timestamp to wall clock)
spark-sql -e "SELECT MAX(event_timestamp) FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings"

# 2. Pull the most-recent failure from the structured log on the driver
ssh hadoop@$MASTER_DNS \
    "grep -E 'GX quality gate FAILED' /var/log/spark/silver-sensor-readings*.log | tail -5"

# 3. Which expectation failed? (json blob under `result.expectations`)
ssh hadoop@$MASTER_DNS \
    "grep -E 'GX quality gate FAILED' /var/log/spark/silver-sensor-readings*.log \
     | tail -1 | jq '.extra_data.result'"
```

Three flavours of GX-drain pages map to four fixes:
1. **Benign producer drift** (new but valid metric, slightly out-of-range vital) → widen the expectation
2. **Schema change** (producer added a field, suite hasn't caught up) → push fix to `data_quality/expectations/`
3. **Real bad data** already isolated → replay quarantine after correction
4. **Operations emergency** (release blocker, ops sign-off in hand) → bypass GX (last resort, do not use without approval)

## Symptoms (what triggered the page)

- Prometheus `records_failed{layer="silver",reason="quality_gate"}` > 0 and rising
- `Silver gate failed — skipping MERGE for batch` log lines (see `data_quality/gx_config.py::validate`)
- `glue_iceberg.pulsetrack_silver_dev.sensor_readings` freshness > `max_age_minutes=5` (`observability/monitors.py::check_freshness`)
- Quarantine path (`s3://pulsetrack-lakehouse-${PT_AWS_ENV}-*/quarantine/`) row count climbing while silver row count flatlines
- Downstream: Snowflake auto-refresh shows stale silver, gold daily summary backlog growing

## Diagnosis

### 1. Pull the actual GX result for the failing batch

Structured logs from `gx_config.validate()` include the full GX result under `extra_data.result`. The `expectations` array names which expectation failed, the column, and a sample of unexpected values.

```bash
ssh hadoop@$MASTER_DNS << 'EOF'
sudo journalctl -u spark-silver-sensor --since "30 min ago" \
  | grep '"GX quality gate FAILED"' \
  | tail -1 \
  | jq '.extra_data.result | {success, expectations: [.expectations[] | select(.success == false) | {type, column, unexpected_count, partial_unexpected_list}]}'
EOF
```

You're looking for one of:
- `ExpectColumnValuesToBeInSet` on `metric_name` → new metric not in `KNOWN_METRICS`
- `ExpectColumnValuesToBeInSet` on `is_valid` → `add_quality_flags` produced `is_valid=False` rows that bypassed the quarantine filter (suite is misconfigured or `_process_batch` was modified)
- `ExpectColumnValuesToNotBeNull` on `device_account_id` → producer regression; Avro `user_device_account_id` came in null
- `ExpectColumnValuesToBeUnique` on `reading_metric_key` → duplicate `(reading_id, metric_name)` pair (watermark/dedup broken)

### 2. Sample the offending rows from quarantine

`data_quality/quarantine.py` writes everything where `is_valid = False` to the quarantine Delta path with the offense reason. For GX failures the rows are NOT in quarantine — they're held in the batch, dropped on the floor when MERGE is skipped, and only the `is_valid=False` subset is quarantined. To see what the gate is rejecting you have to re-derive from bronze:

```bash
# Replay one offset window into a scratch DataFrame; inspect the unique
# metric_name values to find the offending one.
spark-submit --conf spark.driver.memory=4g <<'PY'
from pyspark.sql import SparkSession, functions as F
from data_quality.expectations.silver_sensor_suite import KNOWN_METRICS, prepare_for_validation
from transformations.bronze_to_silver.sensor_silver import transform

spark = SparkSession.builder.appName("gx-triage").getOrCreate()
bronze = spark.read.format("delta").load("/tmp/pulsetrack-lakehouse/bronze/sensor_readings") \
              .filter("ingestion_timestamp > current_timestamp() - INTERVAL 30 MINUTES")
silver = transform(bronze).filter("is_valid")
prepared = prepare_for_validation(silver)
prepared.groupBy("metric_name").count().orderBy(F.desc("count")).show(50, False)
print("Known metrics:", KNOWN_METRICS)
PY
```

The metric in the result that is NOT in `KNOWN_METRICS` is your culprit.

### 3. Confirm whether it's drift vs corruption

If the new metric value looks physiologically plausible (e.g. `respiration_rate=12.4`) → producer added a real metric, suite is stale.
If the value is `NaN`, negative, or clearly garbage → producer regression, fix the source.

## Recovery (ranked fastest first)

### Case A: Widen / extend the expectation (benign drift, ~50 % of pages)

Producer started emitting a new but legitimate metric (e.g. WHOOP rolled out a new sleep stage label) or a previously-rare metric crept past the suite's range.

**Fix:** add to `KNOWN_METRICS` in `data_quality/expectations/silver_sensor_suite.py`:

```python
KNOWN_METRICS = [
    "heart_rate_bpm",
    ...
    "blood_glucose_mgdl",
    "bp_systolic_mmhg",
    "bp_diastolic_mmhg",
    "respiratory_rate_v2",  # ← new
]
```

Deploy:
```bash
# Tests must pass — KNOWN_METRICS is referenced from sensor_silver.METRIC_RANGES
pytest tests/test_sensor_silver.py tests/test_quality_gates.py -x
# Push (no restart needed — the silver streaming app reloads the suite from
# disk on the next batch via gx_config.SUITE_BUILDERS[suite_name]() in validate())
git commit -am "silver_sensor_suite: allow respiratory_rate_v2"
git push origin saran-dev/$BRANCH
# Trigger pipeline redeploy (cluster-side)
bash scripts/submit_emr_step.sh transformations/bronze_to_silver/sensor_silver.py --mode streaming
```

Within one trigger interval (`settings.trigger_interval=30 seconds`) the next batch should pass.

### Case B: Add a corresponding `METRIC_RANGES` entry (drift + new range)

`silver_sensor_suite.KNOWN_METRICS` is the enum; `sensor_silver.METRIC_RANGES` is the physiological-range table that drives `is_valid`. If the new metric isn't in `METRIC_RANGES`, `add_quality_flags` will fall through to the default `valid_expr=True` branch — meaning every row passes — but if you've added it to `KNOWN_METRICS` and not to `METRIC_RANGES` you'll get unbounded values quietly tagged `is_valid=True`. Update both:

```python
# transformations/bronze_to_silver/sensor_silver.py
METRIC_RANGES: dict[str, tuple[float, float]] = {
    ...,
    "respiratory_rate_v2": (4, 60),
}
```

```bash
pytest tests/test_sensor_silver.py::test_metric_ranges_match_known_metrics
```

### Case C: Replay quarantine after a bronze-side correction

If diagnosis showed the offending rows are real-but-bad (corrupt producer), the rows already failed `is_valid` and went to quarantine. After the producer is fixed:

```bash
# 1. Confirm the quarantine batch you're going to replay
spark-sql -e "
  SELECT quarantine_reason, COUNT(*) AS n
  FROM delta.\`/tmp/pulsetrack-lakehouse/quarantine\`
  WHERE quarantine_layer='silver'
    AND quarantined_at > current_timestamp() - INTERVAL 6 HOURS
  GROUP BY quarantine_reason
"

# 2. Re-emit from quarantine through a one-shot Silver batch run
spark-submit transformations/bronze_to_silver/sensor_silver.py \
    --mode batch --format $PT_LAKEHOUSE_FORMAT

# 3. Verify GX passes on the replay
ssh hadoop@$MASTER_DNS "tail -50 /var/log/spark/silver-sensor-batch.log | grep 'GX quality gate'"
```

Note: the quarantine table has `mergeSchema=true`, so you can append the corrected rows from quarantine back into bronze via a small custom job — there's no built-in replay tool. Most operators just let the watermark advance and rely on the producer fix going forward.

### Case D: Emergency bypass (ops approval required, audit trail mandatory)

**Do not use in prod without IC approval and a postmortem ticket open.** This bypasses the silver gate for one deploy cycle so a backlog can drain while you fix the underlying expectation. The pattern: patch `gx_config.validate` to return `True` unconditionally, or short-circuit the gate inside `_process_batch`.

Safer alternative — keep the validation, drop the gating:

```python
# In transformations/bronze_to_silver/sensor_silver.py::_process_batch
# Temporary: log but do not block on gate result.
gate_pass = gx_validate(prepare_silver(valid), suite_name=SILVER_SUITE,
                        layer="silver", source="sensor")
# if gate_pass:                                          # comment out
writer.merge(valid, match_condition="...")              # always merge
```

Track it:
1. Open postmortem stub in `postmortems/$(date +%F)_gx_bypass.md`
2. Set a hard timer — bypass must be reverted within 4 hours
3. Note: `quarantine_records` still runs, so `is_valid=False` rows are still siphoned off; only the suite's batch-level gate is bypassed

## Verification

After applying any of A–D:

1. **Gate passes on the next batch:**
   ```bash
   ssh hadoop@$MASTER_DNS "tail -f /var/log/spark/silver-sensor-readings.log | grep 'GX quality gate'"
   # Expect: "GX quality gate passed" within 30 seconds
   ```
2. **Silver row count advances:**
   ```bash
   spark-sql -e "SELECT COUNT(*), MAX(event_timestamp) FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings"
   # Run twice 60s apart — both numbers should grow
   ```
3. **Freshness monitor green:**
   ```bash
   python -m observability.cli --table silver_sensor --check freshness
   ```
4. **`records_failed{reason="quality_gate"}` stops climbing** in Prometheus on port `settings.metrics_port_silver_sensor` (8004 by default).

## Prevention

- **CI gate:** `tests/test_quality_gates.py` should fail if `KNOWN_METRICS` and `METRIC_RANGES.keys()` drift apart. Add the assertion if it isn't there yet.
- **Producer contract test:** before any change to `data_generators/wearable_generator.py` or `data_generators/synthetic/wearable_generator.py`, run `pytest tests/test_wearable_generator.py` — these enforce the metric-name vocabulary.
- **Avoid all-or-nothing gates on batches > 10k rows:** the silver suite is already permissive (no event-timestamp window, see module docstring). Resist adding new batch-level expectations; prefer per-row flags (`is_valid`, `is_late_arriving`) which quarantine naturally without draining the batch.
- **Schema-registry compat check:** for producer-side field adds, ensure `BACKWARD` compatibility is enforced on the subject (see `runbooks/schema_drift.md`).

## Related postmortems

- *(none yet — this runbook was written before any postmortem)*

## Related runbooks

- `runbooks/kafka_consumer_lag.md` — symptoms overlap when the silver stream skips MERGE and bronze offsets pile up
- `runbooks/schema_drift.md` — root cause when a producer adds a field/metric
- `runbooks/identity_unresolved_spike.md` — the `device_account_id` null-rate failure mode lands you here too
