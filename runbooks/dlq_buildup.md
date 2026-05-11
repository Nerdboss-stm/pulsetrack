# Runbook: DLQ topic `pulsetrack_dlq` buildup

**Severity ladder:**
- SEV3: DLQ events/min > 0 but < 10 for < 10 min on a steady-rate stream (transient producer hiccup, GX one-off)
- SEV2: DLQ events/min sustained > 10 for > 15 min, OR DLQ rate exceeds 0.5% of bronze input rate, OR a single `error_class` accounts for > 80% of DLQ traffic (= one bug, fix-once)
- SEV1: DLQ events/min > 100 for > 5 min (catastrophic ingestion failure — likely schema-registry outage or producer regression), OR quarantine S3 path is filling at > 1 GB/hr

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

## TL;DR (30-second triage)

```bash
# 1. DLQ rate from Prometheus (records_failed counter, see streaming/dlq.py + metrics.py)
curl -s "http://$MASTER_DNS:8001/metrics" | grep -E '^records_failed_total'
# bronze_sensor metrics port is 8001 (config.py:metrics_port_bronze_sensor)

# 2. Read DLQ headers — error_class tells you which failure mode (kafka topic, NOT the silver Iceberg table)
ssh hadoop@$MASTER_DNS \
    "kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP \
     --topic pulsetrack_dlq --from-beginning --max-messages 20 \
     --property print.headers=true --property print.key=true"

# 3. How does the bucket look? (DLQ table lives at ${PT_LAKEHOUSE_BASE}/dlq — see config.py:dlq)
aws s3 ls "s3://${BUCKET}/dlq/" --recursive --summarize | tail -5
```

DLQ records carry these payload fields (see `streaming/dlq.py:DLQRecord` and `DLQ_SCHEMA`):
- `original_topic`, `original_partition`, `original_offset`, `original_key`, `original_value`
- `error_type` ← the categorization field; this is what to triage on
- `error_message`, `stack_trace`
- `failed_at`, `retry_count`

Four DLQ error families:
1. **`avro_deserialization_failure`** — schema-registry rejection / producer pushed bad Avro (most common, ~40%)
2. **`gx_validation_failure`** — GX gate quarantined the batch (goes to quarantine S3, NOT DLQ in the strict sense — but operators conflate)
3. **`manual_deserialization_failure`** — non-Avro decoders raised (JSON, line-delimited)
4. **`identity_resolution_failure`** — patient-identity bridge couldn't resolve `user_device_account_id → patient_id`

## Symptoms (what triggered the page)

- CloudWatch alarm `pulsetrack-{env}-dlq-rate-high` ACTIVE (Prometheus-derived custom metric)
- Prometheus `records_failed_total{layer=*,source=*,reason=*}` rate of change > 10/min
- Bronze input volume is normal but `records_processed_total` lags — gap is going to DLQ
- Silver freshness OK but row counts down — quarantine is siphoning rows
- SNS topic `arn:aws:sns:us-east-1:960341592614:pulsetrack-dev-alerts` fired
- S3 lifecycle on `dlq/` (90 day expiry, see `infrastructure/modules/storage/main.tf`) means buildup is forensically available — but a 1 GB/hr fill rate exhausts the budget before the lifecycle kicks in

## Diagnosis (commands to run first)

### Categorize the errors

```bash
# Tail the DLQ topic for 60s and group by error_type from headers
ssh hadoop@$MASTER_DNS \
    "timeout 60 kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP \
     --topic pulsetrack_dlq --max-messages 500 \
     --property print.headers=true 2>/dev/null" \
    | grep -oE 'error_type:[^,]*' | sort | uniq -c | sort -rn
```

Or query the DLQ table directly (more accurate, but slower):

```bash
ssh hadoop@$MASTER_DNS "spark-sql -e \"
  SELECT error_type, COUNT(*) AS n
  FROM pulsetrack_silver_dev.dlq
  WHERE failed_at > current_timestamp() - INTERVAL 1 HOUR
  GROUP BY error_type ORDER BY n DESC
\""
```

(Note: the DLQ Iceberg table is registered under the silver Glue DB per `streaming/dlq.py:_dlq_writer` — the layer kwarg is intentionally `silver`, see the module docstring.)

### Inspect a sample failed record

```bash
# Pull one record's full payload, decode the original_value
ssh hadoop@$MASTER_DNS "spark-sql -e \"
  SELECT original_topic, original_partition, original_offset,
         error_type, error_message,
         substring(original_value, 1, 200) AS value_preview
  FROM pulsetrack_silver_dev.dlq
  WHERE error_type = '<the_dominant_type>'
  ORDER BY failed_at DESC LIMIT 5
\""
```

For Avro decode failures: the first 5 bytes of `original_value` are the Confluent wire format (1 magic byte `0x00` + 4-byte big-endian schema ID). Decode the schema ID:

```bash
# Quick byte-peek (assumes hex-encoded value in the Spark output)
# Bytes 2-5 of the raw value = schema ID
echo "<value_preview_hex>" | head -c 10
# Then:
curl -s "${PT_SCHEMA_REGISTRY_URL:-http://localhost:8081}/schemas/ids/<schema_id>"
```

If the schema ID isn't registered → producer pushed a payload before/without registering. Root cause: a producer deploy preceded a schema-registry deploy.

### Check the quarantine path (parallel to DLQ)

GX-rejected records don't go to the Kafka DLQ — they land in `${PT_LAKEHOUSE_BASE}/quarantine/` as Delta rows tagged with `quarantine_reason`. See `data_quality/quarantine.py`.

```bash
ssh hadoop@$MASTER_DNS "spark-sql -e \"
  SELECT quarantine_layer, quarantine_source, quarantine_reason, COUNT(*) AS n
  FROM delta.\\\`${PT_LAKEHOUSE_BASE}/quarantine\\\`
  WHERE quarantined_at > current_timestamp() - INTERVAL 1 HOUR
  GROUP BY 1, 2, 3 ORDER BY n DESC
\""
```

## Recovery (ranked by likelihood, fastest first)

### Case A: `avro_deserialization_failure` from one producer — ~40% of pages

Symptom: one `original_topic` dominates (e.g., everything is from `sensor_readings`), schema-ID lookup either returns 404 or returns a schema that doesn't match the bytes.

**Sub-case A1 — schema-registry was unavailable when producer started:**
The producer cached the wrong schema ID. Fix:
```bash
# 1. Confirm schema registry is healthy
curl -sf "${PT_SCHEMA_REGISTRY_URL}/subjects" | jq length

# 2. Restart the producer (it'll re-register the schema)
ssh hadoop@$MASTER_DNS "tmux kill-session -t producer 2>/dev/null; \
    tmux new -d -s producer 'python data_generators/wearable_generator.py'"

# 3. Replay the DLQ — TODO: scripts/replay_dlq.py does not yet exist (see postmortems below)
#    For now, manually re-publish the original_value back to sensor_readings:
ssh hadoop@$MASTER_DNS "spark-sql -e \"
  SELECT original_value FROM pulsetrack_silver_dev.dlq
  WHERE error_type = 'avro_deserialization_failure'
    AND failed_at > current_timestamp() - INTERVAL 1 HOUR
\" > /tmp/replay.txt"
# Then use kafka-console-producer.sh to push the lines back. Note: this won't
# re-encode the Confluent wire prefix; for genuine binary Avro, the replay
# script is the only safe path. Schedule the script implementation.
```

**Sub-case A2 — producer deployed a new schema version, bronze hasn't picked it up:**
Schema evolution is forward-compatible (Avro), but the bronze decoder caches schema definitions. Fix:
```bash
# Force bronze to refresh — restart the stream
ssh hadoop@$MASTER_DNS "yarn application -kill <bronze_appId>"
bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py
```

### Case B: `gx_validation_failure` — quarantine, not DLQ — ~25%

Symptom: DLQ rate is low but quarantine row counts spiking; bronze rows are landing but silver counts are down.

**Diagnose which expectation failed:**
```bash
# Pull the most recent GX result from observability ledger
ssh hadoop@$MASTER_DNS "spark-sql -e \"
  SELECT monitor_name, table_name, check_type, status, detail, run_at
  FROM ${PT_OBSERVABILITY_TABLE:-glue_iceberg.pulsetrack_gold_dev.monitor_runs}
  WHERE check_type LIKE 'gx_%' AND status != 'ok'
  ORDER BY run_at DESC LIMIT 10
\""
```

Common GX-fail roots:
- `metric_name` outside `KNOWN_METRICS` (see `data_quality/expectations/silver_sensor_suite.py:34`) — a new metric was added to the producer without updating the suite
- `device_account_id` null — identity bridge missed a row; falls through to Case D
- `reading_metric_key` not unique — duplicate Kafka offsets, deeper bug

**Fix:**
1. If a new metric is legitimate, add it to `KNOWN_METRICS` in `silver_sensor_suite.py`, redeploy
2. If quarantined rows are bad data, leave them — quarantine S3 lifecycle (90 days, see `modules/storage/main.tf`) cleans up; investigate the producer in parallel
3. Inspect quarantined rows for a fix:
   ```bash
   ssh hadoop@$MASTER_DNS "spark-sql -e \"
     SELECT * FROM delta.\\\`${PT_LAKEHOUSE_BASE}/quarantine\\\`
     WHERE quarantine_layer = 'silver' AND quarantine_source = 'sensor'
     ORDER BY quarantined_at DESC LIMIT 20
   \""
   ```

### Case C: `manual_deserialization_failure` — ~15%

Symptom: failures in a non-Avro path — typically EHR JSON or pharmacy line-delimited.

**Fix:**
- Look at `error_message` for the specific parse error
- Most common: a schema-evolution event that broke a hand-rolled JSON decoder. Update the decoder, redeploy the relevant `bronze_*.py`
- Rows are recoverable from S3 quarantine if the fix is non-trivial; replay after fix lands

### Case D: `identity_resolution_failure` — ~15%

Symptom: errors mention `patient_identity_bridge`; `original_value` is well-formed but the `user_device_account_id` doesn't appear in `silver/identity/patient_identity_bridge`.

**Cause:** the identity bridge job (see `transformations/identity_resolution/patient_identity_bridge.py`) hasn't run yet, or the new account was registered after the last bridge refresh.

**Fix:**
```bash
# 1. Force a bridge rebuild
ssh hadoop@$MASTER_DNS "spark-submit \
    s3://${BUCKET}/code/transformations/identity_resolution/patient_identity_bridge.py"

# 2. Replay the affected DLQ rows once the bridge is fresh
#    (Again — scripts/replay_dlq.py is a TODO; see postmortems)
```

### Case E: Schema-registry total outage — SEV1 path

Symptom: 100% of DLQ traffic is `avro_deserialization_failure`; schema registry health endpoint returns 5xx.

**Fix:**
```bash
# Check the schema registry container/service
curl -sfi "${PT_SCHEMA_REGISTRY_URL}/" | head
# If down — escalate to the Confluent/MSK Connect on-call; this is a hard dependency,
# not a PulseTrack-side fix.
# Mitigation while waiting: stop bronze (it'll only fill DLQ otherwise):
ssh hadoop@$MASTER_DNS "yarn application -kill <bronze_appId>"
# Once schema registry is back, restart bronze. The Kafka topic still has the
# unprocessed records — they replay from the consumer-group's last commit.
```

## Verification (how you know it's fixed)

1. DLQ rate back to baseline:
   ```bash
   curl -s "http://$MASTER_DNS:8001/metrics" | grep 'records_failed_total'
   # Compare to baseline (typically < 1/min on a healthy stream)
   ```
2. No new rows in the DLQ table for 5 min:
   ```bash
   ssh hadoop@$MASTER_DNS "spark-sql -e \"
     SELECT COUNT(*) FROM pulsetrack_silver_dev.dlq
     WHERE failed_at > current_timestamp() - INTERVAL 5 MINUTES
   \""
   ```
3. Quarantine row growth back to baseline (any expected ongoing quarantine for legitimate bad data still happens — focus on the delta)
4. Bronze → silver throughput recovered to pre-page rate (see `runbooks/kafka_consumer_lag.md` verification commands)

## Prevention (post-incident hardening)

1. **Build `scripts/replay_dlq.py`** — every postmortem references it. The script should: (a) read DLQ table rows by `error_type` and time window, (b) re-decode `original_value` with the current schema, (c) re-publish to `original_topic`. Note as TODO in the postmortem this incident produced.
2. **DLQ error_type alert routing:** different `error_type`s route to different on-calls. Schema failures → producer team; identity failures → data-eng. Currently everything pages the streaming on-call.
3. **Schema-registry health probe:** add a 1-min freshness check on `${PT_SCHEMA_REGISTRY_URL}/subjects` to the observability monitor set. Catches Case E before bronze backs up.
4. **Compaction on the DLQ table:** the DLQ is append-only; small files accumulate. Add it to `maintenance/compaction.py` schedule.
5. **Wire this runbook URL** into the alert payload via `observability/alerting.py`.

## Related postmortems

- `postmortems/2026-05-02_silver_cold_start_hang.md` — silver hang caused bronze to backpressure → some retries hit DLQ during recovery
- `postmortems/2026-05-09_chaos_drill_2_app_kill.md` — chaos drill 2 produced a brief DLQ blip during app restart

## Related runbooks

- `runbooks/kafka_consumer_lag.md` — DLQ buildup can correlate with consumer lag (partial-failure feedback loop)
- `runbooks/silver_cold_start_hang.md` — silver hang back-pressures into DLQ via bronze
- `runbooks/emr_step_failure.md` — if the failing component is an EMR step rather than per-record
- `runbooks/s3_503_throttling.md` — adjacent if DLQ writes themselves are being throttled
