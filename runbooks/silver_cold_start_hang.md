# Runbook: silver streaming query starts but produces 0 output

**Severity ladder:**
- SEV3: silver stream produced 0 rows for < 2 min after start (cold-start initialization is normal — Spark needs ~30s for executor claim + checkpoint replay)
- SEV2: silver stream produced 0 output rows for 5+ min while bronze is still receiving data, OR `numInputRows > 0` but `numOutputRows == 0` for 3 consecutive batches
- SEV1: silver stream produced 0 output for 10+ min, OR multiple silver queries hung simultaneously (=cluster-wide issue), OR the query reports RUNNING but driver heartbeat is stale

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

## TL;DR (30-second triage)

```bash
# 1. Query is alive but stuck?
curl -s "http://$MASTER_DNS:4040/api/v1/applications" | jq '.[] | select(.name | contains("Silver"))'
APP_ID="<from above>"
curl -s "http://$MASTER_DNS:4040/api/v1/applications/${APP_ID}/streaming/statistics" \
    | jq '{numActiveBatches, numInactiveReceivers, totalProcessedRecords, lastCompletedBatch}'

# 2. What does the last batch look like?
curl -s "http://$MASTER_DNS:4040/api/v1/applications/${APP_ID}/streaming/batches?limit=5" \
    | jq '.[] | {batchId, numInputRows, processingTime, schedulingDelay}'

# 3. Driver alive? (NoClassDef / NoSuchMethod errors are silent killers)
ssh hadoop@$MASTER_DNS "yarn logs -applicationId $APP_ID -appOwner hadoop -log_files stderr" \
    2>/dev/null | tail -200 | grep -E 'Error|Exception|StuckBatch'
```

Cold-start hang has four shapes; the fix differs sharply:
1. **Checkpoint dir corrupted** — driver replays but a malformed offset/commit file blocks watermark advancement
2. **Watermark advancement blocked** — late-arriving data prevents the watermark from moving past the dedup window
3. **Kafka rebalance loop** — consumer joins → reads metadata → leaves → repeat; never lands on a partition assignment
4. **GX suite slow load** — `data_quality/expectations/silver_sensor_suite.py` is imported on every batch; if Glue / S3 are slow it can stall first batch indefinitely

## Symptoms (what triggered the page)

- CloudWatch alarm `pulsetrack-{env}-silver-stream-zero-output` ACTIVE (custom metric: `numOutputRows == 0 for 5 min` while `streaming_query_active == 1`)
- Prometheus `records_processed_total{layer="silver"}` flat
- Bronze freshness OK (rows still landing in `pulsetrack_bronze_dev.sensor_readings`) → producer is fine
- Silver freshness check failing (`observability/monitors.py:check_freshness`)
- Silver-dependent Gold tables stale (`fact_vital_daily_summary` no new partitions)
- SNS topic `arn:aws:sns:us-east-1:960341592614:pulsetrack-dev-alerts` fired

## Diagnosis (commands to run first)

### Is the query actually running or just claiming to be?

```bash
# Spark UI on master:4040 (live app) or :18080 (history) → Structured Streaming tab
# Look at:
#   - "Run ID" (changes on every restart)
#   - "Last Progress" timestamp (stale = driver hung)
#   - Batch table: numInputRows vs numOutputRows
#
# If numInputRows > 0 but numOutputRows == 0 → watermark or dedup is dropping everything
# If numInputRows == 0 → consumer isn't reading from Kafka

curl -s "http://$MASTER_DNS:4040/api/v1/applications/${APP_ID}/streaming/batches?limit=10" | jq '.'
```

### Driver-log scan for ClassNotFoundError

The most-painful silent failure: a missing class is logged at WARN, the query continues to report "active", but no progress is ever made.

```bash
ssh hadoop@$MASTER_DNS "yarn logs -applicationId $APP_ID -appOwner hadoop -log_files stderr" \
    2>/dev/null | grep -E 'ClassNotFoundException|NoSuchMethodError|NoClassDefFoundError' | head
```

Common offenders:
- `org.apache.iceberg.spark.SparkSessionCatalog` — Iceberg jars not on classpath (bootstrap regression)
- `io.delta.sql.DeltaSparkSessionExtension` — Delta jars not symlinked (see `bootstrap.sh` symlink loop)
- `software.amazon.msk.auth.iam.IAMClientCallbackHandler` — MSK IAM auth jar missing

### Inspect the checkpoint dir

Silver's checkpoint lives at `${PT_LAKEHOUSE_BASE}/checkpoints/silver_sensors/` (config-driven; see `config.py:checkpoint_base`). Layout:

```
s3://$BUCKET/checkpoints/silver_sensors/
  offsets/             ← Kafka offset commits, one file per batch
  commits/             ← marker files: batch N is durably written
  state/               ← Spark state-store (watermark/dedup buffers)
  sources/             ← per-source metadata
  metadata             ← query-level metadata (queryId, runId)
```

```bash
# Last committed batch vs. last offset batch — if offsets > commits + 1, last batch was interrupted
aws s3 ls "s3://${BUCKET}/checkpoints/silver_sensors/offsets/" | tail -5
aws s3 ls "s3://${BUCKET}/checkpoints/silver_sensors/commits/" | tail -5

# Read the latest offset file (it's plain text)
LAST_OFFSET=$(aws s3 ls "s3://${BUCKET}/checkpoints/silver_sensors/offsets/" | tail -1 | awk '{print $4}')
aws s3 cp "s3://${BUCKET}/checkpoints/silver_sensors/offsets/${LAST_OFFSET}" -
```

If the offset file is malformed (empty, truncated, or has a JSON parse error), that's checkpoint corruption.

### Watermark check

Silver uses a 10-minute watermark (`config.py:53` → `watermark_delay: "10 minutes"`). If all incoming data is older than `max(event_time) - 10min`, it's all considered late and dropped silently in `dropDuplicatesWithinWatermark`.

```bash
# Pull recent event_timestamps from bronze — are they reasonable?
ssh hadoop@$MASTER_DNS "spark-sql -e \"
  SELECT MIN(decoded.event_timestamp), MAX(decoded.event_timestamp), COUNT(*)
  FROM pulsetrack_bronze_dev.sensor_readings
  WHERE ingestion_date = date_format(current_date(), 'yyyy-MM-dd')
\""
```

If `MAX(event_timestamp) < current_timestamp() - 10 min`, every row is late → watermark won't advance past anything useful.

### Kafka consumer-group state

```bash
ssh hadoop@$MASTER_DNS \
    "kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
     --describe --group spark-kafka-source-silver-sensors-${APP_ID}"
```

If `CONSUMER-ID` cycles every few seconds = rebalance loop; if `LAG` is 0 across all partitions = nothing to consume (real "nothing to do", not a hang).

## Recovery (ranked by likelihood, fastest first)

### Case A: ClassNotFoundError / NoSuchMethodError — ~35% of cases

Symptom: `grep` hit on classpath errors in driver stderr.

**Fix:**
```bash
# Kill the broken app (it'll never recover on its own)
ssh hadoop@$MASTER_DNS "yarn application -kill $APP_ID"
# Verify bootstrap.sh symlinked the jars on the master:
ssh hadoop@$MASTER_DNS "ls /usr/lib/spark/jars/ | grep -E 'iceberg|delta|msk'"
# Missing? Re-run bootstrap manually:
ssh hadoop@$MASTER_DNS "sudo bash /var/lib/aws/aws-emr-bootstrap-action/<bootstrap-uuid>/bootstrap.sh ${BUCKET}"
# Re-submit:
bash scripts/submit_emr_step.sh streaming/silver_ingestion.py
```

### Case B: Checkpoint corruption — ~25%

Symptom: offset file unreadable, OR `offsets/<N>` exists but `commits/<N-1>` doesn't (last batch was interrupted mid-write), OR driver log shows `Exception while reading offset log`.

**The dangerous fix — checkpoint reset.** This causes Kafka offsets to be re-read from `earliest`, which means data already written to silver will be reprocessed. Silver uses `MERGE` (idempotent), so duplicate rows are deduped, but cost goes up linearly with the reset window.

```bash
# Step 1: STOP the query. Don't reset a checkpoint while a query is still touching it.
ssh hadoop@$MASTER_DNS "yarn application -kill $APP_ID"

# Step 2: Snapshot the broken checkpoint before destroying it (forensics):
aws s3 sync "s3://${BUCKET}/checkpoints/silver_sensors/" \
            "s3://${BUCKET}/checkpoints/_quarantine/silver_sensors_$(date +%s)/"

# Step 3: Delete the corrupted checkpoint
aws s3 rm "s3://${BUCKET}/checkpoints/silver_sensors/" --recursive

# Step 4: Restart — startingOffsets=earliest (default in streaming/silver_ingestion.py)
#   will re-read from the beginning of the bronze topic; silver MERGE handles
#   the duplicates. Expect 30-60 min replay window.
bash scripts/submit_emr_step.sh streaming/silver_ingestion.py
```

**Cost note:** the reset reprocesses up to 30 days of bronze data (S3 lifecycle on `checkpoints/` is 30 days — see `infrastructure/modules/storage/main.tf`). For long-lived corruption, consider explicitly setting `startingOffsets` to a timestamp slightly before the corruption window.

### Case C: Watermark blocked by late data — ~20%

Symptom: `MAX(event_timestamp)` from the diagnosis query is significantly behind wall-clock; producer is generating data with timestamps in the past (backfill, clock skew, paused producer that just resumed).

**Fix path 1 — producer is correct, watermark is too tight:**
```bash
# Temporarily widen the watermark and restart
PT_WATERMARK_DELAY="2 hours" \
    bash scripts/submit_emr_step.sh streaming/silver_ingestion.py
# After the backlog clears, restore the default ("10 minutes" in config.py:53)
```

**Fix path 2 — producer clock is wrong:**
SSH into the producer host, check `timedatectl`, fix the clock. This is rare but happened once during chaos drill 2 (see postmortem).

### Case D: Kafka rebalance loop — ~10%

Symptom: `kafka-consumer-groups.sh --describe` cycles every few seconds; driver stderr has repeated `Group <id> joined` / `Group <id> left` messages.

**Cause:** session timeout (`session.timeout.ms` default 45s) is shorter than the time the consumer takes to process a batch. With Iceberg compaction running in the same JVM, batches can stall briefly and trip the rebalance.

**Fix:**
```bash
ssh hadoop@$MASTER_DNS "yarn application -kill $APP_ID"
# Re-submit with larger session timeout (Kafka client option, set via Spark option):
spark-submit ... \
    --conf "spark.sql.streaming.kafka.consumer.cache.timeout=900s" \
    --conf "spark.streaming.kafka.consumer.poll.ms=120000" \
    streaming/silver_ingestion.py
```

### Case E: GX suite slow load — ~10%

Symptom: first batch never finishes; driver stack trace shows `at data_quality.gx_config.validate`; no Kafka offset advancement.

**Cause:** GX 1.x materializes the suite definitions lazily on first use; if Glue catalog calls or schema registry fetches are slow, the first validate can take minutes.

**Quick fix — skip the gate for cold start:**
```bash
PT_GX_GATE_ENABLED=false \
    bash scripts/submit_emr_step.sh streaming/silver_ingestion.py
# Get the stream caught up; then restart with the gate re-enabled.
```

(Note: bronze gates are advisory — never block; silver gates are blocking. See `data_quality/expectations/silver_sensor_suite.py` module docstring for the design choice.)

### Escalation: full app restart didn't help

If you've killed the app, cleared the checkpoint, and a fresh start STILL produces 0 output:

```bash
# 1. Confirm bronze is actually receiving data:
ssh hadoop@$MASTER_DNS "spark-sql -e \"
  SELECT COUNT(*) FROM pulsetrack_bronze_dev.sensor_readings
  WHERE ingestion_timestamp > current_timestamp() - INTERVAL 5 MINUTES
\""
# 2. If bronze == 0 → pivot to runbooks/kafka_consumer_lag.md (the bronze stream is down)
# 3. If bronze > 0 → escalate to data-eng on-call; this is a code-path bug, not an ops fix
```

## Verification (how you know it's fixed)

1. `numOutputRows > 0` on the next batch:
   ```bash
   curl -s "http://$MASTER_DNS:4040/api/v1/applications/${APP_ID}/streaming/batches?limit=3" \
       | jq '.[] | {batchId, numInputRows, numOutputRows, processingTime}'
   ```
2. Silver Iceberg table receiving commits:
   ```sql
   SELECT MAX(committed_at) FROM pulsetrack_silver_dev.sensor_readings.snapshots;
   ```
3. Freshness probe passes:
   ```bash
   python -m observability.monitors --table silver_sensor --check freshness
   ```
4. Five consecutive batches have `numOutputRows > 0` and no warnings in driver log

## Prevention (post-incident hardening)

1. **Cold-start probe:** add a Prefect-scheduled "did the silver stream make progress in the last 10 min" check. Catches Case A/B/D before the 5-min page threshold.
2. **Checkpoint corruption monitor:** scheduled job that lists `offsets/` and `commits/` — if `len(offsets) - len(commits) > 2`, page (an in-flight batch is one ahead; two ahead means the stream is wedged).
3. **Watermark dashboard:** Grafana panel showing `max(event_timestamp)` per batch. If it goes flat while the producer is alive, the watermark is stuck.
4. **GX suite warmup:** call `prepare_for_validation` once at app start before the first microbatch, not lazily — moves the slow-import cost from a batch to the startup.
5. **Wire this runbook URL** into the SNS alert payload via `observability/alerting.py`.

## Related postmortems

- `postmortems/2026-05-02_silver_cold_start_hang.md` — the postmortem that drove this runbook; Case B (checkpoint corruption after disk full on master)
- `postmortems/2026-05-09_chaos_drill_2_app_kill.md` — controlled `yarn kill -appId` drill; recovery procedure overlaps with the killed-app pattern

## Related runbooks

- `runbooks/kafka_consumer_lag.md` — adjacent symptom when bronze is also stuck
- `runbooks/emr_step_failure.md` — if the silver step actually FAILED rather than hanging
- `runbooks/dlq_buildup.md` — DLQ can grow during a hung silver as bronze backs up its failure path
- `runbooks/s3_503_throttling.md` — checkpoint writes can be throttled during a restart storm
