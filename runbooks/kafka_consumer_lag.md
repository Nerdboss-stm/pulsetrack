# Runbook: Kafka consumer lag growing

**Severity ladder:**
- SEV3: any non-zero lag for <10 min on a steady-rate stream
- SEV2: lag > 100,000 records, OR growing monotonically > 5 min
- SEV1: lag > 1,000,000 records OR any consumer shows ZERO progress for > 10 min

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

## TL;DR (30-second triage)

```bash
# 1. Which consumer group?
ssh hadoop@$MASTER_DNS \
    "kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
     --list" 2>&1 | grep -i pulsetrack

# 2. How much lag?
ssh hadoop@$MASTER_DNS \
    "kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
     --describe --group <group_id>"

# 3. What's the consumer doing? (is it RUNNING, OOM'd, slow, or stuck?)
ssh hadoop@$MASTER_DNS "yarn application -list -appStates RUNNING"
```

Three flavors of consumer-lag pages map to three different fixes:
1. **Consumer is dead** (no executor → restart the streaming app)
2. **Consumer is alive but slow** (insufficient parallelism → scale up)
3. **Consumer is alive and fast but producer outran it** (transient burst → wait)

## Symptoms (what triggered the page)

- Prometheus `consumer_lag{layer="bronze"}` > threshold
- StreamingQueryListener emitted lag in last batch > watermark target
- CloudWatch alarm `pulsetrack-{env}-bronze-stream-lag` ACTIVE
- Silver freshness check failing (`observability/monitors.py` freshness probe)
- Snowflake AUTO_REFRESH appears stale (downstream symptom)

## Diagnosis (commands to run first)

### Identify the lagging group
```bash
ssh hadoop@$MASTER_DNS << 'EOF'
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
    --describe --all-groups --command-config /etc/kafka/iam-client.properties
EOF
```

Output columns: `GROUP, TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, CONSUMER-ID, HOST, CLIENT-ID`.

**Sort by LAG descending.** The top entry is your problem.

### Determine which streaming app owns that group
The consumer-group name maps to the Spark application:
- `spark-kafka-source-bronze-sensors-...` → `streaming/bronze_ingestion.py`
- `spark-kafka-source-bronze-pharmacy-...` → `streaming/pharmacy_bronze_ingestion.py`
- `spark-kafka-source-silver-sensors-...` → `streaming/silver_ingestion.py`

```bash
yarn application -list -appStates RUNNING
```

### Is the consumer alive?
```bash
# Spark UI on master:18080 — open the app, check:
#   - Streaming tab: batch durations rising? (= slow)
#   - Executors tab: any with red task counts? (= OOM or task failures)
#   - Stages: any failed jobs?
#
# Or via CLI:
yarn application -status <appId> | grep -E 'State|Progress'
```

### Spark microbatch durations
```bash
# Pull last 100 batch durations from Spark history server
curl -s "http://$MASTER_DNS:18080/api/v1/applications/$APP_ID/streaming/batches?limit=100" \
    | jq '.[].batchDuration' | awk '{ s+=$1; n++ } END { print "avg=" s/n "ms n=" n }'
```

Target: < 30,000 ms (= 30s). Anything > 60,000 ms = stream can't keep up.

## Recovery (ranked by likelihood, fastest first)

### Case A: Producer burst (most common, 60% of pages)
The producer rate spiked → consumer is catching up.

**Verification:**
```bash
# Producer rate from Prometheus (8000 = wearable, 8001 = bronze sensor, etc.)
curl -s http://$MASTER_DNS:8000/metrics | grep -E 'records_processed_total'
```

If producer rate has *just dropped* and lag is *declining*, do nothing. ETA to drain = `current_lag / consumer_throughput_per_sec`. Acknowledge the page, set a 15-min snooze, re-check.

### Case B: Consumer is alive but slow

**Diagnose:**
- Spark UI Streaming tab: batch duration > microbatch trigger interval (30s default)
- Check Iceberg compaction: many small data files = slow writes

**Remediations (in order):**

1. **Increase shuffle partitions:**
   ```bash
   # On master, dynamically adjust by restarting with more partitions
   spark-submit --conf spark.sql.shuffle.partitions=8 \
       streaming/bronze_ingestion.py --mode streaming
   ```

2. **Increase Spark executor count via dynamic allocation:**
   ```bash
   --conf spark.dynamicAllocation.maxExecutors=16
   ```
   (Current default 12; bump if YARN has resources.)

3. **Increase `maxOffsetsPerTrigger`** in `config.py` from 10,000 → 50,000. Trade-off: larger batches lower overhead but increase peak memory.

4. **Compact Iceberg:** Small files (= many writes per batch) make consumer slow. Run:
   ```sql
   CALL glue_iceberg.system.rewrite_data_files(
       table => 'pulsetrack_bronze_dev.sensor_readings',
       options => map('min-input-files', '5')
   )
   ```

### Case C: Consumer is dead

YARN shows app FINISHED or KILLED, or `kafka-consumer-groups.sh` shows CONSUMER-ID = `-`.

**Verification:**
```bash
yarn application -list -appStates FINISHED,FAILED,KILLED | head
```

**Recovery:**
1. **Restart the streaming app:**
   ```bash
   bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py
   ```
2. **Check the checkpoint dir is intact:**
   ```bash
   aws s3 ls s3://$BUCKET/checkpoints/bronze_sensors/ --recursive | tail -5
   ```
3. If checkpoint is corrupted (rare): see `runbooks/silver_cold_start_hang.md` for checkpoint-recovery procedure.

### Case D: Stream is alive but `LOG-END-OFFSET` is not moving

Producer side died. Check the upstream:
```bash
# tmux session on master
ssh hadoop@$MASTER_DNS "tmux list-sessions"
# Check producer log
ssh hadoop@$MASTER_DNS "tail -20 /tmp/batch-scale.log"
```

If producer is dead but Kafka shows stale offsets, this is a **producer outage**, NOT a consumer lag problem. Pivot to producer triage (see `runbooks/emr_step_failure.md` and producer-specific logs).

## Verification (how you know it's fixed)

After recovery:
1. `kafka-consumer-groups.sh --describe ...` shows LAG declining
2. Spark UI Streaming tab: batch duration < trigger interval
3. Iceberg `sensor_readings` table receives new data files (check `aws s3 ls`)
4. Silver layer freshness check passes:
   ```bash
   python -m observability.monitors --table silver_sensor --check freshness
   ```

Wait 5 min after applying remediation. Lag should be ≤ pre-page steady-state.

## Prevention (post-incident hardening)

After resolving:
1. **Compaction schedule:** ensure `maintenance` Prefect deployment is running nightly. Check via Prefect UI or:
   ```bash
   prefect deployment ls | grep maintenance
   ```
2. **Lag alerting threshold:** if a previously normal lag triggered a page, calibrate alert threshold (`observability/sql/monitor_spec.yaml`)
3. **Producer rate cap:** if Case A is recurring, check producer's `wearable_events_per_second` in `config.py` — should never exceed consumer throughput
4. **Add a runbook reference** to the alert payload in `observability/alerting.py` so the next on-call has this doc one click away

## Related postmortems

- `postmortems/2026-05-02_silver_cold_start_hang.md` — checkpoint-recovery on app restart
- `postmortems/2026-05-09_chaos_drill_2_app_kill.md` (Phase 2) — graduated drill of this exact scenario

## Related runbooks

- `runbooks/silver_cold_start_hang.md` — checkpoint corruption / replay
- `runbooks/emr_step_failure.md` — streaming app died (Case C extension)
- `runbooks/dlq_buildup.md` — symptoms can overlap when batches partially fail
