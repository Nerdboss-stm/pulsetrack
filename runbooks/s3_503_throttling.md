# Runbook: S3 503 SlowDown throttling

**Severity ladder:**
- SEV3: any non-zero `5xxErrors` on the lakehouse bucket for < 5 min, no write-side stalls
- SEV2: `5xxErrors > 50/min` for > 5 min OR Spark stage task-retry rate > 5%
- SEV1: streaming queries falling behind (`consumer_lag` rising) AND `5xxErrors > 200/min`, OR any bronze write stage hits the Spark retry cap (4) and the batch fails

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

## TL;DR (30-second triage)

```bash
# 1. Are we actually seeing 503s? (CloudWatch AWS/S3 namespace — see modules/storage/main.tf)
aws cloudwatch get-metric-statistics \
    --namespace AWS/S3 \
    --metric-name 5xxErrors \
    --dimensions Name=BucketName,Value=$BUCKET Name=FilterId,Value=bucket-wide \
    --start-time "$(date -u -v-15M '+%Y-%m-%dT%H:%M:%SZ')" \
    --end-time   "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --period 60 --statistics Sum

# 2. Which prefix is hot? (reversed-id partitioning lays this out as bronze/sensor_readings/rid=<reversed>/...)
aws s3 ls "s3://${BUCKET}/bronze/sensor_readings/" | head -20

# 3. Are Spark tasks retrying?
ssh hadoop@$MASTER_DNS "yarn application -list -appStates RUNNING" | grep pulsetrack
# Open Spark UI at :18080 → app → Stages → look for "Failed Tasks > 0" or "Tasks Killed: SlowDown"
```

S3 503 SlowDown surfaces three ways:
1. **Concentrated writes on one prefix** — reversed-ID partitioning is supposed to spread load; if a single producer is overrepresented it doesn't
2. **Snapshot commit storm** — checkpoint interval too aggressive, many tiny commits hitting `metadata/`
3. **Genuine prefix throttle** — partition split hasn't propagated yet (S3 silently splits at ~3500 PUT/s; you can outrun the auto-split during a burst)

## Symptoms (what triggered the page)

- CloudWatch alarm `pulsetrack-{env}-s3-slowdown` ACTIVE (`AWS/S3 5xxErrors` Sum > 50 over 5 min)
- Prometheus `records_processed_total` plateaued while producer rate is healthy
- Spark UI shows task retries with `AmazonS3Exception: Please reduce your request rate` (status 503, code `SlowDown`)
- DLQ rate climbing because foreachBatch is retrying then giving up
- SNS topic `arn:aws:sns:us-east-1:960341592614:pulsetrack-dev-alerts` fired
- Iceberg commit failures in driver log: `CommitFailedException: ... S3 SlowDown`

## Diagnosis (commands to run first)

### Confirm the 503s — bucket-wide vs. prefix-scoped

```bash
# Bucket-wide (FilterId=bucket-wide — see aws_s3_bucket_metric.bucket_wide)
aws cloudwatch get-metric-statistics \
    --namespace AWS/S3 --metric-name 5xxErrors \
    --dimensions Name=BucketName,Value=$BUCKET Name=FilterId,Value=bucket-wide \
    --start-time "$(date -u -v-1H '+%Y-%m-%dT%H:%M:%SZ')" \
    --end-time   "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --period 60 --statistics Sum
```

If the count is non-trivial, get a request-rate baseline:

```bash
aws cloudwatch get-metric-statistics \
    --namespace AWS/S3 --metric-name AllRequests \
    --dimensions Name=BucketName,Value=$BUCKET Name=FilterId,Value=bucket-wide \
    --start-time "$(date -u -v-1H '+%Y-%m-%dT%H:%M:%SZ')" \
    --end-time   "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --period 60 --statistics Sum
```

503 rate / All rate > 0.5% = real problem. < 0.1% = noise, leave it.

### Find the hot prefix

Reversed-id partitioning (see `lakehouse/partition_strategy.py`, `ReversedIdStrategy`) lays bronze out as:
```
s3://$BUCKET/bronze/sensor_readings/rid=<reversed_device_id>/dt=<date>/
```

Count objects per top-level `rid=` slot:

```bash
aws s3 ls "s3://${BUCKET}/bronze/sensor_readings/" \
    | awk '{print $2}' \
    | while read prefix; do
        count=$(aws s3 ls "s3://${BUCKET}/bronze/sensor_readings/${prefix}" --recursive --summarize \
                | tail -2 | head -1 | awk '{print $3}')
        echo "$prefix $count"
      done | sort -k2 -n -r | head -20
```

Healthy: top 20 slots within ~3x of each other. Unhealthy: one slot has 100x more objects than median → that producer dominates and is overflowing the prefix.

### Spark task-retry counts

```bash
# Open Spark UI on master:18080 → app → Stages
# A healthy bronze write stage: 0 failed tasks, 0 killed tasks
# Throttled:
#   - "Failed Tasks" > 0 with error containing "SlowDown" / "503"
#   - "Task Retries" > 1 on a non-trivial fraction of tasks
#
# Or via REST:
curl -s "http://$MASTER_DNS:18080/api/v1/applications/$APP_ID/stages" \
    | jq '.[] | select(.numFailedTasks > 0) | {stageId, name, numFailedTasks, numCompletedTasks}'
```

### Driver-log scan

```bash
# Logs are uploaded to s3://$BUCKET/emr-logs/<cluster>/containers/ (see modules/compute/main.tf log_uri)
aws s3 ls "s3://${BUCKET}/emr-logs/${CLUSTER_ID}/containers/" --recursive \
    | grep stderr | tail
# Then pull a recent one:
aws s3 cp "s3://${BUCKET}/emr-logs/${CLUSTER_ID}/containers/application_<id>/container_<id>/stderr" - \
    | grep -E 'SlowDown|503|CommitFailedException' | head
```

## Recovery (ranked by likelihood, fastest first)

### Case A: Snapshot commit storm (most common, ~50% of pages)

Symptom: `5xxErrors` spike correlates with end-of-microbatch boundaries; Iceberg `CommitFailedException` in driver log; `metadata/` PUTs dominate `PutRequests`.

**Root cause:** `trigger_interval` (default `30 seconds`, see `config.py:50`) is too aggressive. Each commit writes 3-5 metadata objects to `metadata/` plus snapshot manifests; at 30s × N tables we're hitting the per-prefix PUT ceiling on `metadata/`.

**Fix — widen the trigger window:**
```bash
# Restart the stream with a wider window (60-120s is the safe zone for steady-rate streams)
PT_TRIGGER_INTERVAL="90 seconds" \
    bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py
```

**Then compact** — small files are both cause and symptom:
```sql
-- Run from EMR master via spark-sql
CALL glue_iceberg.system.rewrite_data_files(
    table => 'pulsetrack_bronze_dev.sensor_readings',
    options => map('min-input-files', '5', 'target-file-size-bytes', '536870912')
);
CALL glue_iceberg.system.rewrite_manifests(
    table => 'pulsetrack_bronze_dev.sensor_readings'
);
```

Verification: `5xxErrors` should drop within one trigger interval.

### Case B: One prefix is hot (concentrated producer)

Symptom: prefix-count distribution from the diagnosis step shows one `rid=` slot with > 10x the median.

**Root cause:** reversed-id partitioning maps `device_id → rid` by string-reverse; if one synthetic-test producer is using a fixed `device_id` (or a narrow id prefix that all reverse to the same first byte), it lands on one S3 prefix.

**Fix — rewrite to redistribute:**
```sql
CALL glue_iceberg.system.rewrite_data_files(
    table => 'pulsetrack_bronze_dev.sensor_readings',
    where => 'rid LIKE ''<hot_prefix>%''',
    options => map('target-file-size-bytes', '268435456')
);
```

If the hot producer is a misbehaving test generator, kill it at the source (`config.py` → `wearable_events_per_second`).

If the hot producer is real, switch the partition strategy for new writes:
```bash
PT_PARTITION_STRATEGY=hash_bucket \
    bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py \
    -- --partition-strategy hash_bucket
```

(See `lakehouse/partition_strategy.py` — `HashBucketStrategy` uniformly distributes regardless of id skew, at the cost of grep-ability.)

### Case C: Genuine prefix throttle, partition split lagging

Symptom: both A and B look clean; 503s are bucket-wide; happens during a known burst (backfill, replay).

**Mitigations:**

1. **Narrow `maxOffsetsPerTrigger`:** `config.py:51` default 10,000 → drop to 5,000 to smooth load:
   ```bash
   PT_MAX_OFFSETS_PER_TRIGGER=5000 \
       bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py
   ```

2. **Open an AWS support ticket** requesting an S3 prefix split (Case III "request rate increase"). Include:
   - Bucket name: `pulsetrack-lakehouse-${env}-${suffix}`
   - Region: `us-east-1`
   - Expected sustained PUT rate (records/s × tables × commit-fan-out)
   - 10-min window of CloudWatch 5xx data as attachment

   AWS typically pre-splits within 24 hr. Until then, throttle the producer (Case A backstop).

3. **Reduce Iceberg metadata churn** — set `commit.manifest-merge.enabled=true` and bump `commit.manifest.min-count-to-merge` so fewer manifest files are written per commit.

## Verification (how you know it's fixed)

1. `5xxErrors` Sum back under 5/min:
   ```bash
   aws cloudwatch get-metric-statistics --namespace AWS/S3 --metric-name 5xxErrors \
       --dimensions Name=BucketName,Value=$BUCKET Name=FilterId,Value=bucket-wide \
       --start-time "$(date -u -v-5M '+%Y-%m-%dT%H:%M:%SZ')" \
       --end-time   "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
       --period 60 --statistics Sum
   ```
2. Spark UI Stages tab: 0 failed tasks on bronze write stages for two consecutive batches
3. Iceberg snapshot history advancing:
   ```sql
   SELECT committed_at, snapshot_id FROM pulsetrack_bronze_dev.sensor_readings.snapshots
   ORDER BY committed_at DESC LIMIT 5;
   ```
4. `consumer_lag` (see `runbooks/kafka_consumer_lag.md`) declining

## Prevention (post-incident hardening)

1. **Nightly compaction:** ensure `maintenance/compaction.py` runs nightly (Prefect deployment). Small files multiply the metadata load and pre-stage Case A.
2. **Prefix-distribution monitor:** add a daily check that computes the per-`rid=` object-count gini coefficient — alert when > 0.7 (skewed).
3. **Trigger-interval baseline:** confirm `PT_TRIGGER_INTERVAL` is at the documented floor (60s for bronze, 120s for silver). The 30s default in `config.py` is a local-dev convenience; production should override.
4. **Wire the runbook URL** into the SNS alert payload (`observability/alerting.py`) so the next on-call doesn't have to dig.

## Related postmortems

- `postmortems/2026-05-02_silver_cold_start_hang.md` — orthogonal but commit-storm-adjacent
- `postmortems/2026-05-09_chaos_drill_2_app_kill.md` — checkpoint-replay scenario; tail end overlaps if recovery happens to coincide with throttling

## Related runbooks

- `runbooks/kafka_consumer_lag.md` — downstream symptom when 503s slow the bronze write
- `runbooks/emr_step_failure.md` — if a step fails because of `CommitFailedException` (S3 SlowDown root cause)
- `runbooks/silver_cold_start_hang.md` — adjacent if a restart catches a throttled S3
