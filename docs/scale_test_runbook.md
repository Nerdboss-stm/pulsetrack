# Scale test runbook — operational doc

The procedural complement to `scale_test_capacity_plan.md`. This is what an operator follows on test day. Every command is copy-pasteable. Every gate has a specific check.

**Audience:** the DE running the test (could be you 3 weeks from now, when you've forgotten the details).

---

## Pre-flight checklist (T-2 hours)

Before clicking go on AWS infrastructure:

- [ ] **Local repo is clean:** `git status` shows no uncommitted changes
- [ ] **AWS profile works:** `AWS_PROFILE=pulsetrack aws sts get-caller-identity` returns your account
- [ ] **SSH key present:** `~/.ssh/pulsetrack-emr.pem` exists with mode 0600
- [ ] **`.env` is populated** with WHOOP, Anthropic, Snowflake, Slack creds (will be migrated to Secrets Manager in step 1)
- [ ] **WHOOP OAuth tokens are fresh** (`ls -la ~/.whoop_tokens.json` — should be modified within last 24h)
- [ ] **`gh` CLI is authenticated** if you want PR-generation working
- [ ] **`prefect` CLI is authenticated** if you want Cloud-side scheduling
- [ ] **Calendar block:** 2 hours uninterrupted (test + observation + close-out)
- [ ] **Coffee.**

## T-30 minutes — Bootstrap secrets

```bash
cd ~/pulsetrack-cm
AWS_PROFILE=pulsetrack PT_AWS_ENV=dev python3 scripts/bootstrap_secrets.py
```

Expected output:
```
Bootstrapping secrets to AWS Secrets Manager (env=dev)

Summary:
  pulsetrack/dev/whoop          OK (updated, 5 fields)
  pulsetrack/dev/whoop-tokens   OK (updated, 3 fields)
  pulsetrack/dev/anthropic      OK (updated, 1 fields)
  pulsetrack/dev/snowflake      OK (updated, 7 fields)
  pulsetrack/dev/slack          OK (updated, 1 fields)
  pulsetrack/dev/pagerduty      OK (updated, 1 fields)

Done. Verify with:  python scripts/bootstrap_secrets.py --check
```

**Gate:** all 6 secrets show `OK`. If any `FAIL`, fix before proceeding.

```bash
AWS_PROFILE=pulsetrack python3 scripts/bootstrap_secrets.py --check
```

## T-25 minutes — Rotate the leaked WHOOP credential

The exploration agent found that the original `.env` committed to git history contained WHOOP client_id + client_secret. That's a real credential leak; treat it as a SEV2 incident.

**Steps:**

1. Visit https://developer.whoop.com → app dashboard → "Generate new client secret"
2. Update Secrets Manager:
   ```bash
   NEW_SECRET="<paste new secret>"
   aws secretsmanager get-secret-value --secret-id pulsetrack/dev/whoop \
       --query SecretString --output text | \
       jq --arg s "$NEW_SECRET" '.client_secret = $s' | \
       aws secretsmanager put-secret-value --secret-id pulsetrack/dev/whoop \
           --secret-string file:///dev/stdin
   ```
3. Revoke the old secret in the WHOOP dashboard
4. Document the rotation in `postmortems/2026-05-09_whoop_secret_in_git.md`
5. Verify:
   ```bash
   python3 scripts/bootstrap_secrets.py --check
   ```

**Gate:** check still passes (the new secret is valid).

## T-20 minutes — Apply infra (with bumped core nodes)

```bash
cd infrastructure
terraform apply -var "emr_core_instance_count=4"
```

**Expected outputs (the 5 critical ones):**
- `emr_cluster_id` = `j-XXXXXXX`
- `emr_master_dns` = `ec2-X-X-X-X.compute-1.amazonaws.com`
- `msk_bootstrap_brokers` = `boot-XXXX.c3.kafka-serverless.us-east-1.amazonaws.com:9098`
- `lakehouse_bucket_name` = `pulsetrack-dev-lakehouse-XXXXXXXX`
- `secrets_kms_key_arn` = `arn:aws:kms:us-east-1:...:key/...`

**Gate:** `terraform output -json` returns all 5 non-empty values.

**Wait 5 min** for EMR cluster to fully bootstrap (state moves WAITING → RUNNING). Verify in console.

## T-10 minutes — Pre-flight credential check

```bash
cd ~/pulsetrack-cm
AWS_PROFILE=pulsetrack PT_AWS_ENV=dev python3 scripts/check_credentials.py
```

**Gate:** all 14 checks PASS. WARNs are OK; FAILs are blockers.

Common WARNs and how to handle:
- `anthropic.ping FAIL` → check `ANTHROPIC_API_KEY` is in `pulsetrack/dev/anthropic`
- `slack.webhook FAIL` → check `pulsetrack/dev/slack` has `webhook_url`
- `snowflake.views WARN` → run `python snowflake/setup/04_create_iceberg_tables.sql` against the right database

## T-5 minutes — Launch the orchestrator

```bash
cd ~/pulsetrack-cm
AWS_PROFILE=pulsetrack PT_AWS_ENV=dev ./scripts/run_scale_test.sh
```

The orchestrator emits to `docs/scale_test_execution_log.md` while running. You'll see:

```
=== PulseTrack scale test — 2026-05-10T14:30:00Z ===
── [14:30:01Z] T-30m Pre-flight credential validator ──
  [PASS] aws.sts        account=960... arn=arn:aws:sts::960...
  ...
```

**Keep this terminal visible.** Open four others:
1. Grafana (or CloudWatch dashboards) — for live observability
2. Prefect Cloud UI — to watch flow runs
3. `ssh hadoop@$MASTER_DNS` → `tmux attach -t pulsetrack` — to watch producers
4. AWS console (EMR + MSK + S3) — for spot checks

## T+0 — First 15 minutes: observation window

Watch:
- **MSK ingress** (Grafana panel: throughput.png): should ramp to 25-35k rec/s within 2 min
- **Producer logs** (tmux master:batch-scale): `window_rate=...` lines every 10K records
- **Bronze write rate** (Spark UI on master:18080): batch durations < 30s
- **Consumer lag** (silver consumer-group): rises during ramp, then plateaus around 5-20K
- **S3 file count** (`aws s3 ls --recursive s3://$BUCKET/bronze/sensor_readings/` | wc -l): should grow by ~24 files / 30s

**What "good" looks like at T+10m:**
- Producer rate: 25k-35k rec/s
- Consumer lag: stable (rising = bad; falling = good; flat = ingest-balanced)
- Spark UI: 0 failed tasks, all batches < 30s
- Bronze file count: 80-120 files

**What "bad" looks like:**
- Producer rate < 10k rec/s → check `BufferError` in logs (network is the bottleneck)
- Consumer lag grows monotonically > 100k → silver can't keep up
- Spark batches > 60s → executor count too low (check YARN UI)
- DLQ topic > 0 events → schema or quality issue

## T+15 — Chaos drill 1: kill executor

Automatic — the orchestrator runs:

```bash
python3 scripts/chaos/kill_spark_task.py \
    --app-name silver_sensor_streaming \
    --recovery-budget-seconds 60
```

**What you'll see in the orchestrator log:**
```
── [14:45:01Z] T+15m Chaos drill 1 — kill ONE silver executor ──
[chaos-1] Target: app_name~='silver_sensor_streaming' host=...
[chaos-1] Found app: application_1746...
[chaos-1] Containers: 7 (5 RUNNING)
[chaos-1] Selected: container_..._000003 on ip-10-X-X-X
[chaos-1] 14:45:08Z Killing container_..._000003
[chaos-1] Waiting for replacement (budget=60s)...
[chaos-1] PASS recovered in 42.0s → new container container_..._000008
```

**Drill SUCCESS criteria:**
- Recovery `<= 60s`
- New container appears in YARN
- Spark Streaming UI shows a momentary blip (1-2 failed/retried tasks) then recovers
- Bronze input rate continues unchanged
- No DLQ events from the drill

**Drill FAILURE (drill exit 1):** Check `docs/chaos_log.jsonl`. Likely causes:
- Executor count too low (YARN couldn't replace) — increase dynamicAllocation.maxExecutors
- Network partition between master and worker — check VPC reachability

10 min observation window after drill 1.

## T+30 — Chaos drill 2: kill entire silver app

Automatic. Larger blast radius.

```bash
python3 scripts/chaos/kill_spark_app.py \
    --app-name silver_sensor_streaming \
    --step-script streaming/silver_ingestion.py \
    --recovery-budget-seconds 300
```

**What you'll see:**
```
── [15:00:01Z] T+30m Chaos drill 2 — kill ENTIRE silver streaming app ──
[chaos-2] Target: app~='silver_sensor_streaming' host=... cluster=j-...
[chaos-2] Found app application_1746... with 7 containers
[chaos-2] 15:00:15Z Killing app application_1746...
[chaos-2] Re-submitting step: streaming/silver_ingestion.py
[chaos-2] New EMR step: s-XXXXX
[chaos-2] Waiting for new app (budget=300s)...
[chaos-2] PASS new app RUNNING after 187s: application_1746...
```

**Drill SUCCESS criteria:**
- New app RUNNING within 300s
- Iceberg checkpoint replayed (driver logs show "Resuming from offsets ...")
- Kafka consumer group resumes from committed offset (no double-processing)
- Producers continue uninterrupted

**Verify no data loss:**
```bash
# On master (post-drill)
spark-sql -e "
    SELECT COUNT(*) FROM pulsetrack_bronze_dev.sensor_readings WHERE ingestion_date = current_date
" -e "
    SELECT COUNT(*) FROM pulsetrack_silver_dev.sensor_readings WHERE event_date = current_date
"
```

Silver count should equal bronze count × ~9 (exploded metrics, depending on device-type mix). Significant gap (>5%) indicates data loss.

## T+45 — Stop producers + drain

Automatic.

```bash
# Orchestrator runs:
ssh hadoop@$MASTER_DNS "tmux send-keys -t pulsetrack:batch-scale C-c && \
                       tmux send-keys -t pulsetrack:whoop-poll C-c && \
                       tmux send-keys -t pulsetrack:openfda C-c && \
                       tmux send-keys -t pulsetrack:fhir C-c"
```

Wait 5 min for the streams to drain. Verify:
- Kafka consumer lag → 0 (all messages consumed)
- Bronze last_modified within last 30s (still committing residuals)
- Silver row count stable for 30s (no more arrivals)

## T+55 — Benchmark report

Automatic. The orchestrator runs:

```bash
python3 benchmarks/scale_test_report.py \
    --output docs/scale_test_results.md \
    --grafana-dir docs/screenshots \
    --emr-cluster-id $CLUSTER_ID \
    --bucket $BUCKET \
    --chaos-log docs/chaos_log.jsonl
```

**Gate:** `docs/scale_test_results.md` has no `<<MEASURE>>` placeholders in mandatory fields.

## T+60 — Capture screenshots

Automatic. 7 panels saved to `docs/screenshots/*.png`.

## T+70 — Consumer-side validation

Manual checks (the orchestrator runs the SELECT COUNT, but you should also eyeball results):

**Snowflake worksheet:**
```sql
-- 1. Patient 360 view returns rows
USE DATABASE PULSETRACK;
USE SCHEMA ANALYTICS;
SELECT COUNT(*), MAX(last_vital_ts) FROM vw_patient_health_360;
-- Expect: rows > 0, last_vital_ts within last 5 min

-- 2. Anomaly dashboard shows critical events
SELECT * FROM vw_anomaly_dashboard
WHERE event_date = CURRENT_DATE
ORDER BY severity_label DESC, event_ts DESC
LIMIT 10;
-- Expect: at least some "critical" or "warning" events from the 0.3% impossible-value seeded data

-- 3. Personal dashboard (your email)
SELECT * FROM vw_whoop_my_health
WHERE patient_email = 'your-email@example.com';
-- Expect: your real WHOOP data if you ran whoop_auth_bootstrap.sh
```

**ML feature query (Python notebook):**
```python
import pandas as pd
from pyathena import connect

conn = connect(s3_staging_dir='s3://your-bucket/_athena_results/')
df = pd.read_sql("""
    SELECT
      f.reading_id, f.event_ts, f.metric_name, f.metric_value,
      p.patient_key, p.age_band, p.sex,
      d.device_type
    FROM pulsetrack_gold_dev.fact_vital_reading f
    JOIN pulsetrack_gold_dev.dim_patient p ON f.patient_key = p.patient_key
    JOIN pulsetrack_gold_dev.dim_device d ON f.device_id = d.device_id
    LIMIT 10000
""", conn)
print(df.head())
print(f"Shape: {df.shape}")
```

Should return ~10k rows with all fields populated. This is the kind of query an ML team would use to build training features.

**Slack channel:**
- Look for `:white_check_mark: PulseTrack` test ping from `check_credentials.py`
- Look for any `:rotating_light:` anomaly alerts from `observability/alerting.py`

## T+90 — Teardown

If everything looks good:

```bash
cd infrastructure && bash teardown-compute.sh
```

This destroys the EMR cluster but **preserves** S3, Glue, IAM, Secrets Manager.

**DO NOT** run `terraform destroy` at the top level — that wipes S3 (data loss). Use the targeted `teardown-compute.sh`.

If you also want to clean up:
- S3 lifecycle rules auto-expire `_preflight/canary.txt`
- Test data partition in bronze/silver/gold has `partition_date = current_date` — you can `DELETE FROM ... WHERE partition_date = '2026-05-10'` if you don't want it cluttering future tests
- Secrets Manager secrets stay (no charge unless retrieved)

## Post-test paperwork

Update / create:
1. `docs/scale_test_results.md` — auto-filled, eyeball + tweak
2. `postmortems/2026-05-XX_chaos_drill_1_*.md` — write within 24h
3. `postmortems/2026-05-XX_chaos_drill_2_*.md` — write within 24h
4. Any organic incident postmortem (e.g., producer crashed → why)
5. `pulsetrack-study/PROMPT_9_REPORT.md` — the exhaustive technical writeup

Then the final commit:

```bash
git add -A
git commit -m "feat(scale-test): 10M execution results — see commits 9b1a229+ for the harness"
git push origin cloud-migration
gh pr create --draft --title "Prompt 9: Scale test + production library"
```

## If you have to abort mid-test

1. **Ctrl+C the orchestrator** (it captures SIGINT, drains gracefully)
2. **If that hangs:** open new terminal, kill the producer tmux:
   ```bash
   ssh hadoop@$MASTER_DNS "tmux kill-server"
   ```
3. **Cancel EMR steps:**
   ```bash
   aws emr list-steps --cluster-id $CLUSTER_ID --step-states PENDING RUNNING \
       --query 'Steps[].Id' --output text | xargs -n1 \
       aws emr cancel-steps --cluster-id $CLUSTER_ID --step-ids
   ```
4. **Tear down compute** (preserves data):
   ```bash
   cd infrastructure && bash teardown-compute.sh
   ```
5. **Write a postmortem.** Even if the test was aborted by you on purpose, a postmortem with the reason + what was learned + what changes for next time is the right artifact.

## What "good" looks like (TL;DR)

A successful test produces:
- `docs/scale_test_results.md` populated with real numbers
- `docs/scale_test_execution_log.md` — full operator timeline
- `docs/chaos_log.jsonl` — 2 successful drill entries
- `docs/screenshots/*.png` — 7 PNG dashboards
- 2 chaos drill postmortems
- 0-1 organic incident postmortems
- AWS cost ≤ $5 actual

If you missed any of those, the test isn't done.
