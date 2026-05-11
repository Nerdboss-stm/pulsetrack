#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# PulseTrack — 10M event scale test orchestrator.
#
# This is the master script for Phase 2 of prompt 9. Runs the full
# end-to-end pipeline at scale with all 4 producers concurrently,
# graduated chaos engineering (two drills), and post-test metrics
# collection.
#
# Total runtime (expected): ~95 minutes from T-30m to teardown.
# AWS cost (expected):       ~$5-8 (EMR + MSK Serverless + S3 PUTs).
#
# Run from repo root:
#     AWS_PROFILE=pulsetrack PT_AWS_ENV=dev ./scripts/run_scale_test.sh
#     AWS_PROFILE=pulsetrack PT_AWS_ENV=dev ./scripts/run_scale_test.sh --smoke 1000
#
# --smoke N flag: runs the full pipeline end-to-end with N events instead of 10M.
# This validates every step (producer → bronze → silver → identity_bridge →
# gold → monitors → OPTIMIZE) in ~5 min before committing to a full 10M / $50
# run. Catches orchestrator bugs without burning AWS budget.
#
# Stdout + stderr are also tee'd to docs/scale_test_execution_log.md
# (the live operator journal — the postmortems anchor here for
# minute-granular timeline references).
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# ── CLI flag parsing ──────────────────────────────────────────────────────
EVENT_COUNT=10000000   # default: full 10M run
USER_COUNT=50000
SMOKE_MODE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --smoke)
            EVENT_COUNT="${2:-1000}"
            USER_COUNT=$(( EVENT_COUNT / 20 ))  # ~20 events per user for smoke
            [[ "$USER_COUNT" -lt 1 ]] && USER_COUNT=1
            SMOKE_MODE=1
            shift 2
            ;;
        *)
            echo "Unknown flag: $1" >&2
            exit 64
            ;;
    esac
done

LOG=docs/scale_test_execution_log.md
[[ "$SMOKE_MODE" -eq 1 ]] && LOG=docs/scale_test_smoke_log.md
mkdir -p docs
exec > >(tee -a "$LOG") 2>&1

# ── Banner ────────────────────────────────────────────────────────────────
TEST_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
MODE_LABEL="full-10M"
[[ "$SMOKE_MODE" -eq 1 ]] && MODE_LABEL="smoke-${EVENT_COUNT}"
echo ""
echo "==============================================================================="
echo "PulseTrack scale test — $TEST_START — mode=$MODE_LABEL"
echo "  event_count=$EVENT_COUNT user_count=$USER_COUNT"
echo "==============================================================================="
echo ""

# Helper: timestamped log line
log_step() {
    echo ""
    echo "── [$(date -u +%H:%M:%SZ)] $1 ──"
}

# Helper: abort with code
abort() {
    echo ""
    echo "ABORT: $1"
    exit "${2:-1}"
}

# ── Phase 0: Pre-flight ───────────────────────────────────────────────────
log_step "T-30m Pre-flight credential validator"
if ! python3 scripts/check_credentials.py; then
    abort "Pre-flight check failed. Fix FAILs before continuing." 1
fi

log_step "T-29m Verifying terraform state"
cd infrastructure
CLUSTER_ID=$(terraform output -raw emr_cluster_id 2>/dev/null || echo "")
MASTER_DNS=$(terraform output -raw emr_master_dns 2>/dev/null || echo "")
BOOTSTRAP=$(terraform output -raw msk_bootstrap_brokers 2>/dev/null || echo "")
BUCKET=$(terraform output -raw lakehouse_bucket_name 2>/dev/null || echo "")
cd "$REPO_ROOT"

[[ -z "$CLUSTER_ID" ]] && abort "EMR cluster_id not in TF output — apply infra first"
[[ -z "$BOOTSTRAP" ]] && abort "MSK bootstrap brokers not in TF output"
[[ -z "$BUCKET" ]] && abort "Lakehouse bucket not in TF output"

echo "  cluster_id=$CLUSTER_ID"
echo "  master_dns=$MASTER_DNS"
echo "  msk=$BOOTSTRAP"
echo "  bucket=$BUCKET"

# ── Phase 1: Apply migrations ─────────────────────────────────────────────
log_step "T-25m Apply pending Glacierbase migrations"
python3 -m migrations.cli run 2>&1 || echo "  WARN: migrations failed or already applied (continuing)"

# ── Phase 2: Deploy code to EMR ───────────────────────────────────────────
log_step "T-20m Sync project to EMR master via S3"

# Upload 1: full source tree as individual files so spark-submit can resolve
# s3://${BUCKET}/code/streaming/bronze_ingestion.py etc. directly.
echo "  syncing source tree to s3://${BUCKET}/code/"
aws s3 sync . "s3://${BUCKET}/code/" \
    --exclude '.git/*' \
    --exclude '.venv/*' \
    --exclude 'venv/*' \
    --exclude '__pycache__/*' \
    --exclude '*/__pycache__/*' \
    --exclude '*.pyc' \
    --exclude '.env*' \
    --exclude 'infrastructure/.terraform/*' \
    --exclude 'spark-warehouse/*' \
    --exclude 'dbt_project/target/*' \
    --exclude 'dbt_project/logs/*' \
    --exclude 'docs/*' \
    --exclude 'runbooks/*' \
    --exclude 'postmortems/*' \
    --exclude 'pulsetrack-study/*' \
    --exclude '*.tar.gz' \
    --exclude '*.zip' \
    --quiet

# Upload 2: project.zip for Spark --py-files (resolves imports in YARN containers).
# Includes all .py modules at their package paths so `from config import settings`,
# `from pt_secrets import ...` etc. work inside the driver / executors.
ZIPFILE=/tmp/pulsetrack-deps.zip
rm -f "$ZIPFILE"
( cd "$REPO_ROOT" && \
  find . \( -name '*.py' -o -name '*.avsc' -o -name '*.json' \) \
      -not -path './.venv/*' \
      -not -path './venv/*' \
      -not -path './.git/*' \
      -not -path '*/__pycache__/*' \
      -not -path './infrastructure/*' \
      -not -path './dbt_project/target/*' \
      -not -path './dbt_project/logs/*' \
      -not -path './docs/*' \
      -not -path './runbooks/*' \
      -not -path './postmortems/*' \
      -not -path './pulsetrack-study/*' \
      -not -path './tests/*' \
      | zip -q -@ "$ZIPFILE" )
aws s3 cp "$ZIPFILE" "s3://${BUCKET}/code/pulsetrack-deps.zip" --quiet
echo "  uploaded deps zip: $(du -h $ZIPFILE | cut -f1)"

# Upload 3: schemas + .avsc files (Avro descriptors loaded at runtime).
aws s3 sync schemas/ "s3://${BUCKET}/code/schemas/" --exclude '__pycache__/*' --quiet || true

# Upload 4: keep the legacy tarball at the expected path for the producer
# tmux launch step (Phase 4 extracts it on the EMR master at /home/hadoop/pulsetrack).
TARBALL=/tmp/pulsetrack-scale-test.tar.gz
tar czf "$TARBALL" \
    --exclude='.git' --exclude='.venv' --exclude='venv' --exclude='__pycache__' \
    --exclude='*.pyc' --exclude='infrastructure/.terraform' \
    --exclude='spark-warehouse' --exclude='dbt_project/target' \
    --exclude='.env' --exclude='.env.cloud' \
    .
aws s3 cp "$TARBALL" "s3://${BUCKET}/code/pulsetrack-scale-test.tar.gz" --quiet
echo "  uploaded producer tarball: $(du -h $TARBALL | cut -f1)"

# ── Phase 3: Start streaming pipeline ─────────────────────────────────────
# Helper function — submit a Spark step in the flattened AWS CLI format
# (the legacy nested HadoopJarStep wrapper is rejected by current aws-cli).
#
# Uses --py-files to ship project.zip alongside the entry-point script so
# imports (`from config import settings`, `from pt_secrets import ...`) resolve
# on YARN driver + executor containers, not just on the master.
submit_spark_step() {
    local name="$1"
    local script_s3_key="$2"
    # Extra CLI args for the script — pass as space-separated; built into the
    # JSON Args array below.
    local extra_args_str="${3:-}"
    # Spark dependencies. spark-sql-kafka-0-10 + spark-avro NOT in EMR stock.
    # MSK IAM auth jar (aws-msk-iam-auth-2.3.2.jar) IS pre-installed on EMR 7.13.
    #
    # NOTE: Originally used --packages org.apache.spark:spark-sql-kafka-0-10:3.5.3
    # but 4 simultaneous spark-submits competing for the same Maven downloads hit
    # Maven Central rate limits and got 0-byte truncated JARs. Replaced with
    # pre-staged S3 JARs to eliminate Maven from the runtime path entirely.
    # See postmortems/2026-05-11_emr_cluster_bringup_13_incidents.md item #14.
    local spark_jars="s3://${BUCKET}/spark-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar"
    spark_jars="${spark_jars},s3://${BUCKET}/spark-jars/spark-avro_2.12-3.5.3.jar"
    spark_jars="${spark_jars},s3://${BUCKET}/spark-jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar"
    spark_jars="${spark_jars},s3://${BUCKET}/spark-jars/kafka-clients-3.4.1.jar"
    spark_jars="${spark_jars},s3://${BUCKET}/spark-jars/commons-pool2-2.11.1.jar"
    spark_jars="${spark_jars},s3://${BUCKET}/spark-jars/lz4-java-1.8.0.jar"
    spark_jars="${spark_jars},s3://${BUCKET}/spark-jars/snappy-java-1.1.10.5.jar"

    # Use JSON file for --steps because the bracketed Args=[...] syntax uses
    # commas as separators — that breaks --packages (which itself uses
    # comma-separated package coords). JSON has no such ambiguity.
    local steps_json
    steps_json=$(mktemp /tmp/emr-step-XXXXXX.json)
    python3 -c "
import json, os, sys, shlex
extra = shlex.split('''${extra_args_str}''')
step = [{
    'Type': 'CUSTOM_JAR',
    'Name': '${name}',
    'ActionOnFailure': 'CONTINUE',
    'Jar': 'command-runner.jar',
    'Args': [
        'spark-submit', '--deploy-mode', 'cluster',
        '--jars', '${spark_jars}',
        '--conf', 'spark.pyspark.python=/usr/bin/python3.11',
        # spark.yarn.am.waitTime — default 100s is too short for our Python
        # driver startup (deps.zip extraction + imports + Spark/Iceberg/Kafka
        # session init blows past 100s, AM future times out, app FAILS even
        # though Python script logged 'Bronze running' successfully).
        '--conf', 'spark.yarn.am.waitTime=600s',
        '--conf', 'spark.network.timeout=600s',
        '--conf', 'spark.yarn.appMasterEnv.PT_ENVIRONMENT=cloud',
        '--conf', 'spark.yarn.appMasterEnv.PT_AWS_ENV=dev',
        '--conf', 'spark.yarn.appMasterEnv.PT_LAKEHOUSE_BASE=s3://${BUCKET}',
        # PT_EHR_BATCH_DIR must be S3 in cluster mode — fhir_producer
        # writes to master FS, then run_scale_test.sh syncs to S3 before
        # ehr_silver runs. dim_patient reads from this same path.
        '--conf', 'spark.yarn.appMasterEnv.PT_EHR_BATCH_DIR=s3://${BUCKET}/ehr-batches',
        '--conf', 'spark.yarn.appMasterEnv.PT_KAFKA_BOOTSTRAP=${BOOTSTRAP}',
        '--conf', 'spark.yarn.appMasterEnv.PT_KAFKA_SECURITY_PROTOCOL=SASL_SSL',
        '--conf', 'spark.yarn.appMasterEnv.PT_ICEBERG_CATALOG_TYPE=glue',
        '--conf', 'spark.yarn.appMasterEnv.PT_GLUE_ICEBERG_WAREHOUSE=s3://${BUCKET}/iceberg/warehouse',
        '--conf', 'spark.yarn.appMasterEnv.PT_SHUFFLE_PARTITIONS=200',
        '--conf', 'spark.yarn.appMasterEnv.PT_MAX_OFFSETS_PER_TRIGGER=50000',
        '--conf', 'spark.yarn.appMasterEnv.AWS_DEFAULT_REGION=us-east-1',
        '--conf', 'spark.executorEnv.PT_ENVIRONMENT=cloud',
        '--conf', 'spark.executorEnv.PT_AWS_ENV=dev',
        '--conf', 'spark.executorEnv.PT_LAKEHOUSE_BASE=s3://${BUCKET}',
        '--conf', 'spark.executorEnv.PT_KAFKA_BOOTSTRAP=${BOOTSTRAP}',
        '--conf', 'spark.executorEnv.PT_KAFKA_SECURITY_PROTOCOL=SASL_SSL',
        '--conf', 'spark.executorEnv.AWS_DEFAULT_REGION=us-east-1',
        '--py-files', 's3://${BUCKET}/code/pulsetrack-deps.zip',
        's3://${BUCKET}/code/${script_s3_key}',
    ] + extra,
}]
json.dump(step, open('${steps_json}', 'w'))
"
    aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "file://${steps_json}" \
        --query 'StepIds[0]' --output text
    rm -f "$steps_json"
}

# ── Phase 2.5: Create Kafka topics before streaming subscribes ────────────
# MSK Serverless does NOT auto-create topics; consumers fail with
# UnknownTopicOrPartitionException if they subscribe before producer creates.
# Pre-create via the existing scripts/produce_sensor_records.py's admin path
# (it does ensure_topic for sensor_readings; we need pharmacy_events too).
log_step "T-16m Create Kafka topics (sensor_readings, pharmacy_events, pulsetrack_dlq)"
# Run admin from EMR master (same VPC) — local execution times out on MSK
# Serverless metadata calls due to higher cross-region latency.
ssh -i ~/.ssh/pulsetrack-emr.pem -o StrictHostKeyChecking=no -o BatchMode=yes \
    "hadoop@$MASTER_DNS" \
    "AWS_DEFAULT_REGION=us-east-1 /usr/bin/python3.11 - <<'PYEOF'
import socket, sys, time
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

REGION = 'us-east-1'
BROKERS = '${BOOTSTRAP}'
TOPICS = [('sensor_readings', 4), ('pharmacy_events', 1), ('pulsetrack_dlq', 1)]

def oauth_cb(_):
    tok, exp = MSKAuthTokenProvider.generate_auth_token(REGION)
    return tok, time.time() + exp / 1000.0

admin = AdminClient({
    'bootstrap.servers': BROKERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
    'client.id': socket.gethostname(),
})
for _ in range(10):
    admin.poll(0.5)

req = [NewTopic(n, num_partitions=p, replication_factor=3) for n, p in TOPICS]
futs = admin.create_topics(req)
for name, fut in futs.items():
    deadline = time.time() + 180
    while not fut.done() and time.time() < deadline:
        admin.poll(0.5)
    try:
        fut.result(timeout=10)
        print(f'  created topic: {name}')
    except KafkaException as e:
        if 'already exists' in str(e).lower():
            print(f'  topic {name} already exists (OK)')
        else:
            print(f'  topic {name} ERROR: {e}', file=sys.stderr)
PYEOF
" || echo "  WARN: topic creation had issues (continuing)"

log_step "T-15m Start streaming bronze (sensor)"
# bronze: --trigger processing (default) + --format iceberg
STEP_BRONZE=$(submit_spark_step "scale-test-bronze-sensor" "streaming/bronze_ingestion.py" "--trigger processing --format iceberg")
echo "  bronze_step_id=$STEP_BRONZE"

log_step "T-14m Start streaming silver (sensor)"
# silver: --mode streaming (default) + --format iceberg
STEP_SILVER=$(submit_spark_step "scale-test-silver-sensor" "transformations/bronze_to_silver/sensor_silver.py" "--mode streaming --format iceberg")
echo "  silver_step_id=$STEP_SILVER"

log_step "T-13m Start gold fact_vital_reading"
# gold fact_vital_reading: default mode is BATCH; explicitly set streaming
STEP_GOLD_FVR=$(submit_spark_step "scale-test-gold-fact-vital-reading" "transformations/silver_to_gold/fact_vital_reading.py" "--mode streaming --format iceberg")
echo "  gold_fvr_step_id=$STEP_GOLD_FVR"

log_step "T-12m Start gold fact_vital_daily_summary"
# gold fact_vital_daily_summary: default mode is BATCH; explicitly set streaming
STEP_GOLD_FVD=$(submit_spark_step "scale-test-gold-fact-vital-daily" "transformations/silver_to_gold/fact_vital_daily_summary.py" "--mode streaming --format iceberg")
echo "  gold_fvd_step_id=$STEP_GOLD_FVD"

# ── Pre-build static dimensions BEFORE producers start ────────────────────
# dim_metric / dim_date / dim_device / dim_time / dim_patient are slowly-
# changing and don't need to stream alongside the high-volume facts. Build
# them once at orchestrator start so the facts can join against populated
# dim tables. dim_patient depends on identity_bridge (which depends on
# silver_ehr — built later in T-2m batch tier), so dim_patient runs LAST
# in the batch tier, not here.
log_step "T-11m Pre-build static dimensions (4 dims, parallel batch)"
STEP_DIM_METRIC=$(submit_spark_step "scale-test-dim_metric" "transformations/silver_to_gold/dim_metric.py" "--format iceberg")
STEP_DIM_DATE=$(submit_spark_step "scale-test-dim_date" "transformations/silver_to_gold/dim_date.py" "--format iceberg")
STEP_DIM_DEVICE=$(submit_spark_step "scale-test-dim_device" "transformations/silver_to_gold/dim_device.py" "--format iceberg")
STEP_DIM_TIME=$(submit_spark_step "scale-test-dim_time" "transformations/silver_to_gold/dim_time.py" "--format iceberg")
echo "  dim_metric=$STEP_DIM_METRIC dim_date=$STEP_DIM_DATE dim_device=$STEP_DIM_DEVICE dim_time=$STEP_DIM_TIME"

# Wait for streams to be ACTIVE (poll Prometheus :8001 :8004 :8006).
log_step "T-12m Wait for streams to be ACTIVE (max 5 min)"
DEADLINE=$(($(date +%s) + 300))
while [[ $(date +%s) -lt $DEADLINE ]]; do
    sleep 30
    # Lightweight: poll YARN app states via SSH (could also hit Prometheus)
    APPS_RUNNING=$(ssh -i ~/.ssh/pulsetrack-emr.pem -o StrictHostKeyChecking=no \
        "hadoop@$MASTER_DNS" \
        "yarn application -list -appStates RUNNING 2>/dev/null | grep -c 'application_' || echo 0" \
        || echo 0)
    echo "  apps_running=$APPS_RUNNING (target=4)"
    [[ "$APPS_RUNNING" -ge 4 ]] && break
done
[[ "$APPS_RUNNING" -ge 3 ]] || abort "Streams did not become active within 5 min (need ≥3 of 4)"

# ── Phase 4: Start all 4 producers concurrently ──────────────────────────
log_step "T-10m Launch all 4 producers on EMR master (tmux sessions)"
SSH="ssh -i ~/.ssh/pulsetrack-emr.pem -o StrictHostKeyChecking=no hadoop@$MASTER_DNS"

# EMR 7.13 master doesn't ship tmux by default. Install via yum (idempotent).
# If install fails we fall back to nohup + & later; either way producers run.
$SSH "command -v tmux >/dev/null || sudo yum install -y tmux 2>/dev/null" || \
    echo "  WARN: tmux install failed; falling back to nohup mode"

$SSH "tmux kill-server 2>/dev/null || true; tmux new-session -d -s pulsetrack 2>/dev/null" || \
    echo "  WARN: tmux session creation failed"

# Stage tarball + unpack on master
$SSH "aws s3 cp s3://${BUCKET}/code/pulsetrack-scale-test.tar.gz /tmp/ && \
      mkdir -p /home/hadoop/pulsetrack && \
      tar xzf /tmp/pulsetrack-scale-test.tar.gz -C /home/hadoop/pulsetrack"

# Producer launches via nohup (works without tmux; SSH-detach safe).
# tmux is best-effort for interactive debugging; the actual producers
# run via nohup so they survive SSH disconnects and run in parallel.
REPORT_INTERVAL=$(( EVENT_COUNT / 200 < 5000 ? 5000 : EVENT_COUNT / 200 ))

# Producer 1: batch scale producer — event count parameterized by --smoke flag.
# Full mode: 10,000,000 events / 50,000 users. Smoke: e.g. 1000 / 50.
$SSH "cd /home/hadoop/pulsetrack && \
      AWS_DEFAULT_REGION=us-east-1 nohup /usr/bin/python3.11 \
         data_generators/batch_scale_producer.py \
         --brokers $BOOTSTRAP --topic sensor_readings \
         --count $EVENT_COUNT --users $USER_COUNT \
         --report-interval $REPORT_INTERVAL \
         > /tmp/batch-scale.log 2>&1 &
      echo \"  batch-scale PID=\$!\""

# Producer 2: WHOOP API (real account)
$SSH "cd /home/hadoop/pulsetrack && \
      AWS_DEFAULT_REGION=us-east-1 nohup /usr/bin/python3.11 \
         -m data_generators.whoop_api.producer \
         > /tmp/whoop.log 2>&1 &
      echo \"  whoop PID=\$!\""

# Producer 3: OpenFDA poller
$SSH "cd /home/hadoop/pulsetrack && \
      AWS_DEFAULT_REGION=us-east-1 nohup /usr/bin/python3.11 \
         data_generators/openfda_producer.py \
         > /tmp/openfda.log 2>&1 &
      echo \"  openfda PID=\$!\""

# Producer 4: FHIR / EHR batch  (single-shot — writes JSON + exits)
$SSH "cd /home/hadoop/pulsetrack && \
      AWS_DEFAULT_REGION=us-east-1 nohup /usr/bin/python3.11 \
         data_generators/fhir_producer.py \
         > /tmp/fhir.log 2>&1 &
      echo \"  fhir PID=\$!\""

echo "  All 4 producers launched (nohup). Logs at /tmp/{batch-scale,whoop,openfda,fhir}.log on master."

# ── Phase 4.5: Periodic batch tier ────────────────────────────────────────
# After producers have been running ~2 min, sync EHR batches to S3, then
# run the slow-side-dish batch transforms. silver_ehr + dim_patient need
# EHR JSON files to exist on S3 (Spark cluster-mode driver runs on a YARN
# container, not the EMR master where fhir_producer writes).
log_step "T-8m Sync EHR batches to S3 (master FS → s3://${BUCKET}/ehr-batches/)"
$SSH "aws s3 sync /home/hadoop/pulsetrack/data/ehr_batches/ \
        s3://${BUCKET}/ehr-batches/ --quiet 2>&1 | head -5" || \
    echo "  WARN: no EHR batches to sync (fhir_producer may have errored)"

log_step "T-8m Submit batch tier (silver_ehr + silver_pharmacy + identity_bridge + dim_patient)"
STEP_EHR_SILVER=$(submit_spark_step "scale-test-silver-ehr-batch" "transformations/bronze_to_silver/ehr_silver.py" "--format iceberg")
STEP_PHARMACY_SILVER=$(submit_spark_step "scale-test-silver-pharmacy-batch" "transformations/bronze_to_silver/pharmacy_silver.py" "--mode batch")
echo "  silver_ehr=$STEP_EHR_SILVER silver_pharmacy=$STEP_PHARMACY_SILVER"

# identity_bridge depends on silver_ehr + silver_sensor + silver_pharmacy.
# Submit AFTER those are queued — EMR step concurrency means it'll wait
# its turn behind them anyway, but explicit ordering avoids 'bronze_pharmacy
# table missing' early-exit fallthrough.
sleep 60
STEP_IDENTITY=$(submit_spark_step "scale-test-identity-bridge" "transformations/identity_resolution/patient_identity_bridge.py" "--format iceberg")
echo "  identity_bridge=$STEP_IDENTITY"

# dim_patient depends on identity_bridge. Same logic — submit a bit later
# so it lands after the bridge step.
sleep 60
STEP_DIM_PATIENT=$(submit_spark_step "scale-test-dim_patient" "transformations/silver_to_gold/dim_patient.py" "--format iceberg")
echo "  dim_patient=$STEP_DIM_PATIENT"

# ── Phase 5: Trigger Prefect deployments ─────────────────────────────────
log_step "T-5m Trigger Prefect deployments (ad-hoc)"
if command -v prefect >/dev/null; then
    prefect deployment run dbt-weekly/dbt-weekly 2>&1 || \
        echo "  WARN: prefect dbt-weekly trigger failed (continuing — flow may be unconfigured)"
    prefect deployment run streaming-monitor/streaming-monitor 2>&1 || \
        echo "  WARN: prefect streaming-monitor trigger failed"
fi

# ── Phase 6: Active monitoring window ────────────────────────────────────
log_step "T+0m Active monitoring (15 min before chaos)"
echo "  Open: Grafana, CloudWatch, Prefect UI."
echo "  Watching for: producer rate, MSK ingress, bronze write rate."

# Smoke mode: skip the 15-min monitoring window and the chaos drills entirely.
# Smoke goal = validate orchestrator end-to-end, not test recovery semantics.
if [[ "$SMOKE_MODE" -eq 1 ]]; then
    echo "  SMOKE mode: monitoring 3 min instead of 15, chaos drills skipped."
    sleep 180
else
    sleep 900  # 15 minutes
fi

# ── Phase 7+8: Chaos drills (skipped in smoke mode) ──────────────────────
if [[ "$SMOKE_MODE" -eq 0 ]]; then
    log_step "T+15m Chaos drill 1 — kill ONE silver executor"
    python3 scripts/chaos/kill_spark_task.py \
        --app-name "silver_sensor_streaming" \
        --recovery-budget-seconds 60 \
        --host "$MASTER_DNS" || \
        echo "  WARN: drill 1 failed — postmortem will capture details"

    sleep 600  # 10 min recovery + observation window

    log_step "T+30m Chaos drill 2 — kill ENTIRE silver streaming app"
    python3 scripts/chaos/kill_spark_app.py \
        --app-name "silver_sensor_streaming" \
        --step-script "transformations/bronze_to_silver/sensor_silver.py" \
        --recovery-budget-seconds 300 \
        --host "$MASTER_DNS" || \
        echo "  WARN: drill 2 failed — postmortem will capture details"

    sleep 600  # full app-level recovery window
else
    log_step "T+15m Chaos drills SKIPPED in smoke mode"
fi

# ── Phase 9: Stop producers + drain ──────────────────────────────────────
log_step "T+45m Stop producers + drain streams"
# nohup'd producers — SIGTERM by name. batch_scale_producer + WHOOP poller
# + openfda poller are long-running; fhir_producer is single-shot and may
# already be gone (pkill -f returns 1 for "no matches", we ignore via || true).
$SSH "pkill -TERM -f batch_scale_producer || true; \
      pkill -TERM -f 'whoop_api.producer' || true; \
      pkill -TERM -f openfda_producer || true; \
      pkill -TERM -f fhir_producer || true; \
      echo 'producer stop signals sent'"

echo "  Waiting 5 min for streams to drain (lag → 0)..."
sleep 300

# ── Phase 9.5: Re-run batch tier to capture late-arriving data ───────────
# Run identity_bridge + dim_patient one more time so the final state
# captures everything that landed during the chaos-drill window.
log_step "T+50m Final batch tier (identity_bridge + dim_patient re-run)"
STEP_IDENTITY_FINAL=$(submit_spark_step "scale-test-identity-bridge-final" "transformations/identity_resolution/patient_identity_bridge.py" "--format iceberg")
sleep 30
STEP_DIM_PATIENT_FINAL=$(submit_spark_step "scale-test-dim_patient-final" "transformations/silver_to_gold/dim_patient.py" "--format iceberg")
echo "  identity_bridge_final=$STEP_IDENTITY_FINAL dim_patient_final=$STEP_DIM_PATIENT_FINAL"

# ── Phase 9.7: OPTIMIZE all Delta/Iceberg tables (compact small files) ────
# Bronze accumulates many small parquets during high-throughput streaming
# (78,087 files at 23 KB avg in our prior 10M run). OPTIMIZE compacts to
# the configured target file size (typically 128 MB). VACUUM removes the
# now-orphan files older than the retention window.
log_step "T+52m OPTIMIZE (compact small files) + VACUUM"
STEP_OPTIMIZE=$(submit_spark_step "scale-test-optimize-compact" "maintenance/compaction.py" "")
echo "  optimize_step_id=$STEP_OPTIMIZE"

# ── Phase 9.9: Observability monitors run ─────────────────────────────────
# Run the freshness / volume / schema / distribution monitors against the
# now-stable tables. Results land in the observability ledger (Iceberg
# table) and any alert thresholds fire via Slack/SNS.
log_step "T+54m Observability monitors (freshness + volume + distribution)"
$SSH "cd /home/hadoop/pulsetrack && \
      AWS_DEFAULT_REGION=us-east-1 PT_ENVIRONMENT=cloud PT_AWS_ENV=dev \
      /usr/bin/python3.11 -m observability.cli run \
         --spec observability/sql/monitor_spec.yaml 2>&1 | tee /tmp/monitors.log" || \
    echo "  WARN: monitor run failed (continuing — check /tmp/monitors.log on master)"

# ── Phase 10: Benchmarks ─────────────────────────────────────────────────
log_step "T+55m Running benchmark report"
python3 benchmarks/scale_test_report.py \
    --output docs/scale_test_results.md \
    --grafana-dir docs/screenshots \
    --emr-cluster-id "$CLUSTER_ID" \
    --bucket "$BUCKET" \
    --chaos-log docs/chaos_log.jsonl

# ── Phase 11: Screenshots ────────────────────────────────────────────────
log_step "T+60m Capturing Grafana + CloudWatch screenshots"
python3 scripts/capture_grafana_screenshots.py \
    --output-dir docs/screenshots \
    --test-start "$TEST_START" || \
    echo "  WARN: screenshot capture skipped (Grafana endpoint may be unset)"

# ── Phase 11.5: E2E latency measurement ──────────────────────────────────
log_step "T+62m Measure E2E latency (Kafka publish → silver write)"
$SSH "cd /home/hadoop/pulsetrack && \
      AWS_DEFAULT_REGION=us-east-1 PT_ENVIRONMENT=cloud PT_AWS_ENV=dev \
      /usr/bin/python3.11 -m benchmarks.measure_e2e_latency \
         --format iceberg \
         --output-json /tmp/e2e_latency.json 2>&1 | tee /tmp/e2e_latency.log" || \
    echo "  WARN: latency measurement skipped — see /tmp/e2e_latency.log"
# Copy the JSON results back for the benchmark report to pick up
scp -i ~/.ssh/pulsetrack-emr.pem -o StrictHostKeyChecking=no \
    "hadoop@$MASTER_DNS:/tmp/e2e_latency.json" docs/e2e_latency.json 2>/dev/null || \
    echo "  WARN: e2e_latency.json not retrieved"

# ── Phase 12: Consumer-side validation (Snowflake + Athena + ML + Slack) ──
log_step "T+70m Consumer-side validation (4 surfaces)"

# 12a. Snowflake EXTERNAL TABLE refresh + view queries
echo "  [12a] Snowflake EXTERNAL TABLE refresh + view queries..."
python3 - <<PYEOF || echo "  WARN: Snowflake validation failed"
from pt_secrets import get_secret
try:
    import snowflake.connector
    creds = get_secret("snowflake")
    with snowflake.connector.connect(**{k: creds[k] for k in
                                        ("account","user","password","role","warehouse","database")}) as conn:
        with conn.cursor() as cur:
            # Refresh EXTERNAL TABLE pointers so Snowflake sees the latest
            # Glue catalog state (Iceberg snapshot moves with every commit).
            for ext in ("EXT_FACT_VITAL_READING", "EXT_FACT_VITAL_DAILY_SUMMARY"):
                try:
                    cur.execute(f"ALTER EXTERNAL TABLE PULSETRACK.GOLD.{ext} REFRESH")
                    print(f"    refreshed: PULSETRACK.GOLD.{ext}")
                except Exception as e:
                    print(f"    refresh skipped {ext}: {e}")
            # Query the 4 analytics views
            for view in ("VW_PATIENT_HEALTH_360", "VW_ANOMALY_DASHBOARD",
                         "VW_VITAL_TRENDS", "VW_DEVICE_FLEET_HEALTH"):
                try:
                    cur.execute(f"SELECT COUNT(*) FROM PULSETRACK.ANALYTICS.{view}")
                    n = cur.fetchone()[0]
                    print(f"    PULSETRACK.ANALYTICS.{view}: {n:,} rows")
                except Exception as e:
                    print(f"    view {view} failed: {e}")
except Exception as e:
    print(f"  WARN: Snowflake setup failed — {e}")
PYEOF

# 12b. Athena query on Iceberg gold tables (direct Glue catalog)
echo "  [12b] Athena queries on Iceberg gold (via Glue catalog)..."
ATHENA_BUCKET="s3://${BUCKET}/athena-results/"
for QUERY_LABEL in fact_count fact_max_date dim_metric_count fact_dim_join; do
    case "$QUERY_LABEL" in
        fact_count)
            Q="SELECT COUNT(*) FROM pulsetrack_gold_dev.fact_vital_reading"
            ;;
        fact_max_date)
            Q="SELECT MAX(event_timestamp) FROM pulsetrack_gold_dev.fact_vital_reading"
            ;;
        dim_metric_count)
            Q="SELECT metric_code, COUNT(*) FROM pulsetrack_gold_dev.fact_vital_reading f JOIN pulsetrack_gold_dev.dim_metric m ON f.dim_metric_key = m.dim_metric_key GROUP BY metric_code ORDER BY 2 DESC LIMIT 10"
            ;;
        fact_dim_join)
            Q="SELECT d.device_type, COUNT(*) FROM pulsetrack_gold_dev.fact_vital_reading f JOIN pulsetrack_gold_dev.dim_device d ON f.dim_device_key = d.dim_device_key GROUP BY 1 ORDER BY 2 DESC"
            ;;
    esac
    QID=$(aws athena start-query-execution \
            --query-string "$Q" \
            --result-configuration "OutputLocation=$ATHENA_BUCKET" \
            --query-execution-context "Database=pulsetrack_gold_dev" \
            --query 'QueryExecutionId' --output text 2>/dev/null || echo "")
    if [[ -n "$QID" ]]; then
        # Poll for completion (up to 60s)
        for _ in $(seq 1 30); do
            STATUS=$(aws athena get-query-execution --query-execution-id "$QID" \
                       --query 'QueryExecution.Status.State' --output text 2>/dev/null)
            [[ "$STATUS" == "SUCCEEDED" ]] && break
            [[ "$STATUS" == "FAILED" || "$STATUS" == "CANCELLED" ]] && break
            sleep 2
        done
        echo "    athena[$QUERY_LABEL]: $STATUS (id=$QID)"
    else
        echo "    athena[$QUERY_LABEL]: SUBMIT FAILED"
    fi
done

# 12c. ML feature query — fact + dim join via Spark on EMR master
echo "  [12c] ML feature query (10k rows fact+dim join)..."
$SSH "cd /home/hadoop/pulsetrack && \
      AWS_DEFAULT_REGION=us-east-1 PT_ENVIRONMENT=cloud PT_AWS_ENV=dev \
      /usr/bin/python3.11 -c '
import sys
sys.path.insert(0, \".\")
from streaming.spark_config import get_spark_session
spark = get_spark_session(\"ml-feature-query\")
df = (spark.read.table(\"glue_iceberg.pulsetrack_gold_dev.fact_vital_reading\")
        .join(spark.read.table(\"glue_iceberg.pulsetrack_gold_dev.dim_metric\"),
              \"dim_metric_key\")
        .join(spark.read.table(\"glue_iceberg.pulsetrack_gold_dev.dim_device\"),
              \"dim_device_key\")
        .limit(10000))
n, cols = df.count(), len(df.columns)
print(f\"ML feature query: rows={n} cols={cols}\")
spark.stop()
' 2>&1 | tail -10" || echo "  WARN: ML feature query failed"

# 12d. Slack anomaly-explainer alert routing test
echo "  [12d] Slack alert routing test (anomaly explainer)..."
python3 - <<PYEOF || echo "  WARN: Slack test failed"
try:
    from observability.alerting import alert_slack
    alert_slack(
        title="scale-test smoke",
        message=f"PulseTrack scale test completed at $TEST_END. This is a routing test.",
        severity="info",
    )
    print("    Slack alert dispatched (check #pulsetrack-alerts)")
except Exception as e:
    print(f"    Slack routing test failed: {e}")
PYEOF

# ── Final: report + tear-down recommendation ─────────────────────────────
log_step "T+90m Scale test complete"
TEST_END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""
echo "==============================================================================="
echo "Test window: $TEST_START → $TEST_END"
echo "Results:     docs/scale_test_results.md"
echo "Chaos log:   docs/chaos_log.jsonl"
echo "Execution:   docs/scale_test_execution_log.md (this file)"
echo ""
echo "Next steps:"
echo "  1. Review docs/scale_test_results.md"
echo "  2. Write live postmortems for chaos drills + any organic incidents"
echo "  3. Tear down compute: cd infrastructure && bash teardown-compute.sh"
echo "==============================================================================="
