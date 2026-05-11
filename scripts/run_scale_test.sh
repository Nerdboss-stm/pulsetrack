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
#
# Stdout + stderr are also tee'd to docs/scale_test_execution_log.md
# (the live operator journal — the postmortems anchor here for
# minute-granular timeline references).
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

LOG=docs/scale_test_execution_log.md
mkdir -p docs
exec > >(tee -a "$LOG") 2>&1

# ── Banner ────────────────────────────────────────────────────────────────
TEST_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""
echo "==============================================================================="
echo "PulseTrack scale test — $TEST_START"
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
    # Maven Central only has stock 3.5.x releases (no 3.5.6-amzn-2); 3.5.3 is fine.
    local spark_packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-avro_2.12:3.5.3"

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
        '--packages', '${spark_packages}',
        '--conf', 'spark.pyspark.python=/usr/bin/python3.11',
        '--conf', 'spark.yarn.appMasterEnv.PT_ENVIRONMENT=cloud',
        '--conf', 'spark.yarn.appMasterEnv.PT_AWS_ENV=dev',
        '--conf', 'spark.yarn.appMasterEnv.PT_LAKEHOUSE_BASE=s3://${BUCKET}',
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

$SSH "tmux kill-server 2>/dev/null || true; tmux new-session -d -s pulsetrack"

# Stage tarball + unpack on master
$SSH "aws s3 cp s3://${BUCKET}/code/pulsetrack-scale-test.tar.gz /tmp/ && \
      mkdir -p /home/hadoop/pulsetrack && \
      tar xzf /tmp/pulsetrack-scale-test.tar.gz -C /home/hadoop/pulsetrack"

# Producer 1: batch scale producer (10M events)
$SSH "tmux new-window -t pulsetrack -n batch-scale \
        'cd /home/hadoop/pulsetrack && \
         AWS_DEFAULT_REGION=us-east-1 \
         /usr/bin/python3.11 data_generators/batch_scale_producer.py \
            --brokers $BOOTSTRAP \
            --topic sensor_readings \
            --count 10000000 \
            --users 50000 \
            --report-interval 50000 \
            2>&1 | tee /tmp/batch-scale.log'"

# Producer 2: WHOOP API (real account)
$SSH "tmux new-window -t pulsetrack -n whoop-poll \
        'cd /home/hadoop/pulsetrack && \
         AWS_DEFAULT_REGION=us-east-1 \
         /usr/bin/python3.11 -m data_generators.whoop_api.producer \
            2>&1 | tee /tmp/whoop.log'"

# Producer 3: OpenFDA poller
$SSH "tmux new-window -t pulsetrack -n openfda \
        'cd /home/hadoop/pulsetrack && \
         AWS_DEFAULT_REGION=us-east-1 \
         /usr/bin/python3.11 data_generators/openfda_producer.py \
            2>&1 | tee /tmp/openfda.log'"

# Producer 4: FHIR / EHR batch
$SSH "tmux new-window -t pulsetrack -n fhir \
        'cd /home/hadoop/pulsetrack && \
         AWS_DEFAULT_REGION=us-east-1 \
         /usr/bin/python3.11 data_generators/fhir_producer.py \
            2>&1 | tee /tmp/fhir.log'"

echo "  All 4 producers launched. Use 'tmux attach' on master to monitor."

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

sleep 900  # 15 minutes

# ── Phase 7: Chaos drill 1 ───────────────────────────────────────────────
log_step "T+15m Chaos drill 1 — kill ONE silver executor"
python3 scripts/chaos/kill_spark_task.py \
    --app-name "silver_sensor_streaming" \
    --recovery-budget-seconds 60 \
    --host "$MASTER_DNS" || \
    echo "  WARN: drill 1 failed — postmortem will capture details"

sleep 600  # 10 min recovery + observation window

# ── Phase 8: Chaos drill 2 ───────────────────────────────────────────────
log_step "T+30m Chaos drill 2 — kill ENTIRE silver streaming app"
python3 scripts/chaos/kill_spark_app.py \
    --app-name "silver_sensor_streaming" \
    --step-script "transformations/bronze_to_silver/sensor_silver.py" \
    --recovery-budget-seconds 300 \
    --host "$MASTER_DNS" || \
    echo "  WARN: drill 2 failed — postmortem will capture details"

sleep 600  # full app-level recovery window

# ── Phase 9: Stop producers + drain ──────────────────────────────────────
log_step "T+45m Stop producers + drain streams"
$SSH "tmux send-keys -t pulsetrack:batch-scale C-c && \
      tmux send-keys -t pulsetrack:whoop-poll C-c && \
      tmux send-keys -t pulsetrack:openfda C-c && \
      tmux send-keys -t pulsetrack:fhir C-c"

echo "  Waiting 5 min for streams to drain (lag → 0)..."
sleep 300

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

# ── Phase 12: Consumer-side smoke ────────────────────────────────────────
log_step "T+70m Consumer-side smoke (Snowflake + ML feature query)"
python3 - <<PYEOF
from pt_secrets import get_secret
try:
    import snowflake.connector
    creds = get_secret("snowflake")
    with snowflake.connector.connect(**{k: creds[k] for k in
                                        ("account","user","password","role","warehouse","database")}) as conn:
        with conn.cursor() as cur:
            for view in ("VW_PATIENT_HEALTH_360", "VW_ANOMALY_DASHBOARD"):
                cur.execute(f"SELECT COUNT(*) FROM ANALYTICS.{view}")
                n = cur.fetchone()[0]
                print(f"  ANALYTICS.{view}: {n:,} rows")
except Exception as e:
    print(f"  WARN: consumer smoke failed — {e}")
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
