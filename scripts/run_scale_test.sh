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
python3 migrations/run_migrations.py --apply || abort "Migrations failed"

# ── Phase 2: Deploy code to EMR ───────────────────────────────────────────
log_step "T-20m Sync project to EMR master via S3"
bash scripts/deploy_to_emr.sh 2>/dev/null || {
    # Inline deploy if the script doesn't exist
    TARBALL=/tmp/pulsetrack-scale-test.tar.gz
    tar czf "$TARBALL" \
        --exclude='.git' --exclude='.venv' --exclude='__pycache__' \
        --exclude='*.pyc' --exclude='infrastructure/.terraform' \
        --exclude='spark-warehouse' --exclude='dbt_project/target' \
        .
    aws s3 cp "$TARBALL" "s3://${BUCKET}/code/pulsetrack-scale-test.tar.gz"
    echo "  Uploaded $(du -h $TARBALL | cut -f1) tarball"
}

# ── Phase 3: Start streaming pipeline ─────────────────────────────────────
log_step "T-15m Start streaming bronze (sensor)"
STEP_BRONZE=$(aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "[{
    \"Name\": \"scale-test:bronze-sensor\",
    \"ActionOnFailure\": \"CONTINUE\",
    \"HadoopJarStep\": {
        \"Jar\": \"command-runner.jar\",
        \"Args\": [\"spark-submit\", \"--deploy-mode\", \"cluster\",
                  \"s3://${BUCKET}/code/streaming/bronze_ingestion.py\",
                  \"--mode\", \"streaming\"]
    }
}]" --query 'StepIds[0]' --output text)
echo "  bronze_step_id=$STEP_BRONZE"

log_step "T-14m Start streaming silver (sensor)"
STEP_SILVER=$(aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "[{
    \"Name\": \"scale-test:silver-sensor\",
    \"ActionOnFailure\": \"CONTINUE\",
    \"HadoopJarStep\": {
        \"Jar\": \"command-runner.jar\",
        \"Args\": [\"spark-submit\", \"--deploy-mode\", \"cluster\",
                  \"s3://${BUCKET}/code/streaming/silver_ingestion.py\",
                  \"--mode\", \"streaming\"]
    }
}]" --query 'StepIds[0]' --output text)
echo "  silver_step_id=$STEP_SILVER"

log_step "T-13m Start gold facts (fact_vital_reading + fact_vital_daily_summary)"
STEP_GOLD=$(aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "[{
    \"Name\": \"scale-test:gold-vitals\",
    \"ActionOnFailure\": \"CONTINUE\",
    \"HadoopJarStep\": {
        \"Jar\": \"command-runner.jar\",
        \"Args\": [\"spark-submit\", \"--deploy-mode\", \"cluster\",
                  \"s3://${BUCKET}/code/streaming/gold_fact_vitals.py\",
                  \"--mode\", \"streaming\"]
    }
}]" --query 'StepIds[0]' --output text)
echo "  gold_step_id=$STEP_GOLD"

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
    echo "  apps_running=$APPS_RUNNING (target=3)"
    [[ "$APPS_RUNNING" -ge 3 ]] && break
done
[[ "$APPS_RUNNING" -ge 3 ]] || abort "Streams did not become active within 5 min"

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
         python3 data_generators/batch_scale_producer.py \
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
         python3 -m data_generators.whoop_api.producer \
            2>&1 | tee /tmp/whoop.log'"

# Producer 3: OpenFDA poller
$SSH "tmux new-window -t pulsetrack -n openfda \
        'cd /home/hadoop/pulsetrack && \
         AWS_DEFAULT_REGION=us-east-1 \
         python3 data_generators/openfda_producer.py \
            2>&1 | tee /tmp/openfda.log'"

# Producer 4: FHIR / EHR batch
$SSH "tmux new-window -t pulsetrack -n fhir \
        'cd /home/hadoop/pulsetrack && \
         AWS_DEFAULT_REGION=us-east-1 \
         python3 data_generators/fhir_producer.py \
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
    --step-script "streaming/silver_ingestion.py" \
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
