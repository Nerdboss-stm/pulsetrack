# Runbook: EMR step lands in FAILED state

**Severity ladder:**
- SEV3: a one-off batch step (smoke test, ad-hoc query) fails; no streaming impact
- SEV2: a scheduled batch step (gold rebuild, compaction, GX run) fails OR a streaming step fails once and self-restarts via Prefect retry
- SEV1: a streaming step (`bronze_ingestion.py`, `silver_ingestion.py`, `pharmacy_bronze_ingestion.py`) lands FAILED and does not recover within 10 min, OR three consecutive steps fail on the same cluster (cluster-level fault)

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

## TL;DR (30-second triage)

```bash
# 1. Which step failed?
aws emr list-steps --cluster-id "$CLUSTER_ID" \
    --step-states FAILED --max-items 5 \
    --query 'Steps[].{Id:Id,Name:Name,State:Status.State,Reason:Status.FailureDetails.Reason}'

# 2. Why? (FailureDetails carries the high-signal reason)
aws emr describe-step --cluster-id "$CLUSTER_ID" --step-id "$STEP_ID" \
    --query 'Step.Status.{State:State,FailureDetails:FailureDetails,Timeline:Timeline}'

# 3. Driver/executor stderr (logs land in s3://$BUCKET/emr-logs/ — see modules/compute/main.tf log_uri)
aws s3 ls "s3://${BUCKET}/emr-logs/${CLUSTER_ID}/steps/${STEP_ID}/" --recursive
aws s3 cp "s3://${BUCKET}/emr-logs/${CLUSTER_ID}/steps/${STEP_ID}/stderr.gz" - | gunzip | tail -200
```

Step-failure root causes cluster into six families, distinguishable by exit code + stderr first 100 lines:

| Exit code | Family | First-pass diagnosis |
|-----------|--------|----------------------|
| 1 | Python exception (ImportError, AttributeError, etc.) | `grep -E 'Error|Exception' stderr` |
| 13 | Spark driver OOM | Look for `java.lang.OutOfMemoryError: Java heap space` |
| 137 | YARN killed driver (memory request exceeded NM limit) | Container limit exceeded, see YARN nodemanager logs |
| 143 | SIGTERM during graceful shutdown | Usually benign (cluster scale-in) — re-submit |
| 255 | Spark config error / spark-submit rejected the args | First 50 lines of stderr explain |
| 0 + step FAILED | Job ran but `ActionOnFailure: CONTINUE` swallowed the failure | Check inside the job for raised exceptions logged at WARN |

## Symptoms (what triggered the page)

- CloudWatch alarm `pulsetrack-{env}-emr-step-failed` ACTIVE (EMR `StepFailureCount > 0` over 5 min)
- Prefect deployment shows `Failed` flow run with `EMRStepFailedException`
- SNS topic `arn:aws:sns:us-east-1:960341592614:pulsetrack-dev-alerts` fired
- Prometheus `streaming_query_active{query_name="bronze-sensor-readings"} == 0` while the step was supposed to be RUNNING
- Glue table writes stopped (downstream symptom — last `committed_at` is stale)

## Diagnosis (commands to run first)

### Classify by exit code

```bash
aws emr describe-step --cluster-id "$CLUSTER_ID" --step-id "$STEP_ID" \
    --query 'Step.Status.FailureDetails'
```

Look at `Reason`, `Message`, and `LogFile`. `Reason` is usually `Unknown Error` (unhelpful); `Message` is the exit code; `LogFile` points at the S3 path under `s3://$BUCKET/emr-logs/...`.

### Fetch logs

EMR uploads logs to S3 at the path defined in `modules/compute/main.tf` (`log_uri = "s3://${var.lakehouse_bucket}/emr-logs/"`). Layout:

```
s3://$BUCKET/emr-logs/$CLUSTER_ID/
  steps/$STEP_ID/
    stderr.gz       ← spark-submit driver stderr (start here)
    stdout.gz
    controller.gz
    syslog.gz
  containers/application_<appId>/
    container_<containerId>/
      stderr         ← YARN container logs (executor stderr)
      stdout
```

Pull and decode:
```bash
aws s3 cp "s3://${BUCKET}/emr-logs/${CLUSTER_ID}/steps/${STEP_ID}/stderr.gz" - \
    | gunzip | tail -500 | grep -E 'Error|Exception|Traceback' -A 5
```

Container logs (when the failure is in an executor, not the driver):
```bash
# Find the YARN appId in the driver stderr first:
APP_ID="$(aws s3 cp s3://${BUCKET}/emr-logs/${CLUSTER_ID}/steps/${STEP_ID}/stderr.gz - \
          | gunzip | grep -oE 'application_[0-9_]+' | head -1)"
aws s3 ls "s3://${BUCKET}/emr-logs/${CLUSTER_ID}/containers/${APP_ID}/" --recursive
aws s3 cp "s3://${BUCKET}/emr-logs/${CLUSTER_ID}/containers/${APP_ID}/container_<id>/stderr" -
```

### YARN view (when the cluster is still alive)

```bash
ssh hadoop@$MASTER_DNS "yarn logs -applicationId $APP_ID -appOwner hadoop" \
    | tail -500
ssh hadoop@$MASTER_DNS "yarn application -status $APP_ID"
```

## Recovery (ranked by likelihood, fastest first)

### Case A: Driver OOM (exit 13 or 137) — ~30% of failures

Symptom: `java.lang.OutOfMemoryError: Java heap space` in stderr, or container killed with `Container [...] is running beyond physical memory limits`.

**Cause:** the driver ran a wide collect/aggregate, OR a foreachBatch closure (see `streaming/bronze_ingestion.py:_make_batch_processor`) cached a DataFrame too large for the heap.

**Fix:**
```bash
# Re-submit with more driver memory (default is whatever spark-defaults sets — usually 4G on m5.xlarge master)
aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "[{
    \"Name\": \"${JOB_NAME}-retry\",
    \"ActionOnFailure\": \"CONTINUE\",
    \"HadoopJarStep\": {
        \"Jar\": \"command-runner.jar\",
        \"Args\": [
            \"spark-submit\",
            \"--deploy-mode\", \"cluster\",
            \"--conf\", \"spark.driver.memory=8g\",
            \"--conf\", \"spark.driver.memoryOverhead=2g\",
            \"s3://${BUCKET}/code/${JOB}\"
        ]
    }
}]"
```

If it OOMs again at 8g, the root cause is in the code (`.collect()` on an unbounded DataFrame, unbounded `.cache()` in foreachBatch). Open a fix-it ticket; don't keep raising heap.

### Case B: Missing import on master / cluster (exit 1) — ~25%

Symptom: `ModuleNotFoundError: No module named '<x>'` in stderr.

**Cause:** `bootstrap.sh` (see `infrastructure/modules/compute/bootstrap.sh`) didn't install the dep, OR `submit_emr_step.sh` packaged the tarball before the dep was added to `requirements.txt`.

**Fix path 1 — dep is in repo, tarball is stale:**
```bash
# Re-package + re-submit (submit_emr_step.sh re-tars on every run)
bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py
```

**Fix path 2 — dep was never installed on cluster:**
```bash
# Hotfix the running cluster (won't survive cluster replacement):
ssh hadoop@$MASTER_DNS "sudo pip3 install <missing-package>"
# Permanent fix: add to infrastructure/modules/compute/bootstrap.sh, then
#   cd infrastructure && terraform apply
# (Note: bootstrap_action only runs on cluster CREATE — see modules/compute/main.tf — so
#  Terraform will force-replace the cluster. Schedule a maintenance window.)
```

### Case C: Spark config error (exit 255) — ~15%

Symptom: first 50 lines of stderr show `spark-submit` rejecting an arg, or `IllegalArgumentException: System memory ... must be at least ...`.

**Fix:**
```bash
# Inspect the step's submitted args:
aws emr describe-step --cluster-id "$CLUSTER_ID" --step-id "$STEP_ID" \
    --query 'Step.Config.Args'
# Re-submit with corrected --conf flags (see scripts/submit_emr_step.sh — it's a thin wrapper)
```

Common offenders:
- `spark.sql.shuffle.partitions=0` (default `config.py:19` is 4; never set to 0)
- `spark.executor.memory` set higher than `yarn.nodemanager.resource.memory-mb` permits
- A `--conf` arg with a typo (`spark.sql.catalog.glue_iceberg` misspelled)

### Case D: IAM perms (any exit code, but usually 1) — ~10%

Symptom: stderr contains `AccessDenied`, `NoSuchBucket`, `User: arn:aws:sts::...:assumed-role/<role>/... is not authorized to perform: <action>`.

**Cause:** the EMR EC2 instance profile (`infrastructure/modules/iam/main.tf:aws_iam_role.emr_ec2`) is missing a permission. The role has S3 r/w on the lakehouse bucket, Glue catalog ops, MSK IAM auth, and DynamoDB on the Glacierbase lock table — anything else is a new requirement.

**Diagnosis:**
```bash
# What role does the cluster run as?
aws emr describe-cluster --cluster-id "$CLUSTER_ID" --query 'Cluster.Ec2InstanceAttributes.IamInstanceProfile'
# Simulate the failing action:
aws iam simulate-principal-policy \
    --policy-source-arn "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/<role>" \
    --action-names "<action-from-stderr>" \
    --resource-arns "<resource-from-stderr>"
```

**Fix:** add the missing policy to `infrastructure/modules/iam/main.tf` (follow the pattern of `lakehouse_access` / `glue_catalog_access` / `msk_iam_auth`), `terraform apply`. The instance profile picks up the new policy without a cluster restart — but a step that's already running is dead; re-submit after apply.

### Case E: S3 path typo / missing object (exit 1) — ~10%

Symptom: stderr contains `Path does not exist: s3://...` or `NoSuchKey: ...`.

**Cause:** `submit_emr_step.sh` derives `s3://$BUCKET/code/$JOB` from Terraform output `lakehouse_bucket_name`. If `$JOB` is wrong or the tarball upload failed silently, the step won't find the entry point.

**Fix:**
```bash
# Confirm the entry point landed in S3:
aws s3 ls "s3://${BUCKET}/code/${JOB}"
# Re-run the submit script (it re-uploads):
bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py
```

### Case F: Glue catalog rejection (exit 1) — ~10%

Symptom: stderr contains `EntityNotFoundException`, `AlreadyExistsException`, or `glue:CreateTable failed`.

**Cause:** Iceberg table DDL mismatch — bronze writer's `BRONZE_SENSOR_DDL` (see `streaming/bronze_ingestion.py:64`) drifted from what Glue already holds, OR the Glue database (`pulsetrack_bronze_dev`, etc.) doesn't exist because Terraform didn't run.

**Fix:**
```bash
# Confirm the DB exists:
aws glue get-database --name "pulsetrack_bronze_${PT_AWS_ENV:-dev}"
# Run pending migrations (Glacierbase DynamoDB lock will serialize this):
python -m migrations.cli apply --catalog bronze
# If the DDL drifted, write a new migration version (see migrations/versions/) — DO NOT
# manually edit the Glue table; that breaks state tracking.
```

### Cluster-level fault (three+ consecutive failures)

If three steps fail back-to-back with different errors, the cluster itself is bad (disk full, NM dead, network partition). Escalate:
```bash
# Quick checks:
ssh hadoop@$MASTER_DNS "df -h /mnt; yarn node -list -all | grep -v Healthy"
# If unhealthy, replace the cluster (Terraform recreates):
cd infrastructure && terraform taint module.compute.aws_emr_cluster.spark && terraform apply
```

## Verification (how you know it's fixed)

1. New step transitions COMPLETED:
   ```bash
   aws emr describe-step --cluster-id "$CLUSTER_ID" --step-id "$NEW_STEP_ID" \
       --query 'Step.Status.{State:State,Timeline:Timeline}'
   ```
2. For streaming steps: `streaming_query_active{query_name="<name>"} == 1` on Prometheus
3. Downstream table receives a new commit:
   ```sql
   SELECT MAX(committed_at) FROM pulsetrack_bronze_dev.sensor_readings.snapshots;
   ```
4. No new ERROR entries in driver stderr for 5 min

## Prevention (post-incident hardening)

1. **`ActionOnFailure: CONTINUE` is misleading** — `submit_emr_step.sh` uses it so one failed step doesn't kill subsequent ones, but it also means a silent failure looks like success in `list-steps`. Wire a CloudWatch alarm on `StepFailureCount` (it already exists; verify it's not muted).
2. **Pin dep versions:** `requirements.txt` should pin every dep; `bootstrap.sh` should `pip install -r requirements.txt --no-deps` then a targeted re-resolve to catch a missing transitive.
3. **For Case D**, audit the EMR EC2 role quarterly — Glue/MSK/S3 boundaries shift as new tables/topics are added.
4. **Cluster-level health monitor:** add a Prefect-scheduled probe that runs a no-op step every hour. If it fails, the cluster is sick before a real workload hits.

## Related postmortems

- `postmortems/2026-05-02_silver_cold_start_hang.md` — silver step came up but didn't produce output; the step is RUNNING, not FAILED, so this runbook doesn't directly apply — see `silver_cold_start_hang.md`
- `postmortems/2026-05-09_chaos_drill_2_app_kill.md` — controlled YARN-kill drill; recovery procedure overlaps with Case A/B

## Related runbooks

- `runbooks/kafka_consumer_lag.md` — common downstream symptom when a streaming step fails
- `runbooks/silver_cold_start_hang.md` — step is RUNNING but stuck (orthogonal failure mode)
- `runbooks/s3_503_throttling.md` — if the failure was `CommitFailedException` from S3 throttling
- `runbooks/dlq_buildup.md` — if the step failed mid-batch and partial writes landed in DLQ
