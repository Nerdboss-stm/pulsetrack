# Phase 2 pause state — 2026-05-11

Scale test execution was paused before the 10M event run completed. This document captures the state of the world so Phase 2 can resume tomorrow without re-doing the setup steps.

## What's intact (cost $0/hr, do NOT destroy)

| Layer | State |
|---|---|
| S3 bucket `pulsetrack-lakehouse-dev-03a28ee7` | Live — all prior Iceberg data preserved |
| Glue catalog (`pulsetrack_bronze_dev/_silver_dev/_gold_dev`) | Live, populated by prior runs |
| **Secrets Manager — all 6 secrets populated** | Live: whoop, whoop-tokens, anthropic, snowflake, slack, pagerduty |
| **KMS customer key for secrets** | Live |
| IAM roles incl. EMR EC2 + Snowflake assume roles | Live |
| VPC + subnets + security groups | Live |
| Snowflake account (BXHLGNB-BE10357) | Live |
| Snowflake `PULSETRACK` database + schemas + 4 roles + `PULSETRACK_WH` warehouse | Live |
| Snowflake `PULSETRACK_S3` storage integration + `PULSETRACK_VOL` external volume + `PULSETRACK_GLUE` catalog integration | Live (external IDs match AWS trust policies) |
| Snowflake `BRONZE.SENSOR_READINGS` Iceberg table | Created (empty until pipeline runs) |
| CloudWatch dashboard + budget alarm | Live |

## What's destroyed (will re-create tomorrow, ~5 min)

| Layer | State |
|---|---|
| EMR cluster `j-2M0QZS6DNOHK1` | Destroyed (was costing ~$1/hr) |
| MSK Serverless cluster | Destroyed (was costing ~$0.30/hr) |

## Phase 2 resume sequence (tomorrow)

```bash
cd /Users/nerdboss-stm/pulsetrack-cm

# 1. Restore compute (~5 min terraform + ~5 min EMR boot)
cd infrastructure
AWS_PROFILE=pulsetrack terraform apply \
    -var-file=environments/dev.tfvars \
    -var "emr_core_instance_count=4"

# 2. Get the new resource IDs (cluster_id and MSK brokers change on re-create)
terraform output emr_cluster_id
terraform output msk_bootstrap_brokers
cd ..

# 3. Set EMR step concurrency to 4 (so all 4 streaming queries run in parallel,
#    not serialized — the gotcha we hit today). Replace $CLUSTER_ID with the
#    fresh one from step 2.
AWS_PROFILE=pulsetrack aws emr modify-cluster \
    --cluster-id <CLUSTER_ID> \
    --step-concurrency-level 4

# 4. Pre-flight is already populated in Secrets Manager — no need to re-bootstrap
#    unless rotation happened
AWS_PROFILE=pulsetrack AWS_DEFAULT_REGION=us-east-1 PT_AWS_ENV=dev \
    python3 scripts/check_credentials.py

# 5. Run the orchestrator
AWS_PROFILE=pulsetrack AWS_DEFAULT_REGION=us-east-1 PT_AWS_ENV=dev \
    ./scripts/run_scale_test.sh

# 6. Post-test teardown (preserve data)
cd infrastructure && bash teardown-compute.sh
```

## Open issues from today's session

### 1. EMR step concurrency = 1 (default) — needs explicit fix

EMR clusters default to `StepConcurrencyLevel = 1` (steps run sequentially). Our pipeline has 4 streaming queries that need to run concurrently (they're each long-running). The orchestrator submitted all 4 but only one (silver) entered RUNNING — the other 3 stayed PENDING.

**Fix:** run `aws emr modify-cluster --step-concurrency-level 4` AFTER terraform apply but BEFORE orchestrator launch. Step 3 above. This was queued for the previous run but user paused before I could execute it.

**Better fix (long-term):** add `step_concurrency_level = 4` to `infrastructure/modules/compute/main.tf`'s `aws_emr_cluster` resource so terraform handles it. P1 follow-up — log in `runbooks/emr_step_failure.md` under "concurrency gotchas".

### 2. `pydantic_settings` is installed only in python3.11, not python3.9

EMR's `/usr/bin/python3` symlinks to python3.9 by default, but the cluster's bootstrap script installs Python packages via the pip3 that targets python3.11. Result: `spark.pyspark.python=/usr/bin/python3` fails with `ModuleNotFoundError: No module named 'pydantic_settings'`.

**Fix (already applied to orchestrator):** `spark.pyspark.python=/usr/bin/python3.11` everywhere. Producer launches also use `/usr/bin/python3.11` explicitly.

**Better fix (long-term):** update `infrastructure/modules/compute/bootstrap.sh` to install packages into both 3.9 and 3.11 with `sudo /usr/bin/python3 -m pip install ...`. P1 follow-up.

### 3. The `--py-files` deps zip pattern needs verification

Today's runs failed before Spark could test that the deps.zip-based imports actually work. The deps.zip is 236K and includes config.py + pt_secrets/ + observability/ + ai/ + etc. Tomorrow's first apps_running check will confirm whether the import chain resolves correctly through `--py-files`.

If it doesn't, fallback: use `--archives` with a venv-pack, OR pre-extract deps onto every EMR node via the bootstrap.

### 4. EMR step format (already fixed)

`HadoopJarStep` (nested) format is rejected by current aws-cli. Now using flat `Type=CUSTOM_JAR,Name=...,Jar=...,Args=[...]` format. Will roll out as a fix to `scripts/submit_emr_step.sh` too (same legacy pattern there).

### 5. Snowflake external IDs regenerate on CREATE OR REPLACE

Three different Snowflake objects (`STORAGE INTEGRATION`, `CATALOG INTEGRATION`, `EXTERNAL VOLUME`) each have their own AWS external ID, and `CREATE OR REPLACE` on any of them regenerates the ID — breaking the IAM trust policy. Documented in updated `snowflake/setup/02_create_storage_integration.sql` header comment.

**Resolution if Snowflake → AWS assume-role breaks tomorrow:**
```sql
-- In Snowsight:
DESC INTEGRATION PULSETRACK_S3;          -- copy STORAGE_AWS_EXTERNAL_ID
DESC CATALOG INTEGRATION PULSETRACK_GLUE;-- copy GLUE_AWS_EXTERNAL_ID
DESC EXTERNAL VOLUME PULSETRACK_VOL;     -- copy STORAGE_AWS_EXTERNAL_ID (in STORAGE_LOCATION_1)
```
Then update the 2 IAM role trust policies in AWS via `aws iam update-assume-role-policy`.

### 6. Anthropic + Slack credentials need rotation (postmortem 2026-05-11)

The `set -a && source .env` shell pattern echoed the Anthropic API key and Slack webhook URL into the bash error output. Per Path B decision, these stay in use until post-Phase-2 rotation. Tomorrow:
- Rotate Anthropic key at https://console.anthropic.com/settings/keys
- Rotate Slack webhook URL at https://api.slack.com/apps/
- Re-run `python scripts/bootstrap_secrets.py --include anthropic slack --force` to push new values

The fix to prevent recurrence (python-dotenv) is already in `scripts/bootstrap_secrets.py`.

## Files committed in this pause

- `scripts/run_scale_test.sh` — all path/format/python3.11/--py-files/env fixes
- `scripts/bootstrap_secrets.py` — python-dotenv migration
- `snowflake/setup/02_create_storage_integration.sql` — bucket root fix + ID-regeneration warning
- `postmortems/2026-05-11_secrets_leaked_via_shell_source.md` — secret leak postmortem
- `docs/scale_test_execution_log.md` — execution log from today's runs

## Cost ledger (today)

| Item | Cost |
|---|---|
| EMR + MSK runtime (~2h, mostly idle) | ~$2.50 |
| S3 PUTs / lifecycle | ~$0.05 |
| KMS GenerateDataKey | ~$0.01 |
| Secrets Manager API | ~$0.01 |
| **Today's total** | **~$2.60** |

Remaining budget against $40 cap: ~$37. Plenty of room for tomorrow's run.
