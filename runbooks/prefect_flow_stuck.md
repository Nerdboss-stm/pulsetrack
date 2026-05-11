# Runbook: Prefect flow stuck or scheduled run missed

**Severity ladder:**
- SEV3: one missed run on a low-criticality deployment (`streaming-monitor`, `whoop-poll` — next scheduled run catches up), OR a flow run RUNNING > 1.5x expected duration
- SEV2: any deployment shows two consecutive LATE/MISSED runs, OR a flow stuck RUNNING > 4x expected duration, OR `ehr-daily`/`pharmacy-daily` not completed by 09:00 UTC (downstream SLA)
- SEV1: worker pool has zero healthy workers, OR `dbt-weekly` failed to start on Friday (warehouse marts stale), OR `maintenance-nightly` missed ≥ 2 nights (compaction debt → producer-lag cascade)

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

## TL;DR (30-second triage)

```bash
# Which deployment is unhealthy + are workers alive + what's the stuck run doing?
prefect deployment ls
prefect worker ls --pool pulsetrack-pool
prefect flow-run inspect <run_id>           # id from UI or `prefect flow-run ls`
prefect worker logs --pool pulsetrack-pool --tail 200
```

Three flavors of "stuck flow" map to three fixes:
1. **Worker dead** (no workers in pool → restart it)
2. **Worker alive, flow dead-stuck** (subprocess hung — usually `dbt_build` past its 3600s timeout, or an EMR step polling a cluster in a non-terminal state)
3. **Worker alive, scheduler hasn't dispatched** (deployment paused, work-pool concurrency cap, infrastructure block creds expired)

## Symptoms (what triggered the page)

- Prefect Cloud UI: deployment row shows `Late` or `Missed` next-run badge
- Cloud alert email/Slack: "Flow run `<name>/<id>` has been running for Xh"
- `notify_pipeline_failure` did NOT fire — flow never completed → no exception path → no Slack post. This is the gap that masks stuck runs.
- Downstream: `streaming-monitor` alerts on stale silver freshness (02:00 `maintenance` didn't compact → next-day reads slow)
- `ehr-daily`/`pharmacy-daily` consumer queries against `pulsetrack_silver_<env>` show no new partitions past cron time

## Diagnosis (commands to run first)

```bash
prefect flow-run ls --state Running --limit 20
prefect flow-run inspect <run_id>           # look at state.timestamp, task_runs[], infrastructure_pid
```

Open Cloud UI → flow run → task graph. Deepest non-terminal task is the culprit. Common offenders:

| Flow | Likely-hung task | Default timeout | Notes |
|------|------------------|-----------------|-------|
| `ehr-daily`, `pharmacy-daily`, `full-refresh` | `submit_emr_step` | 7200s | Cluster in `STARTING` or `TERMINATING_WITH_ERRORS` — step never reaches terminal |
| `dbt-weekly`, `full-refresh` | `dbt_build` | 3600s (`orchestration/tasks/dbt_tasks.py:101`) | `subprocess.run(... timeout=3600)` raises after 1h; flow keeps waiting on wrapper |
| `whoop-poll` | `run_whoop_poller` | none from Prefect side | WHOOP 429 retry loop or expired token (see `runbooks/whoop_oauth_renewal.md`) |
| `streaming-monitor` | `list_yarn_apps` / `check_consumer_lag` | none | EMR ResourceManager unreachable, or Athena query stuck in QUEUED |
| `maintenance-nightly` | `iceberg_optimize` | 1800s | Many small files → rewrite genuinely takes long |

```bash
# Worker health
prefect worker ls --pool pulsetrack-pool
prefect worker inspect --pool pulsetrack-pool <worker_name>
ssh ec2-user@$WORKER_HOST "ps aux | grep 'prefect worker'; free -m | head -3; dmesg | tail"

# Infrastructure/credentials blocks (watch for expired AWS session tokens)
prefect block ls --block-type-slug aws-credentials

# Work-pool concurrency (all 7 deployments share pulsetrack-pool — full-refresh can starve daily flows)
prefect work-pool inspect pulsetrack-pool | grep -i concurrency
```

## Recovery (ranked by likelihood, fastest first)

### Case A: Worker dead — restart the worker (~50% of pages)

**Verify:** `prefect worker ls --pool pulsetrack-pool` shows `OFFLINE` or empty.

```bash
ssh ec2-user@$WORKER_HOST
sudo systemctl restart prefect-worker    # if systemd-managed
# OR (tmux/manual):
prefect worker start --pool pulsetrack-pool --name "$(hostname)-1"
```

Wait 60s; confirm `IDLE` via `prefect worker ls`. LATE runs auto-dispatch within one heartbeat (~15s). For stacked LATEs after a multi-hour outage, see Case D.

### Case B: Stuck flow run — cancel + re-dispatch

**Verify:** `prefect flow-run inspect <run_id>` shows RUNNING with `state.timestamp` > expected × 4 and no recent task-run state changes.

```bash
prefect flow-run cancel <run_id>                                       # SIGTERM to worker subprocess
prefect flow-run delete <run_id>                                       # if cancel doesn't take in 60s
prefect deployment run "ehr-daily/ehr-daily" --param aws_env=dev       # re-dispatch
```

**Critical:** if the hung task is `submit_emr_step`, the EMR step keeps running after Prefect cancel — the `cancel_steps` call inside `emr_tasks.py:166` only fires on the *timeout* code path, NOT on SIGTERM. Cancel the step explicitly:
```bash
aws emr list-steps --cluster-id "$PT_EMR_CLUSTER_ID" --step-states RUNNING PENDING
aws emr cancel-steps --cluster-id "$PT_EMR_CLUSTER_ID" --step-ids s-XXXXXXXX
```

### Case C: Hung `dbt_build` (dbt-weekly / full-refresh)

3600s in `orchestration/tasks/dbt_tasks.py:101` is generous — if past it, the warehouse is wedged (Snowflake clustering, Iceberg compaction-during-write, dep lock).

```bash
ssh ec2-user@$WORKER_HOST "ps -ef | grep 'dbt build' | grep -v grep"
# Kill subprocess; Prefect marks task FAILED:
ssh ec2-user@$WORKER_HOST "kill -TERM <pid>"
# Cancel the warehouse query (Snowflake SYSTEM$CANCEL_QUERY / Athena stop-query-execution).
# Re-run with --select narrowed:
prefect deployment run "dbt-weekly/dbt-weekly" --param run_docs=false
```

Follow-up issue: which model exceeded 3600s? Needs incrementalisation or its own deployment.

### Case D: Stacked LATE runs after extended outage

After a multi-hour worker outage, dozens of cron runs are LATE. Letting Prefect dispatch all at once will overload EMR + warehouse.

```bash
prefect flow-run ls --state Late --limit 100

# Daily flows are idempotent on grain key — keep only the most recent per deployment:
for run_id in $(prefect flow-run ls --state Late --deployment-name ehr-daily/ehr-daily --json \
    | jq -r '.[1:] | .[].id'); do
    prefect flow-run delete "$run_id"
done
# Repeat for pharmacy-daily, maintenance-nightly, dbt-weekly. Then manually re-run:
prefect deployment run "ehr-daily/ehr-daily"
```

For `whoop-poll` (15-min) and `streaming-monitor` (5-min), delete ALL LATEs — next tick is < 15 min away.

### Case E: Scheduler hasn't dispatched

**Verify:** workers alive but no RUNNING/LATE runs. Check `prefect deployment inspect ...` for `paused: True`, or concurrency cap reached.

```bash
prefect deployment resume "ehr-daily/ehr-daily"
prefect work-pool set-concurrency-limit pulsetrack-pool 10
# Rotate expired AWS creds block:
prefect block delete aws-credentials/pulsetrack-emr
prefect block create aws-credentials --name pulsetrack-emr \
    --aws-access-key-id "$AWS_ACCESS_KEY_ID" \
    --aws-secret-access-key "$AWS_SECRET_ACCESS_KEY"
```

## Verification (how you know it's fixed)

1. `prefect worker ls --pool pulsetrack-pool` shows ≥ 1 worker in `IDLE` or `RUNNING`
2. `prefect flow-run ls --state Running` no longer shows the stuck id
3. The next scheduled tick produces a `Completed` run (one cron cycle for daily; 15 min for whoop-poll; 5 min for streaming-monitor)
4. Slack `notify_pipeline_complete` fires from `orchestration/tasks/notification_tasks.py`
5. For `ehr-daily`/`pharmacy-daily`, confirm gold rows landed:
   ```bash
   aws athena start-query-execution \
       --query-string "SELECT MAX(load_dt) FROM pulsetrack_gold_dev.fact_lab_result" \
       --query-execution-context Database=pulsetrack_gold_dev \
       --result-configuration "OutputLocation=s3://$PT_LAKEHOUSE_BUCKET/athena-results/"
   ```

## Prevention (post-incident hardening)

1. **Worker watchdog:** systemd unit with `Restart=always` + `RestartSec=30` on the worker host; OOM/crash recovers without paging.
2. **Flow-level timeouts:** every `@flow` should set `timeout_seconds=` so Prefect itself fails a stuck run instead of relying on the subprocess timeout. None of the 7 flows do this today — follow-up.
3. **Heartbeat alert:** enable Prefect Cloud's built-in `Flow Run Heartbeat` automation at the work-pool level; pages on > 60s gap.
4. **Schedule-skew check:** extend `streaming-monitor` to query `prefect deployment ls --json` and page if any `next_run` is in the past — catches the silent "scheduler stuck" mode.
5. **`full-refresh` isolation:** because it's long-running and shares `pulsetrack-pool`, give it its own work pool so it can't starve `ehr-daily`/`pharmacy-daily`. Update `orchestration/prefect.yaml`.

## Related postmortems

- (None yet — file `postmortems/YYYY-MM-DD_prefect_<short_cause>.md` after the first SEV2 page using this runbook.)

## Related runbooks

- `runbooks/whoop_oauth_renewal.md` — `whoop-poll` deployment specifically; 401 on the WHOOP API masquerades as a stuck poller
- `runbooks/kafka_consumer_lag.md` — `streaming-monitor` deployment's downstream-symptom path
- `runbooks/secret_leak_response.md` — if a stuck flow turns out to be 403/InvalidClientTokenId from a rotated-but-not-republished credential
