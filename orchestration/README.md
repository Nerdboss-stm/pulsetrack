# PulseTrack — Prefect Cloud Orchestration

Replaces the Makefile + cron-on-laptop orchestration with Prefect Cloud.
Matches WHOOP's published migration from Makefile-driven jobs to
Prefect-managed flows.

## Structure

```
orchestration/
├── flows/                       7 flows
│   ├── daily_ehr_pipeline.py
│   ├── daily_pharmacy_pipeline.py
│   ├── streaming_monitor.py
│   ├── whoop_poll_pipeline.py
│   ├── maintenance_pipeline.py
│   ├── dbt_pipeline.py
│   └── full_refresh.py
├── tasks/                       5 task modules
│   ├── emr_tasks.py             EMR step submission
│   ├── spark_tasks.py           Spark job wrappers
│   ├── dbt_tasks.py             dbt subprocess wrappers
│   ├── quality_tasks.py         GX + monitor tasks
│   └── notification_tasks.py    Slack + SNS
├── deployments.py               serve() registration script
├── prefect.yaml                 deployment config for `prefect deploy`
├── requirements.txt
└── README.md
```

## Deployments + cadence

| Deployment | Cadence | Purpose |
|------------|---------|---------|
| `ehr-daily` | cron `0 6 * * *` (06:00 UTC) | Daily EHR ingestion → silver → identity → gold |
| `pharmacy-daily` | cron `0 7 * * *` (07:00 UTC) | OpenFDA poll → bronze → silver → identity → gold |
| `whoop-poll` | interval 900 (15 min) | Poll WHOOP API for new wearable data |
| `streaming-monitor` | interval 300 (5 min) | Watchdog for bronze/silver/gold streaming queries |
| `maintenance-nightly` | cron `0 2 * * *` (02:00 UTC) | OPTIMIZE / expire_snapshots / orphan-cleanup |
| `dbt-weekly` | cron `0 10 * * 5` (Friday 10:00 UTC) | Full dbt build (matches WHOOP's deploy day) |
| `full-refresh` | none — manual only | Rebuild silver+gold from bronze (recovery) |

## Quick start

### Local agent (no Prefect Cloud)

```bash
pip install -r orchestration/requirements.txt

# In one terminal — runs the agent + serves all 7 deployments locally:
python -m orchestration.deployments

# In another — kick off a flow run on demand:
python -c "
from orchestration.flows.dbt_pipeline import dbt_weekly
dbt_weekly()
"
```

### Prefect Cloud

```bash
# One-time setup
prefect cloud login --key $PREFECT_API_KEY --workspace your-org/your-workspace
prefect work-pool create pulsetrack-pool --type process

# Register deployments from prefect.yaml
cd orchestration
prefect deploy --all

# Start a worker that pulls runs from Prefect Cloud
prefect worker start --pool pulsetrack-pool
```

## Environment variables

Each flow + task module reads config from environment. Set these in the
Prefect worker environment (or via Prefect Blocks for secrets):

| Var | Default | Used by |
|-----|---------|---------|
| `AWS_DEFAULT_REGION` | `us-east-1` | All AWS-touching tasks |
| `PT_EMR_CLUSTER_ID` | (required) | `submit_emr_step` |
| `PT_LAKEHOUSE_BUCKET` | (required) | `submit_emr_step`, S3 uploads |
| `PT_AWS_ENV` | `dev` | Quality tasks, Glue DB suffix |
| `PT_DBT_PROJECT_DIR` | `/Users/.../dbt_project` | `dbt_*` tasks |
| `PT_DBT_TARGET` | `dev` | `dbt_*` tasks default |
| `PT_PROJECT_ROOT` | `/Users/.../pulsetrack-cm` | producer subprocess cwd |
| `PT_SLACK_WEBHOOK_URL` | (none) | `notify_slack` |
| `PT_SNS_ALERT_TOPIC_ARN` | (defaults to dev) | `notify_sns` |
| `WHOOP_CLIENT_ID` | (required for whoop-poll) | WHOOP API |
| `WHOOP_CLIENT_SECRET` | (required for whoop-poll) | WHOOP API |

For Prefect Cloud, store secrets as **Prefect Blocks** (Secret + AWS
Credentials types) and reference them in flow code via
`Secret.load("name").get()`.

## Design notes

### Why subprocess invocation for dbt + producer, EMR API for Spark?

Three different execution contexts:

1. **dbt** — runs as a single Python process. The Prefect worker has
   dbt-core installed; subprocess invocation is straightforward.
   Output capture via `run_results.json` parsing.

2. **Producer / OpenFDA poller** — single Python process, REST
   client + Kafka producer. Doesn't need Spark/YARN. Subprocess on
   the worker.

3. **Spark transforms** — heavy YARN apps that need cluster compute.
   Cannot run on the Prefect worker; must be submitted as EMR steps
   via boto3. EMR steps:
   - Get first-class lifecycle tracking (PENDING → RUNNING →
     COMPLETED/FAILED).
   - Persist in EMR's step history (audit).
   - Honor cluster auto-termination policies correctly.

### Retry strategy

| Task type | Retries | Backoff | Why |
|-----------|---------|---------|-----|
| EMR step submit | 2 | exponential (60s base) | API rate limits |
| Cluster state check | 1 | none | Polling; one retry is enough |
| dbt subprocess | 0 / 1 | none | dbt's own retries inside; one outer retry for transient process issues |
| dbt tests | 0 | none | Test failures should fail the flow |
| Slack notification | 2 | none | Best-effort; webhook timeouts |
| Producer | 2 | none | Transient API failures |
| Quality check | 1 | none | Athena flaky on rate-limited |

### Failure handling

Each flow wraps its task graph in try/except. On exception:
1. Log the error with full context.
2. Call `notify_pipeline_failure` (Slack + SNS).
3. Re-raise so Prefect marks the run FAILED.

The `notify_*` tasks are best-effort and don't raise — a Slack outage
shouldn't take down the flow's notification path.

### Idempotency

All EMR-step jobs are idempotent on their grain key:
- `ehr_silver` — MERGE INTO on (condition_id) / (medication_id).
- `identity_bridge` — full rebuild from sources.
- `gold_dim_*` — `INSERT OVERWRITE` on dim primary keys.
- `gold_fact_*` — MERGE INTO on grain key.

Re-running a failed flow (after fixing the underlying issue) is safe.
The dbt models are also idempotent (full-refresh + MERGE INTO).

The producer/poller maintain offset files (.whoop_poll_offsets.json,
.openfda_offset) to avoid re-publishing already-fetched data on
re-runs.

### Why streaming-monitor instead of relying on Prefect's flow-level alerts?

Prefect alerts on flow failure. The streaming pipeline isn't a flow —
it's a long-running YARN application that the Prefect framework
doesn't see. A separate monitor flow (running every 5 min) checks the
streams' health and surfaces alerts through the same Slack/SNS
channels.

This is the same pattern WHOOP uses: orchestrator schedules + monitors
the streaming workload; the streaming workload itself runs outside
the orchestrator's control plane.

## References

- WHOOP's Prefect migration (their engineering blog).
- Prefect 3 docs: https://docs.prefect.io
- PulseTrack runbook: `docs/PRODUCTION_RUNBOOK.md` for the streaming
  layer this orchestration sits above.
