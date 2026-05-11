# PulseTrack Service Level Objectives

The SLO catalog. Defines what "working" means in measurable terms, per layer, and how alerts map to error budgets.

**Audience:** on-call DEs, the eng lead reviewing burn rates at the Monday standup, and anyone asking "is this normal?". Review quarterly; revise after every SEV1.

**Reporting cadence:** SLI values are emitted by `observability/monitors.py` into `glue_iceberg.pulsetrack_gold_dev.monitor_runs`. The Snowflake `vw_slo_dashboard` view rolls them up over 30-day windows. Error budgets are computed off that view.

---

## 1. Error-budget primer

Every SLO has an **objective** (target percentage) and an **error budget** (the allowed shortfall over the rolling window). Budget is *spent*, not *banked* — when you blow through 100% in a window, you stop shipping risky changes until the SLI recovers.

| Objective | Error budget per 30 days | Per week | Per day | Per hour |
|---|---|---|---|---|
| 99.9% | 43m 12s | 10m 04s | 1m 26s | 3.6s |
| 99.5% | 3h 36m | 50m 24s | 7m 12s | 18.0s |
| 99.0% | 7h 12m | 1h 40m 48s | 14m 24s | 36.0s |
| 95.0% | 36h 00m | 8h 24m | 1h 12m | 3m 00s |

PulseTrack's default tier is **99.5% availability + 99.0% freshness** across most pipelines. Higher targets (99.9%) are reserved for layers where a real consumer would page (silver `is_valid`, identity-bridge). Lower targets (95%) are tolerated where degraded mode is acceptable (Anthropic anomaly explainer, OpenFDA hourly poll).

### Burn-rate alerting (two-window, two-rate)

Adapted from the Google SRE workbook. Two simultaneous conditions catch both fast-burn (page) and slow-burn (ticket) failures.

| Window pair | Threshold | Action | Routing |
|---|---|---|---|
| 1h *and* 5m | 2% of 30-day budget burned | **Page on-call** (SEV1/SEV2) | PagerDuty (mocked); Slack `#pulsetrack-alerts` |
| 6h *and* 30m | 5% of 30-day budget burned | **Open ticket** | Slack `#pulsetrack-alerts`; Linear |
| 3d *and* 6h | 10% of 30-day budget burned | **Email weekly review** | `data-platform@pulsetrack` |

Encoded in `observability/sql/monitor_spec.yaml`. Two-window pairing prevents single-spike flapping: a 1-minute outage doesn't page, but a sustained 5-minute breach does.

---

## 2. SLO catalog

13 SLOs grouped by dimension. Each row is a contract; "current actual" placeholders fill in from the Snowflake rollup view.

### 2.1 Freshness — how stale is the data?

Freshness is measured per layer as `max(timestamp_col) - now()`, sampled every minute by `observability/monitors.py:check_freshness`.

| # | SLO name | Layer | SLI definition | Target | Alert threshold | Current actual | Runbook |
|---|---|---|---|---|---|---|---|
| F1 | `bronze_sensor_freshness` | Bronze | p95 of `now - max(ingestion_timestamp)` over 5-min windows on `pulsetrack_bronze_dev.sensor_readings` | < 5 min | breach for 3 consecutive minutes | _TBD (placeholder until first month of prod telemetry)_ | `runbooks/kafka_consumer_lag.md` |
| F2 | `silver_sensor_lag` | Silver | p95 lag from Kafka commit → silver visible row, measured by Spark `StreamingQueryListener` `batchDuration + processingDelay` | < 60s | p95 > 90s for 5m | _TBD_ | `runbooks/kafka_consumer_lag.md` |
| F3 | `silver_ehr_freshness` | Silver | `now - max(ingestion_timestamp)` on `pulsetrack_silver_dev.ehr_conditions` (batch, daily) | < 26h | breach > 28h | _TBD_ | `runbooks/ehr_batch_stale.md` (TODO) |
| F4 | `gold_fact_freshness` | Gold | p95 of `now - max(updated_at)` on `fact_vital_daily_summary` | < 10 min | p95 > 15 min for 10m | _TBD_ | `runbooks/gold_stream_stuck.md` (TODO) |
| F5 | `snowflake_view_freshness` | Snowflake | `now - max(event_ts)` from `vw_patient_health_360`, polled by `observability/cli.py snowflake-check` every 5m | < 15 min | breach > 20 min | _TBD_ | `docs/PRODUCTION_RUNBOOK.md` § 4.1 |

**Notes on freshness measurement:**

- We use **wall-clock ingestion lag**, not event-time skew. Event-time skew (event happened in the past, arrives now) is normal for WHOOP backfills; what we care about is "did the pipeline stall?".
- F2 includes the trigger interval (`30 seconds` default in `config.py:trigger_interval`). A 60s p95 target implies one trigger of slack + one of processing. Tighter would force `processingTime` trigger to `15 seconds` and 2× the EMR cost.
- F4's 10-min target reflects bronze → silver (60s) + silver → gold (≤9 min on `streaming-skip-overwrite-snapshots=true` reader; see `PRODUCTION_RUNBOOK.md` § 4.1).

### 2.2 Completeness — did we get everything?

Completeness asks: "of the events that *should* be here, what fraction *are* here?" Measured by reconciling producer-emit counts to consumer-visible counts.

| # | SLO name | Layer | SLI definition | Target | Alert threshold | Current actual | Runbook |
|---|---|---|---|---|---|---|---|
| C1 | `bronze_completeness` | Bronze | `count(bronze) / count(producer_emit_metric)` rolled per 1h. Producer metric `records_produced_total` from `metrics.py` divided by bronze rowcount for the same hour. | ≥ 99.95% | < 99.9% over 2 consecutive hours | _TBD_ | `runbooks/kafka_consumer_lag.md` Case D |
| C2 | `dlq_rate` | Bronze→Silver | `count(dlq) / count(bronze)` per 1h, computed from `pulsetrack_dlq` topic offsets vs bronze rowcount | < 0.5% | > 1% over 1h | _TBD_ | `runbooks/dlq_buildup.md` (TODO) |
| C3 | `silver_explosion_ratio` | Silver | `count(silver_sensor) / (count(bronze_sensor) * avg_metrics_per_event)` — expect ~9× (smartwatch emits ~9 metrics per reading). Deviation > 5% indicates lost rows in the explode step. | 0.95 ≤ ratio ≤ 1.05 | outside band for 2 consecutive 1h windows | _TBD_ | `docs/PRODUCTION_RUNBOOK.md` § 4.1 |
| C4 | `identity_resolution` | Silver | `count(silver_sensor rows with patient_key not null) / count(silver_sensor)` over 24h | ≥ 95% | < 92% over 24h | _TBD_ | `runbooks/identity_bridge_drift.md` (TODO) |

**Notes:**

- C1's 99.95% target leaves room for the librdkafka in-flight queue at producer shutdown (we use `flush(timeout=30)` + waitable produce confirmation, but extreme network blips can still drop the last ~50 messages). Anything below 99.9% indicates real loss — investigate.
- C3's window is the explosion ratio, not raw count parity. Bronze is one row per device-event; silver is one row per device-event-metric. A consistent ~9× explosion is the baseline; sudden drops to ~6× mean we lost metric keys somewhere.
- C4 is the foundation for any cross-device patient analytics. 95% is the publishable threshold; 90% is degraded but tolerable; below 90% we redesign the bridge.
- **Reality-check (2026-05-11 scale test):** C4 measured **0%** during the 10M-event run. Two compounding root causes: (a) the `identity_bridge` Prefect step was not included in the scale-test orchestration sequence (`scripts/run_scale_test.sh` runs bronze → silver → gold without the bridge step in between), and (b) the synthetic event payloads carry no patient identity columns (no email, MRN, or device-pairing telemetry) so even if the bridge had run it would have produced 0 rows. C4 is therefore aspirational on synthetic data and only meaningful once either (i) producer payloads include identity columns, or (ii) the EHR-feed silver tables are populated. See §4.5 below for the action items.

### 2.3 Accuracy — is the data correct?

Accuracy is checked by Great Expectations suites against silver and gold (see `data_quality/expectations/*_suite.py`) plus distribution monitors on individual columns.

| # | SLO name | Layer | SLI definition | Target | Alert threshold | Current actual | Runbook |
|---|---|---|---|---|---|---|---|
| A1 | `silver_validity_rate` | Silver | `count(is_valid=true) / count(*)` over 1h on `pulsetrack_silver_dev.sensor_readings` | ≥ 99.5% | < 99% over 1h | _TBD_ | `runbooks/gx_failure_drains_batch.md` |
| A2 | `gold_gx_pass_rate` | Gold | `gold_vitals_suite` GX suite — count(passed expectations) / count(total expectations) over the last 24h batch | 100% | any expectation fails | _TBD_ | `runbooks/gx_failure_drains_batch.md` |
| A3 | `schema_compatibility` | All | every Avro schema version is BACKWARD-compatible with all previously deployed versions (checked at Schema Registry on producer register) | 100% | any incompatible attempt | n/a — enforced at registry write | `docs/data_contracts.md` § 2 |

**Notes:**

- A1 is the silver business-rule gate: out-of-range values (HR > 250, SpO₂ > 100, temperature outside 30–45 °C) are flagged in `is_valid=false`. They're kept (for analysis) but excluded from gold by the silver→gold join condition.
- A2 is binary — any GX failure on a daily batch fails the run and pages on-call. Gold facts are append-only and the suite is the contract; "warn-on-fail" is not appropriate here.
- A3 is enforced by Confluent Schema Registry (when running against MSK Serverless with a schema-registry sidecar) or by the in-repo `schemas/registry.py` shim. Backward-only ensures consumers running version N can read producers writing N+1.

### 2.4 Availability — is the layer up?

Availability is *not* "did Spark crash" — it's "could a downstream consumer get fresh data?". Measured as the union of freshness + completeness staying inside SLO over the window.

| # | SLO name | Layer | SLI definition | Target | Alert threshold | Current actual | Runbook |
|---|---|---|---|---|---|---|---|
| V1 | `bronze_availability` | Bronze | minute is "up" if F1 + C1 both pass for that minute; SLO = uptime fraction over 30d | ≥ 99.5% | budget-burn > 2% / 1h (page) | _TBD_ | `runbooks/emr_step_failure.md` (TODO) |
| V2 | `silver_availability` | Silver | minute is "up" if F2 + C3 + A1 all pass | ≥ 99.5% | budget-burn > 2% / 1h (page) | _TBD_ | `runbooks/silver_cold_start_hang.md` (TODO) |
| V3 | `gold_availability` | Gold | F4 + A2 (rolling 24h gate, evaluated daily) | ≥ 99.0% | any single-day GX failure | _TBD_ | `runbooks/gx_failure_drains_batch.md` |
| V4 | `snowflake_consumer_availability` | Snowflake | F5 + auto-refresh success rate from `vw_snowflake_refresh_log` | ≥ 99.0% | refresh fails 2 consecutive runs | _TBD_ | `docs/PRODUCTION_RUNBOOK.md` § 4.1 |

**Notes:**

- 99.5% bronze + silver gives 3h 36m monthly downtime budget. The chaos drills consume ~30m of that per quarter; the rest is overhead for real incidents.
- 99.0% gold is intentionally lower than silver — daily batch jobs have legitimate 30-minute outages during business hours (devs running migrations, OPTIMIZE compactions, etc.) that we don't want to page on.
- V4 includes Snowflake's own AUTO_REFRESH lag (variable, AWS-side). When the AWS-side refresh interval blows past 15 min, we manually `ALTER ICEBERG TABLE ... REFRESH` (see scale-test runbook).

---

## 3. Computing burn rates

Implementation lives in `observability/cli.py burn-rates`. The math:

```
budget_burned_pct(window) = (1 - SLI(window)) / (1 - SLO) × 100
```

where `SLI(window)` is the success ratio over the window and `SLO` is the target. For a 99.5% SLO with 99.0% measured over 1h: `(1 - 0.99) / (1 - 0.995) = 2.0` — 200% of the per-hour budget burned, **page** triggered.

**Worked example for F2 (silver_sensor_lag):**

- 30-day window: 30 × 24 × 60 = 43,200 minutes
- 99.5% objective → 216 minutes monthly budget
- 1h alert window: 60 minutes
- 2% of monthly budget burned in 1h = 4.32 minutes
- → if silver lag exceeds 60s for more than 4.32 minutes in any 1h window, the burn-rate alert fires

The actual SLI is measured by counting (lagged-minutes / total-minutes) in the Spark `StreamingQueryListener` and aggregating to `monitor_runs`.

---

## 4. Operational rituals

### Weekly review (Monday 30m)

- Review `vw_slo_dashboard` for the prior week
- Any SLO with >20% budget burned: add a Linear ticket
- Any SLO that hit 100% budget: schedule a postmortem the same week
- Note any planned capacity-affecting changes (migrations, large backfills) — confirm they don't blow remaining budget

### Quarterly review

- Tune targets based on actual: if F2's actual p95 has been 25s for 3 months, tighten target to 45s and reclaim alert sensitivity
- Add SLOs for any new pipeline layer (e.g., dbt mart freshness when dbt-Cloud goes live)
- Remove SLOs for retired layers
- Re-validate burn-rate thresholds — too sensitive = alert fatigue; too lax = miss real outages

### Per SEV1

- Review SLO incident: did the alert fire? Did it fire in time?
- If alert fired late: tighten the burn-rate window
- If alert fired but operator ignored: review escalation (`docs/on_call.md`)
- If no alert fired but should have: this is an SLO gap — add or tighten

---

## 4.5 SLO violations observed in 2026-05-11 scale test

The 10M-event scale test (cluster `j-T5OF7WBI2I4V`, 2-core m5.xlarge on-demand;
see `docs/scale_test_results.md` and postmortem `2026-05-11_emr_cluster_bringup_13_incidents.md`)
exposed five SLOs that were either violated or could not be measured. They are listed
here as the source-of-truth gap analysis between the SLO contract and what the
production-grade test run actually delivered.

| SLO | Target | Observed | Status | Root cause |
|---|---|---|---|---|
| **C4** `identity_resolution` | ≥ 95% over 24h | **0% actual** | **VIOLATED** | `identity_bridge` Prefect step was not orchestrated as part of `scripts/run_scale_test.sh`; additionally, synthetic event payloads carry no identity columns (no email, MRN, device-pairing) and no EHR feed was produced. `dim_patient` Spark step subsequently failed with `UNRESOLVED_COLUMN: age_group` because the bridge output had 0 rows. |
| **F2** `silver_sensor_lag` (p50/p95/p99) | p95 < 60s | **NOT MEASURED** (target referenced as p50 < 30s, p95 < 60s, p99 < 120s in the test plan) | **UNVERIFIED** | The test ran in batch mode (`--trigger available_now` + `--mode batch`, see ADR-008) after the streaming topology hit YARN starvation on the 2-core cluster. Batch mode has no Kafka-commit-to-silver-visible lag to measure. Latency instrumentation also missing from the event payload: producer does not emit `producer_ts`, so even in streaming mode we would compute lag from `ingestion_timestamp` only (one-sided). |
| **F4** `gold_fact_freshness` | p95 < 10 min | **NOT MEASURED** | Same root cause as F2: gold ran as batch (`--mode batch`) after silver completed. No live-stream freshness signal was emitted. |
| **Chaos drill — kill-task recovery** | < 60s recovery via checkpoint replay | **NOT TESTED** | Chaos drill scripts (`scripts/chaos/kill_spark_task.py`, `scripts/chaos/kill_spark_app.py`) require live streaming queries to disrupt. The pipeline was running in batch mode for the duration of the test, so there were no live streams. Drill scripts remain code-complete and reusable. |
| **V1/V2** stream uptime during chaos | 100% | **N/A** | Streams not running; uptime contract not applicable to a batch-mode run. |

### Action items to close the gaps

| # | Action | Owner | Target SLO | Priority |
|---|---|---|---|---|
| SLO-1 | Add `identity_bridge` step to `scripts/run_scale_test.sh` between silver and gold-dim steps; bridge must produce ≥ 1 row before `dim_patient` runs. | PulseTrack DE | C4 | P0 |
| SLO-2 | Inject patient identity columns into synthetic event payloads at the producer (`data_generators/synthetic/wearable_generator.py`) — minimum: `device_user_uuid` mapping to a patient pool of 50K. Backfill the same field into the wearable simulator schema. | PulseTrack DE | C4 | P0 |
| SLO-3 | Emit `producer_ts` (Kafka publish wall-clock) in every event payload; capture `silver_write_ts` in `sensor_silver.foreachBatch` sink; compute and persist Kafka→silver latency to `monitor_runs` for F2 p50/p95/p99 telemetry. | PulseTrack DE | F2 | P0 |
| SLO-4 | Same `producer_ts` → `gold_write_ts` flow for F4. | PulseTrack DE | F4 | P0 |
| SLO-5 | Procure 4-core m5.xlarge cluster (or equivalent) for the next scale test so the streaming topology of ADR-005 actually runs end-to-end; only then are F2/F4 and chaos drills measurable. See ADR-008 for the streaming/batch threshold. | PulseTrack DE | F2, F4, V1, V2 | P0 |
| SLO-6 | Execute `scripts/chaos/kill_spark_task.py` against the running bronze streaming query on the 4-core run; record recovery time from checkpoint replay; budget the result against the 60s target. | PulseTrack DE | V1, V2 | P1 |
| SLO-7 | Re-validate C4 once SLO-1 + SLO-2 land. Target: 95% on first run with synthetic identity columns; if below, redesign the bridge logic before promoting C4 to a production SLI. | PulseTrack DE | C4 | P1 |

### Honest assessment

This scale test proved bronze + silver + gold can move 10M events end-to-end on
a constrained cluster. It did **not** prove the freshness, identity, or chaos-resilience
SLOs at all. The next test must run on a 4-core cluster with the action items above
landed; only then is the SLO catalog reality-tested against production-shape behavior.
Until then, F2, F4, C4, V1, V2 should be treated as **unvalidated targets**, not as
SLIs we have confidence in.

---

## 5. What is explicitly NOT an SLO

To avoid sprawl, these are explicitly *not* SLOs (they're monitored but not budgeted):

- **EMR cluster uptime.** EMR is an implementation detail. The streaming app must recover when EMR fails; the SLO is on F2/V2, not on the cluster.
- **Anthropic API success rate.** The anomaly explainer is best-effort; failures degrade to "explanation unavailable" without paging.
- **OpenFDA poll success rate.** External feed; transient failures are expected. We page only if 24h of polls fail in a row.
- **Prefect flow scheduling reliability.** Prefect Cloud handles this; if it fails, the daily flows are late, not lost (each flow is idempotent).
- **dbt build wall-clock time.** Measured but not budgeted; tracked for capacity planning, not paged on.

---

## 6. References

- `observability/sql/monitor_spec.yaml` — concrete thresholds per table
- `observability/monitors.py` — SLI implementations
- `observability/alerting.py` — burn-rate → channel routing
- `data_quality/expectations/*_suite.py` — GX suites backing A1/A2
- `docs/PRODUCTION_RUNBOOK.md` — overarching pipeline ops
- `docs/on_call.md` — paging and escalation
- `docs/data_contracts.md` — A3 schema-compatibility rules
- Google SRE Workbook ch. 5 (multi-window/multi-burn-rate alerts) — original source of the two-window pattern
