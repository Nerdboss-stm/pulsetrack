# Scale test results — 10M events, 50K users

**Status:** TEMPLATE — populated by `benchmarks/scale_test_report.py` after Phase 2 execution

**Test window:** `<<TEST_START>>` → `<<TEST_END>>`
**Cluster:** `<<EMR_CLUSTER_ID>>` (4 core nodes m5.xlarge spot)

---

## 1. Test summary

| Metric | Value |
|---|---|
| Target events | 10,000,000 |
| Target users | 50,000 |
| Actual events produced | `<<FILL>>` |
| Actual events landed in bronze | `<<FILL>>` |
| Producer duration | `<<FILL>>` |
| Pipeline drain duration (last producer event → silver visible) | `<<FILL>>` |
| Total wall-clock test duration | `<<FILL>>` |

## 2. Throughput

| Metric | Target | Actual |
|---|---|---|
| batch_scale_producer peak | 25,000 rec/s | `<<FILL>>` |
| batch_scale_producer avg | 20,000 rec/s | `<<FILL>>` |
| whoop_api producer | 1-5 rec/s (real account size) | `<<FILL>>` |
| openfda producer | 0.01 rec/s | `<<FILL>>` |
| fhir producer | 0.05 rec/s | `<<FILL>>` |
| Aggregate peak | 25,000 rec/s | `<<FILL>>` |
| MSK ingress peak (MB/s) | 7 MB/s | `<<FILL>>` |

## 3. Latency (Kafka publish → silver visible)

| Percentile | Target | Actual |
|---|---|---|
| p50 | < 30s | `<<FILL>>` |
| p95 | < 60s | `<<FILL>>` |
| p99 | < 120s | `<<FILL>>` |
| max | < 300s | `<<FILL>>` |

Methodology: sampled 1000 events by reading `event_timestamp` from bronze and finding their first appearance in silver (matched on `reading_id`).

## 4. Iceberg metadata

| Table | Data files | Total size (MB) | Avg file (MB) | Snapshots |
|---|---|---|---|---|
| bronze/sensor_readings | `<<FILL>>` | `<<FILL>>` | `<<FILL>>` | `<<FILL>>` |
| silver/sensor_readings | `<<FILL>>` | `<<FILL>>` | `<<FILL>>` | `<<FILL>>` |
| gold/fact_vital_reading | `<<FILL>>` | `<<FILL>>` | `<<FILL>>` | `<<FILL>>` |
| gold/fact_vital_daily_summary | `<<FILL>>` | `<<FILL>>` | `<<FILL>>` | `<<FILL>>` |

**Expected after compaction (post-test OPTIMIZE):** ~30 large files per table at ≤512MB each.

## 5. Identity bridge resolution

| Metric | Target | Actual |
|---|---|---|
| Total silver rows | ~50M (10M events × ~5 metrics/event avg) | `<<FILL>>` |
| Rows with `patient_key NOT NULL` | ≥95% | `<<FILL>>` |
| Resolution method breakdown | (email/MRN/device/FDA) | `<<FILL>>` |
| Unresolved bucket (DLQ identity) | ≤5% | `<<FILL>>` |

## 6. EMR cluster metrics

| Metric | Value |
|---|---|
| Cluster state | `<<FILL>>` |
| Release label | `<<FILL>>` |
| Steps submitted | `<<FILL>>` |
| Steps completed | `<<FILL>>` |
| Step duration p50 | `<<FILL>>` |
| Step duration p95 | `<<FILL>>` |
| Peak executor count | `<<FILL>>` |
| Peak active tasks | `<<FILL>>` |

## 7. Chaos engineering drills

### Drill 1 — Single executor kill

| Metric | Target | Actual |
|---|---|---|
| Result | PASS | `<<FILL>>` |
| Recovery time | < 60s | `<<FILL>>` |
| Killed container | (specific ID) | `<<FILL>>` |
| Replacement container | (specific ID) | `<<FILL>>` |
| Data loss observed | 0 events | `<<FILL>>` |
| Downstream impact | None (producer rate unchanged) | `<<FILL>>` |

Full timeline + log excerpts: [`postmortems/2026-05-XX_chaos_drill_1_executor_kill.md`](../postmortems/)

### Drill 2 — Full app kill

| Metric | Target | Actual |
|---|---|---|
| Result | PASS | `<<FILL>>` |
| Kill → new app RUNNING | < 5 min | `<<FILL>>` |
| Killed app ID | (specific) | `<<FILL>>` |
| Replacement app ID | (specific) | `<<FILL>>` |
| Data loss observed | 0 events | `<<FILL>>` |
| Bronze ingestion paused? | No (independent stream) | `<<FILL>>` |
| Iceberg checkpoint replay successful | Yes | `<<FILL>>` |

Full timeline + log excerpts: [`postmortems/2026-05-XX_chaos_drill_2_app_kill.md`](../postmortems/)

## 8. Cost (actuals from Cost Explorer, 24h lag)

| Line | Estimate | Actual |
|---|---|---|
| EMR | $1.39 | `<<FILL>>` |
| EBS | $0.08 | `<<FILL>>` |
| MSK Serverless | $0.018 | `<<FILL>>` |
| S3 | $0.05 | `<<FILL>>` |
| CloudWatch | $0.30 | `<<FILL>>` |
| Misc | $0.01 | `<<FILL>>` |
| **Total** | **$1.85** | **`<<FILL>>`** |

Per-million-events: `<<FILL>>` (target: $0.20 / 1M events).

## 9. SLO compliance

From `docs/slos.md`:

| SLO | Target | Actual | Met? |
|---|---|---|---|
| Silver lag p95 | < 60s | `<<FILL>>` | `<<FILL>>` |
| Stream uptime during test (ex. chaos drills) | 100% | `<<FILL>>` | `<<FILL>>` |
| `is_valid` rate in silver | ≥95% | `<<FILL>>` | `<<FILL>>` |
| Identity resolution rate | ≥95% | `<<FILL>>` | `<<FILL>>` |
| Chaos drill 1 recovery | <60s | `<<FILL>>` | `<<FILL>>` |
| Chaos drill 2 recovery | <300s | `<<FILL>>` | `<<FILL>>` |
| Producer throughput | ≥25k rec/s | `<<FILL>>` | `<<FILL>>` |
| Cost per million events | <$1.00 | `<<FILL>>` | `<<FILL>>` |

## 10. Screenshots

Captured during the test by `scripts/capture_grafana_screenshots.py`:

- [`throughput.png`](screenshots/throughput.png) — MSK ingress + per-stream record rate
- [`consumer_lag.png`](screenshots/consumer_lag.png) — Kafka consumer-group lag
- [`silver_processing_latency.png`](screenshots/silver_processing_latency.png) — p50/p95/p99
- [`iceberg_file_count.png`](screenshots/iceberg_file_count.png) — file count growth + post-OPTIMIZE
- [`chaos_recovery.png`](screenshots/chaos_recovery.png) — moment-of-kill + recovery
- [`cost_burn.png`](screenshots/cost_burn.png) — AWS Cost Explorer for the test window
- [`prefect_flow_status.png`](screenshots/prefect_flow_status.png) — all 7 deployments

## 11. Consumer-side validation

| Consumer | Query | Expected | Actual |
|---|---|---|---|
| BI | `SELECT COUNT(*) FROM ANALYTICS.vw_patient_health_360` | > 0 | `<<FILL>>` |
| BI | `SELECT MAX(last_vital_ts) FROM ANALYTICS.vw_patient_health_360` | within 5 min | `<<FILL>>` |
| Anomaly | `SELECT COUNT(*) FROM ANALYTICS.vw_anomaly_dashboard WHERE event_date = CURRENT_DATE` | > 0 (seeded 0.3% impossible) | `<<FILL>>` |
| Personal | `SELECT COUNT(*) FROM ANALYTICS.vw_whoop_my_health WHERE patient_email = '...'` | > 0 if WHOOP creds set | `<<FILL>>` |
| ML | Athena fact + dim join (10k rows) | shape (10000, N) | `<<FILL>>` |
| Slack | Anomaly alert routed | at least 1 received | `<<FILL>>` |

## 12. Postmortems written

| Type | File |
|---|---|
| Chaos drill 1 | `<<FILL>>` |
| Chaos drill 2 | `<<FILL>>` |
| Organic incidents (if any) | `<<FILL>>` |

## 13. What we'd change for the next scale test

(Populated post-test from the operator journal.)

- `<<FILL>>`
- `<<FILL>>`
- `<<FILL>>`

## 14. Sign-off

| Role | Name | Date |
|---|---|---|
| Test owner | `<<FILL>>` | `<<FILL>>` |
| Reviewer | `<<FILL>>` | `<<FILL>>` |
