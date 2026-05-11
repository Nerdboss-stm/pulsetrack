# Scale test capacity plan — 10M events, 50K users

**Test owner:** PulseTrack DE
**Test date:** 2026-05-11 (Phase 2 of Prompt 9; revised after gap-closure run)
**Cluster:** EMR 7.13.0 dev — 4 core m5.xlarge **on-demand** (was originally 2-core spot; learned the hard way that streaming workloads can't tolerate spot reclamation and that 2 cores starve under 3 concurrent streaming queries — see ADR-007 + ADR-008 + `postmortems/2026-05-11_emr_cluster_bringup_13_incidents.md`)
**Table format:** Iceberg (restored after the prior run retreated to Delta on 2-core; per ADR-007, Iceberg is the correct choice for ≥4-core clusters)
**Target SLAs:**
- Total ingestion ≤ 30 min
- Silver lag (Kafka → silver visible) p95 < 60s (now measurable via `kafka_timestamp` + `silver_write_ts`; `benchmarks/measure_e2e_latency.py`)
- No data loss across chaos drills
- Cost ≤ $50 (revised up from $10 — on-demand 4-core for ~3h is structurally more expensive than the original 2-core spot estimate)
- Identity resolution rate ≥ 95% (now achievable: orchestrator finally runs `identity_bridge` step explicitly)
- All 5 gold dims build successfully (`dim_patient` fix shipped — empty-EHR schema now includes `age_group`)

This document is the pre-test capacity math. Run before any actual money is spent. If any number here is wrong by an order of magnitude, the test is misconfigured — stop, fix, re-plan.

## 1. Objectives + non-objectives

**In scope (objectives):**
1. Sustain producer throughput ≥ 25,000 rec/s for ≥ 30 min
2. End-to-end streaming pipeline processes all 10M events within 45 min of producer completion
3. Identity bridge resolves ≥ 95% of silver rows to a `patient_key` within 24h
4. Two chaos drills succeed:
   - Drill 1: silver executor kill → <60s recovery
   - Drill 2: silver app kill → <5min recovery
5. Snowflake consumer views return data within 5 min of gold writes
6. All 7 Prefect deployments retain their schedules during the test
7. Total AWS cost ≤ $10

**Explicitly out of scope:**
- Multi-region failover
- Cross-AZ writes (single-AZ test for cost)
- HIPAA-grade encryption test (BAA + KMS+customer-key is Phase-3 work)
- Anomaly-detection ML pipeline at scale (LLM cost cap is $1)

## 2. Cluster sizing

| Component | Config | Capacity |
|---|---|---|
| EMR master | m5.xlarge (4 vCPU, 16GB) | Driver + YARN RM + ApplicationMaster overhead |
| EMR core | m5.xlarge × **4** **on-demand** (~$0.192/h × 4 = $0.77/h core) | 16 vCPU, 64GB total |
| EBS per node | 64 GB | Shuffle space + log retention |
| Spark default | 7 executors × 2 cores × 5GB | dynamic alloc 1-12 |
| Idle timeout | 7200s | Auto-terminate if streams stop |

**Why on-demand (not spot):** Streaming workloads need stable executor capacity to keep up with the producer write rate and to maintain Kafka offset checkpoints. Spot reclamation causes mid-run executor loss → consumer lag spike → producer outpaces consumer → unbounded Kafka topic growth. See `postmortems/2026-05-11_emr_cluster_bringup_13_incidents.md` incident #11.

**Why 4-core (not 2):** Running 4 concurrent streaming queries (bronze + silver + 2 gold facts) on 2 × m5.xlarge starved YARN. Each streaming query needs ≥1 executor slot; 2-core cluster has ~2-4 effective slots after AM overhead → "Initial job has not accepted any resources" on the 3rd-4th query. 4 cores doubles available slots to ~8-10 effective. See ADR-008.

**Effective Spark capacity at burst:**
- 14 cores active for ingestion (1 per executor reserved for tasks management)
- ~35GB heap across the cluster

**Sanity check:** can 14 cores process 10M events?
- Per-record processing (bronze + silver): ~2ms in CPU
- 10M × 2ms / 14 cores = 1,429 core-seconds = 23.8 wall-clock minutes
- ✓ Comfortably under our 30-minute SLA

## 3. MSK Serverless envelope

**Ingress projection:**
- 10M events × ~250 bytes avg (Avro + wire prefix) = **2.5 GB ingress**
- Producer window: 30 min = 1,800s
- Average rate: 2.5 GB / 1,800s = **1.39 MB/s**
- Burst rate (top decile): 1.39 × 5 = **~7 MB/s**

**MSK Serverless limits:**
- Ingress per cluster: 200 MB/s
- Egress per cluster: 400 MB/s
- Partitions per cluster: 120

**Headroom:** 200 / 7 = **28.6×** ingress headroom. Comfortable.

**Per-topic provisioning:**
| Topic | Partitions | Reason |
|---|---|---|
| `sensor_readings` | 1 | Cheaper than over-provisioning; ordered by device_id key |
| `pharmacy_events` | 1 | Low cardinality |
| `pulsetrack_dlq` | 1 | Quarantine — rare writes |

**$ cost:**
- Ingress: 2.5 GB × $0.0015/GB = $0.004
- Partition-hour: 3 partitions × 2h × $0.0024 = $0.014
- **Total MSK: ~$0.02**

## 4. S3 prefix-throttling math

**Per-prefix limits (current AWS):**
- 3,500 PUT/COPY/POST/DELETE/s
- 5,500 GET/HEAD/s

**Projected PUT rate:**
- Bronze: 10M events / 1,800s = 5,556 events/s, but writes are micro-batched. Iceberg commits roughly one data file per partition per 30s. With 24 (hour) × 1 (date) = 24 hour-partitions over a 24h window and ~30s micro-batches, we get ~24 PUTs per 30s = **0.8 PUT/s** to any one prefix
- Silver: similar — ~0.8 PUT/s/prefix
- Gold: even slower (only writes on watermark advance) — ~0.2 PUT/s/prefix

**Reversed-ID partitioning (already in place):**
- Distributes writes across `00`/`01`/.../`99` prefix slots
- Combined with hour partitioning: 100 × 24 = **2,400 distinct prefixes**

**Headroom:** 3,500 / 0.8 = **4,375×** under cap. Reversed-ID was implemented as future-proofing; even at 100M events we'd be at 8 PUT/s/prefix (400× under cap).

## 5. Iceberg snapshot envelope

- Snapshot per commit: ~1 every 30s → **60 snapshots / 30min** per table
- 4 tables (bronze_sensor, silver_sensor, gold_fact_vital_reading, gold_fact_vital_daily_summary) = **240 snapshots** total
- Manifest file size at 10M scale: ~50KB per snapshot
- Metadata overhead: 240 × 50KB × 4 = ~50 MB
- **Within Iceberg + Glue catalog comfortable operating range**

Post-test maintenance:
- `OPTIMIZE` rewrites ~3,000 small files → ~30 large (≤512MB) files
- `expire_snapshots(older_than=now())` drops 90% of intermediate snapshots
- Both run via Prefect `maintenance` deployment nightly

## 6. Producer worker math

**Single producer process:**
- librdkafka in-flight queue: 500,000 messages OR 128 MiB
- LZ4 compression ratio: ~3:1 for our Avro payloads
- Per-record CPU cost: ~30μs (Python serialize + lz4 + queue)
- Saturated rate: ~33,000 rec/s
- Network egress per producer: ~1 MB/s after compression

**For 10M records in 30 min** (5,556 rec/s required):
- One producer suffices (using 17% of saturation rate)
- Headroom for 4 concurrent producers (scale + WHOOP + OpenFDA + FHIR)

## 7. Cost projection (revised for 4-core on-demand + Iceberg restored)

| Line item | Calculation | Cost (USD) |
|---|---|---|
| EMR core (4 × m5.xlarge × on-demand $0.192/h × 3h) | core executor nodes | $2.30 |
| EMR master (1 × m5.xlarge × $0.192/h × 3h) | driver + RM | $0.58 |
| EMR managed-scaling control plane | $0.096/h × 3h | $0.29 |
| EBS (5 × 64GB × $0.10/GB-month / 720h × 3h) | scratch + logs | $0.13 |
| MSK Serverless cluster-hour | 3 × $0.75/h (we tear down sooner if possible) | $2.25 |
| MSK Serverless ingress | 2.5 GB × $0.0015 | $0.004 |
| MSK partition-hours | 6 × 3h × $0.0024 | $0.043 |
| S3 PUT (10M ingestion + Iceberg metadata commits) | ~150k × $0.005/1k | $0.75 |
| S3 storage (~10 GB × 1 day) | $0.023/GB-month / 30 | $0.008 |
| Athena queries (4 validation queries × 50MB) | $5/TB scanned | $0.001 |
| Glue API calls (Iceberg catalog ops) | ~50k × $0.44/1M | $0.022 |
| CloudWatch metrics + logs | ~200 metrics, ~3GB logs | $0.85 |
| Secrets Manager API | ~50 × $0.05/10k | $0.0003 |
| KMS GenerateDataKey | ~50 × $0.03/10k | $0.0002 |
| SNS publish (alerts) | ~10 × $0.50/1M | $0.000005 |
| **Total estimated** | | **~$7.25** |

**Sensitivity:**
- 6h cluster window (if extra debugging): ~$13
- 12h cluster window: ~$24
- MSK left up overnight (which we MUST NOT do): adds $0.75/h × N

**Budget hard cap (terraform `budget_limit_usd`):** $40. With ~5× headroom for a planned 3h run. **Threshold for abort:** if Cost Explorer projects daily run-rate >$50 during the test, halt immediately.

**Cost-discipline rules:**
1. MSK Serverless torn down the moment all producers finish (saves $0.75/h × every-hour-after)
2. EMR cluster torn down within 1h of test completion (saves $0.96/h)
3. S3 + Glue persist at near-zero idle cost (no compute)

## 8. Risk register

| Risk | Probability | Impact | Detection | Mitigation |
|---|---|---|---|---|
| Spot reclaim during test | N/A | N/A | n/a | **NOT APPLICABLE: cluster is now on-demand**. See ADR-008 + postmortem incident #11. |
| MSK Serverless ingress throttle | Very low | High | Producer `BufferError` rate jumps | 28× headroom; abort if sustained throttle >2min |
| S3 prefix throttling | Very low | Medium | `SlowDown` 503 errors in Spark logs | 4,375× headroom; reversed-ID partitioning |
| Chaos drill 2 doesn't recover | Low | High | New app not RUNNING within 5min | Re-submit step again; if 2nd attempt fails, abort + postmortem |
| Glue catalog rate limit | Low | Low | `ThrottlingException` in driver | Glue has 1,000 req/s; we'll see ~3 req/s. Retry with backoff handled by AWS SDK |
| EMR auto-terminate fires mid-test | Low | High | Cluster TERMINATED status | Producers + streaming queries = activity, no idle. Idle timeout 7200s vs. test 5400s |
| Snowflake AUTO_REFRESH lag | Medium | Low | View row counts behind S3 | Manual `ALTER ICEBERG TABLE ... REFRESH` in runbook |
| WHOOP refresh-token expires mid-test | Very low | Medium | producer logs `401 unauthorized` | We bootstrap tokens fresh in T-25m; 24h refresh window |
| Anthropic API outage | Low | Low | anomaly_explainer fails | Module is best-effort; degrades to "explanation unavailable" |
| Producer process OOM | Low | High | tmux session shows Python killed | Spec says 128MB librdkafka buffer; m5.xlarge has 16GB RAM. 125× headroom |

## 9. Abort criteria

Stop the test immediately if ANY of the following hold for > 60 seconds:

1. **Cost burn:** Cost Explorer projected daily run-rate > $20
2. **Producer failure:** any producer's `failed` count > 1000 (vs. ~0 expected with idempotent producer)
3. **Silver lag:** Kafka consumer lag for silver consumer-group > 100,000 (vs. ~10,000 normal)
4. **DLQ growth:** DLQ topic events/min > 5% of producer rate
5. **Cluster instability:** > 2 executors lost in a 60s window outside of chaos drill windows
6. **MSK errors:** any `BrokerNotAvailable` or `NetworkException` rate > 1/min in producer logs

Abort procedure: `Ctrl+C` the orchestrator (it captures signals + drains gracefully). If it doesn't respond in 30s:
1. SSH master, `tmux kill-server` to stop producers
2. `aws emr cancel-steps --cluster-id $CID --step-ids ...` to stop streaming queries
3. Write up a postmortem.

## 10. Go/no-go gates

The orchestrator (`scripts/run_scale_test.sh`) checks each gate and aborts on failure:

| Gate | Pass criterion |
|---|---|
| Pre-flight | `check_credentials.py` exits 0 |
| Terraform outputs | All 5 outputs (cluster, master, msk, bucket, sns) resolve |
| Migrations | `run_migrations.py --apply` exits 0 |
| Code deployment | tarball present at `s3://$BUCKET/code/...` |
| Streams active | 3 YARN apps RUNNING within 5 min |
| Producers launched | 4 tmux windows alive on master |
| Chaos 1 result | Recovery within 60s OR explicit "failed drill" recorded |
| Chaos 2 result | Recovery within 300s OR explicit "failed drill" recorded |
| Drain | Consumer lag → 0 within 5 min of producer stop |

If ANY gate fails, the test is incomplete — the postmortem captures which gate and why.
