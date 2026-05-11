# Scale test capacity plan — 10M events, 50K users

**Test owner:** PulseTrack DE
**Test date:** 2026-05-10 (Phase 2 of Prompt 9)
**Cluster:** EMR 7.13.0 dev (4 core nodes — bumped from default 2 for this test)
**Target SLAs:**
- Total ingestion ≤ 30 min
- Silver lag (Kafka → silver visible) p95 < 60s
- No data loss across chaos drills
- Cost ≤ $10

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
| EMR core | m5.xlarge × 4 (spot @ $0.08 max bid) | 16 vCPU, 64GB total |
| EBS per node | 64 GB | Shuffle space + log retention |
| Spark default | 7 executors × 2 cores × 5GB | dynamic alloc 1-12 |
| Idle timeout | 7200s | Auto-terminate if streams stop |

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

## 7. Cost projection

| Line item | Calculation | Cost (USD) |
|---|---|---|
| EMR (5 nodes × spot $0.176/h × 1.58h) | core nodes + master | $1.39 |
| EBS (5 × 64GB × $0.10/GB-month / 720h × 1.58h) | scratch + logs | $0.08 |
| MSK Serverless ingress | 2.5 GB × $0.0015 | $0.004 |
| MSK partition-hours | 3 × 2h × $0.0024 | $0.014 |
| S3 PUT | ~10k requests × $0.005/1k | $0.05 |
| S3 storage (~2 GB × 1 day) | $0.023/GB-month / 30 | $0.0015 |
| Athena queries (validation) | 5 queries × ~5MB scanned × $5/TB | $0.0001 |
| Glue API calls | ~10k × $0.44/1M | $0.005 |
| CloudWatch metrics + logs | ~100 metrics, ~1GB logs | $0.30 |
| Secrets Manager API | ~50 calls × $0.05/10k | $0.0003 |
| KMS GenerateDataKey | ~50 × $0.03/10k | $0.0002 |
| SNS publish (alerts) | ~10 × $0.50/1M | $0.000005 |
| **Total estimated** | | **$1.85** |

**Budget hard cap (terraform `budget_limit_usd`):** $40 — set in `infrastructure/environments/dev.tfvars`.

With 22× cost cushion, the test is comfortably within budget. **Threshold for abort:** if Cost Explorer shows the projected daily run-rate exceeds $20 during the test, halt immediately and investigate.

## 8. Risk register

| Risk | Probability | Impact | Detection | Mitigation |
|---|---|---|---|---|
| Spot reclaim during test | Medium | High | EMR step state moves to `FAILED` | Use ON_DEMAND fallback (`emr_core_spot_bid_price` higher than current spot price); we've bid $0.08 vs. typical $0.06 |
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
