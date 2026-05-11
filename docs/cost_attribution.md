# PulseTrack Cost Attribution

Per-source, per-layer, and per-consumer cost breakdown. Extrapolates from the 10M-event scale test to an annualized 50K-user run-rate. The goal is for any DE to know "if I add X, what does it cost?" within 60 seconds.

**Audience:** DEs proposing capacity changes, the eng lead doing quarterly budget reviews, finance asking what's driving AWS spend.

**Inputs to this doc:**

- Scale-test actual: `docs/scale_test_results.md` — $1.85 actual cost over a 1.5h test ingesting 10M events for 50K simulated users
- Capacity plan projection: `docs/scale_test_capacity_plan.md` § 7 — projected $1.85
- Terraform sizing: `infrastructure/environments/dev.tfvars` (dev — what we're on) and `infrastructure/environments/prod.tfvars` (prod template — not deployed)

---

## 1. Annual run-rate projection (50K sustained users)

If we extrapolate the scale-test workload to 24×7×365 at the same per-user throughput (10M events / 50K users / 1.5h ≈ 27 events/user/hour, comparable to a busy day on WHOOP), the gross numbers:

| Quantity | Scale-test (1.5h) | Extrapolated to 1 year |
|---|---|---|
| Events processed | 10M | 58.4B |
| MSK ingress | 2.5 GB | 14.6 TB |
| Bronze storage at rest (Iceberg, snappy compressed) | ~0.6 GB | ~3.5 TB (raw) → ~1.4 TB (post-OPTIMIZE) |
| EMR core-hours | 4 nodes × 1.5h = 6 | 4 nodes × 8,760h = 35,040 (if always-on) |

The "always-on EMR" projection is wildly conservative. Phase 2 will move to per-flow ephemeral clusters or on-demand-only EMR with idle-shutdown. For the projection below, we assume **8h/day production cluster** (32% utilization) as a realistic post-Phase-2 number.

---

## 2. Per-source cost (the producer side)

PulseTrack has 4 data sources. Their cost differs by an order of magnitude.

| Source | Cadence | Volume / day | $ / day (steady-state) | % of source total |
|---|---|---|---|---|
| **Wearable batch (scale generator)** | Continuous, 10 events/s | 864K events | $0.30 (MSK ingress + S3 PUT) | 89% |
| **WHOOP poll** | Every 15 min | ~5K events (one per user) | $0.005 (small calls, real users) | 1.5% |
| **OpenFDA poll** | Every 6h | ~500 adverse-event records | $0.002 | 0.6% |
| **FHIR daily** | Daily, ~1 batch | ~5K patient records | $0.025 (HAPI FHIR demo egress, S3 write) | 7.5% |
| **Total per source layer** | | | **~$0.34 / day** | 100% |

**Why wearable batch dominates:** every other source is metered (poll cadence × small payload). Wearable is a continuous stream — both the MSK ingress charges (small per-GB but additive) and the S3 PUT cost (one file per micro-batch per partition) add up.

**Marginal cost of adding a wearable user:** ≈ 27 events/hour × 250 bytes × $0.0015/GB + 1 S3 PUT/30s. Roughly **$0.00012 / user / day**, or $0.044 / user / year at this scale.

---

## 3. Per-layer cost (the pipeline)

Where does $$ go inside the pipeline? Decomposed from the scale-test billing:

| Layer | Driver | Scale-test cost | Cost/event | Annualized (50K users, 24×7) |
|---|---|---|---|---|
| **MSK Serverless** | Ingress + partition-hours | $0.018 | $0.0018/M events | ~$32 / year |
| **Bronze write** | EMR core-h + S3 PUT | $0.55 | $0.055/M events | ~$32,000 / year (always-on) → ~$10,500 (32% utilization) |
| **Silver MERGE** | EMR core-h (the shuffle-heavy step) | $0.65 | $0.065/M events | ~$38,000 (always-on) → ~$12,000 (32%) |
| **Gold MERGE** | EMR core-h (lighter — incremental) | $0.20 | $0.020/M events | ~$12,000 (always-on) → ~$3,800 (32%) |
| **Storage (Iceberg at rest)** | S3 standard, Intelligent-Tiering | $0.0015 | $0.0001/M | ~$50 / year (steady-state ~1.4 TB compressed) |
| **Catalog + Lock** | Glue API + DynamoDB | $0.005 | — | ~$30 / year |
| **CloudWatch + logs** | metrics + log groups | $0.30 | — | ~$3,800 / year |
| **Per-layer total** | | **$1.72** | | **~$32,000 / year (32% utilization)** |

**Why silver MERGE is the heaviest:**

- The bronze→silver explode → 9× row inflation per device-event
- MERGE INTO requires a shuffle by merge key (`reading_id, metric_name`); 9× data × shuffle = O(N log N) cost
- The watermark + dedup-within-watermark window holds state for 10 min, requiring memory and S3 checkpoint I/O

**Bronze write is second-heaviest** because of the foreachBatch + Iceberg commit overhead: every batch commits at the foreachBatch boundary, producing a new manifest file. Compaction (run nightly) keeps this manageable at rest.

**Gold MERGE is comparatively cheap** because it's incremental — only the watermark advance triggers a write, and the write rate is ~0.2 PUT/s/prefix (see `docs/scale_test_capacity_plan.md` § 4).

---

## 4. Per-consumer cost (the read side)

Consumer-side spend is more variable than the producer side. Two real consumers + Anthropic for AI.

### 4.1 Snowflake

- Compute: XSMALL warehouse, `$2/credit-h`, AUTO_SUSPEND 60s
- Storage: included in Iceberg refresh; Snowflake reads via External Table over our Iceberg metadata — no separate storage cost
- AUTO_REFRESH: free (Snowflake's iceberg-table refresh is metered as a tiny query)

| Workload | Credits / day | $ / day |
|---|---|---|
| BI dashboard auto-refresh (4×/day) | 0.05 | $0.10 |
| Ad-hoc ML feature queries (5× per DE per day, ~10 DEs) | 0.30 | $0.60 |
| Anomaly dashboard polling (every 5 min by an alerting cron) | 1.5 | $3.00 |
| Daily KPI reports (1× per morning) | 0.10 | $0.20 |
| **Subtotal** | **~1.95** | **~$3.90 / day** |

Annualized: **~$1,425 / year** (Snowflake compute alone). AUTO_SUSPEND keeps the warehouse off when nobody's querying — without it we'd be paying ~$48/day = ~$17K/year.

### 4.2 Athena

| Workload | TB scanned / day | $ / day |
|---|---|---|
| Ad-hoc DE queries (10 DEs × 5 queries × ~100 MB) | 0.005 | $0.025 |
| ML training feature extraction (weekly, 50 GB scan) | 0.007 | $0.035 |
| `query_gold.py` validation queries (test runs) | 0.001 | $0.005 |
| **Subtotal** | | **~$0.065 / day** |

Annualized: **~$25 / year**. Athena is cheap because we partition aggressively (date + reversed-ID) so most queries scan < 100 MB.

### 4.3 Anthropic (the anomaly explainer)

The `ai/anomaly_explainer.py` module calls Claude when silver detects an anomaly that needs human-readable explanation. Cost varies wildly with anomaly rate.

| Scenario | Anomalies / day | Tokens / call | $ / day |
|---|---|---|---|
| Steady state | ~50 | ~2K input + ~500 output | $0.40 |
| Spike (synthetic test, 0.3% impossible-value seeding) | ~5,000 | ~2K + ~500 | $40 (would hit the cap) |
| Scale-test cap (`anthropic_budget_cap=$1.00`) | capped at 100 calls | — | $1.00 max |

Annualized at steady state: **~$150 / year**. Cap-enforced; once $1/day is reached, the module degrades to "explanation unavailable" and emits a SEV3.

---

## 5. Cost optimizations already in place

Things that already keep cost down and shouldn't be undone without thinking it through:

| Optimization | Effect | Code reference |
|---|---|---|
| **Reversed-ID partitioning** for bronze/silver writes | Distributes writes across 100 S3 prefix slots; avoided ~4× higher S3 throttling cost when retrying 503s | `docs/s3_partitioning_analysis.md`, `transformations/bronze_to_silver/sensor_silver.py` |
| **Spot instances for EMR core nodes** | ~50% of on-demand cost | `infrastructure/environments/dev.tfvars:emr_core_spot_bid_price=0.08` |
| **EMR idle auto-terminate** | Cluster shuts down after 2h idle; avoids 22h × 5 nodes × $0.176 = $19/day waste | `infrastructure/environments/dev.tfvars:emr_idle_timeout_seconds=7200` |
| **Snowflake AUTO_SUSPEND 60s** | Saves ~$13/day vs. always-on | snowflake setup SQL |
| **S3 Intelligent-Tiering** | Auto-tiers infrequent-access data; ~50% storage savings on > 30-day-old data | `infrastructure/modules/storage/main.tf` (lifecycle rules) |
| **Glacier tier for bronze > 90 days** | Drops cold-storage cost from $0.023 to $0.004 per GB-month | `infrastructure/modules/storage/main.tf` (lifecycle rules) |
| **`OPTIMIZE` consolidation nightly** | Combines small files into few large ones; reduces both storage cost (compression) and query cost (fewer file-opens) | `maintenance/compaction.py`, `orchestration/flows/maintenance_pipeline.py` |
| **MSK Serverless (vs. provisioned)** | Pay-per-GB-ingressed; cheaper at our scale (< 100 MB/s) | `infrastructure/modules/kafka/main.tf` |
| **Anthropic per-day cap** | Hard ceiling on the worst-case Claude bill | `ai/anomaly_explainer.py:_check_budget` |

---

## 6. Future cost levers (Phase-2+)

Things we know would save more, ordered by impact. Each is a Phase-2 or Phase-3 ticket.

### 6.1 Iceberg sort-on-write

Today: silver MERGE shuffles data on every batch. With Iceberg `WRITE ORDERED BY (patient_key, event_timestamp)`, writes pre-sort and downstream MERGEs are faster.

- Expected savings: ~20% of silver core-hours ≈ **$2,400 / year**
- Effort: 2 days; requires Glacierbase migration
- Risk: changes write performance; need to validate with chaos drill

### 6.2 Smaller MSK retention

Today: MSK Serverless default retention 24h. We checkpoint Kafka offsets in S3, so we never re-read past the trigger interval (30s).

- Expected savings: ~30% of MSK storage cost ≈ **$10 / year** (low absolute $$)
- Effort: 30 min config change
- Risk: if a streaming app dies and we lose checkpoint, we can only replay 1h of data; for SEV1 recovery we already require checkpoint-replay (`runbooks/silver_cold_start_hang.md`) — so the risk is bounded

### 6.3 Smaller dev instance sizes

Dev runs `m5.xlarge` (4 vCPU, 16 GB). We rarely need that much.

- Switch master to `m5.large` (2 vCPU, 8 GB) — fine for YARN RM + AM at our load
- Expected savings: $0.044/h × 24h × 365 = **$385 / year** (always-on EMR scenario)
- Effort: 30 min Terraform change
- Risk: medium — need to confirm no OOM under chaos drills

### 6.4 dbt incremental over full-refresh

Several gold dbt models are still `materialized='table'` (full-refresh). Convert to `materialized='incremental'` with `unique_key` on the natural-grain key.

- Expected savings: ~50% of dbt compute time → ~$5 / day saved → **$1,825 / year**
- Effort: 1-2 days per model; we have 8 mart models
- Risk: low; incremental dbt is well-understood

### 6.5 Reserved capacity for steady-state EMR (Phase-3)

If we go always-on, switch EMR core nodes to Reserved Instances (1-year, no upfront).

- Savings: ~40% off on-demand → **$13K / year** (worth doing once always-on is committed)
- Risk: 1-year commit; only do this when usage is stable

---

## 7. 12-month projection table

Assumes **50K users sustained, 32% EMR utilization (8h/day) post-Phase-2, current code paths, no further optimizations**. The numbers are the realistic-budget plan.

| Cost category | Monthly | Annual | Notes |
|---|---|---|---|
| EMR core (4 × m5.xlarge, spot, 8h/day) | $169 | $2,025 | Largest single cost |
| EMR master (1 × m5.xlarge, on-demand, 8h/day) | $43 | $510 | |
| S3 storage (1.4 TB compressed, Intelligent-Tiering) | $30 | $360 | After post-90d Glacier |
| S3 PUT/GET requests | $5 | $60 | |
| MSK Serverless | $3 | $32 | Mostly partition-hours |
| Snowflake compute (XSMALL) | $118 | $1,425 | AUTO_SUSPEND keeps this contained |
| Athena | $2 | $25 | |
| Glue catalog + Schema Registry | $3 | $30 | |
| CloudWatch + logs | $320 | $3,800 | High — opt for reduced retention in Phase 2 |
| DynamoDB (Glacierbase locks + WHOOP offsets) | $3 | $30 | |
| Secrets Manager + KMS | $1 | $15 | |
| SNS publish + email | $0.50 | $6 | |
| Anthropic API (steady-state) | $13 | $150 | Anomaly explainer; capped |
| **Total** | **~$710 / month** | **~$8,500 / year** | At 50K users sustained |

**Per-user cost:** $8,500 / 50,000 = **$0.17 / user / year**. This is the unit-economics number to report up.

**Budget alarms:**

- `pulsetrack-dev-budget-alarm` fires at 80% of `budget_limit_usd` (currently $40/month for dev). Re-tune to $850/month if we go to prod scale.
- Daily Cost Explorer check: if projected daily run-rate > 2× the monthly-target/30, page SEV2.

---

## 8. References

- `docs/scale_test_results.md` — measured costs from the 10M-event test
- `docs/scale_test_capacity_plan.md` § 7 — pre-test math
- `docs/s3_partitioning_analysis.md` — reversed-ID prefix design
- `infrastructure/environments/dev.tfvars`, `prod.tfvars` — instance sizing
- `infrastructure/modules/monitoring/main.tf` — budget alarm
- `maintenance/compaction.py` — OPTIMIZE / expire_snapshots automation
- AWS pricing pages: MSK Serverless, EMR Spot, S3 Standard + IT, Snowflake on-demand
