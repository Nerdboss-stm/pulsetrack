# Kappa vs Lambda Architecture — PulseTrack vs GhostKitchen

## Summary

| Aspect | PulseTrack (Kappa) | GhostKitchen (Lambda) |
|--------|-------------------|----------------------|
| **Data nature** | Append-only sensor readings | Stateful order lifecycle |
| **Corrections** | Readings don't change | Orders cancelled, refunded |
| **Architecture** | Single streaming pipeline | Dual pipeline (batch + streaming) |
| **Backfill** | Replay through same pipeline | Separate batch Spark job |
| **Reconciliation** | Not needed | Daily batch overwrites streaming Gold |
| **Code paths** | 1 | 2 |
| **Complexity** | Lower | Higher |
| **State stores** | RocksDB (anomaly rolling stats) | None (batch is truth) |
| **DLQ** | Not needed | Yes (events > 24h) |
| **Late data** | Native — same pipeline handles it | DLQ → nightly reconciliation |

---

## Kappa Architecture (PulseTrack)

### Core Principle
**One codebase, one logic path.** The streaming pipeline IS the ETL engine.

```
Kafka → Spark Structured Streaming → Bronze Delta → Silver Delta → Gold Delta
                                                                      ↑
                              SAME pipeline for real-time + backfill ─┘
```

### Why Kappa works for wearables
Sensor readings are **immutable facts**. A heart rate of 72 bpm at 2:34pm is recorded once and never changes. There is no "correction" or "state transition" like an order being cancelled. This makes the streaming pipeline naturally idempotent — replaying the same event produces the same result.

### Backfill in Kappa
```
1. Stop the streaming pipeline
2. Reset Kafka consumer offsets to an earlier point
3. Restart — same code replays all events since that point
4. MERGE into Silver/Gold handles deduplication (idempotent)
```

No separate batch job. No second codebase to maintain. No reconciliation step.

### Airflow Role in Kappa: JANITOR (not conductor)
Airflow does NOT orchestrate the main ETL. It handles maintenance:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `dag_ehr_batch_ingest` | Daily 2AM | New EHR files → Bronze |
| `dag_identity_resolution` | Daily 2AM | Refresh patient_identity_bridge |
| `dag_data_quality_sweep` | Every 6h | Quality checks on Gold |
| `dag_compaction_maintenance` | Daily | OPTIMIZE + Z-ORDER hot tables |
| `dag_hipaa_audit` | Weekly | PII scan, compliance check |
| `dag_backfill_replay` | Manual | Reset offsets, replay pipeline |

The continuous Spark Streaming job is the conductor. Airflow cleans up after it.

### State Management
The anomaly detector maintains **per-patient rolling statistics** in RocksDB state store:
- Key: `(patient_key, metric_name)`
- State: running mean, stddev, circular buffer of last 30 days
- TTL: 90 days for inactive patients (auto-evicted from RocksDB)

This enables personalized anomaly detection: a runner's resting HR of 52 compared against THEIR own baseline, not a population average.

---

## Lambda Architecture (GhostKitchen)

### Core Principle
**Batch is truth. Streaming is approximate.** Two pipelines converge on the Gold layer.

```
Kafka → Spark Streaming → Silver (streaming) → Gold (streaming, approximate)
                                                        ↓
S3 Bronze → Spark Batch job (nightly) ──────→ Gold (batch, overwrites streaming)
```

### Why Lambda works for dark kitchens
Orders have a **lifecycle**: created → accepted → preparing → delivered → cancelled/refunded. An order's state changes multiple times. The final financial facts (revenue, refund amounts) can only be computed accurately after the order lifecycle completes — which requires batch processing with full historical context.

Key differences:
- **Corrections**: Order cancelled 2 hours later → batch job picks this up and corrects Gold
- **Financial reconciliation**: Daily settlement requires exact counts — streaming approximations aren't acceptable
- **State transitions**: Order lifecycle spans hours; streaming window would need to hold state for hours → expensive

### DLQ in Lambda
Late events (> 24h old) are sent to a Dead Letter Queue (DLQ) and picked up by the nightly batch job. The batch job is the "source of truth" — late events don't need streaming urgency.

---

## Decision Framework: When to use which

### Use **Kappa** when:
- ✅ Data is naturally append-only (IoT sensors, logs, clickstreams)
- ✅ Events don't have complex state transitions
- ✅ Backfill = simply replay the stream
- ✅ Late-arriving data doesn't require special reconciliation
- ✅ Team size / simplicity matters

### Use **Lambda** when:
- ✅ Business events have complex state machines (order lifecycle, financial transactions)
- ✅ Financial reconciliation requires batch-exact numbers
- ✅ Corrections retroactively change historical facts
- ✅ Streaming is used for low-latency "good enough" approximations
- ✅ Batch provides the authoritative "truth" at end-of-day

### The anti-pattern: Lambda for append-only data
Using Lambda for wearable data would mean:
1. Streaming pipeline processes sensor readings (path 1)
2. Nightly batch job reprocesses the SAME sensor readings (path 2)
3. Batch overwrites streaming Gold nightly

Both paths produce identical results (readings don't change). This is pure complexity overhead with no correctness benefit. **Kappa eliminates this unnecessary duplication.**

---

## Interview Talking Points

1. **"Why did you use different architectures for your two projects?"**
   > "I matched the architecture to the data characteristics. Wearable sensor data is append-only — a reading doesn't change after it's recorded. Kappa handles this with one pipeline and no reconciliation. Ghost Kitchen has orders that transition through states, get cancelled, get refunded — Lambda's batch path can recompute historical facts correctly after state changes. The key is knowing WHEN each pattern applies."

2. **"Isn't Kappa just Lambda without the batch layer?"**
   > "Conceptually yes, but the implications are significant. In Lambda, you maintain two codebases, two data contracts, and a reconciliation step. In Kappa, you maintain one. For sensor data where the streaming result IS the correct result, that simplification is justified. Lambda's extra complexity is only worth it when the batch layer produces a materially different (more correct) result than streaming."

3. **"What happens if the Spark streaming job crashes in Kappa?"**
   > "Delta Lake checkpoints record exactly where we were in the Kafka offset. On restart, the job continues from the last checkpoint — no data loss, no duplicates. The MERGE into Silver/Gold is idempotent. This is a major advantage of combining Kappa with Delta Lake."

4. **"How do you handle late-arriving data in Kappa?"**
   > "The same pipeline handles it naturally. A sensor reading that arrives 2 hours late goes through the same Bronze → Silver → Gold transformations. Silver's is_late_arriving flag is set (event timestamp vs sync timestamp > 2h). Gold MERGE handles deduplication — if the reading was already written, it's a no-op. No special DLQ needed."
