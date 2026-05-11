# PulseTrack Disaster Recovery

RTO / RPO objectives per layer, recovery procedures per failure mode, and the quarterly DR-drill plan. Single-region today; cross-region is Phase 3 (documented as a gap).

**Audience:** on-call DEs during an incident; eng lead reviewing DR drill results; anyone scoping "what would actually happen if X failed".

---

## 1. Objectives

| Layer | RPO (max data loss) | RTO (max recovery time) | Rationale |
|---|---|---|---|
| Bronze | 15 min | 30 min | Kafka retention is the floor — anything beyond MSK retention is unreplayable. 24h default retention gives us slack. |
| Silver | 30 min | 1 h | Bronze → silver MERGE is idempotent on `(reading_id, metric_name)`; replay from bronze is the recovery. |
| Gold (facts) | 1 h | 4 h | Daily aggregates can be rebuilt; intra-day visibility is lost during recovery but not data. |
| Gold (marts / dbt) | 24 h | 8 h | Full-refresh dbt build covers a day; only same-day marts are degraded. |
| Snowflake views | 24 h | 8 h | AUTO_REFRESH catches up once Iceberg is healthy. |

**RPO** = how much data can we afford to lose? Measured as "time between last good snapshot and disaster".

**RTO** = how long can the layer be unavailable? Measured from incident-declared to consumer-visible-fresh.

These are objectives, not guarantees. The drill cadence (§ 4) validates them.

---

## 2. Architecture-aware DR primitives

What we can lean on:

| Primitive | Where | Use |
|---|---|---|
| **Iceberg snapshots** | Every Iceberg table (`pulsetrack_bronze_dev.sensor_readings` and 19 others) | Time-travel reads, snapshot rollback (§ 3.6) |
| **S3 versioning** | Enabled on `s3://pulsetrack-lakehouse-dev-XXXX/` (set in `infrastructure/modules/storage/main.tf`) | Object recovery within 30 days |
| **Kafka offsets in S3 checkpoint** | `s3://lakehouse/checkpoints/{bronze,silver,gold}_*/` | Exact-once resume from a known offset |
| **Glacierbase migration ledger** | `pulsetrack_gold_dev.schema_migrations` | Verify schema state mid-recovery; never out of sync with code |
| **DynamoDB Glacierbase locks** | `pulsetrack-{env}-glacierbase-locks` | Concurrency safety during recovery — won't fight with a manual hand |
| **Secrets Manager 30-day recovery window** | `infrastructure/modules/secrets/main.tf` | Restoration of accidentally-deleted secrets |
| **KMS customer-managed key (CMK)** | `alias/pulsetrack-{env}-secrets` | Key is in our account; deleting the account doesn't delete the key for 30 days |
| **Glue catalog (Iceberg metadata)** | Per-environment Glue database | Restorable from Iceberg metadata files — schemas stay in sync |

What we do NOT have:

- **Cross-region replication.** Single-region today (Phase-3 gap; § 5).
- **Multi-AZ Kafka.** MSK Serverless is multi-AZ by default (managed by AWS), so this isn't a gap for MSK; but our EMR cluster is single-AZ.
- **Off-AWS backup.** All data is in one AWS account. An account-compromise event (§ 3.3) is recoverable only via AWS support.

---

## 3. Recovery procedures per failure mode

### 3.1 S3 bucket corruption (object overwritten / deleted)

Symptom: an Iceberg `_metadata.json` is missing or returns 403; queries error with `FileNotFoundException`.

Recovery: S3 versioning is enabled — every object has a version history retained 30 days.

```bash
# 1. Identify the missing object
aws s3api list-object-versions --bucket pulsetrack-lakehouse-dev-XXXX \
    --prefix iceberg/warehouse/pulsetrack_bronze_dev.db/sensor_readings/metadata/

# 2. Find the version-id you want (last "DeleteMarker" tells you when it was
#    deleted; preceding version is the one to restore)

# 3. Remove the DeleteMarker
aws s3api delete-object --bucket pulsetrack-lakehouse-dev-XXXX \
    --key iceberg/warehouse/.../00043.metadata.json \
    --version-id <delete-marker-id>

# 4. Or copy a specific version forward as the "current" version
aws s3api copy-object --bucket pulsetrack-lakehouse-dev-XXXX \
    --copy-source pulsetrack-lakehouse-dev-XXXX/iceberg/.../00043.metadata.json?versionId=<good-id> \
    --key iceberg/warehouse/.../00043.metadata.json
```

If the corruption pre-dates the 30-day versioning window: time-travel to the last known-good Iceberg snapshot (§ 3.6).

### 3.2 DynamoDB lock corruption

Symptom: `migrations/cli.py run` hangs on `LockAcquisitionError: catalog 'glue_iceberg' is held by …` and the holder is known-dead (e.g., a crashed CI runner).

Recovery — `scripts/recover_glacierbase_lock.sh` does this for you (TODO: script doesn't exist yet — write it; below is the inline procedure):

```bash
# Identify the stale lock holder
aws dynamodb scan --table-name pulsetrack-dev-glacierbase-locks \
    --query 'Items[].{catalog:catalog.S,holder:holder.S,expires:expires_at.N}'

# If the holder is unambiguously dead (e.g., CI run completed > 1h ago):
aws dynamodb delete-item --table-name pulsetrack-dev-glacierbase-locks \
    --key '{"catalog": {"S": "glue_iceberg"}}'

# Then re-run migrations
python3 migrations/cli.py --catalog glue_iceberg run
```

The lock has a TTL attribute (`expires_at`, default 600s), so stale locks usually auto-expire within 10 minutes. Manual deletion is for "I can't wait 10 min" cases.

TODO: actually write `scripts/recover_glacierbase_lock.sh` — the inline version is what it should do.

### 3.3 AWS account compromise

The worst case. Treat per AWS Incident Response runbook (https://docs.aws.amazon.com/security-ir/latest/userguide/security-incident-response-guide.html).

Symptom: unauthorized API calls visible in CloudTrail; resources you didn't create; bill spike.

Immediate (within 1 hour):

1. **Page eng lead + director.** SEV1.
2. **Rotate all IAM credentials.** No exceptions:
   ```bash
   # List all access keys
   aws iam list-access-keys --user-name <user>
   # Deactivate immediately
   aws iam update-access-key --user-name <user> --access-key-id <key> --status Inactive
   # Then delete
   aws iam delete-access-key --user-name <user> --access-key-id <key>
   ```
3. **Audit CloudTrail.** Filter on suspect principals; identify the resources touched.
4. **Open AWS support case at Severity 1.** They have an internal IR team that walks you through containment.
5. **Snapshot all S3 buckets.** Cross-account copy is hard but possible — for now, ensure versioning is on and bucket policies deny deletes.

Recovery (24-48h):

1. Audit every resource — Iceberg tables, Secrets, KMS keys. Confirm encryption keys are intact.
2. Rotate all credentials in Secrets Manager (`docs/secret_rotation.md` § 3 — every secret).
3. Apply IAM access-keys cleanup: long-term keys are forbidden going forward (we already use EMR instance profiles; this is enforced).
4. Postmortem with full-eng audience.

**Prevention:** we follow least-privilege IAM (every role scoped to its needs in `infrastructure/modules/iam/main.tf`); GuardDuty is enabled (`infrastructure/modules/monitoring/main.tf` — TODO if not enabled); MFA required on root + admin IAM users.

### 3.4 Region outage

This is the Phase-3 gap. Single-region today (us-east-1).

If us-east-1 is fully down:

- **Bronze:** unrecoverable for the duration of the outage. Producers retry but can't write.
- **Silver / Gold:** existing data is preserved (Iceberg metadata + S3 are durable across AZs even within us-east-1), but no new writes are happening.
- **Snowflake views:** stale until us-east-1 returns and the Iceberg AUTO_REFRESH catches up.

**Mitigation we currently have:** none beyond AWS's own multi-AZ guarantees within us-east-1.

**Phase-3 plan (NOT in scope today):**

1. Enable S3 Cross-Region Replication (CRR) to us-west-2 for the lakehouse bucket
2. Enable Multi-Region MSK (or replicate via MirrorMaker 2)
3. Run a parallel EMR cluster in us-west-2 (cold standby — only spun up if us-east-1 is out)
4. Update Snowflake external tables to point to either region's metadata

**Estimated cost:** ~30% increase on monthly AWS bill (CRR + standby EMR). Decision point: a real production-customer commitment that requires multi-region SLA.

### 3.5 EMR cluster destroyed mid-test

Symptom: `terraform destroy` (or an AWS-side hardware issue) terminated the EMR cluster during active streaming. The cluster ID is gone; streaming queries lost their executors.

Recovery: **EMR is stateless**. Re-apply Terraform and replay Kafka offsets from S3 checkpoint.

```bash
# 1. Re-create cluster
cd infrastructure
terraform apply

# Wait ~5 min for bootstrap

# 2. SSH to new master, re-deploy code
MASTER_DNS=$(terraform output -raw emr_master_dns)
bash scripts/submit_emr_step.sh streaming/bronze_ingestion.py

# 3. Streaming app reads from S3 checkpoint, resumes from last committed offset.
#    Spark Structured Streaming guarantees exactly-once if checkpoint is intact.
```

The Iceberg `lakehouse/checkpoints/` directory survives across cluster lifecycles. The new cluster sees `_metadata.json`, reads the committed offset, and resumes. No data lost — possibly some seconds of latency added.

### 3.6 Iceberg snapshot corruption / rollback

Symptom: a query against an Iceberg table errors with `Cannot find snapshot <id>` or returns incorrect data after a bad MERGE.

Recovery: roll back to a prior known-good snapshot.

```bash
# List recent snapshots — most recent first
spark-sql -e "
    SELECT snapshot_id, committed_at, operation, summary
    FROM glue_iceberg.pulsetrack_silver_dev.sensor_readings.snapshots
    ORDER BY committed_at DESC
    LIMIT 20
"

# Roll the table back to a known-good snapshot
spark-sql -e "
    CALL glue_iceberg.system.set_current_snapshot(
        table => 'pulsetrack_silver_dev.sensor_readings',
        snapshot_id => 123456789012345678
    )
"

# Verify
spark-sql -e "SELECT COUNT(*) FROM glue_iceberg.pulsetrack_silver_dev.sensor_readings"
```

Notes:

- Snapshot rollback is **non-destructive** — the bad snapshot still exists, just not as `current`. You can roll forward again if needed.
- Downstream tables (silver→gold) need to be recomputed from the rolled-back state. Trigger gold re-MERGE.
- Iceberg keeps snapshots indefinitely until `expire_snapshots` runs. Our compaction job (nightly) keeps 7 days of snapshots; rollback further back means the snapshot files have been GC'd.

### 3.7 Kafka topic offset reset (accidental consumer-group reset)

Symptom: someone ran `kafka-consumer-groups.sh --reset-offsets --to-earliest` against a production group. The stream is now re-reading from the start.

Recovery: this is mostly self-correcting — re-reading is idempotent (silver MERGE is keyed on `reading_id`), so downstream rows are not double-counted. But the lag will be massive and Spark may take hours to catch up.

Options:

1. **Wait it out.** If business-day-OK, let the stream catch up. Monitor `kafka_consumer_lag` until it drains.
2. **Restart from a specific offset.** If you know roughly the timestamp to resume from:
   ```bash
   kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP \
       --group spark-kafka-source-bronze-sensors-... \
       --reset-offsets --to-datetime 2026-05-10T10:00:00.000 \
       --topic sensor_readings --execute
   ```
3. **Restart from latest.** If you've decided to accept the gap (e.g., it was a test environment):
   ```bash
   kafka-consumer-groups.sh ... --reset-offsets --to-latest --execute
   ```

### 3.8 Schema registry corruption

Symptom: producers get `SchemaParseException` because the registry returned a malformed schema, or the schema-id resolution fails.

Recovery: the schema is committed to git (`schemas/*.avsc`). Re-register from source:

```bash
python3 schemas/registry.py register --topic sensor_readings --file schemas/sensor_reading.avsc
```

Schema registry data isn't authoritative in our deployment — git is. Producers can fall back to a local schema file (via `PT_SCHEMA_FILE` env var) if the registry is unreachable.

---

## 4. DR drill cadence

Quarterly. Each drill validates one failure mode end-to-end. The drill is announced (we're not chaos-engineering in prod yet), executed during business hours with a war-room channel up, and a postmortem published within a week.

| Quarter | Drill | What's tested | Owner |
|---|---|---|---|
| Q1 (Jan) | Iceberg snapshot rollback | § 3.6 procedure; verify gold re-MERGE works | DE rotation |
| Q2 (Apr) | EMR full rebuild | § 3.5 procedure; checkpoint resume | DE rotation |
| Q3 (Jul) | S3 versioning recovery | § 3.1 procedure; restore a deleted metadata file | DE rotation |
| Q4 (Oct) | Account compromise tabletop | § 3.3 walkthrough with eng lead + director; no actual rotation | DE + eng lead |

Drill success criteria:

1. RPO / RTO targets per § 1 are met
2. The relevant runbook is sufficient — operator could follow it without unwritten knowledge
3. Postmortem published, action items filed in Linear

Failed drills are not punished — they're the entire point. A failed drill = a real gap that would have hurt in a real incident. Fix it.

---

## 5. Communication plan during DR

Mirrors `docs/on_call.md` § 4 but with DR-specific notes.

| Audience | Channel | When | Owner |
|---|---|---|---|
| Eng team | Slack `#pulsetrack-alerts` | T+5 min (incident declared) | Primary on-call |
| Eng lead | Direct Slack DM | T+5 min | Primary on-call |
| Director | Email + Slack DM | T+30 min if SEV1, OR immediately if customer-impact | Eng lead |
| Affected consumers (BI / ML team) | Slack `#pulsetrack-consumers` | T+30 min with rough ETA | Primary on-call |
| AWS Support | Support case | When AWS is the suspected root cause | Eng lead |

**Do NOT broadcast externally.** No customer-facing comms until eng lead + director sign off. PulseTrack is internal; a real customer-comms procedure is Phase-3 (when we have real external customers).

**Status-page convention:**

- War-room channel pinned message has current status: "INVESTIGATING / MITIGATING / RESOLVED"
- Update every 30 min minimum during SEV1, even with "no new info"
- Final all-clear posted to `#pulsetrack-alerts` (parent channel)

---

## 6. References

- `docs/on_call.md` — paging, escalation, postmortem template
- `docs/PRODUCTION_RUNBOOK.md` § 6 — recovery procedures (checkpoint, migrations)
- `docs/scale_test_runbook.md` § "If you have to abort mid-test" — aborts as planned-DR
- `docs/secret_rotation.md` § 5 — break-glass for Secrets Manager outage
- `infrastructure/modules/storage/main.tf` — S3 versioning + lifecycle config
- `runbooks/silver_cold_start_hang.md` — checkpoint replay (TODO if missing)
- AWS Security IR Guide: https://docs.aws.amazon.com/security-ir/
- Iceberg docs on snapshot rollback: https://iceberg.apache.org/docs/latest/spark-procedures/#set_current_snapshot
