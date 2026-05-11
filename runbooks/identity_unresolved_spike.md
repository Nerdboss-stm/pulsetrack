# Runbook: `patient_key NULL` rate spike (identity bridge unresolved)

**Severity ladder:**
- SEV3: `link_rate_pct` drops below 95 % for one bridge run
- SEV2: `link_rate_pct` < 90 % OR a single `identifier_type` shows > 5 % unresolved on two consecutive runs
- SEV1: device-side resolution collapses (`device_account_id` link rate < 80 %) — downstream `dim_patient` and `fact_vital_*` row counts diverge from sensor row counts

**On-call response SLA:** SEV2 → 15 min ack; SEV1 → page within 5 min.

`transformations/identity_resolution/patient_identity_bridge.py` resolves `patient_key` in four phases (run sequentially, each phase reads what the previous one wrote):

| Phase | Source | Identifier | Linkage method |
|-------|--------|-----------|----------------|
| 0 | `whoop_user_seed` (operator) | `email` + `device_account_id` | `operator_seed` |
| 1 | EHR Silver (`conditions` ∪ `medications`) | `hospital_mrn`, `email` | `exact_mrn_email` (SHA-256 of `lower(trim(email))`) |
| 2 | Sensor Silver | `device_account_id` | `exact_email_match` (transitive via Phase 1 email rows) |
| 3 | Pharmacy Bronze | `fda_report_id` | `none` — always `pending_registration` until a future linkage source |

Phase 4 publishes Prometheus gauges (`pt_identity_link_rate_pct`, `pt_identity_pending_rows`, etc.) via `data_quality/identity_metrics.py::compute_resolution_metrics`. The page fires when those gauges deviate.

## TL;DR (30-second triage)

```bash
# 1. What's the current link rate?
curl -s http://$MASTER_DNS:8004/metrics | grep -E '^pt_identity_'

# 2. Which identifier_type is failing?
spark-sql -e "
  SELECT identifier_type, link_status, COUNT(*) AS n
  FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge
  GROUP BY identifier_type, link_status
  ORDER BY identifier_type, link_status
"

# 3. Sample the unresolved rows for the failing identifier_type
spark-sql -e "
  SELECT identifier_value, source, first_seen, last_seen
  FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge
  WHERE link_status='pending_registration'
    AND identifier_type='device_account_id'
  ORDER BY first_seen DESC LIMIT 20
"
```

Four flavours of unresolved-spike map to four fixes:
1. **New device IDs not yet onboarded** (most common — Phase 2 can't find a matching email in Phase 1)
2. **FHIR producer MRN-format change** (Phase 1 builds the wrong `patient_key`, downstream Phase 2 misses)
3. **Bridge job lagging** (the streaming silver writes new sensor rows faster than the batch identity bridge runs)
4. **Operator's own WHOOP identity not seeded** (Phase 0 skipped because `PT_WHOOP_USER_EMAIL`/`PT_WHOOP_ACCOUNT_ID` aren't set)

## Symptoms

- Prometheus `pt_identity_link_rate_pct{job="identity_bridge"}` drops; `pt_identity_pending_rows` climbs
- Structured log line `Identity resolution metrics` shows `link_rate_pct` below threshold
- `dim_patient` row count plateaus while sensor row count keeps growing
- Gold facts (`fact_vital_reading`, `fact_vital_daily_summary`) show fewer distinct `patient_key`s than sensor readings imply
- Alerting via `observability.alerting` on the `pt_identity_*` gauges

## Diagnosis

### 1. Quantify the gap per phase

```bash
spark-sql -e "
  SELECT
    identifier_type,
    match_method,
    link_status,
    COUNT(*) AS rows,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY identifier_type), 1) AS pct
  FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge
  GROUP BY identifier_type, match_method, link_status
  ORDER BY identifier_type, link_status
"
```

Expected distribution in steady state:
- `email`: 100 % `linked` via `exact_mrn_email` (Phase 1) or `operator_seed` (Phase 0)
- `hospital_mrn`: 100 % `linked` via `exact_mrn_email`
- `device_account_id`: > 95 % `linked` via `exact_email_match`; rest `pending_registration`
- `fda_report_id`: 100 % `pending_registration` (by design — Phase 3 has no linkage source yet)

If `device_account_id` `match_method='none'` is spiking, that's Phase 2 not finding its transitive email join. If `hospital_mrn` shows `pending_registration`, that's Phase 1 broken — much rarer.

### 2. Compare unresolved devices to EHR emails

The transitive join in `build_device_bridge_rows` looks up `patient_email` from sensor silver against the bridge's `identifier_type='email'` rows. Verify both sides:

```bash
# Side A: emails on the EHR side
spark-sql -e "
  SELECT COUNT(DISTINCT identifier_value)
  FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge
  WHERE identifier_type='email' AND source='ehr_batch'
"

# Side B: emails on the sensor side
spark-sql -e "
  SELECT COUNT(DISTINCT patient_email)
  FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings
  WHERE patient_email IS NOT NULL
"

# The set difference — emails sensor sees that EHR doesn't
spark-sql -e "
  WITH ehr AS (
    SELECT DISTINCT identifier_value AS email
    FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge
    WHERE identifier_type='email' AND source IN ('ehr_batch','whoop_user_seed')
  ),
  dev AS (
    SELECT DISTINCT patient_email FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings
    WHERE patient_email IS NOT NULL
  )
  SELECT d.patient_email
  FROM dev d LEFT JOIN ehr e ON e.email = d.patient_email
  WHERE e.email IS NULL LIMIT 50
"
```

Every email in the last query is a device that won't resolve — either the EHR side hasn't onboarded them yet, or the case/whitespace doesn't match. The hash is over `lower(trim(...))` (`build_ehr_identities`), so trailing whitespace or capitalization differences ARE the cause more often than people expect.

### 3. Confirm the bridge has actually run recently

The identity bridge is a batch job, not streaming. If it hasn't run since the last device cohort came online, you'll see the spike even though nothing is "broken":

```bash
spark-sql -e "
  SELECT MAX(last_seen) FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge
"
# Compare to current_timestamp; if > 2 hours stale, the bridge needs to run
```

### 4. Check Phase 0 (operator's own WHOOP seed)

```bash
env | grep -E 'PT_WHOOP_(USER_EMAIL|ACCOUNT_ID)'
# Both must be non-empty for build_whoop_user_seed to return a DataFrame
spark-sql -e "
  SELECT * FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge
  WHERE source='whoop_user_seed'
"
# Expect 2 rows: one email, one device_account_id
```

## Recovery (ranked fastest first)

### Case A: New devices, EHR side hasn't caught up (60 % of pages)

The producer onboarded new device accounts but their corresponding EHR rows haven't been ingested yet. Re-run the bridge after the next EHR batch lands:

```bash
# Verify the next EHR batch is in silver
spark-sql -e "
  SELECT MAX(ingestion_timestamp), COUNT(*)
  FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.ehr_conditions
"

# Re-run the bridge
spark-submit transformations/identity_resolution/patient_identity_bridge.py \
    --format $PT_LAKEHOUSE_FORMAT
```

The bridge re-runs Phase 1 + Phase 2 idempotently (`merge` keyed by `(identifier_type, identifier_value)`). Phase 2 picks up the new email→key links and flips previously-pending devices to `linked`.

### Case B: FHIR / EHR producer MRN-format mismatch

If `email` rows on the EHR side are present but `patient_email` from sensor silver doesn't match (capitalization, whitespace, alias domains), the hashes diverge.

**Verify:**
```bash
spark-sql -e "
  -- Are there near-matches that differ only by case/whitespace?
  SELECT
    sensor.patient_email AS sensor_email,
    ehr.identifier_value AS ehr_email
  FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.sensor_readings sensor
  JOIN ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge ehr
    ON LOWER(TRIM(sensor.patient_email)) = LOWER(TRIM(ehr.identifier_value))
   AND sensor.patient_email != ehr.identifier_value
  WHERE ehr.identifier_type='email'
  LIMIT 20
"
```

**Fix:** `build_ehr_identities` already does `sha2(lower(trim(...)), 256)` so both sides should normalize identically. If you see near-matches in the query above, sensor side is NOT normalizing — check `data_generators/fhir_producer.py` and the wearable producer's `patient_email` assignment for stray whitespace/capitalisation.

After fixing the producer:
```bash
# Re-run silver sensor batch to refresh patient_email values
spark-submit transformations/bronze_to_silver/sensor_silver.py --mode batch \
    --format $PT_LAKEHOUSE_FORMAT
# Then the bridge
spark-submit transformations/identity_resolution/patient_identity_bridge.py \
    --format $PT_LAKEHOUSE_FORMAT
```

### Case C: Manual operator seed for a known cohort

When a small set of operator/staff emails should be seeded by hand (e.g., the WHOOP user, ops engineers running personal devices), use the Phase 0 seed mechanism — set the env vars and the bridge will idempotently inject them:

```bash
export PT_WHOOP_USER_EMAIL="ops-engineer@pulsetrack.local"
export PT_WHOOP_ACCOUNT_ID="WHOOP-0001"

spark-submit transformations/identity_resolution/patient_identity_bridge.py \
    --format $PT_LAKEHOUSE_FORMAT
# Look for "Phase 0: Seeding operator WHOOP identity" in the log
```

For larger ad-hoc seed cohorts, append rows directly to the bridge keyed `source='manual_seed'`, `match_method='operator_seed'`. The merge upserts on `(identifier_type, identifier_value)` so this is safe to run multiple times. There is no dedicated CLI for this — write a one-off PySpark snippet that constructs a DataFrame with the bridge schema and calls `load_bridge(...)`.

### Case D: Bridge job stale — schedule fix

If diagnosis step 3 showed the bridge hasn't run in hours, the issue isn't unresolved IDs — it's that the scheduler stopped. Check the Prefect deployment:

```bash
prefect deployment ls | grep identity_bridge
prefect deployment run identity-bridge/daily   # force a run
```

If Prefect isn't running on the EMR master, fall back to a direct submission (Case A command).

## Verification

After recovery:

1. **Re-run the bridge and check the metrics line:**
   ```bash
   spark-submit transformations/identity_resolution/patient_identity_bridge.py \
       --format $PT_LAKEHOUSE_FORMAT 2>&1 | grep 'Identity resolution metrics'
   ```
   Expect `link_rate_pct` ≥ 95 and `pending` ≈ the count of FDA rows + any genuinely-new devices.

2. **Per-identifier breakdown:**
   ```bash
   spark-sql -e "
     SELECT identifier_type, link_status, COUNT(*) AS n
     FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge
     GROUP BY identifier_type, link_status ORDER BY identifier_type
   "
   ```

3. **Prometheus gauges back in range:**
   ```bash
   curl -s http://$MASTER_DNS:8004/metrics | grep '^pt_identity_link_rate_pct'
   # Expect: pt_identity_link_rate_pct >= 95
   ```

4. **Downstream `dim_patient` row count matches distinct `patient_key`:**
   ```bash
   spark-sql -e "
     SELECT
       (SELECT COUNT(*) FROM ${PT_GLUE_DB_GOLD:-pulsetrack_gold_dev}.dim_patient WHERE current_flag=true) AS dim_patient,
       (SELECT COUNT(DISTINCT patient_key) FROM ${PT_GLUE_DB_SILVER:-pulsetrack_silver_dev}.identity_bridge WHERE link_status='linked') AS bridge_linked
   "
   ```

## Prevention

- **Pre-flight normalization tests:** `tests/test_identity_bridge.py` covers the `sha2(lower(trim()))` path. Extend with negative cases — emails with trailing whitespace, mixed case, IDN — if Case B recurs.
- **Phase 1 freshness gate:** add a freshness check on `identity_bridge.last_seen` to `observability/sql/monitor_spec.yaml` (the bridge is a batch table, so use a 4-hour `max_age_minutes`). This catches the Case D scheduler-dead scenario before it pages.
- **Producer contract:** all producers writing `patient_email` MUST `.lower().strip()` before publishing. Add to `data_generators/synthetic/wearable_generator.py` and `data_generators/fhir_producer.py` as a guard. The bridge normalizes on the read side too, but defense in depth matters when investigation requires correlating raw values.
- **Capacity:** Phase 2's join is on a distinct email set; if the EHR cohort grows past ~1M, consider broadcasting the `ehr_emails` DataFrame explicitly.
- **Document `fda_report_id` baseline:** the FDA identifier type is *always* `pending_registration` by design (see `build_pharmacy_bridge_rows` docstring). Make sure the alerting in `data_quality/identity_metrics.py` excludes `identifier_type='fda_report_id'` from the `pending` count, or it will trigger noise on every run.

## Related postmortems

- *(none yet)*

## Related runbooks

- `runbooks/schema_drift.md` — a producer-side schema change that drops/renames `patient_email` or `user_device_account_id` lands here
- `runbooks/gx_failure_drains_batch.md` — the silver gate's `device_account_id IS NOT NULL` expectation also fires when Phase 2 inputs are broken
- `runbooks/kafka_consumer_lag.md` — if the silver sensor stream is lagging, the bridge's view of "current" devices is stale (Case D extension)
