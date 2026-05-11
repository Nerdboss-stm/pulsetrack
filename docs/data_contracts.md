# PulseTrack Data Contracts

The producer↔consumer contracts that every layer in the lakehouse honors. This is the binding-API doc — schema files in `schemas/` and `dbt_project/models/marts/core/_core.yml` are the source-of-truth; this doc is the human-readable summary plus the change-management procedure.

**Audience:** anyone adding a column, changing a type, or building a new consumer of an existing layer. Read § 5 before opening a schema PR.

---

## 1. Layer overview

| Layer | Source of truth | Format | Catalog | Compatibility mode |
|---|---|---|---|---|
| Bronze | `schemas/sensor_reading.avsc`, `schemas/pharmacy_event.avsc` | Avro on the wire, Iceberg at rest | Glue (`pulsetrack_bronze_dev`) | BACKWARD (consumer survives producer upgrade) |
| Silver | `transformations/bronze_to_silver/sensor_silver.py:SCHEMA` (Spark DDL) | Iceberg | Glue (`pulsetrack_silver_dev`) | BACKWARD (additive only without coordination) |
| Gold | `dbt_project/models/marts/core/_core.yml` | Iceberg | Glue (`pulsetrack_gold_dev`) | BACKWARD; SCD2 on dim_device |
| Marts / views | `dbt_project/models/marts/analytics/*.sql` | Snowflake views over Iceberg | Snowflake `PULSETRACK.ANALYTICS` | semantic versioning (`vw_*_v2`) |

The rule: each layer's *outputs* are the contract its consumers depend on. Producers can refactor internals freely; outputs require a contract-change PR (§ 5).

---

## 2. Bronze contract — Avro wire format

### 2.1 Wire format

Every record on `sensor_readings` and `pharmacy_events` is the Confluent Avro wire format:

```
| 0x00 | 4-byte schema-id | Avro-binary-payload |
   magic    big-endian          (schemaless body —
                                 needs the schema-id to
                                 deserialize)
```

The magic byte distinguishes from raw Avro; the schema-id resolves via Schema Registry (cloud) or `schemas/registry.py` (local). Producers serialize with `confluent_kafka.avro.AvroProducer`; consumers deserialize via `confluent_kafka.avro.AvroConsumer` or Spark's `from_avro`.

### 2.2 `sensor_readings` schema

Source: `schemas/sensor_reading.avsc`. Reproduced as a table for review:

| Field | Type | Nullable | Default | Notes |
|---|---|---|---|---|
| `reading_id` | string (UUID v7 preferred) | no | — | Idempotency key. Producer MUST generate unique per-event. |
| `device_id` | string | no | — | Stable device serial. Foreign key to `dim_device`. |
| `device_type` | enum {`smartwatch`, `chest_strap`, `sleep_ring`, `blood_pressure_cuff`} | no | — | New device types require contract change. |
| `user_device_account_id` | string | no | — | Vendor-side account ID (e.g., WHOOP user_id). Joined to patients via `identity_bridge`. |
| `patient_email` | string | yes | null | Optional email — populated when WHOOP-derived; null for synthetic. PII; never logged. |
| `metrics` | map<string, double or null> | no | — | Metric name → value. Schema-less by design — new metric names are non-breaking. |
| `firmware_version` | string | no | — | Device-side firmware. Used for cohort filtering. |
| `battery_pct` | int (0–100) | no | — | Used by `device_reliability` mart. |
| `event_timestamp` | timestamp-millis | no | — | When the device measured. Watermarked. |
| `sync_timestamp` | timestamp-millis | no | — | When the device synced upstream. `sync - event` = sync lag. |
| `source_type` | enum {`simulator`, `whoop_api`, `openfda`} | no | `simulator` | Provenance. |

### 2.3 `pharmacy_events` schema

Source: `schemas/pharmacy_event.avsc`.

| Field | Type | Nullable | Default | Notes |
|---|---|---|---|---|
| `event_id` | string | no | — | UUID. Idempotency key. |
| `event_type` | enum {`new_fill`, `refill`, `cancellation`, `adverse_event`} | no | — | Drives gold fact split. |
| `patient_id` | string | no | — | Joined to patients via identity bridge (LOINC-style hash). |
| `drug_name` | string | no | — | Free-text; cleaned in silver to NDC if `ndc_code` is null. |
| `ndc_code` | string | yes | null | National Drug Code. Required for FDA report joins. |
| `prescriber_npi` | string | yes | null | NPI; used for prescriber-side analytics (Phase 2). |
| `fill_date` | date (Avro logical) | no | — | When the prescription was filled. |
| `quantity` | int | no | — | Units dispensed. |
| `fda_report_id` | string | yes | null | OpenFDA adverse-event report ID, joined post-hoc. |
| `event_timestamp` | timestamp-millis | no | — | When the pharmacy system emitted. |

### 2.4 Schema-evolution rules (BACKWARD)

The Avro registry is configured for **BACKWARD compatibility**: new versions must be readable by old consumers. Concretely:

| Change | Allowed under BACKWARD? | Procedure |
|---|---|---|
| Add a nullable field with a default | yes — non-breaking | Submit schema PR; auto-deploy after CI passes |
| Add a non-nullable field | **no** — would break old readers | Migrate via deprecated-field swap (§ 5) |
| Remove a field with a default | yes | Mark deprecated for 30 days first (§ 5) |
| Rename a field | **no** | Add new + populate both + deprecate old (30 days) + remove old |
| Change a type (int→long, float→double) | sometimes — promotion only | int→long OK; long→int **no**; float↔int **no** |
| Add an enum value | yes for `default` symbol; otherwise **no** | Producers MUST emit only the existing symbols until N+1 deploy |
| Remove an enum value | **no** | Treat as a breaking change, follow § 5 deprecation |
| Reorder fields | yes (Avro is name-keyed) | Style preference: keep new fields at the end |

CI gate: `.github/workflows/migration-check.yml` runs `python schemas/registry.py check` against every PR that touches `schemas/*.avsc`. The check fetches the latest registered version from Glue Schema Registry (or the local file fixture for CI) and runs `org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility` in BACKWARD mode.

---

## 3. Silver contract — column-level schema

### 3.1 `pulsetrack_silver_dev.sensor_readings`

Each row is one (event, metric) pair — bronze rows are exploded by the `metrics` map.

| Column | Type | Nullable | Value set / range | Notes |
|---|---|---|---|---|
| `reading_id` | string | no | UUID | From bronze. |
| `device_id` | string | no | — | FK → `dim_device`. |
| `device_type` | string | no | {smartwatch, chest_strap, sleep_ring, blood_pressure_cuff} | Enforced by silver; bronze enum unwrapped. |
| `user_device_account_id` | string | no | — | Pre-bridge identifier. |
| `patient_key` | bigint | yes | — | Populated post-bridge; nullable until identity resolution runs. |
| `metric_name` | string | no | {heart_rate, hrv, spo2, body_temp, sleep_score, recovery_score, strain_score, calories, steps, ...} | One row per metric. Free-form to allow new metrics from `bronze.metrics` map. |
| `metric_value` | double | yes | — | The metric reading; null if device returned null. |
| `unit` | string | no | {bpm, ms, %, C, score, kcal, count, ...} | Derived from `metric_name` in silver via lookup table. |
| `is_valid` | boolean | no | — | Result of business-rule validation (range check + null check + cross-metric sanity). |
| `validation_error` | string | yes | — | Set when `is_valid=false`. Human-readable. |
| `event_timestamp` | timestamp | no | — | From bronze. |
| `ingestion_timestamp` | timestamp | no | — | When the silver row was written. Used for freshness SLOs. |
| `source_type` | string | no | {simulator, whoop_api, openfda} | From bronze. |
| `_silver_version` | int | no | current = 1 | Bumped on breaking schema changes — see § 5.2. |

**Explosion rule:** for a bronze row with `metrics = {hr: 72, hrv: 45}`, silver emits 2 rows — one per (reading_id, metric_name). The grain is `(reading_id, metric_name)`. Deduplication is done at this grain inside `dropDuplicatesWithinWatermark(["reading_id", "metric_name"], "10 minutes")`.

**`is_valid` semantics:**

- `is_valid=true`: row is publishable to gold. All business-rule checks passed.
- `is_valid=false`: row is retained in silver but excluded from gold facts via `WHERE is_valid` in transformations. The `validation_error` column has the reason.
- Business rules live in `transformations/bronze_to_silver/sensor_silver.py:validate_metric` (range checks) and `data_quality/expectations/silver_sensor_suite.py` (GX expectations).

**Watermark + dedup window:** the Spark stream uses `withWatermark("event_timestamp", "10 minutes")` + `dropDuplicatesWithinWatermark(...)`. Implication: late-arriving duplicates *within* 10 minutes are dropped; beyond that, they re-emit (idempotent at gold via merge keys, so this is safe but not free).

### 3.2 `pulsetrack_silver_dev.identity_bridge`

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `bridge_id` | bigint | no | Surrogate. |
| `patient_key` | bigint | no | The resolved patient. FK → `dim_patient`. |
| `user_device_account_id` | string | no | The vendor-side identifier being bridged. |
| `source_system` | string | no | {whoop, ehr, openfda} |
| `confidence_score` | double | no | 0.0–1.0; resolution confidence. |
| `match_method` | string | no | {email_exact, name_dob_fuzzy, manual_override} |
| `created_at` | timestamp | no | When the bridge entry was first formed. |
| `valid_from` / `valid_to` | timestamp / timestamp | no/yes | SCD2-style — `valid_to` is null for current. |

Bridge invariant: at any `as_of` timestamp, a `(user_device_account_id, source_system)` resolves to ≤1 `patient_key` via `valid_from ≤ as_of < coalesce(valid_to, '9999-12-31')`. The identity-bridge job enforces this on commit.

### 3.3 `pulsetrack_silver_dev.ehr_conditions`, `ehr_medications`, `ehr_observations`

Output of the FHIR batch silver job. Columns match FHIR R4 resource fields one-to-one (`code`, `category`, `subject.reference`, `onsetDateTime`, etc.). The mapping is in `transformations/bronze_to_silver/ehr_silver.py` and is itself the contract.

---

## 4. Gold contract — star-schema grain

The gold layer is the published "API of the lakehouse". 3 facts + 9 dimensions. Documented column-by-column in `dbt_project/models/marts/core/_core.yml` with dbt tests for every column.

### 4.1 Dimension grain

| Dimension | Grain | Surrogate-key derivation | SCD type |
|---|---|---|---|
| `dim_date` | One row per calendar date | `YYYYMMDD::int` | static |
| `dim_time` | One row per minute-of-day | `HHMM::int` | static |
| `dim_patient` | One row per patient | `SHA-256(natural_key) :: bigint` (first 8 bytes) | SCD1 (overwrite — privacy preference) |
| `dim_device` | One row per device-firmware pair | `SHA-256(device_id || firmware_version)` | SCD2 (`valid_from`/`valid_to`, `is_current`) |
| `dim_metric` | One row per metric definition | `SHA-256(metric_name)` | static (registry-managed) |
| `dim_condition` | One row per condition code | `SHA-256(coding_system || code)` | SCD1 |
| `dim_condition_category` | One row per ICD-10 category | category code | static |
| `dim_medication` | One row per medication (NDC or fallback drug_name) | `SHA-256(ndc_code or drug_name)` | SCD1 |
| `dim_drug_class` | One row per ATC class | class code | static |

**Surrogate keys are deterministic.** Same inputs → same key, always. This is why we use SHA-256 over the natural key, not a sequence/identity column — reruns of the dimension job produce stable keys, so fact joins don't break.

### 4.2 Fact grain

| Fact | Grain | Primary key (composite) |
|---|---|---|
| `fact_vital_reading` | One row per (event, metric) — same grain as silver | `(reading_id, metric_key)` |
| `fact_vital_daily_summary` | One row per (patient, metric, date) | `(patient_key, metric_key, date_key)` |
| `fact_lab_result` | One row per FHIR Observation resource | `(observation_id, code_system, code)` |

Facts use composite primary keys, not surrogate fact-keys. We MERGE on the composite key; idempotent re-runs produce the same row.

### 4.3 SCD2 details for `dim_device`

The only SCD2 table — device firmware updates and reassignments change the device's effective attributes:

| Column | Notes |
|---|---|
| `device_key` | Surrogate (firmware-aware: same device, different firmware → different key) |
| `device_id` | Natural key (stable across firmware updates) |
| `firmware_version` | Changes when device updates |
| `device_type` | Stable per device, but type changes when device class is re-categorized |
| `manufacturer` | Stable |
| `valid_from` | When this firmware/configuration became active |
| `valid_to` | When superseded; null for current |
| `is_current` | Boolean — exists for query convenience |

Facts join on `device_key` (firmware-aware) for analytics that need to attribute readings to a specific firmware. For "device → patient" lookups, join on `device_id` and filter `is_current = true`.

---

## 5. Breaking-change procedure

Any change that fails BACKWARD compatibility — adding a non-nullable field, removing a field without a default, renaming, narrowing a type — follows this dance.

### 5.1 Deprecation timeline (30 days minimum)

```
Day 0:   PR opened. New schema written; old field marked deprecated in docs.
Day 0:   Slack #pulsetrack-eng announcement: "{field} deprecated in N+1".
Day 7:   Consumer audit complete (§ 5.3); list of consumers shared in PR.
Day 14:  Reminder broadcast.
Day 30:  Old schema retired. Glue/Iceberg schema dropped.
```

The 30-day clock is for **production**. Dev/staging deprecations can move faster if all consumers are PulseTrack-internal and have been notified.

### 5.2 Add a field

Backward-compatible by definition — but the new column needs a tested transformation path:

1. Open PR: add the field with `nullable=true` and a default in the Avro schema (bronze) or DDL (silver/gold)
2. Add silver/gold transformation logic that populates it from the new bronze field
3. Add a dbt test in `_core.yml` (at minimum `not_null` if it should be populated; usually a `accepted_range` or `accepted_values` too)
4. Add a backfill plan if historical rows need the value (gold MERGE will populate it forward on next run; backfill = `INSERT OVERWRITE` from silver into the gold partition for the past N days)
5. Bump `_silver_version` (silver only) so downstream consumers can detect the new schema

### 5.3 Deprecate a field

Most common breaking change. Procedure:

1. Mark the field deprecated in the schema file (Avro `doc` field) AND in this doc
2. Audit consumers (in code: `rg <field_name>` across `dbt_project/`, `transformations/`, `streaming/`, `ai/`, `snowflake/`)
3. Open Linear tickets for each consumer to migrate
4. Wait 30 days
5. Remove the field (silver: drop column via Glacierbase migration; bronze: register new schema sans field; gold: same)
6. Bump `_silver_version` or rename the table version (e.g., `vw_anomaly_dashboard_v2`)

### 5.4 Rename a field

Worst case — requires both old and new fields to coexist for the deprecation window:

1. Add the new field (5.2)
2. Populate both old and new in the same transformation (writes 2× the data temporarily)
3. Mark old field deprecated (5.3)
4. After 30 days, drop the old field

### 5.5 Narrow a type (long → int, double → float)

Same procedure as a rename — add new column with new type, dual-write, deprecate old.

### 5.6 Add an enum value (e.g., new `device_type`)

| Strategy | Allowed? |
|---|---|
| Add value, producer doesn't emit it yet | yes — non-breaking until producer emits |
| Add value with `default` symbol set to it | yes |
| Producer immediately emits new value to old consumers | **no** — old consumers throw |

The supported path: register the new schema with the added value at least one full deploy cycle before any producer emits it.

---

## 6. Consumer change-management

### 6.1 Who notifies whom

| Change | Author | Notify |
|---|---|---|
| New nullable column | DE | Slack `#pulsetrack-eng`, 24h before merge |
| Deprecated column | DE | Slack `#pulsetrack-eng` + Linear ticket on every consumer team |
| Breaking change (rare) | DE + Architect | Eng-wide email, schedule walkthrough, 30-day clock starts |
| New table/view | DE | Slack `#pulsetrack-eng` |
| Renamed view | DE | All consumers via Linear; create alias view for 30 days |

### 6.2 Versioning

- **Avro schemas:** versioned in Schema Registry; the schema-id in the wire format pins consumers to a specific version. Consumers should specify minimum compatible version, not exact version.
- **Iceberg tables:** versioned via the `_silver_version` (or `_gold_version`) column when a breaking change is unavoidable. Most changes don't need this; only when the table's grain or core key changes.
- **Snowflake views:** versioned via name suffix (`vw_patient_health_360` → `vw_patient_health_360_v2`). The old view is preserved for 30 days; both point at the same gold tables but with different projections / column selections.
- **Marts / dbt models:** version via dbt's standard `models/v2/` directory pattern (or model alias).

### 6.3 What consumers can rely on

Implicit contract that producers commit to:

1. **No silent semantic changes.** If `metric_value` for `metric_name='hr'` ever changes meaning (e.g., shifts from bpm to bps), that's a renaming-class change. Goes through § 5.4.
2. **No silent unit changes.** Adding a new unit means a new column or an explicit unit column (`unit`, already in silver schema).
3. **No silent dedup behavior changes.** If we change the dedup window from 10 min to 30 min, that's a breaking-class change for any consumer that has its own dedup logic.

### 6.4 What consumers cannot rely on

- **Row arrival order.** Bronze is partitioned by hour; ordering within an hour is not guaranteed. Use `event_timestamp` for ordering, not insertion order.
- **Exact row counts at any instant.** Counts converge as streaming catches up. Use `vw_*_freshness` views to know when data is "stable" for the period.
- **The continued existence of `is_valid=false` rows in silver.** We may move these to a separate quarantine table in the future (§ 5.2 with 30-day notice). Don't rely on filtering them out at consumer side.

---

## 7. References

- `schemas/sensor_reading.avsc`, `schemas/pharmacy_event.avsc` — Avro source-of-truth
- `schemas/registry.py` — local registry shim + CI compatibility checker
- `transformations/bronze_to_silver/sensor_silver.py` — silver schema + validation rules
- `dbt_project/models/marts/core/_core.yml` — gold column-level contracts + dbt tests
- `migrations/versions/V*` — Glacierbase migrations enforcing silver+gold schema changes
- `docs/PRODUCTION_RUNBOOK.md` § 3 — Glacierbase operational procedures
- Confluent BACKWARD compatibility reference: https://docs.confluent.io/platform/current/schema-registry/avro.html#backward-compatibility
