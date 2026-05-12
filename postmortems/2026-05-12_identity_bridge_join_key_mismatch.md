# 2026-05-12 — Identity bridge ↔ dim_patient SHA-256 join-key mismatch

**Severity:** SEV3
**Authors:** PulseTrack DE
**Anchor commit / change:** Discovered while provisioning Snowflake analytics views (commit upcoming)
**Status:** Documented; downstream impact known; fix scoped (next iteration)

## Summary

While provisioning the 6 analytics views on Snowflake (closing the `❌ Don't exist` row
in `docs/scale_test_results.md`), I discovered that the `silver.identity_bridge.patient_key`
and `gold.dim_patient.patient_id_masked` are both 64-character SHA-256 hex strings — but
they hash different inputs with different salts and **do not cross-reference**.

This means `vw_patient_health_360` can't enrich `dim_patient` rows with EHR-derived
columns (active medications, active conditions) directly, because no join predicate
between the two tables resolves more than 0 patients.

Practical impact:
- `vw_patient_health_360` returns the correct **shape** (one row per dim_patient — 359
  rows) and the correct **vitals enrichment** (where the join goes through fact tables)
  — but `active_conditions` and `active_medications` are `NULL` for every row.
- `vw_anomaly_dashboard` works correctly because it only joins `fact_vital_reading` to
  `dim_patient` via `patient_key` (BIGINT, internal surrogate) — no SHA-256 involved.
- `vw_identity_resolution` works perfectly (it operates on identity_bridge alone).

## Impact

* No data corruption — both tables are correctly populated for their own purpose.
* `vw_patient_health_360` enrichment columns (`active_conditions`,
  `active_medications`, `primary_condition_list`, `active_medication_list`) are NULL
  for all 359 patients.
* Single-source views (`vw_device_fleet_health`, `vw_anomaly_dashboard`,
  `vw_whoop_my_health`, `vw_vital_trends`) are unaffected — they only join through
  the BIGINT `patient_key`.

## Detection

Running the freshly-provisioned `vw_patient_health_360` returned 359 rows but with all
EHR enrichment columns NULL. Debug query in `scripts/snowflake_debug_joins.py` revealed:

```
DEBUG 4b: Try SHA1(patient_id) = patient_id_masked
  SHA1(patient_id) match:                                 0
  SHA1(patient_email) match:                              0
  SHA2(patient_id, 256) match:                            0
  Direct bridge.patient_key=patient_id_masked match:      0
  dim_patient.patient_id_masked length:                  64
  identity_bridge.patient_key length:                    64
```

Both columns are 64-character SHA-256, but no transformation of `patient_id` or
`patient_email` from the silver layer produces a hash that matches either.

## Root cause

The transformation logic for `dim_patient.patient_id_masked` and
`identity_bridge.patient_key` was developed in two different files at two different
times:

| Field | Built by | Algorithm |
|-------|----------|-----------|
| `dim_patient.patient_id_masked` | `transformations/silver_to_gold/dim_patient.py` | likely SHA-256(canonical_email_or_mrn + some salt or composition) |
| `identity_bridge.patient_key` | `transformations/silver_to_gold/identity_bridge.py` | likely SHA-256(LOWER(identifier_value)) — derived per (identifier_type, identifier_value) |

The two were independently developed under different SPARC iterations and never
explicitly tested for cross-table join compatibility. Each works correctly for its
own purpose; the joinability between them was an implicit assumption that wasn't
encoded as a test.

This is the classic _convergent evolution_ data model issue — two pipelines
that "should" share a key but were keyed against different canonical inputs.

## What didn't go well

* No cross-table integration test caught it. A simple
  `assert dim_patient INNER JOIN identity_bridge USING (some_key) == 359 rows` test
  in the dbt project would have caught this at PR time.
* The view DDL in `snowflake/models/` assumed an enriched dbt-built `dim_patient`
  with explicit `patient_email_hash` and `health_complexity_*` columns. The EMR-built
  `dim_patient` doesn't have those — only the minimal schema. Discovering this
  mid-provisioning required rewriting 4 of the 6 view files.

## What went well

* The view DDL provisioned anyway (359 rows show, just NULL enrichment).
* Debug-script-first investigation isolated the root cause in ~5 min.
* `vw_identity_resolution` correctly reports the bridge state (93.5% link rate) —
  the bridge logic itself works; only its connection back to `dim_patient` is broken.

## Mitigation (immediate)

1. **Documented** in this postmortem.
2. **Updated** `vw_patient_health_360` to use `LEFT JOIN` with the enrichment
   so NULL-valued enrichment columns don't break the view — the view still
   returns 359 rows with the dim_patient fields plus blank EHR enrichment.

## Action items

* [P1] Add canonical-key column to `dim_patient` — emit
  `dim_patient.patient_key_bridge` = same SHA-256(LOWER(email)) that
  `identity_bridge` uses, so views can join through it.
  File to change: `transformations/silver_to_gold/dim_patient.py`.
* [P1] Add a dbt test `tests/assert_dim_patient_joinable_to_bridge.sql` that
  enforces the join produces ≥99% of rows linked. Tests run on every PR.
* [P2] Once `patient_key_bridge` is emitted, swap `vw_patient_health_360` to
  join through it and re-validate the EHR enrichment lands.
* [P3] Reconcile `dim_metric` duplicate rows for `heart_rate_bpm` (3 rows with
  different normal ranges) — uncovered during the same investigation. Likely
  another silver→gold idempotency issue.
* [P3] Document the canonical-identity policy in
  `docs/data_contracts.md` — what hash, what input, what salt; one truth.

## Lessons

* **Integration tests for cross-table join keys are tier-1, not optional.** A
  bridge table is only useful if its keys actually link to the dimensions it
  claims to bridge. We had the bridge logic and the dim builder both working
  in isolation; we never tested them together.
* **Different SPARC iterations on the same data model produce convergent-but-
  incompatible keys.** When two engineers (or two AI iterations) develop the
  bridge and the dimension independently, they each pick a "reasonable" hash.
  Those reasonable hashes don't agree. The data contract needs to be explicit.
* **Snowflake views revealed the gap that EMR-internal queries hid.** EMR-side
  Spark code uses `patient_key` (BIGINT) everywhere because that's what
  `dim_patient` exposes. EMR never tried to join to `identity_bridge`. Bringing
  in a second consumer (Snowflake) that needs both surfaces uncovered the gap.

## Cross-references

* [`scripts/provision_snowflake_views.py`](../scripts/provision_snowflake_views.py)
  — the provisioner that triggered this discovery.
* [`scripts/snowflake_debug_joins.py`](../scripts/snowflake_debug_joins.py)
  — the diagnostic script that isolated the SHA mismatch.
* [`snowflake/models/vw_patient_health_360.sql`](../snowflake/models/vw_patient_health_360.sql)
  — the view whose enrichment columns return NULL.
* [`docs/scale_test_results.md`](../docs/scale_test_results.md) §0 — original
  `❌ Don't exist` row that was closed by the view provisioning that uncovered
  this.
