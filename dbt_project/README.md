# PulseTrack — dbt Project

Gold-layer transformation pipeline. Reads PulseTrack's silver lakehouse
(Iceberg on S3 via Glue catalog in production; fixture seeds in CI) and
produces a curated star schema + analytics-ready wide tables for BI and
ML consumers.

This project is the **WHOOP "commons" pattern**: shared macros for
SHA-256 surrogate keys, vital-range classification, SCD2 merges,
NULL-safe division, and date spines. 100% column-level documentation,
weekly release cadence, dbt-Cloud + dbt-DuckDB local parity.

## Project Structure

```
dbt_project/
├── dbt_project.yml          Project-level config + materialization defaults
├── profiles.yml             DuckDB (local/CI) + Snowflake (prod) profiles
├── packages.yml             dbt_utils, dbt_expectations, elementary
│
├── models/
│   ├── _sources.yml         Silver source declarations + freshness SLAs
│   ├── staging/             1:1 views over silver. Surrogate keys + casts only.
│   │   ├── stg_sensor_readings.sql
│   │   ├── stg_ehr_conditions.sql
│   │   ├── stg_ehr_medications.sql
│   │   ├── stg_pharmacy_fills.sql
│   │   ├── stg_identity_bridge.sql
│   │   └── _stg.yml         100% column docs + tests
│   │
│   ├── intermediate/        Ephemeral; compiled into downstream marts.
│   │   ├── int_vital_daily_agg.sql       Patient × metric × date aggregation
│   │   ├── int_patient_enriched.sql      Per-patient roll-up across all sources
│   │   ├── int_device_firmware_scd2.sql  SCD2 firmware history
│   │   └── int_medication_timeline.sql   Per-patient medication episodes
│   │
│   └── marts/
│       ├── core/            Published gold-layer dims and facts.
│       │   ├── dim_date.sql              Calendar with medical-domain attrs
│       │   ├── dim_metric.sql            Junk dim from metric_normal_ranges
│       │   ├── dim_condition_category.sql ICD-10 chapter rollup
│       │   ├── dim_patient.sql           PII-masked patient dimension
│       │   ├── dim_device.sql            SCD2 device dimension
│       │   ├── fact_vital_daily_summary.sql   Daily aggregation grain
│       │   ├── fact_vital_reading.sql    Atomic per-reading grain (ML)
│       │   ├── fact_lab_result.sql       LOINC lab fact
│       │   └── _core.yml                 100% column docs + tests
│       │
│       └── analytics/       Wide tables / KPIs for BI dashboards.
│           ├── patient_health_360.sql     Wide patient view
│           ├── vital_trend_analysis.sql   7/14/30-day rolling + z-score
│           ├── device_reliability.sql     Per-firmware failure rates
│           ├── anomaly_investigation.sql  Critical readings + context
│           └── identity_resolution_kpis.sql Bridge link-rate KPIs
│
├── macros/
│   ├── commons/             ← THE WHOOP COMMONS PATTERN
│   │   ├── generate_sha256_key.sql    SHA-256 surrogate (override dbt_utils MD5)
│   │   ├── classify_vital_range.sql   Range-based normal/warning/critical
│   │   ├── scd2_merge.sql             Generic SCD2 macro
│   │   ├── safe_divide.sql            NULL-safe division
│   │   └── date_spine.sql             Date-series wrapper
│   └── project/
│       └── grant_select_to_role.sql   Snowflake-only on-run-end hook
│
├── seeds/
│   ├── metric_normal_ranges.csv     14 metric × clinical bounds
│   ├── condition_categories.csv     ICD-10 chapters 1-22
│   ├── icd10_codes.csv              25 ICD-10 codes for reference
│   └── fixture_silver/              Stand-in for cloud silver in DuckDB
│       ├── fixture_silver_sensor_readings.csv
│       ├── fixture_silver_ehr_conditions.csv
│       ├── fixture_silver_ehr_medications.csv
│       ├── fixture_silver_pharmacy_fills.csv
│       └── fixture_silver_identity_bridge.csv
│
├── snapshots/
│   └── snap_dim_device.sql           dbt-managed SCD2 snapshot of devices
│
├── tests/                            Singular tests (custom SQL)
│   ├── assert_patient_key_not_orphaned.sql
│   ├── assert_daily_summary_math.sql
│   ├── assert_no_future_dates.sql
│   ├── assert_bridge_no_duplicate_identifiers.sql
│   └── assert_fact_vital_referential_integrity.sql
│
└── analyses/                         dbt compile-only — ad-hoc queries
    ├── identity_resolution_metrics.sql
    └── data_freshness_report.sql
```

## Quick Start (Local DuckDB)

```bash
cd dbt_project
pip install dbt-duckdb dbt-utils

# Install package dependencies
dbt deps

# Load fixture seeds (DuckDB target reads these as the "silver" sources)
dbt seed --target dev

# Build models
dbt build --target dev

# Generate documentation site
dbt docs generate --target dev
dbt docs serve   # → http://localhost:8080
```

## Cloud (Snowflake)

```bash
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...

dbt deps
dbt build --target snowflake
```

The Snowflake target reads from `PULSETRACK.SILVER.*` Iceberg tables exposed
via Snowflake's external Iceberg catalog integration (single source of
truth shared with the EMR pipeline).

## The "Commons" Pattern

`macros/commons/` is intentionally small and curated. Each macro:

1. Replaces a default-but-suboptimal pattern (SHA-256 instead of MD5;
   NULL-safe divide instead of warehouse-default-error).
2. Encodes a project-wide policy (vital-range thresholds from the seed,
   not hardcoded; SCD2 strategy that handles deletes correctly).
3. Has a single, opinionated implementation that all callers use without
   reinventing.

When someone proposes a 6th commons macro, the question is "is this used
in 3+ models, AND is it more nuanced than a one-line dbt_utils call, AND
does it encode a project-wide policy?" If yes → commons. If no → leave
as inline SQL or a model-local macro.

## 100% Documentation Coverage

Every column in `_stg.yml`, `_core.yml`, and `_sources.yml` has a
`description` field. Every column has at least one test (often
`not_null` for required fields, `accepted_values` for enums, or
`relationships` for FKs).

CI gate: `dbt-checkpoint` runs `check-model-has-description` and
`check-column-has-description` against every model touched by a PR.

## Weekly Release Cadence

The dbt project releases weekly via `.github/workflows/dbt_ci.yml`:

- **PRs** trigger a `dbt build --target dev` + `dbt test` against fixture
  seeds (DuckDB). Fast feedback.
- **Merge to main** triggers a snapshot run and tag.
- **Weekly cron** triggers `dbt build --target snowflake` for prod
  refreshes (data freshness from the streaming pipeline is continuous;
  the dbt rebuild cadence is for derived/aggregated marts).

## Materialization Strategy

| Layer | Materialization | Why |
|-------|-----------------|-----|
| Staging | `view` | Cheap; just casts + surrogate keys; freshness from silver is free |
| Intermediate | `ephemeral` | Compiled into marts; never persisted |
| Core | `table` | Rebuilt per dbt run; the published gold layer |
| Analytics | `table` | Rebuilt; consider `incremental` for high-row tables |

## Testing

- **Generic tests**: `not_null`, `unique`, `accepted_values`,
  `relationships`, `dbt_utils.accepted_range`,
  `dbt_utils.unique_combination_of_columns`.
- **Singular tests**: 5 SQL files in `tests/` for arithmetic and
  cross-table integrity assertions.
- **Source freshness**: `_sources.yml` declares `loaded_at_field` and
  warn/error thresholds for each silver table.

Run all tests:
```bash
dbt test
```

Run a specific test:
```bash
dbt test --select test_name:assert_daily_summary_math
```

## References

- **WHOOP Engineering blog** — dbt commons pattern (the spirit of this
  project).
- **dbt Style Guide** — naming conventions matched: `stg_*`, `int_*`,
  `dim_*`, `fact_*`.
- **PulseTrack runbook** — `docs/PRODUCTION_RUNBOOK.md` — how the
  silver tables this dbt project reads from are populated by the
  Spark streaming pipeline.

---

*PulseTrack data platform — gold layer in dbt; bronze/silver in Spark
Structured Streaming. Single source of truth for the gold tables is the
output of `dbt run` against the appropriate target.*
