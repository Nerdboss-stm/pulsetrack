# PulseTrack — Snowflake Integration

Read-side companion to the EMR streaming lakehouse. Snowflake reads
the same Iceberg tables + parquet files that the Spark pipeline writes
to S3 via Glue catalog — single source of truth, two compute engines.

Used for:
- **BI dashboards** (Tableau, Looker, Mode) querying gold facts.
- **Ad-hoc analytics** by clinicians / data analysts.
- **dbt prod target** (the `snowflake` profile in `dbt_project/profiles.yml`).
- **Personal WHOOP dashboard** via `VW_WHOOP_MY_HEALTH`.

## Directory layout

```
snowflake/
├── setup/                          One-time DDL run as ACCOUNTADMIN
│   ├── 01_create_infrastructure.sql       DB + warehouse + roles + schemas
│   ├── 02_create_storage_integration.sql  S3 + Glue catalog integration
│   ├── 03_create_stage.sql                External stages (parquet fallback)
│   ├── 04_create_iceberg_tables.sql       Iceberg tables via Glue
│   ├── 05_create_external_tables.sql      Fallback when Iceberg unavailable
│   └── 06_create_views.sql                Run-all wrapper
├── models/                         Analytics views (run as PULSETRACK_RW)
│   ├── vw_patient_health_360.sql          Wide patient view
│   ├── vw_vital_trends.sql                Rolling 7/14/30-day + z-score
│   ├── vw_anomaly_dashboard.sql           Critical readings + context
│   ├── vw_device_fleet_health.sql         Firmware failure-rate scoreboard
│   ├── vw_identity_resolution.sql         Bridge link-rate KPIs
│   └── vw_whoop_my_health.sql             Personal WHOOP dashboard
└── queries/                        Ad-hoc analytical queries
    ├── top_anomalies_this_week.sql
    ├── patient_medication_vital_correlation.sql
    ├── device_reliability_by_firmware.sql
    └── identity_resolution_funnel.sql
```

## Setup (trial account)

### 1. Create a Snowflake trial account

https://signup.snowflake.com — $400 free credits, 30-day trial.

### 2. Install SnowSQL

```bash
brew install --cask snowflake-snowsql
# or download from https://docs.snowflake.com/en/user-guide/snowsql-install-config
```

### 3. Set credentials

```bash
export SNOWFLAKE_ACCOUNT='<account>.<region>'   # e.g., abc12345.us-east-1
export SNOWFLAKE_USER='YOUR_USER'
export SNOWFLAKE_PASSWORD='YOUR_PASSWORD'
```

Or use `~/.snowsql/config`:
```ini
[connections.pulsetrack]
accountname = <account>.<region>
username = YOUR_USER
password = YOUR_PASSWORD
```

### 4. Run the setup scripts

```bash
bash scripts/setup_snowflake.sh
```

Or step by step (review each before running):
```bash
snowsql -c pulsetrack -f snowflake/setup/01_create_infrastructure.sql
# Manually grant your user the PULSETRACK_ADMIN role:
#   GRANT ROLE PULSETRACK_ADMIN TO USER YOUR_USER;
snowsql -c pulsetrack -f snowflake/setup/02_create_storage_integration.sql
# Now go to AWS:
#   - Capture DESC INTEGRATION PULSETRACK_S3 → STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID
#   - Create IAM role snowflake-pulsetrack-s3 with trust policy + S3 r/w policy
snowsql -c pulsetrack -f snowflake/setup/03_create_stage.sql
snowsql -c pulsetrack -f snowflake/setup/04_create_iceberg_tables.sql
snowsql -c pulsetrack -f snowflake/setup/05_create_external_tables.sql

# Load each view model:
for f in snowflake/models/*.sql; do
    snowsql -c pulsetrack -f "$f"
done
```

### 5. Verify

```sql
USE WAREHOUSE PULSETRACK_WH;
USE DATABASE PULSETRACK;

SHOW ICEBERG TABLES IN SCHEMA SILVER;       -- should list sensor_readings + 4 more
SHOW VIEWS IN SCHEMA ANALYTICS;             -- should list the 6 vw_* views

SELECT COUNT(*) FROM SILVER.SENSOR_READINGS;
SELECT * FROM ANALYTICS.VW_PATIENT_HEALTH_360 LIMIT 5;
SELECT * FROM ANALYTICS.VW_ANOMALY_DASHBOARD WHERE severity_label = 'CRITICAL_ANOMALY' LIMIT 10;
```

## Common queries

### "Top anomalies this week"
```sql
!source snowflake/queries/top_anomalies_this_week.sql
```

### "Did medication X affect heart rate?"
```sql
!source snowflake/queries/patient_medication_vital_correlation.sql
```

### "Should we roll firmware 1.1.0 back?"
```sql
!source snowflake/queries/device_reliability_by_firmware.sql
```

### "Identity-bridge funnel"
```sql
!source snowflake/queries/identity_resolution_funnel.sql
```

### "My personal WHOOP data"
```sql
ALTER SESSION SET WHOOP_OPERATOR_EMAIL = 'your-whoop-email@example.com';
SELECT * FROM PULSETRACK.ANALYTICS.VW_WHOOP_MY_HEALTH ORDER BY event_timestamp DESC LIMIT 20;
```

## Cost notes

- **Warehouse:** `XSMALL` × `AUTO_SUSPEND 60s` × `AUTO_RESUME TRUE`. Idle
  cost is $0 (suspended); active cost is ~$0.0025/credit-second × 1
  credit/hour XSMALL = $0.0025/sec when running.
- **Storage:** Iceberg tables are EXTERNAL — Snowflake doesn't pay for
  the data files (those are in our S3 bucket). Only catalog metadata
  is stored in Snowflake.
- **Compute:** trial account has $400 free credits. A typical analytics
  query on this dataset uses 0.001-0.01 credits → 40k-400k queries
  before exhausting.

## Operator runbook

### Refresh Iceberg metadata after EMR commits

Snowflake's `AUTO_REFRESH=TRUE` polls Glue every few minutes. For fresher
metadata, explicitly:
```sql
ALTER ICEBERG TABLE PULSETRACK.GOLD.fact_vital_daily_summary REFRESH;
```

The setup script's `REFRESH_ICEBERG_METADATA` task does this on a 5-min
schedule. Resume the task to activate:
```sql
ALTER TASK PULSETRACK.GOLD.REFRESH_ICEBERG_METADATA RESUME;
```

### Roll back a Snowflake view

Snowflake supports `UNDROP` for tables but views aren't versioned the
same way. To roll back a view, re-run the previous version from git
history.

### Query a specific Iceberg snapshot (time-travel)

Snowflake's Iceberg support exposes time-travel:
```sql
SELECT * FROM PULSETRACK.GOLD.fact_vital_daily_summary
AT (TIMESTAMP => '2026-05-09 00:00:00'::TIMESTAMP_TZ);
```

Time-travel window is bounded by the Iceberg snapshot retention
(7 days per our maintenance flow's `expire_snapshots` cadence).

## References

- Snowflake Iceberg docs: https://docs.snowflake.com/en/user-guide/tables-iceberg
- Snowflake Glue catalog integration: https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-integration
- Storage integration: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
- The dbt project's Snowflake target: `dbt_project/profiles.yml`
- PulseTrack runbook: `docs/PRODUCTION_RUNBOOK.md`
