#!/usr/bin/env bash
# ============================================================================
# setup_snowflake.sh — run all Snowflake setup scripts in dependency order.
#
# Prerequisites:
#   - snowsql installed (brew install --cask snowflake-snowsql)
#   - Environment vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
#     (or a connection named "pulsetrack" in ~/.snowsql/config)
#   - YOUR Snowflake user granted ACCOUNTADMIN role (for steps 01, 02)
#
# Manual AWS step required BETWEEN 02 and 03:
#   1. After 02 runs, capture the STORAGE_AWS_IAM_USER_ARN +
#      STORAGE_AWS_EXTERNAL_ID via `DESC INTEGRATION PULSETRACK_S3`.
#   2. In AWS IAM, create role `snowflake-pulsetrack-s3` with trust policy
#      pointing at that ARN + external ID, and an S3 read+write policy
#      on the pulsetrack-lakehouse-* bucket.
#   3. Update STORAGE_AWS_ROLE_ARN in
#      snowflake/setup/02_create_storage_integration.sql if needed.
#
# Usage:
#   bash scripts/setup_snowflake.sh
#   bash scripts/setup_snowflake.sh --skip-aws-pause   # don't pause for manual AWS step
# ============================================================================

set -euo pipefail

cd "$(dirname "$0")/.."

SNOWSQL="${SNOWSQL:-snowsql}"
CONN="${SNOWFLAKE_CONNECTION:-pulsetrack}"

# Allow override via env or config file.
SF_AUTH_OPTS=""
if [[ -n "${SNOWFLAKE_ACCOUNT:-}" ]]; then
    SF_AUTH_OPTS="-a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER"
fi

SKIP_AWS_PAUSE=false
if [[ "${1:-}" == "--skip-aws-pause" ]]; then
    SKIP_AWS_PAUSE=true
fi

echo "[setup] checking snowsql availability..."
if ! command -v "$SNOWSQL" >/dev/null 2>&1; then
    echo "ERROR: snowsql not found. Install: brew install --cask snowflake-snowsql"
    exit 1
fi

echo "[setup] step 1/5 — infrastructure (DB / WH / roles / schemas)..."
"$SNOWSQL" -c "$CONN" $SF_AUTH_OPTS -f snowflake/setup/01_create_infrastructure.sql

echo "[setup] step 2/5 — storage + catalog integration (S3 + Glue)..."
"$SNOWSQL" -c "$CONN" $SF_AUTH_OPTS -f snowflake/setup/02_create_storage_integration.sql

if [[ "$SKIP_AWS_PAUSE" == "false" ]]; then
    cat <<EOF

══════════════════════════════════════════════════════════════════════
[setup] PAUSED — manual AWS configuration required.

Run this in Snowflake:
  USE ROLE ACCOUNTADMIN;
  DESC INTEGRATION PULSETRACK_S3;

Capture STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID.
Then in AWS:
  1. Create IAM role snowflake-pulsetrack-s3 with that trust + S3 policy
  2. Verify role ARN matches snowflake/setup/02_create_storage_integration.sql
  3. Same for snowflake-pulsetrack-glue (for the catalog integration)

Press ENTER when AWS is configured. Ctrl+C to abort.
══════════════════════════════════════════════════════════════════════

EOF
    read -r _
fi

echo "[setup] step 3/5 — external stages..."
"$SNOWSQL" -c "$CONN" $SF_AUTH_OPTS -f snowflake/setup/03_create_stage.sql

echo "[setup] step 4/5 — Iceberg tables (via Glue catalog)..."
"$SNOWSQL" -c "$CONN" $SF_AUTH_OPTS -f snowflake/setup/04_create_iceberg_tables.sql

echo "[setup] step 4b/5 — external tables (parquet fallback)..."
"$SNOWSQL" -c "$CONN" $SF_AUTH_OPTS -f snowflake/setup/05_create_external_tables.sql

echo "[setup] step 5/5 — analytics views..."
for f in snowflake/models/*.sql; do
    echo "  - $(basename "$f")"
    "$SNOWSQL" -c "$CONN" $SF_AUTH_OPTS -f "$f"
done

cat <<EOF

══════════════════════════════════════════════════════════════════════
[setup] complete.

Verify:
  USE WAREHOUSE PULSETRACK_WH;
  USE DATABASE PULSETRACK;
  SHOW ICEBERG TABLES IN SCHEMA SILVER;
  SHOW VIEWS IN SCHEMA ANALYTICS;
  SELECT COUNT(*) FROM SILVER.SENSOR_READINGS;
  SELECT * FROM ANALYTICS.VW_PATIENT_HEALTH_360 LIMIT 5;
══════════════════════════════════════════════════════════════════════
EOF
