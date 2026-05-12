"""Provision Snowflake Iceberg tables + 6 analytics views over PulseTrack gold.

Closes the gap documented in docs/scale_test_results.md §0:
    | Snowflake VIEWS (`VW_PATIENT_HEALTH_360` etc.) | ❌ Don't exist |

Reality check during provisioning (2026-05-12):
  - Only 4 of 14 Iceberg tables had been registered in Snowflake from prior
    runs (BRONZE.SENSOR_READINGS + SILVER.{SENSOR_READINGS, EHR_CONDITIONS,
    EHR_MEDICATIONS}). The rest registered fresh here.
  - The 6 view DDL files in snowflake/models/ originally targeted the
    dbt-built enriched schema (rich dim_patient with health_complexity_*
    columns, fact_vital_reading with vital_status, etc.). The EMR-built
    raw gold schema is simpler. View SQLs have been rewritten to be
    schema-compatible with what EMR actually produces, with derivations
    in the view DDL (e.g. vital_status derived via dim_metric range).

Dependency order:
    1. VW_VITAL_TRENDS         (no view dependencies)
    2. VW_DEVICE_FLEET_HEALTH  (no view dependencies)
    3. VW_IDENTITY_RESOLUTION  (no view dependencies)
    4. VW_PATIENT_HEALTH_360   (no view dependencies)
    5. VW_ANOMALY_DASHBOARD    (depends on VW_VITAL_TRENDS)
    6. VW_WHOOP_MY_HEALTH      (depends on VW_VITAL_TRENDS)

Reads Snowflake credentials from AWS Secrets Manager via pt_secrets.
NEVER prints any credential value.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Tuple

import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pt_secrets import get_secret  # noqa: E402

# ---------------------------------------------------------------------------
# Iceberg tables that need to exist in Snowflake. (db_name, schema, table)
# Catalog namespace in Glue follows convention: pulsetrack_{schema}_dev.
# ---------------------------------------------------------------------------
ICEBERG_TABLES: List[Tuple[str, str, str]] = [
    # BRONZE
    ("bronze",  "sensor_readings",            "pulsetrack_bronze_dev"),
    # SILVER
    ("silver",  "sensor_readings",            "pulsetrack_silver_dev"),
    ("silver",  "ehr_conditions",             "pulsetrack_silver_dev"),
    ("silver",  "ehr_medications",            "pulsetrack_silver_dev"),
    ("silver",  "ehr_lab_results",            "pulsetrack_silver_dev"),
    ("silver",  "identity_bridge",            "pulsetrack_silver_dev"),
    # GOLD — dimensions
    ("gold",    "dim_patient",                "pulsetrack_gold_dev"),
    ("gold",    "dim_device",                 "pulsetrack_gold_dev"),
    ("gold",    "dim_metric",                 "pulsetrack_gold_dev"),
    ("gold",    "dim_date",                   "pulsetrack_gold_dev"),
    ("gold",    "dim_time",                   "pulsetrack_gold_dev"),
    # GOLD — facts
    ("gold",    "fact_vital_reading",         "pulsetrack_gold_dev"),
    ("gold",    "fact_vital_daily_summary",   "pulsetrack_gold_dev"),
    ("gold",    "fact_lab_result",            "pulsetrack_gold_dev"),
    ("gold",    "fact_pharmacy_fill",         "pulsetrack_gold_dev"),
]

# View dependency order — vw_vital_trends MUST come before vw_anomaly_dashboard
# and vw_whoop_my_health which reference it.
VIEW_ORDER: List[str] = [
    "vw_vital_trends.sql",
    "vw_device_fleet_health.sql",
    "vw_identity_resolution.sql",
    "vw_patient_health_360.sql",
    "vw_anomaly_dashboard.sql",
    "vw_whoop_my_health.sql",
]

REPO_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = REPO_ROOT / "snowflake" / "models"


def connect() -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection using credentials from AWS Secrets Manager."""
    creds = get_secret("snowflake")
    return snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        role=creds["role"],
        warehouse=creds["warehouse"],
        database=creds["database"],
        schema="ANALYTICS",
    )


def register_iceberg_table(cur, schema: str, table: str, namespace: str) -> Tuple[bool, str]:
    """Register a single Iceberg table via CATALOG + EXTERNAL_VOLUME binding."""
    fq = f"PULSETRACK.{schema.upper()}.{table.upper()}"
    ddl = f"""
        CREATE OR REPLACE ICEBERG TABLE {fq}
            EXTERNAL_VOLUME    = 'PULSETRACK_VOL'
            CATALOG            = 'PULSETRACK_GLUE'
            CATALOG_TABLE_NAME = '{table}'
            CATALOG_NAMESPACE  = '{namespace}'
            AUTO_REFRESH       = TRUE
    """
    try:
        cur.execute(ddl)
        return True, fq
    except (ProgrammingError, DatabaseError) as e:
        msg = str(e)[:200].replace("\n", " ")
        return False, f"{fq}: {msg}"


def execute_sql_file(cur, sql_path: Path) -> Tuple[bool, str]:
    """Execute a multi-statement SQL file."""
    raw = sql_path.read_text()
    try:
        cur.connection.execute_string(raw)
        return True, f"OK — {sql_path.name}"
    except (ProgrammingError, DatabaseError) as e:
        msg = str(e)[:400].replace("\n", " ")
        return False, f"FAIL — {sql_path.name}: {msg}"


def validate_view(cur, view_name: str) -> Tuple[bool, str]:
    """COUNT(*) on the view to prove it's queryable."""
    fq = f"PULSETRACK.ANALYTICS.{view_name.upper()}"
    try:
        cur.execute(f"SELECT COUNT(*) FROM {fq}")
        row = cur.fetchone()
        cnt = row[0] if row else "?"
        return True, f"{view_name:<32} {cnt:>10,} rows"
    except (ProgrammingError, DatabaseError) as e:
        msg = str(e)[:300].replace("\n", " ")
        return False, f"{view_name:<32} ERR: {msg}"


def main() -> int:
    print("=" * 78)
    print("PulseTrack — Provisioning Iceberg tables + 6 Snowflake analytics views")
    print("=" * 78)

    try:
        conn = connect()
    except Exception as e:
        print(f"[FATAL] Snowflake connect failed: {type(e).__name__}: {str(e)[:200]}")
        return 2

    cur = conn.cursor()

    # Context setup.
    print("\n[1/5] Setting Snowflake context...")
    for ctx_sql in (
        "USE WAREHOUSE PULSETRACK_WH",
        "USE DATABASE PULSETRACK",
        "USE SCHEMA ANALYTICS",
    ):
        try:
            cur.execute(ctx_sql)
            print(f"  OK — {ctx_sql}")
        except Exception as e:
            print(f"  WARN — {ctx_sql}: {type(e).__name__}: {str(e)[:120]}")

    # Step 2: Register Iceberg tables.
    print(f"\n[2/5] Registering {len(ICEBERG_TABLES)} Iceberg tables via Glue catalog...")
    iceberg_results: List[Tuple[bool, str]] = []
    for schema, table, namespace in ICEBERG_TABLES:
        ok, msg = register_iceberg_table(cur, schema, table, namespace)
        marker = "✓" if ok else "✗"
        print(f"  {marker} {msg}")
        iceberg_results.append((ok, msg))
    ice_ok = sum(1 for ok, _ in iceberg_results if ok)
    print(f"  → {ice_ok}/{len(ICEBERG_TABLES)} Iceberg tables registered")

    # Step 3: Quick row-count sanity check on a few tables.
    print("\n[3/5] Row-count sanity check (gold + silver):")
    sanity_tables = [
        ("PULSETRACK.SILVER.SENSOR_READINGS",     "expect 697,828"),
        ("PULSETRACK.SILVER.IDENTITY_BRIDGE",     "expect 768"),
        ("PULSETRACK.SILVER.EHR_CONDITIONS",      "expect 711"),
        ("PULSETRACK.SILVER.EHR_MEDICATIONS",     "expect 619"),
        ("PULSETRACK.GOLD.DIM_PATIENT",           "expect 359"),
        ("PULSETRACK.GOLD.DIM_METRIC",            "expect 14"),
        ("PULSETRACK.GOLD.DIM_DEVICE",            "expect 250"),
        ("PULSETRACK.GOLD.FACT_VITAL_READING",    "expect 130,774"),
        ("PULSETRACK.GOLD.FACT_VITAL_DAILY_SUMMARY", "expect 0"),
    ]
    for fq, hint in sanity_tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {fq}")
            cnt = cur.fetchone()[0]
            print(f"  {fq:<48} {cnt:>10,}  ({hint})")
        except Exception as e:
            print(f"  {fq:<48} ERR: {str(e)[:100]}")

    # Step 4: Create the 6 analytics views in dependency order.
    print(f"\n[4/5] Creating {len(VIEW_ORDER)} analytics views (dependency order):")
    results: List[Tuple[str, bool, str]] = []
    for fname in VIEW_ORDER:
        path = MODELS_DIR / fname
        if not path.exists():
            print(f"  SKIP — {fname}: file not found")
            results.append((fname, False, "file not found"))
            continue
        ok, summary = execute_sql_file(cur, path)
        marker = "✓" if ok else "✗"
        print(f"  {marker} {summary}")
        results.append((fname, ok, summary))

    succeeded = sum(1 for _, ok, _ in results if ok)
    failed = len(results) - succeeded

    # Step 5: Validate each view via COUNT(*).
    print(f"\n[5/5] Validating views (COUNT *) — {succeeded} created, {failed} failed:")
    validations: List[Tuple[str, bool, str]] = []
    for fname, ok, _ in results:
        if not ok:
            continue
        view_name = fname.replace(".sql", "")
        v_ok, v_summary = validate_view(cur, view_name)
        marker = "✓" if v_ok else "✗"
        print(f"  {marker} {v_summary}")
        validations.append((view_name, v_ok, v_summary))

    # Final summary.
    print("\n" + "=" * 78)
    print(f"ICEBERG TABLES:  {ice_ok}/{len(ICEBERG_TABLES)} registered")
    print(f"VIEWS CREATED:   {succeeded}/{len(VIEW_ORDER)}")
    print(f"VIEWS VALIDATED: {sum(1 for _,o,_ in validations if o)}/{len(validations)}")
    print("=" * 78)

    cur.close()
    conn.close()

    return 0 if (failed == 0 and all(o for _, o, _ in validations)) else 1


if __name__ == "__main__":
    sys.exit(main())
