"""Inspect Snowflake state — what databases, schemas, tables, views exist."""

from __future__ import annotations

import sys
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pt_secrets import get_secret  # noqa: E402


def main() -> int:
    creds = get_secret("snowflake")
    conn = snowflake.connector.connect(
        account=creds["account"],
        user=creds["user"],
        password=creds["password"],
        role=creds["role"],
        warehouse=creds["warehouse"],
        database=creds["database"],
    )
    cur = conn.cursor()

    print("=" * 72)
    print("Snowflake state inspection")
    print("=" * 72)

    # Account context
    cur.execute("SELECT CURRENT_ROLE(), CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_WAREHOUSE()")
    r = cur.fetchone()
    print(f"role={r[0]}  user={r[1]}  account={r[2]}  warehouse={r[3]}")

    # Databases
    print("\n=== DATABASES ===")
    cur.execute("SHOW DATABASES")
    for row in cur.fetchall():
        print(f"  {row[1]}")

    # Schemas in PULSETRACK
    print("\n=== SCHEMAS IN PULSETRACK ===")
    try:
        cur.execute("SHOW SCHEMAS IN DATABASE PULSETRACK")
        for row in cur.fetchall():
            print(f"  {row[1]}")
    except Exception as e:
        print(f"  ERR: {e}")

    # Tables per schema
    for schema in ("BRONZE", "SILVER", "GOLD", "ANALYTICS", "OBSERVABILITY"):
        print(f"\n=== TABLES IN PULSETRACK.{schema} ===")
        try:
            cur.execute(f"SHOW TABLES IN SCHEMA PULSETRACK.{schema}")
            rows = cur.fetchall()
            if rows:
                for row in rows:
                    print(f"  {row[1]}")
            else:
                print("  (empty)")
        except Exception as e:
            print(f"  ERR: {str(e)[:200]}")

    # Iceberg tables
    print("\n=== ICEBERG TABLES IN PULSETRACK ===")
    for schema in ("BRONZE", "SILVER", "GOLD"):
        print(f"\n  -- {schema} --")
        try:
            cur.execute(f"SHOW ICEBERG TABLES IN SCHEMA PULSETRACK.{schema}")
            rows = cur.fetchall()
            if rows:
                for row in rows:
                    print(f"  {row[1]}")
            else:
                print("  (none)")
        except Exception as e:
            print(f"  ERR: {str(e)[:150]}")

    # External volumes + catalog integrations
    print("\n=== EXTERNAL VOLUMES ===")
    try:
        cur.execute("SHOW EXTERNAL VOLUMES")
        for row in cur.fetchall():
            print(f"  {row[1]}")
    except Exception as e:
        print(f"  ERR: {e}")

    print("\n=== CATALOG INTEGRATIONS ===")
    try:
        cur.execute("SHOW CATALOG INTEGRATIONS")
        for row in cur.fetchall():
            print(f"  {row[1]}  table_format={row[3] if len(row) > 3 else '?'}")
    except Exception as e:
        print(f"  ERR: {e}")

    print("\n=== VIEWS IN PULSETRACK.ANALYTICS ===")
    try:
        cur.execute("SHOW VIEWS IN SCHEMA PULSETRACK.ANALYTICS")
        rows = cur.fetchall()
        if rows:
            for row in rows:
                print(f"  {row[1]}")
        else:
            print("  (none)")
    except Exception as e:
        print(f"  ERR: {e}")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
