"""Sample 3 rows from each analytics view to prove they're query-able for BI."""

from __future__ import annotations

import sys
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pt_secrets import get_secret  # noqa: E402


VIEWS_TO_SAMPLE = [
    ("vw_device_fleet_health",
     "SELECT device_type, firmware_version, total_readings, invalid_reading_rate, "
     "late_arrival_rate, composite_failure_rate, reliability_tier "
     "FROM PULSETRACK.ANALYTICS.VW_DEVICE_FLEET_HEALTH "
     "ORDER BY composite_failure_rate DESC LIMIT 3"),
    ("vw_identity_resolution",
     "SELECT row_kind, identifier_type, link_status, link_method, row_count, "
     "unique_patients, overall_link_rate "
     "FROM PULSETRACK.ANALYTICS.VW_IDENTITY_RESOLUTION ORDER BY row_kind, row_count DESC"),
    ("vw_patient_health_360",
     "SELECT patient_key, age_group, gender, health_complexity_bucket, "
     "active_conditions, active_medications, device_count, "
     "TO_VARCHAR(last_sensor_event) AS last_sensor_event "
     "FROM PULSETRACK.ANALYTICS.VW_PATIENT_HEALTH_360 "
     "WHERE active_conditions > 0 ORDER BY active_conditions DESC LIMIT 3"),
    ("vw_anomaly_dashboard",
     "SELECT metric_name, metric_value, vital_status, severity_label, "
     "TO_VARCHAR(event_timestamp) AS event_timestamp "
     "FROM PULSETRACK.ANALYTICS.VW_ANOMALY_DASHBOARD "
     "WHERE vital_status='critical' LIMIT 3"),
]


def main() -> int:
    creds = get_secret("snowflake")
    conn = snowflake.connector.connect(
        account=creds["account"], user=creds["user"], password=creds["password"],
        role=creds["role"], warehouse=creds["warehouse"], database=creds["database"],
        schema="ANALYTICS",
    )
    cur = conn.cursor()

    for name, sql in VIEWS_TO_SAMPLE:
        print("=" * 78)
        print(f"  {name}")
        print("=" * 78)
        try:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            print("  " + " | ".join(cols))
            print("  " + "-" * 70)
            rows = cur.fetchall()
            if not rows:
                print("  (no rows)")
            for r in rows:
                # Stringify each cell, truncating long fields.
                cells = []
                for v in r:
                    s = str(v) if v is not None else "NULL"
                    if len(s) > 30:
                        s = s[:27] + "..."
                    cells.append(s)
                print("  " + " | ".join(cells))
        except Exception as e:
            print(f"  ERR: {type(e).__name__}: {str(e)[:200]}")
        print()

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
