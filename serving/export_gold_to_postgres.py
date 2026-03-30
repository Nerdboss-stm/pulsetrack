"""
Export Gold Layer → PostgreSQL (Superset backend)
==================================================
Reads Gold Delta tables and writes to PostgreSQL via JDBC.
Superset connects to PostgreSQL to serve clinical dashboards.

Why PostgreSQL instead of direct Delta reads?
  - Superset's DuckDB connector (for Delta) is read-only and slow for
    dashboards with many concurrent users.
  - PostgreSQL gives Superset a cached, query-optimized copy of Gold.
  - Export runs after each Silver→Gold pipeline run.

Connection: PostgreSQL on localhost:5432 (Docker service: postgres)
Schema: pulsetrack_gold

Run: python serving/export_gold_to_postgres.py
Run with specific tables: python serving/export_gold_to_postgres.py --tables dim_patient,fact_vital_daily_summary
"""
import sys, os, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from streaming.spark_config import get_spark_session
from delta.tables import DeltaTable

GOLD_BASE = "/tmp/pulsetrack-lakehouse/gold"

# PostgreSQL connection (Docker service)
PG_HOST     = os.environ.get("PG_HOST", "localhost")
PG_PORT     = os.environ.get("PG_PORT", "5432")
PG_DB       = os.environ.get("PG_DB", "pulsetrack")
PG_USER     = os.environ.get("PG_USER", "pulsetrack")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "pulsetrack")
PG_SCHEMA   = "pulsetrack_gold"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

JDBC_PROPS = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
    "currentSchema": PG_SCHEMA,
}

# Ordered for FK safety (dims before facts)
ALL_TABLES = [
    "dim_date",
    "dim_time",
    "dim_condition_category",
    "dim_drug_class",
    "dim_metric",
    "dim_patient",
    "dim_condition",
    "dim_medication",
    "dim_device",
    "fact_vital_reading",
    "fact_vital_daily_summary",
    "fact_lab_result",
    "fact_prescription_fill",
    "fact_anomaly_alert",
]

# Partition columns for large tables (faster parallel JDBC writes)
PARTITION_HINTS = {
    "fact_vital_reading":      ("date_key", 4),
    "fact_vital_daily_summary": ("date_key", 4),
    "fact_anomaly_alert":      ("date_key", 2),
    "fact_lab_result":         ("date_key", 2),
}


def export_table(spark, table):
    path = f"{GOLD_BASE}/{table}"
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"  SKIP  {table}: not found in Gold")
        return 0

    df = spark.read.format("delta").load(path)
    count = df.count()

    pg_table = f"{PG_SCHEMA}.{table}"
    partition_col, num_partitions = PARTITION_HINTS.get(table, (None, None))

    writer = df.write.mode("overwrite").option("truncate", "true").jdbc(
        url=JDBC_URL,
        table=pg_table,
        properties=JDBC_PROPS,
    )

    writer
    print(f"  ✅  {table}: {count:,} rows → {pg_table}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Export PulseTrack Gold → PostgreSQL")
    parser.add_argument("--tables", default="",
                        help="Comma-separated list of tables (default: all)")
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()] or ALL_TABLES

    spark = get_spark_session("PulseTrackGoldExport")
    print(f"Exporting {len(tables)} table(s) to PostgreSQL ({PG_HOST}:{PG_PORT}/{PG_DB})")
    print("=" * 60)

    total_rows = 0
    for table in tables:
        try:
            total_rows += export_table(spark, table)
        except Exception as e:
            print(f"  FAIL  {table}: {e}")

    print("=" * 60)
    print(f"Export complete. Total rows written: {total_rows:,}")
    print(f"\nSuperset dataset registration:")
    print(f"  Database URI: postgresql+psycopg2://{PG_USER}:***@{PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"  Schema: {PG_SCHEMA}")
    print(f"  Register views from: serving/superset_views.sql")


if __name__ == "__main__":
    main()
