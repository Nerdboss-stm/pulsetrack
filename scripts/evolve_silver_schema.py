"""Evolve the silver sensor_readings Iceberg table to add the e2e-latency columns.

Idempotent — uses ALTER TABLE ADD COLUMNS IF NOT EXISTS so re-running on a
table that already has the columns is a no-op.

Background
----------
The original 10M scale test (2026-05-11) discovered the silver sensor
Iceberg table on S3 had been written with an OLDER DDL that didn't include
``kafka_timestamp`` and ``silver_write_ts``. The streaming write either
dropped the new columns silently or failed. Either way, the
``benchmarks/measure_e2e_latency.py`` script couldn't run because the
columns weren't there to subtract.

This script is the explicit "evolve schema before silver stream starts"
step run as a Spark/EMR step BEFORE the silver streaming app launches.
After this runs, the silver writer's idempotent CREATE TABLE IF NOT EXISTS
becomes a no-op (table exists with the right schema), and the
foreachBatch projection writes the new columns natively.

Usage on EMR
------------
::

    spark-submit s3://${bucket}/code/scripts/evolve_silver_schema.py

Exits 0 on success (idempotent), 1 on schema-evolution failure.
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession


def main() -> int:
    spark = (
        SparkSession.builder.appName("evolve-silver-schema")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.glue_iceberg",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.glue_iceberg.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            "spark.sql.catalog.glue_iceberg.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    table = "glue_iceberg.pulsetrack_silver_dev.sensor_readings"

    # Idempotent ADD COLUMN — Iceberg supports "IF NOT EXISTS" via the
    # spark-sql Iceberg extension (registered above).
    statements = [
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS kafka_timestamp timestamp",
        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS silver_write_ts timestamp",
    ]

    failures = []
    for sql in statements:
        try:
            spark.sql(sql)
            print(f"[evolve-silver] OK: {sql}")
        except Exception as e:
            # If the table doesn't exist yet (fresh run), that's fine — the
            # silver writer's create_table will build it with the new DDL.
            msg = str(e).lower()
            if "table or view not found" in msg or "table_or_view_not_found" in msg:
                print(f"[evolve-silver] SKIP (table doesn't exist yet): {sql}")
            else:
                print(f"[evolve-silver] FAIL: {sql} → {type(e).__name__}: {str(e)[:200]}")
                failures.append((sql, type(e).__name__))

    # Show final schema for the log trail.
    try:
        print(f"[evolve-silver] Post-evolution schema for {table}:")
        rows = spark.sql(f"DESCRIBE TABLE {table}").collect()
        for r in rows:
            print(f"  {r.col_name:<30} {r.data_type}")
    except Exception as e:
        print(f"[evolve-silver] (DESCRIBE not available — table likely fresh): {type(e).__name__}")

    spark.stop()
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
