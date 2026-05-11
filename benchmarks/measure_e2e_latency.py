"""End-to-end latency measurement — Kafka publish → silver write.

Reads silver.sensor_readings and computes the p50/p95/p99 of
``silver_write_ts - kafka_timestamp``. These columns are stamped by:

* ``streaming/bronze_ingestion.py`` (the bronze Spark Kafka source surfaces
  the Kafka broker-stamped publish time as ``timestamp`` → aliased to
  ``kafka_timestamp``).
* ``transformations/bronze_to_silver/sensor_silver.py`` (the silver projection
  adds ``F.current_timestamp().alias("silver_write_ts")``, stamped within
  foreachBatch).

Run from the EMR master (best performance) or anywhere with cluster access.

    /usr/bin/python3.11 -m benchmarks.measure_e2e_latency \\
        --format iceberg --sample-rows 1000000

If silver lacks kafka_timestamp/silver_write_ts (pre-instrumentation runs),
the script reports the gap and exits non-zero.
"""
from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--format", choices=["delta", "iceberg"], default="iceberg")
    parser.add_argument(
        "--sample-rows", type=int, default=1_000_000,
        help="Sample N rows for percentile computation (faster than full-table).",
    )
    parser.add_argument(
        "--silver-path",
        default="s3://pulsetrack-lakehouse-dev-03a28ee7/silver/sensor_readings/",
        help="S3 path or Iceberg table identifier for silver.sensor_readings.",
    )
    parser.add_argument(
        "--iceberg-table",
        default="glue_iceberg.pulsetrack_silver_dev.sensor_readings",
        help="Iceberg table identifier (used only when --format iceberg).",
    )
    parser.add_argument(
        "--output-json", default="docs/e2e_latency.json",
        help="Where to write the percentile results.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("measure_e2e_latency")
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if args.format == "iceberg":
        df = spark.read.table(args.iceberg_table)
    else:
        df = spark.read.format("delta").load(args.silver_path)

    # Validate the instrumentation columns exist; pre-instrumentation runs
    # will not have them.
    required = {"kafka_timestamp", "silver_write_ts"}
    missing = required - set(df.columns)
    if missing:
        print(
            "ERROR: silver missing latency columns {}.\n"
            "Run the silver layer post-commit 'feat(latency): kafka_timestamp + silver_write_ts'\n"
            "before re-running this measurement.".format(missing),
            file=sys.stderr,
        )
        return 2

    # Latency in milliseconds. Filter out nulls and impossible values
    # (e.g. wrap-around when kafka_timestamp > silver_write_ts, which
    # happens for rows produced before the instrumentation rolled out).
    latency = (
        df.filter(F.col("kafka_timestamp").isNotNull())
        .filter(F.col("silver_write_ts").isNotNull())
        .withColumn(
            "latency_ms",
            (F.col("silver_write_ts").cast("long") - F.col("kafka_timestamp").cast("long")) * 1000,
        )
        .filter(F.col("latency_ms") >= 0)
        .filter(F.col("latency_ms") < 24 * 3600 * 1000)  # < 24h, sanity
        .limit(args.sample_rows)
        .cache()
    )

    total = latency.count()
    if total == 0:
        print("ERROR: 0 rows with valid latency data in sample.", file=sys.stderr)
        return 3

    # Spark's approxQuantile is fast on large datasets.
    p50, p95, p99 = latency.stat.approxQuantile("latency_ms", [0.50, 0.95, 0.99], 0.01)
    max_v = latency.agg(F.max("latency_ms")).collect()[0][0]
    mean_v = latency.agg(F.mean("latency_ms")).collect()[0][0]

    result = {
        "sample_rows": total,
        "p50_ms": int(p50),
        "p95_ms": int(p95),
        "p99_ms": int(p99),
        "mean_ms": int(mean_v),
        "max_ms": int(max_v),
        "p50_s": round(p50 / 1000, 2),
        "p95_s": round(p95 / 1000, 2),
        "p99_s": round(p99 / 1000, 2),
        "mean_s": round(mean_v / 1000, 2),
        "max_s": round(max_v / 1000, 2),
    }
    print("\n========== END-TO-END LATENCY (Kafka publish → silver write) ==========")
    print(f"sample rows         : {result['sample_rows']:,}")
    print(f"p50                 : {result['p50_s']:>10.2f}s  ({result['p50_ms']:,} ms)")
    print(f"p95                 : {result['p95_s']:>10.2f}s  ({result['p95_ms']:,} ms)")
    print(f"p99                 : {result['p99_s']:>10.2f}s  ({result['p99_ms']:,} ms)")
    print(f"mean                : {result['mean_s']:>10.2f}s")
    print(f"max                 : {result['max_s']:>10.2f}s")
    print("========================================================================\n")

    # Persist JSON for downstream rendering into scale_test_results.md
    with open(args.output_json, "w") as fh:
        json.dump(result, fh, indent=2)
    print(f"results persisted: {args.output_json}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
