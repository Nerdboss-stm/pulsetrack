"""
Real-S3 partition strategy benchmark.

Runs ON EMR against the real lakehouse bucket. Generates a synthetic
dataset, writes it three times under different S3 partition strategies,
and measures wall-clock + Spark-side IO metrics + S3 request counts +
5xx error counts from CloudWatch.

The point is to make the WHOOP "reversed-id beats date-first" claim
empirically defensible with our own numbers, not just to cite their
blog post.

Usage on EMR master:

    PYSPARK_PYTHON=/usr/bin/python3.11 \\
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11 \\
    PT_AWS_REGION=us-east-1 \\
    /usr/lib/spark/bin/spark-submit \\
        --master yarn --deploy-mode client \\
        --conf spark.executor.memory=2g \\
        --conf spark.executor.instances=2 \\
        --jars file:///usr/share/aws/delta/lib/delta-spark.jar,\\
file:///usr/share/aws/delta/lib/delta-storage.jar \\
        benchmarks/s3_partition_benchmark.py \\
            --records 1000000 \\
            --devices 1000 \\
            --bucket pulsetrack-lakehouse-dev-03a28ee7 \\
            --output-prefix benchmarks/s3-partition \\
            --thundering-herd-records 500000

Produces ``benchmarks/s3-partition/results.json`` and a markdown
summary printed to stdout. CloudWatch S3 RequestMetrics (enabled via
``infrastructure/modules/storage/main.tf``) take ~15 min to surface;
the script optionally polls them via ``--cloudwatch-poll``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

# Make project imports work both as a script and as a module on EMR.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from lakehouse.partition_strategy import (  # noqa: E402
    DateFirstStrategy,
    HashBucketStrategy,
    PartitionStrategy,
    ReversedIdStrategy,
)
from streaming.spark_config import get_spark_session  # noqa: E402


# ── Synthetic data generation ───────────────────────────────────────────


def _device_id_for(i: int) -> str:
    """Deterministic per-index device_id matching the producer regex
    ``^[A-Z]{2}-[A-Z0-9]{3}-\\d{5}$``.

    Format: ``<family>-<bucket-letter><digit>-<5-digit-serial>``.
    e.g., index 0 → ``WT-A00-00000``, index 1 → ``CS-A00-00001``, …
    """
    families = ["WT", "CS", "SR"]  # smartwatch, chest_strap, sleep_ring
    family = families[i % len(families)]
    # bucket-letter rotates A..Z then 0..9 for higher cardinality
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    bucket = alphabet[(i // len(families)) % len(alphabet)]
    return f"{family}-{bucket}{i % 10:02d}-{i:05d}"


def _generate_dataset(
    spark: SparkSession,
    records: int,
    devices: int,
    base_date: str = "2026-05-10",
) -> DataFrame:
    """Generate ``records`` synthetic sensor rows across ``devices`` device_ids.

    Each row has a ``decoded`` struct that mirrors the bronze schema so
    the partition strategies can apply ``add_partition_columns`` directly.

    The dataset spans 1 calendar day so the ``date_first`` strategy
    produces exactly one ``dt=`` partition — the worst case for
    midnight thundering-herd.
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    base = spark.range(records).withColumnRenamed("id", "row_idx")
    return base.select(
        F.col("row_idx"),
        F.struct(
            F.format_string("rdg-%010d", F.col("row_idx")).alias("reading_id"),
            # Round-robin across `devices` device_ids — each device gets
            # ``records / devices`` rows.
            F.format_string(
                "%s",
                F.expr(
                    "case "
                    + " ".join(
                        f"when row_idx % {devices} = {i} then '{_device_id_for(i)}'"
                        for i in range(min(devices, 200))  # cap inline cases for SQL size
                    )
                    + " else concat('WT-A00-', lpad(cast(row_idx % "
                    + str(devices)
                    + " as string), 5, '0')) end"
                ),
            ).alias("device_id"),
            F.lit("smartwatch").alias("device_type"),
        ).alias("decoded"),
        F.lit(base_date).cast("timestamp").alias("ingestion_timestamp"),
    )


# ── Per-strategy run ────────────────────────────────────────────────────


@dataclass
class BenchmarkRun:
    """One write of one strategy."""

    strategy: str
    records: int
    devices: int
    output_path: str
    wall_clock_seconds: float
    files_written: int
    distinct_partition_dirs: int
    bytes_written: Optional[int] = None
    cloudwatch_5xx: Optional[int] = None
    cloudwatch_put_requests: Optional[int] = None
    notes: list[str] = field(default_factory=list)


def _run_one_strategy(
    spark: SparkSession,
    df: DataFrame,
    strategy: PartitionStrategy,
    output_path: str,
    records: int,
    devices: int,
) -> BenchmarkRun:
    """Apply the strategy to ``df``, write to ``output_path``, return metrics."""
    print(f"\n=== strategy: {strategy.name} ===", flush=True)
    print(f"    output: {output_path}", flush=True)
    print(f"    layout: {strategy.s3_path_pattern()}", flush=True)
    partitioned = strategy.add_partition_columns(df)
    # Materialize partition cols + cache so the wall-clock measurement
    # captures S3 write latency, not the partition computation.
    partitioned = partitioned.persist()
    _ = partitioned.count()  # trigger partition column materialization
    cols = list(strategy.partition_columns)
    started = time.time()
    (
        partitioned.write.mode("overwrite")
        .partitionBy(*cols)
        .parquet(output_path)
    )
    elapsed = time.time() - started
    partitioned.unpersist()

    # File-count + partition-dir-count via Hadoop FileSystem API.
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(output_path), hadoop_conf
    )
    files = 0
    parts = 0
    bytes_total = 0
    stack = [sc._jvm.org.apache.hadoop.fs.Path(output_path)]
    while stack:
        p = stack.pop()
        try:
            for item in fs.listStatus(p):
                if item.isDirectory():
                    stack.append(item.getPath())
                else:
                    name = item.getPath().getName()
                    if name.endswith(".parquet"):
                        files += 1
                        bytes_total += item.getLen()
                    if "=" in name:
                        # listing the partition directory itself
                        parts += 1
        except Exception:  # noqa: BLE001 — directory missing is fine
            pass
    # Count distinct partition directories (one per leaf partition).
    parts = max(parts, _count_partition_leaves(fs, output_path, sc))

    print(
        f"    wall-clock: {elapsed:.2f}s | files: {files} | "
        f"partition-dirs: {parts} | bytes: {bytes_total:,}",
        flush=True,
    )
    return BenchmarkRun(
        strategy=strategy.name,
        records=records,
        devices=devices,
        output_path=output_path,
        wall_clock_seconds=round(elapsed, 3),
        files_written=files,
        distinct_partition_dirs=parts,
        bytes_written=bytes_total,
    )


def _count_partition_leaves(fs, root_uri: str, sc) -> int:
    """Count leaf partition directories (those holding parquet files)."""
    Path = sc._jvm.org.apache.hadoop.fs.Path
    try:
        leaves = 0
        stack = [Path(root_uri)]
        while stack:
            p = stack.pop()
            children = fs.listStatus(p)
            has_subdirs = any(c.isDirectory() for c in children)
            if not has_subdirs:
                leaves += 1
                continue
            for c in children:
                if c.isDirectory():
                    stack.append(c.getPath())
        return leaves
    except Exception:
        return 0


# ── CloudWatch S3 metrics polling ───────────────────────────────────────


def _poll_cloudwatch_metrics(
    bucket: str,
    region: str,
    started_at: datetime,
    ended_at: datetime,
    filter_id: Optional[str] = None,
) -> dict:
    """Pull AWS/S3 request metrics for the run window.

    Returns a dict with ``put_requests`` and ``5xx_errors`` summed over
    the time window. Returns NaN-equivalent (None) on failure since
    CloudWatch propagation is not deterministic.
    """
    try:
        import boto3
    except ImportError:
        return {"put_requests": None, "5xx_errors": None, "note": "boto3 unavailable"}

    cw = boto3.client("cloudwatch", region_name=region)
    dimensions = [{"Name": "BucketName", "Value": bucket}]
    if filter_id:
        dimensions.append({"Name": "FilterId", "Value": filter_id})

    def _get(metric: str) -> Optional[int]:
        try:
            resp = cw.get_metric_statistics(
                Namespace="AWS/S3",
                MetricName=metric,
                Dimensions=dimensions,
                StartTime=started_at,
                EndTime=ended_at,
                Period=60,
                Statistics=["Sum"],
            )
            return int(sum(p["Sum"] for p in resp["Datapoints"]))
        except Exception:  # noqa: BLE001
            return None

    return {
        "put_requests": _get("PutRequests"),
        "5xx_errors": _get("5xxErrors"),
        "all_requests": _get("AllRequests"),
        "first_byte_latency_avg_ms": _get("FirstByteLatency"),
    }


# ── Markdown summary ────────────────────────────────────────────────────


def _markdown_table(runs: list[BenchmarkRun]) -> str:
    """Emit a markdown table comparing strategies."""
    lines = [
        "| Strategy | Wall-Clock (s) | Files | Partition Dirs | Bytes | "
        "S3 PUT Requests | 5xx Errors | Notes |",
        "|----------|---------------:|------:|---------------:|------:|"
        "----------------:|-----------:|-------|",
    ]
    for r in runs:
        notes = "; ".join(r.notes) if r.notes else ""
        put = (
            "—" if r.cloudwatch_put_requests is None else f"{r.cloudwatch_put_requests:,}"
        )
        err = "—" if r.cloudwatch_5xx is None else f"{r.cloudwatch_5xx:,}"
        bytes_str = "—" if r.bytes_written is None else f"{r.bytes_written:,}"
        lines.append(
            f"| `{r.strategy}` | {r.wall_clock_seconds:,.2f} | "
            f"{r.files_written:,} | {r.distinct_partition_dirs:,} | "
            f"{bytes_str} | {put} | {err} | {notes} |"
        )
    return "\n".join(lines)


# ── Main ────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Real-S3 benchmark of partition strategies.",
    )
    parser.add_argument("--records", type=int, default=1_000_000)
    parser.add_argument("--devices", type=int, default=1_000)
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name (no s3:// prefix). Must have CloudWatch RequestMetrics enabled.",
    )
    parser.add_argument(
        "--output-prefix",
        default="benchmarks/s3-partition",
        help="Prefix under the bucket where each strategy gets its own subdir.",
    )
    parser.add_argument(
        "--strategies",
        nargs="+",
        default=["date_first", "reversed_id", "hash_bucket"],
        choices=["date_first", "reversed_id", "hash_bucket"],
    )
    parser.add_argument(
        "--thundering-herd-records",
        type=int,
        default=0,
        help=(
            "If > 0, run a second pass writing this many records all in "
            "the same calendar day to amplify date-first's hot-prefix "
            "collision. 500_000 is a useful default."
        ),
    )
    parser.add_argument(
        "--cloudwatch-poll",
        action="store_true",
        help=(
            "Wait 18 minutes after writes complete and poll CloudWatch "
            "for S3 RequestMetrics. Propagation lag is ~15 min."
        ),
    )
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    args = parser.parse_args()

    spark = get_spark_session("PulseTrack-S3-Partition-Benchmark")

    print(f"\n[bench] generating {args.records:,} records across "
          f"{args.devices:,} devices...", flush=True)
    df = _generate_dataset(spark, records=args.records, devices=args.devices)
    df = df.persist()
    print(f"[bench] dataset materialized: {df.count():,} rows", flush=True)

    strategies: dict[str, PartitionStrategy] = {
        "date_first": DateFirstStrategy(),
        "reversed_id": ReversedIdStrategy(),
        "hash_bucket": HashBucketStrategy(),
    }

    started_at = datetime.now(timezone.utc)
    runs: list[BenchmarkRun] = []
    for name in args.strategies:
        out = f"s3://{args.bucket}/{args.output_prefix}/{name}/"
        run = _run_one_strategy(
            spark, df, strategies[name], out,
            records=args.records, devices=args.devices,
        )
        runs.append(run)

    # ── Optional thundering-herd pass ───────────────────────────────────
    # All records share a single calendar day to amplify date-first's
    # hot-prefix collision. Reversed-id should remain ~flat.
    if args.thundering_herd_records > 0:
        print(
            f"\n[bench] thundering-herd pass: "
            f"{args.thundering_herd_records:,} records in 1 calendar day",
            flush=True,
        )
        herd_df = _generate_dataset(
            spark,
            records=args.thundering_herd_records,
            devices=args.devices,
            base_date="2026-05-11",  # different day, same midnight collision
        ).persist()
        _ = herd_df.count()
        for name in args.strategies:
            out = f"s3://{args.bucket}/{args.output_prefix}/herd-{name}/"
            run = _run_one_strategy(
                spark, herd_df, strategies[name], out,
                records=args.thundering_herd_records, devices=args.devices,
            )
            run.notes.append("thundering-herd: all records on 1 day")
            runs.append(run)
        herd_df.unpersist()

    df.unpersist()
    ended_at = datetime.now(timezone.utc)

    # ── CloudWatch ──────────────────────────────────────────────────────
    if args.cloudwatch_poll:
        wait_s = 18 * 60
        print(f"\n[bench] sleeping {wait_s}s for CloudWatch propagation...", flush=True)
        time.sleep(wait_s)
        for r in runs:
            cw = _poll_cloudwatch_metrics(
                bucket=args.bucket,
                region=args.region,
                started_at=started_at - timedelta(minutes=1),
                ended_at=ended_at + timedelta(minutes=20),
            )
            r.cloudwatch_put_requests = cw.get("put_requests")
            r.cloudwatch_5xx = cw.get("5xx_errors")

    # ── Output ─────────────────────────────────────────────────────────
    md = _markdown_table(runs)
    print("\n" + "=" * 78)
    print("BENCHMARK RESULTS")
    print("=" * 78)
    print(md)
    print()

    out_dir = "/tmp"
    json_out = os.path.join(out_dir, "s3_partition_benchmark_results.json")
    md_out = os.path.join(out_dir, "s3_partition_benchmark_results.md")
    with open(json_out, "w") as f:
        json.dump(
            {
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "records": args.records,
                "devices": args.devices,
                "thundering_herd_records": args.thundering_herd_records,
                "runs": [asdict(r) for r in runs],
            },
            f,
            indent=2,
        )
    with open(md_out, "w") as f:
        f.write("# S3 Partition Benchmark — Real-S3 Results\n\n")
        f.write(f"- Started: {started_at.isoformat()}\n")
        f.write(f"- Ended:   {ended_at.isoformat()}\n")
        f.write(f"- Records: {args.records:,} primary + "
                f"{args.thundering_herd_records:,} herd\n")
        f.write(f"- Devices: {args.devices:,}\n\n")
        f.write(md + "\n")
    print(f"[bench] wrote {json_out}")
    print(f"[bench] wrote {md_out}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
