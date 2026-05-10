"""
S3 partition strategies — implements WHOOP's reversed-ID approach.

Three strategies benchmarked on REAL S3:

1. ``date_first``  — ``s3://bucket/dt=2026-05-06/device_id=SW-A1B-12345/<file>.parquet``
   Hot prefix at midnight: ALL writes for the new date target the same
   ``dt=`` prefix. S3's per-prefix request-rate limit (3500 PUT/s) becomes
   a global write bottleneck for the entire fleet at the date rollover.
   Operational hazard: bursts of 503 SlowDown errors and exponential
   client-side backoff cascade across the streaming pipeline.

2. ``reversed_id`` — ``s3://bucket/rid=54321-B1A-TW/dt=2026-05-06/<file>.parquet``
   Reverse the device_id so leading characters become trailing. Different
   devices land in completely different prefixes — S3's prefix-level
   request quota is no longer global. Crucially the prefix stays
   human-readable: an operator can reverse a known device_id and grep
   for it during incidents.

3. ``hash_bucket`` — ``s3://bucket/hb=042/dt=2026-05-06/<file>.parquet``
   Compute ``crc32(device_id) % N`` and bucket. Distribution is uniform
   by construction (no Benford skew), but bucket numbers are opaque —
   you can't grep S3 paths to find a specific device without first
   running a Spark job to compute its hash.

WHOOP chose (2) because it matches (3) on write distribution while
preserving (1)'s grep-ability. PulseTrack inherits the same choice.

References:
  * WHOOP engineering — Optimizing S3 Partition Strategy:
    https://www.whoop.com/engineering/blog/s3-partition-strategy
  * AWS S3 Performance — request rates per prefix:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
  * Benford's Law on leading-digit distribution:
    sequential device_ids skew toward leading-digit '1' (~30%); this
    forces hot-spot collisions in date-first layouts where the ID
    appears as the second partition column.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Default bucket count for hash strategy. 256 gives a good distribution
# with a small alphabet (000..255) — small enough to keep partition
# directory listings manageable, large enough to spread S3 PUT load.
DEFAULT_HASH_BUCKETS = 256


@runtime_checkable
class PartitionStrategy(Protocol):
    """Common interface for S3 partition strategies.

    Each strategy:
      * exposes a stable ``name`` for benchmarking + telemetry
      * lists the columns it partitions by (used as ``partitionBy(...)``
        for parquet writes and as Iceberg partition-spec evolution)
      * adds the partition columns to a DataFrame via
        :meth:`add_partition_columns` so the writer doesn't need to know
        which strategy is in play
      * documents the resulting S3 path layout for the operator
    """

    name: str
    partition_columns: tuple[str, ...]

    def add_partition_columns(self, df: DataFrame) -> DataFrame:
        """Add the strategy-specific partition columns to ``df``.

        Idempotent — calling twice is a no-op for the second call (the
        columns are overwritten with the same values).
        """
        ...

    def s3_path_pattern(self) -> str:
        """Return a one-line example of where data lands on S3."""
        ...


# ── Strategy implementations ─────────────────────────────────────────────


@dataclass(frozen=True)
class DateFirstStrategy:
    """Date-first partitioning — the naïve default.

    Layout:
        s3://<bucket>/<table>/dt=2026-05-06/device_id=SW-A1B-12345/<file>.parquet

    Why it's the default: chronological browsing is intuitive; partition
    pruning by date is fast.

    Why it breaks at scale: every wearable in the fleet writes to the
    same ``dt=`` prefix on the same calendar day. S3 limits PUT requests
    to ~3500/s per prefix. A 100k-device fleet streaming hourly summaries
    can saturate this — and at midnight, ALL devices simultaneously
    transition to the new ``dt=`` prefix, producing a thundering herd
    that cascades 503 SlowDown errors through the pipeline.

    Use this strategy ONLY for: (a) low-volume audit logs, (b) tables
    where chronological access dominates and write rate is < 1k/s, or
    (c) the first iteration of a new pipeline before scale is proven.
    """

    name: str = "date_first"
    partition_columns: tuple[str, ...] = ("dt", "device_id")

    def add_partition_columns(self, df: DataFrame) -> DataFrame:
        """Compute ``dt`` from ``ingestion_timestamp`` + project
        ``device_id`` from ``decoded.device_id`` if not already top-level.

        Spark's ``partitionBy`` requires partition columns at top level.
        Bronze's wire format keeps device_id inside the ``decoded`` struct
        (preserves the Avro envelope shape); we project it out here.
        """
        out = df.withColumn("dt", F.to_date("ingestion_timestamp"))
        if "device_id" not in df.columns:
            out = out.withColumn("device_id", F.col("decoded.device_id"))
        return out

    def s3_path_pattern(self) -> str:
        return (
            "s3://<bucket>/<table>/"
            "dt=2026-05-06/device_id=SW-A1B-12345/<file>.parquet"
        )


@dataclass(frozen=True)
class ReversedIdStrategy:
    """Reversed-ID-first partitioning — WHOOP's chosen strategy.

    Layout:
        s3://<bucket>/<table>/rid=54321-B1A-TW/dt=2026-05-06/<file>.parquet

    The transform is :func:`pyspark.sql.functions.reverse` on
    ``decoded.device_id``. ``WT-A0F-12345`` → ``54321-F0A-TW``. Reversing
    a structured ID:

      * Spreads writes across S3 prefixes — S3 hashes prefixes for rate
        limiting, so reversed IDs hash to different rate-limit buckets.
      * Survives Benford's-Law skew — natural-order device_ids cluster
        on leading character; reversed IDs put the variable suffix
        (the per-device serial) in the high-entropy slot.
      * Stays grep-able — given a device_id from an incident report, an
        operator can compute ``device_id[::-1]`` mentally and find the
        S3 prefix without a Spark job.

    Tradeoff vs. ``date_first``: chronological browsing requires
    knowing the device_id first. For PulseTrack this is fine — most
    downstream queries filter by device anyway (per-patient analytics),
    and Iceberg's metadata layer makes date-range scans efficient
    even when date isn't the leading partition column.
    """

    name: str = "reversed_id"
    partition_columns: tuple[str, ...] = ("rid", "dt")

    def add_partition_columns(self, df: DataFrame) -> DataFrame:
        """Compute ``rid = reverse(decoded.device_id)`` and ``dt``.

        Falls back to reversing top-level ``device_id`` if the decoded
        struct isn't present (e.g., when running over already-flattened
        silver/gold rows in tests).
        """
        device_col = (
            F.col("decoded.device_id")
            if "decoded" in df.columns
            else F.col("device_id")
        )
        return (
            df.withColumn("rid", F.reverse(device_col))
            .withColumn("dt", F.to_date("ingestion_timestamp"))
        )

    def s3_path_pattern(self) -> str:
        return (
            "s3://<bucket>/<table>/"
            "rid=54321-F0A-TW/dt=2026-05-06/<file>.parquet"
        )


@dataclass(frozen=True)
class HashBucketStrategy:
    """Hash-bucket partitioning — uniform distribution, opaque keys.

    Layout:
        s3://<bucket>/<table>/hb=042/dt=2026-05-06/<file>.parquet

    Compute ``crc32(device_id) % num_buckets`` and zero-pad to 3 digits.
    Distribution is uniform by construction — no Benford skew is
    possible because the hash output is uniformly distributed over the
    bucket space.

    Why it's strictly worse than reversed-ID for our use-case: bucket
    numbers are opaque. During an incident, the operator cannot grep
    S3 paths to find a specific device's data — they must spin up a
    Spark job to compute ``crc32(device_id) % N``, look up the bucket,
    then grep. Adds 5-10 minutes to incident response.

    Reasons to choose hash-bucket anyway:
      * IDs lack inherent structure (random UUIDs already distribute
        well, but reversal doesn't help — the entire string is high
        entropy already).
      * Compliance forbids exposing IDs in S3 paths even within a
        controlled bucket (rare; reversed IDs are still encoded).
    """

    name: str = "hash_bucket"
    partition_columns: tuple[str, ...] = ("hb", "dt")
    num_buckets: int = DEFAULT_HASH_BUCKETS

    def add_partition_columns(self, df: DataFrame) -> DataFrame:
        """Compute ``hb`` and ``dt``. ``hb`` is zero-padded to 3 digits."""
        device_col = (
            F.col("decoded.device_id")
            if "decoded" in df.columns
            else F.col("device_id")
        )
        return (
            df.withColumn(
                "hb",
                F.format_string(
                    "%03d", F.crc32(device_col) % F.lit(self.num_buckets)
                ),
            )
            .withColumn("dt", F.to_date("ingestion_timestamp"))
        )

    def s3_path_pattern(self) -> str:
        return (
            f"s3://<bucket>/<table>/"
            f"hb=042/dt=2026-05-06/<file>.parquet "
            f"(hb in [000..{self.num_buckets - 1:03d}])"
        )


# ── Registry + factory ──────────────────────────────────────────────────


_STRATEGIES: dict[str, type[PartitionStrategy]] = {
    DateFirstStrategy().name: DateFirstStrategy,
    ReversedIdStrategy().name: ReversedIdStrategy,
    HashBucketStrategy().name: HashBucketStrategy,
}


def get_strategy(name: str, **kwargs) -> PartitionStrategy:
    """Look up a strategy by name. Extra kwargs forwarded to the dataclass.

    ``HashBucketStrategy`` supports ``num_buckets``; the others ignore extras.

    Raises:
        ValueError: unknown strategy name.
    """
    if name not in _STRATEGIES:
        raise ValueError(
            f"unknown partition strategy: {name!r}. "
            f"Available: {sorted(_STRATEGIES)}"
        )
    cls = _STRATEGIES[name]
    # Filter kwargs to only those the dataclass accepts.
    allowed = {f.name for f in cls.__dataclass_fields__.values()}
    return cls(**{k: v for k, v in kwargs.items() if k in allowed})


def list_strategies() -> list[str]:
    """Return the registered strategy names (for argparse choices)."""
    return sorted(_STRATEGIES)


# ── Iceberg partition-spec helpers ──────────────────────────────────────


def iceberg_partition_transforms(strategy: PartitionStrategy) -> list[str]:
    """Translate a strategy into Iceberg ``ADD PARTITION FIELD`` arguments.

    Iceberg accepts both identity transforms (column name verbatim) and
    function transforms like ``days(ingestion_timestamp)``. We always
    use ``days(ingestion_timestamp)`` for the date dimension instead of
    the materialized ``dt`` column so Iceberg can do hidden partition
    pruning — readers don't need to know about ``dt``.
    """
    if strategy.name == "date_first":
        # ``device_id`` is identity-partitioned; date is hidden via days().
        return ["days(ingestion_timestamp)", "device_id"]
    if strategy.name == "reversed_id":
        # ``rid`` is identity (we materialize it in the row); date hidden.
        return ["rid", "days(ingestion_timestamp)"]
    if strategy.name == "hash_bucket":
        # bucket() is a native Iceberg transform — better than our
        # materialized ``hb`` column because Iceberg manages the
        # buckets internally and rebalances on partition evolution.
        n = getattr(strategy, "num_buckets", DEFAULT_HASH_BUCKETS)
        return [f"bucket({n}, decoded.device_id)", "days(ingestion_timestamp)"]
    raise ValueError(f"no Iceberg transform mapping for strategy {strategy.name!r}")
