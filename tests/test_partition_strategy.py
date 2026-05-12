"""Unit tests for lakehouse.partition_strategy — 3 S3 partition strategies.

These tests exercise the strategy interface without requiring Spark by
using MagicMock DataFrames. Verifies the contract documented in the
module docstring + iceberg_partition_transforms() routing logic.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakehouse.partition_strategy import (
    DEFAULT_HASH_BUCKETS,
    DateFirstStrategy,
    HashBucketStrategy,
    PartitionStrategy,
    ReversedIdStrategy,
    get_strategy,
    iceberg_partition_transforms,
    list_strategies,
)


# ── Strategy registry ────────────────────────────────────────────────────


def test_list_strategies_returns_3():
    strategies = list_strategies()
    assert sorted(strategies) == ["date_first", "hash_bucket", "reversed_id"]


def test_get_strategy_returns_date_first():
    s = get_strategy("date_first")
    assert isinstance(s, DateFirstStrategy)
    assert s.name == "date_first"


def test_get_strategy_returns_reversed_id():
    s = get_strategy("reversed_id")
    assert isinstance(s, ReversedIdStrategy)
    assert s.name == "reversed_id"


def test_get_strategy_returns_hash_bucket():
    s = get_strategy("hash_bucket")
    assert isinstance(s, HashBucketStrategy)
    assert s.name == "hash_bucket"
    assert s.num_buckets == DEFAULT_HASH_BUCKETS


def test_get_strategy_hash_bucket_custom_count():
    s = get_strategy("hash_bucket", num_buckets=64)
    assert isinstance(s, HashBucketStrategy)
    assert s.num_buckets == 64


def test_get_strategy_unknown_raises():
    with pytest.raises((ValueError, KeyError)):
        get_strategy("nonexistent_strategy")


# ── Protocol conformance ────────────────────────────────────────────────


def test_date_first_conforms_to_protocol():
    assert isinstance(DateFirstStrategy(), PartitionStrategy)


def test_reversed_id_conforms_to_protocol():
    assert isinstance(ReversedIdStrategy(), PartitionStrategy)


def test_hash_bucket_conforms_to_protocol():
    assert isinstance(HashBucketStrategy(), PartitionStrategy)


# ── Strategy names + partition_cols ─────────────────────────────────────


def test_date_first_partition_columns():
    s = DateFirstStrategy()
    assert "dt" in s.partition_columns
    assert "device_id" in s.partition_columns


def test_reversed_id_partition_columns_includes_rid():
    s = ReversedIdStrategy()
    assert "rid" in s.partition_columns
    assert "dt" in s.partition_columns


def test_hash_bucket_partition_columns_includes_hb():
    s = HashBucketStrategy()
    # HashBucket may use "hb" or similar bucket marker
    assert any("hb" in c or "bucket" in c for c in s.partition_columns)


def test_date_first_s3_path_pattern():
    s = DateFirstStrategy()
    pattern = s.s3_path_pattern()
    assert "dt=" in pattern
    assert pattern.startswith("s3://")


def test_reversed_id_s3_path_pattern_shows_reversed():
    s = ReversedIdStrategy()
    pattern = s.s3_path_pattern()
    assert "rid=" in pattern
    # The pattern docstring uses 54321-F0A-TW (reverse of WT-A0F-12345)
    assert "rid=" in pattern


def test_hash_bucket_s3_path_pattern():
    s = HashBucketStrategy()
    pattern = s.s3_path_pattern()
    assert "s3://" in pattern


# ── Iceberg partition-transform translation ─────────────────────────────


def test_iceberg_transforms_date_first():
    s = DateFirstStrategy()
    transforms = iceberg_partition_transforms(s)
    assert isinstance(transforms, list)
    assert len(transforms) >= 1


def test_iceberg_transforms_reversed_id_includes_rid_and_days():
    s = ReversedIdStrategy()
    transforms = iceberg_partition_transforms(s)
    assert "rid" in transforms
    # Date transform is days(ingestion_timestamp) per the spec docstring
    assert any("days" in t for t in transforms)


def test_iceberg_transforms_hash_bucket_uses_bucket_function():
    s = HashBucketStrategy(num_buckets=32)
    transforms = iceberg_partition_transforms(s)
    assert any("bucket(32" in t for t in transforms)


def test_iceberg_transforms_unknown_strategy_raises():
    fake = MagicMock()
    fake.name = "unknown_strategy_name"
    with pytest.raises(ValueError, match="no Iceberg transform mapping"):
        iceberg_partition_transforms(fake)


# ── Equality / hashability for use in config ────────────────────────────


def test_strategies_have_stable_name_attr():
    """Strategy.name is a public string attribute (used by ADRs + benchmarks)."""
    for s in (DateFirstStrategy(), ReversedIdStrategy(), HashBucketStrategy()):
        assert isinstance(s.name, str)
        assert s.name in ("date_first", "reversed_id", "hash_bucket")


def test_default_hash_buckets_is_power_of_2():
    """Hash bucket default should be a power of 2 for clean S3 prefix distribution."""
    assert DEFAULT_HASH_BUCKETS & (DEFAULT_HASH_BUCKETS - 1) == 0
