"""Unit tests for ``lakehouse.format_writer``.

The tests focus on the parts of FormatWriter that are pure Python — argument
validation, SQL generation, table-identity correctness — without spinning up a
real SparkSession. The Spark-driven paths (actual MERGE, append, create_table)
are validated by the integration runs that target the live cluster.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakehouse.format_writer import (
    FormatWriter,
    IcebergWriteProperties,
    TableIdentity,
)


class TestTableIdentity:
    def test_fqn_requires_all_three_parts(self) -> None:
        ti = TableIdentity(catalog="c", database="d", table="t")
        assert ti.fqn == "c.d.t"

    def test_fqn_raises_when_missing_part(self) -> None:
        ti = TableIdentity(catalog="c", database="d")  # no table
        with pytest.raises(ValueError, match="missing FQN"):
            _ = ti.fqn

    def test_path_only_is_valid_for_delta(self) -> None:
        ti = TableIdentity(path="s3://bucket/x")
        assert ti.path == "s3://bucket/x"
        # FQN access fails — that's by design
        with pytest.raises(ValueError):
            _ = ti.fqn


class TestIcebergWriteProperties:
    def test_defaults_match_v001_migration(self) -> None:
        # V001's TBLPROPERTIES set the same values; if these defaults drift,
        # the migration framework and the auto-create path will produce
        # tables with different file layouts.
        p = IcebergWriteProperties()
        clause = p.as_tblproperties()
        assert "'write.target-file-size-bytes'='134217728'" in clause
        assert "'write.parquet.compression-codec'='zstd'" in clause
        assert "'write.distribution-mode'='hash'" in clause
        assert "'format-version'='2'" in clause

    def test_custom_values_render_correctly(self) -> None:
        p = IcebergWriteProperties(
            target_file_size_bytes=64_000_000,
            parquet_compression="snappy",
            distribution_mode="none",
            format_version="2",
        )
        clause = p.as_tblproperties()
        assert "64000000" in clause
        assert "snappy" in clause
        assert "'write.distribution-mode'='none'" in clause


class TestFormatWriterConstruction:
    def test_unsupported_format_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported format"):
            FormatWriter(spark=MagicMock(), identity=TableIdentity(path="x"), fmt="parquet")

    def test_delta_requires_path(self) -> None:
        with pytest.raises(ValueError, match="path"):
            FormatWriter(
                spark=MagicMock(),
                identity=TableIdentity(catalog="c", database="d", table="t"),
                fmt="delta",
            )

    def test_iceberg_requires_fqn_parts(self) -> None:
        with pytest.raises(ValueError, match="missing FQN"):
            FormatWriter(
                spark=MagicMock(),
                identity=TableIdentity(path="s3://x"),  # no catalog/db/table
                fmt="iceberg",
            )

    def test_delta_with_full_identity_is_fine(self) -> None:
        # path-only is enough; supplying FQN parts as well doesn't break anything.
        w = FormatWriter(
            spark=MagicMock(),
            identity=TableIdentity(
                path="s3://x",
                catalog="c",
                database="d",
                table="t",
            ),
            fmt="delta",
        )
        assert w.fmt == "delta"
        assert w.identity.path == "s3://x"


class TestMergeArgValidation:
    def test_merge_with_both_branches_off_raises(self) -> None:
        w = FormatWriter(spark=MagicMock(), identity=TableIdentity(path="s3://x"), fmt="delta")
        with pytest.raises(ValueError, match="no-op"):
            w.merge(
                MagicMock(),
                match_condition="t.k = s.k",
                with_update=False,
                with_insert=False,
            )


class TestMakeWriterFor:
    def test_layer_routing(self) -> None:
        # Lazy import to keep pyspark out of the test stack.
        from lakehouse import make_writer_for

        # Stub settings just enough for the helper to look up the Glue DBs.
        import config

        # snapshot to restore
        original = (config.settings.iceberg_catalog,)
        try:
            config.settings.aws_env = "test"
            spark = MagicMock()
            for layer, expected_db in [
                ("bronze", "pulsetrack_bronze_test"),
                ("silver", "pulsetrack_silver_test"),
                ("gold", "pulsetrack_gold_test"),
            ]:
                w = make_writer_for(
                    spark, "iceberg", path="s3://x", table_name="t", layer=layer
                )
                assert w.identity.database == expected_db
                assert w.identity.catalog == config.settings.iceberg_catalog
                assert w.identity.table == "t"
        finally:
            (config.settings.iceberg_catalog,) = original

    def test_invalid_layer_keyerror(self) -> None:
        from lakehouse import make_writer_for

        with pytest.raises(KeyError):
            make_writer_for(
                MagicMock(), "iceberg", path="s3://x", table_name="t", layer="quartz"  # type: ignore[arg-type]
            )
