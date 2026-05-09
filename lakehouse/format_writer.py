"""
PulseTrack — table-format abstraction layer.

Why this exists:
    The medallion writes have to support both Delta Lake (legacy / local dev)
    and Apache Iceberg via Glue Catalog (cloud production, queryable from
    Athena and other engines). Without an abstraction the same MERGE / append /
    OPTIMIZE logic gets copy-pasted with minor format-specific tweaks across
    20+ transforms. ``FormatWriter`` collapses both behind one API.

Design choices:
    * **Path vs. table identity.** Delta tables are identified by a path
      (``s3://bucket/silver/sensor_readings``). Iceberg tables are identified
      by a fully-qualified name (``glue_iceberg.pulsetrack_silver_dev.sensor_readings``).
      ``FormatWriter`` takes both at construction so callers can stay format-blind.
    * **MERGE semantics.** Delta uses the Java DeltaTable Python API; Iceberg
      uses Spark SQL (``MERGE INTO`` is part of Iceberg's SQL extensions).
      Same logical operation, different invocation.
    * **Hidden vs. explicit partitioning.** Iceberg supports hidden partition
      transforms (``days(ts)``, ``bucket(16, k)``, ``truncate(10, s)``) — the
      partition columns are derived, not stored. Delta needs the columns
      materialized before write. ``create_table`` accepts an Iceberg-style
      ``partition_transforms=`` and translates to Delta's flat partitioning
      list when format=delta.
    * **Optimize / vacuum.** Delta has ``OPTIMIZE`` and ``VACUUM``; Iceberg
      has ``rewrite_data_files`` and ``expire_snapshots`` + ``remove_orphan_files``.
      The methods are namespaced the Delta way because that's how operators
      tend to think about table maintenance — but they dispatch correctly.

This module deliberately does NOT manage SparkSession. Callers pass a
configured session in. The Iceberg path assumes the session has the
``glue_iceberg`` catalog wired up via ``spark.sql.catalog.glue_iceberg=...``
(see ``streaming/spark_config.py``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


SUPPORTED_FORMATS = ("delta", "iceberg")


@dataclass(frozen=True)
class TableIdentity:
    """How to address a table in either format.

    For Delta, only ``path`` is required.
    For Iceberg, only ``catalog``, ``database``, ``table`` are required —
    ``path`` is informational (Iceberg manages the storage layout itself).
    """

    path: Optional[str] = None
    catalog: Optional[str] = None
    database: Optional[str] = None
    table: Optional[str] = None

    @property
    def fqn(self) -> str:
        """Iceberg fully-qualified name."""
        if not (self.catalog and self.database and self.table):
            raise ValueError(
                f"TableIdentity missing FQN parts (catalog/database/table): {self}"
            )
        return f"{self.catalog}.{self.database}.{self.table}"


@dataclass(frozen=True)
class IcebergWriteProperties:
    """Iceberg TBLPROPERTIES applied at CREATE TABLE time.

    Defaults are PulseTrack production-leaning:
      * 128 MiB target file size — good balance for S3 GET latency vs.
        per-file overhead, and aligned with the EMR Spark default block.
      * zstd Parquet compression — ~30% smaller than snappy at similar
        decode cost.
      * Hash-distributed writes — reduces small-file proliferation when
        writing partitioned tables from a wide source.
      * Format v2 — required for row-level deletes (MERGE semantics).
    """

    target_file_size_bytes: int = 134_217_728
    parquet_compression: str = "zstd"
    distribution_mode: str = "hash"
    format_version: str = "2"

    def as_tblproperties(self) -> str:
        items = {
            "write.target-file-size-bytes": str(self.target_file_size_bytes),
            "write.parquet.compression-codec": self.parquet_compression,
            "write.distribution-mode": self.distribution_mode,
            "format-version": self.format_version,
        }
        return ", ".join(f"'{k}'='{v}'" for k, v in items.items())


@dataclass
class FormatWriter:
    """Format-agnostic writer.

    Build once per (table, format), reuse across calls. Stateless beyond the
    constructor args — safe to share across foreachBatch closures.
    """

    spark: SparkSession
    identity: TableIdentity
    fmt: str = "delta"

    def __post_init__(self) -> None:
        if self.fmt not in SUPPORTED_FORMATS:
            raise ValueError(
                f"unsupported format {self.fmt!r}; expected one of {SUPPORTED_FORMATS}"
            )
        if self.fmt == "delta" and not self.identity.path:
            raise ValueError("Delta writer requires identity.path")
        if self.fmt == "iceberg":
            # FQN raises if any part is missing
            _ = self.identity.fqn

    # ── DDL ────────────────────────────────────────────────────────────────

    def create_table(
        self,
        schema_ddl: str,
        partition_transforms: Optional[list[str]] = None,
        partition_columns: Optional[list[str]] = None,
        properties: Optional[IcebergWriteProperties] = None,
        sort_order: Optional[list[str]] = None,
    ) -> None:
        """Idempotent CREATE TABLE.

        Args:
            schema_ddl: e.g. "id BIGINT, ts TIMESTAMP, value DOUBLE".
            partition_transforms: Iceberg-style transforms,
                e.g. ["days(event_timestamp)", "bucket(16, patient_key)"].
                Delta uses ``partition_columns`` instead — pass both if you want
                this writer to work in either mode.
            partition_columns: Delta-style flat list, e.g. ["ingestion_date", "ingestion_hour"].
            properties: Iceberg TBLPROPERTIES (no-op for Delta).
            sort_order: Iceberg WRITE ORDERED BY columns (no-op for Delta —
                Delta uses ZORDER applied at OPTIMIZE time, see ``optimize``).
        """
        if self.fmt == "iceberg":
            props = properties or IcebergWriteProperties()
            partition_clause = (
                f"PARTITIONED BY ({', '.join(partition_transforms)})"
                if partition_transforms
                else ""
            )
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {self.identity.fqn} "
                f"({schema_ddl}) USING iceberg "
                f"{partition_clause} "
                f"TBLPROPERTIES ({props.as_tblproperties()})"
            )
            self.spark.sql(ddl)
            if sort_order:
                self.spark.sql(
                    f"ALTER TABLE {self.identity.fqn} "
                    f"WRITE ORDERED BY ({', '.join(sort_order)})"
                )
        else:
            # Delta: tables are typically created lazily on first write.
            # We honor an explicit DDL request by emitting a CREATE TABLE
            # with LOCATION pointing at the same path the appends/merges use.
            # This is safe — Delta makes path-based and table-based addressing
            # interchangeable when LOCATION matches.
            partition_clause = (
                f"PARTITIONED BY ({', '.join(partition_columns)})"
                if partition_columns
                else ""
            )
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS delta.`{self.identity.path}` "
                f"({schema_ddl}) USING delta {partition_clause} "
                f"LOCATION '{self.identity.path}'"
            )

    # ── DML ────────────────────────────────────────────────────────────────

    def append(
        self,
        df: DataFrame,
        partition_columns: Optional[list[str]] = None,
    ) -> None:
        """Append-only write. partition_columns only used for Delta."""
        if self.fmt == "iceberg":
            (
                df.writeTo(self.identity.fqn).append()
            )
        else:
            writer = df.write.format("delta").mode("append")
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            writer.option("mergeSchema", "true").save(self.identity.path)

    def overwrite(
        self,
        df: DataFrame,
        partition_columns: Optional[list[str]] = None,
    ) -> None:
        """Full overwrite — used by static dimensions (dim_date, dim_metric)."""
        if self.fmt == "iceberg":
            df.writeTo(self.identity.fqn).createOrReplace()
        else:
            writer = df.write.format("delta").mode("overwrite")
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            writer.option("overwriteSchema", "true").save(self.identity.path)

    def merge(
        self,
        source_df: DataFrame,
        match_condition: str,
        update_set: Optional[dict[str, str]] = None,
        insert_values: Optional[dict[str, str]] = None,
        target_alias: str = "t",
        source_alias: str = "s",
        with_update: bool = True,
        with_insert: bool = True,
    ) -> None:
        """MERGE INTO — upsert by ``match_condition``.

        Args:
            source_df: incoming rows.
            match_condition: SQL fragment using ``target_alias`` and
                ``source_alias``, e.g. "t.reading_id = s.reading_id".
            update_set: ``{target_col: source_expr}`` for matched rows.
                Default: update all source columns.
            insert_values: ``{target_col: source_expr}`` for unmatched rows.
                Default: insert all source columns.
            with_update: include the ``WHEN MATCHED THEN UPDATE`` branch.
                Set to ``False`` for insert-only patterns (e.g., append-only
                fact tables that dedup by primary key).
            with_insert: include the ``WHEN NOT MATCHED THEN INSERT`` branch.
                Set to ``False`` for update-only patterns (e.g., SCD2
                expire-current step that only modifies existing rows).
        """
        if not with_update and not with_insert:
            raise ValueError("merge() with both branches off is a no-op")
        if self.fmt == "iceberg":
            # Iceberg MERGE INTO via Spark SQL extensions.
            #
            # IMPORTANT: when called from inside foreachBatch, the DataFrame's
            # sparkSession can be a sub-session distinct from the
            # constructor-time ``self.spark``. Temp views are
            # session-scoped — register and query MUST run on the same
            # session, so we use ``source_df.sparkSession`` for both.
            spark = source_df.sparkSession

            # First-write fallback: if the target Iceberg table doesn't exist
            # (no migration created it), create+populate from source. Mirrors
            # Delta's "first append creates the table" behavior so callers
            # don't have to special-case the cold-start path.
            #
            # The catalog raises ``AnalysisException`` for missing tables in
            # Spark 3.5; older paths and Iceberg-specific code can also raise
            # ``NoSuchTableException``. Both bubble up through pyspark as
            # ``AnalysisException``. Anything else (auth failure, network)
            # should propagate — those aren't "table doesn't exist".
            from pyspark.errors.exceptions.captured import AnalysisException

            try:
                spark.read.table(self.identity.fqn).limit(0).collect()
                table_exists = True
            except AnalysisException as exc:
                # AnalysisException covers TABLE_OR_VIEW_NOT_FOUND and the
                # NoSuchTableException pyspark wraps. Match on text rather
                # than error class because the SQLSTATE varies across
                # catalog implementations.
                msg = str(exc).lower()
                if "not found" in msg or "no such table" in msg or "cannot be found" in msg:
                    table_exists = False
                else:
                    raise
            if not table_exists:
                source_df.writeTo(self.identity.fqn).using("iceberg").create()
                return

            tmp = f"_pt_merge_src_{abs(hash(self.identity.fqn)) % 10**8}"
            source_df.createOrReplaceTempView(tmp)

            # Default to ``UPDATE SET *`` / ``INSERT *`` — Iceberg's MERGE
            # semantics: match source columns to target columns by name. This
            # is more robust than enumerating the column list because we can't
            # know in this layer whether the source has every target column
            # (e.g., when target gained a nullable column via V002).
            clauses: list[str] = []
            if with_update:
                if update_set:
                    update_clause = ", ".join(f"{k} = {v}" for k, v in update_set.items())
                    clauses.append(f"WHEN MATCHED THEN UPDATE SET {update_clause}")
                else:
                    clauses.append("WHEN MATCHED THEN UPDATE SET *")
            if with_insert:
                if insert_values:
                    insert_cols = ", ".join(insert_values.keys())
                    insert_vals = ", ".join(insert_values.values())
                    clauses.append(f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})")
                else:
                    clauses.append("WHEN NOT MATCHED THEN INSERT *")

            sql = (
                f"MERGE INTO {self.identity.fqn} AS {target_alias} "
                f"USING {tmp} AS {source_alias} "
                f"ON {match_condition} "
                + " ".join(clauses)
            )
            try:
                spark.sql(sql)
            finally:
                spark.catalog.dropTempView(tmp)
        else:
            # Delta path-based MERGE via the DeltaTable Python API.
            # First write to a non-existent path can't MERGE — fall back to
            # append, which materializes the Delta log and lets subsequent
            # batches MERGE normally.
            from delta.tables import DeltaTable  # local import — Delta jar may not be on classpath in iceberg-only runs

            if not DeltaTable.isDeltaTable(self.spark, self.identity.path):
                source_df.write.format("delta").save(self.identity.path)
                return

            tgt = DeltaTable.forPath(self.spark, self.identity.path)
            builder = (
                tgt.alias(target_alias)
                .merge(source_df.alias(source_alias), match_condition)
            )
            if with_update:
                if update_set:
                    builder = builder.whenMatchedUpdate(set=update_set)
                else:
                    builder = builder.whenMatchedUpdateAll()
            if with_insert:
                if insert_values:
                    builder = builder.whenNotMatchedInsert(values=insert_values)
                else:
                    builder = builder.whenNotMatchedInsertAll()
            builder.execute()

    # ── Maintenance ────────────────────────────────────────────────────────

    def optimize(self, zorder_cols: Optional[list[str]] = None) -> None:
        """Compact small files; ZORDER (Delta) or rewrite + sort (Iceberg)."""
        if self.fmt == "iceberg":
            # rewrite_data_files compacts; the table-level WRITE ORDERED BY
            # set in create_table drives sort order.
            self.spark.sql(
                f"CALL {self.identity.catalog}.system.rewrite_data_files("
                f"table => '{self.identity.database}.{self.identity.table}')"
            )
        else:
            base = f"OPTIMIZE delta.`{self.identity.path}`"
            if zorder_cols:
                base = f"{base} ZORDER BY ({', '.join(zorder_cols)})"
            self.spark.sql(base)

    def vacuum(self, retention_hours: int = 168) -> None:
        """Reclaim storage from old snapshots / files."""
        if self.fmt == "iceberg":
            # Expire snapshots older than retention_hours and clean orphan files.
            self.spark.sql(
                f"CALL {self.identity.catalog}.system.expire_snapshots("
                f"table => '{self.identity.database}.{self.identity.table}', "
                f"older_than => TIMESTAMP '"
                + self.spark.sql(
                    f"SELECT current_timestamp() - INTERVAL {retention_hours} HOURS"
                ).collect()[0][0].strftime("%Y-%m-%d %H:%M:%S")
                + "')"
            )
            self.spark.sql(
                f"CALL {self.identity.catalog}.system.remove_orphan_files("
                f"table => '{self.identity.database}.{self.identity.table}')"
            )
        else:
            self.spark.sql(
                f"VACUUM delta.`{self.identity.path}` RETAIN {retention_hours} HOURS"
            )

    # ── Read-side helpers ──────────────────────────────────────────────────

    def read_batch(self) -> DataFrame:
        """Format-agnostic batch read of the whole table."""
        if self.fmt == "iceberg":
            return self.spark.read.table(self.identity.fqn)
        return self.spark.read.format("delta").load(self.identity.path)

    def read_stream(self, options: Optional[dict[str, str]] = None) -> DataFrame:
        """Format-agnostic streaming read.

        Iceberg: requires Spark 3.5 + Iceberg 1.5+ (which EMR 7.2 has).
        Use ``stream-from-timestamp`` and ``streaming-skip-overwrite-snapshots``
        if the caller passes them in ``options``.
        """
        opts = options or {}
        if self.fmt == "iceberg":
            reader = self.spark.readStream.format("iceberg")
            for k, v in opts.items():
                reader = reader.option(k, v)
            return reader.load(self.identity.fqn)
        reader = self.spark.readStream.format("delta")
        for k, v in opts.items():
            reader = reader.option(k, v)
        return reader.load(self.identity.path)
