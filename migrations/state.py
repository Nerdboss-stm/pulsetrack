"""
Migration state — read/write the ``schema_migrations`` ledger.

The state table location is configured per-catalog in the YAML config
(``state.table`` in ``migrations/catalogs/<name>.yaml``) and passed in
from the CLI. Two flavors:

  * Fully-qualified Iceberg name (``catalog.db.schema_migrations``) — used
    when the YAML's ``state.table`` contains dots. This is the production
    path on EMR.
  * Bare path (``s3://bucket/_migrations/schema_migrations``, or a local
    path) — Delta backend. Useful for local dev without an Iceberg catalog
    wired up.

The CLI bootstraps the SparkSession and hands it in. This module never
constructs Spark itself.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


# ── Schema ──────────────────────────────────────────────────────────────────

STATE_COLUMNS = [
    "version",
    "name",
    "sha256",
    "applied_at",
    "applied_by",
    "duration_ms",
    "rolled_back_at",
]

_DDL_COLUMNS = (
    "version STRING, "
    "name STRING, "
    "sha256 STRING, "
    "applied_at TIMESTAMP, "
    "applied_by STRING, "
    "duration_ms BIGINT, "
    "rolled_back_at TIMESTAMP"
)


@dataclass
class AppliedMigration:
    version: str
    name: str
    sha256: str
    applied_at: datetime | None
    applied_by: str | None
    duration_ms: int | None
    rolled_back_at: datetime | None


# ── Backend selection ───────────────────────────────────────────────────────


def _classify_state_table(state_table: str) -> tuple[bool, str | None, str | None]:
    """Decide whether ``state_table`` names an Iceberg table or a Delta path.

    Heuristic: if it contains dots (``catalog.db.table``) and no ``s3://``
    scheme, treat it as an Iceberg fully-qualified name. Otherwise, treat
    it as a path for the Delta backend.

    Returns ``(is_iceberg, iceberg_fqn, delta_path)`` — exactly one of the
    last two is set.
    """
    if "://" not in state_table and state_table.count(".") >= 2:
        return True, state_table, None
    if state_table.startswith("s3://") or state_table.startswith("/"):
        return False, None, state_table
    if state_table:
        return False, None, f"s3://{state_table}"
    return False, None, "/tmp/pulsetrack-migrations/schema_migrations"  # nosec B108


# ── Public API ──────────────────────────────────────────────────────────────


class MigrationState:
    """Manages the ``schema_migrations`` ledger.

    All methods are no-ops in ``dry_run=True`` mode — they read from the
    ledger if it exists but never write.

    Args:
        spark: SparkSession.
        state_table: Either an Iceberg FQN (``catalog.db.schema_migrations``)
            or a Delta path (``s3://...`` or local). When ``None`` (legacy
            path), falls back to ``ICEBERG_CATALOG`` / ``GLUE_DATABASE_GOLD``
            / ``LAKEHOUSE_BUCKET`` env vars for backwards compat with the
            pre-catalog-config CLI.
        dry_run: If true, all writes become no-ops.
    """

    def __init__(
        self,
        spark: "SparkSession",
        state_table: str | None = None,
        dry_run: bool = False,
    ) -> None:
        self.spark = spark
        self.dry_run = dry_run
        if state_table is not None:
            self._is_iceberg, self._table_ref, self._delta_path = _classify_state_table(
                state_table
            )
        else:
            # Backwards compat: derive from env vars (legacy pre-Glacierbase mode).
            if os.environ.get("ICEBERG_CATALOG"):
                self._is_iceberg = True
                self._table_ref = (
                    f"{os.environ['ICEBERG_CATALOG']}."
                    f"{os.environ['GLUE_DATABASE_GOLD']}."
                    f"schema_migrations"
                )
                self._delta_path = None
            else:
                bucket = os.environ.get("LAKEHOUSE_BUCKET", "")
                self._is_iceberg = False
                self._table_ref = None
                if bucket.startswith("s3://"):
                    self._delta_path = f"{bucket.rstrip('/')}/_migrations/schema_migrations"
                elif bucket:
                    self._delta_path = f"s3://{bucket}/_migrations/schema_migrations"
                else:
                    self._delta_path = "/tmp/pulsetrack-migrations/schema_migrations"  # nosec B108

    # ── Initialisation ─────────────────────────────────────────────────

    def ensure_initialized(self) -> None:
        """Create the ledger if it does not yet exist. Idempotent."""
        if self.dry_run:
            return
        if self._is_iceberg:
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {self._table_ref} ({_DDL_COLUMNS}) "
                "USING iceberg "
                "TBLPROPERTIES ("
                "'write.target-file-size-bytes'='16777216',"
                "'write.parquet.compression-codec'='zstd',"
                "'format-version'='2')"
            )
        else:
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS delta.`{self._delta_path}` "
                f"({_DDL_COLUMNS}) USING delta"
            )

    # ── Reads ──────────────────────────────────────────────────────────

    def list_applied(self) -> dict[str, AppliedMigration]:
        """Return a ``{version: AppliedMigration}`` map.

        Reads the ledger if it exists; returns ``{}`` on first run or in
        dry-run mode when no ledger exists.
        """
        try:
            df = self._read_table()
        except Exception:
            return {}
        if df is None:
            return {}

        applied: dict[str, AppliedMigration] = {}
        for row in df.collect():
            data = row.asDict()
            applied[data["version"]] = AppliedMigration(
                version=data.get("version"),
                name=data.get("name"),
                sha256=data.get("sha256"),
                applied_at=data.get("applied_at"),
                applied_by=data.get("applied_by"),
                duration_ms=data.get("duration_ms"),
                rolled_back_at=data.get("rolled_back_at"),
            )
        # An applied migration that was later rolled back is no longer applied.
        return {v: m for v, m in applied.items() if m.rolled_back_at is None}

    def _read_table(self):
        if self._is_iceberg:
            try:
                return self.spark.table(self._table_ref)
            except Exception:
                return None
        # Delta path
        try:
            return self.spark.read.format("delta").load(self._delta_path)
        except Exception:
            return None

    # ── Writes ─────────────────────────────────────────────────────────

    def record_applied(
        self,
        version: str,
        name: str,
        sha256: str,
        duration_ms: int,
        applied_by: str | None = None,
    ) -> None:
        if self.dry_run:
            return
        applied_by = applied_by or os.environ.get("USER", "unknown")
        applied_at = datetime.utcnow()
        if self._is_iceberg:
            self.spark.sql(
                f"INSERT INTO {self._table_ref} VALUES ("
                f"'{_sql_lit(version)}', "
                f"'{_sql_lit(name)}', "
                f"'{_sql_lit(sha256)}', "
                f"TIMESTAMP '{applied_at.isoformat(sep=' ', timespec='seconds')}', "
                f"'{_sql_lit(applied_by)}', "
                f"{int(duration_ms)}, "
                f"NULL"
                f")"
            )
        else:
            self.spark.sql(
                f"INSERT INTO delta.`{self._delta_path}` VALUES ("
                f"'{_sql_lit(version)}', "
                f"'{_sql_lit(name)}', "
                f"'{_sql_lit(sha256)}', "
                f"TIMESTAMP '{applied_at.isoformat(sep=' ', timespec='seconds')}', "
                f"'{_sql_lit(applied_by)}', "
                f"{int(duration_ms)}, "
                f"NULL"
                f")"
            )

    def mark_rolled_back(self, version: str) -> None:
        if self.dry_run:
            return
        rolled_at = datetime.utcnow()
        target = (
            self._table_ref
            if self._is_iceberg
            else f"delta.`{self._delta_path}`"
        )
        self.spark.sql(
            f"UPDATE {target} "
            f"SET rolled_back_at = TIMESTAMP "
            f"'{rolled_at.isoformat(sep=' ', timespec='seconds')}' "
            f"WHERE version = '{_sql_lit(version)}' AND rolled_back_at IS NULL"
        )

    # ── Display ────────────────────────────────────────────────────────

    @property
    def location(self) -> str:
        return self._table_ref if self._is_iceberg else f"delta:{self._delta_path}"


# ── Helpers ─────────────────────────────────────────────────────────────────


def _sql_lit(value: Optional[str]) -> str:
    """Escape a string for inline SQL literal use. Doubles single quotes."""
    if value is None:
        return ""
    return value.replace("'", "''")
