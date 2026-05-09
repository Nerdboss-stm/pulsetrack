"""
Migration state — read/write the ``schema_migrations`` ledger.

State backend is chosen by env at runtime:

  * ``ICEBERG_CATALOG`` set  -> Iceberg table at
    ``<ICEBERG_CATALOG>.<GLUE_DATABASE_GOLD>.schema_migrations``.
  * Otherwise                 -> Delta table at
    ``<LAKEHOUSE_BUCKET>/_migrations/schema_migrations`` (or local path
    if no ``s3://`` scheme on the bucket).

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


def _is_iceberg() -> bool:
    return bool(os.environ.get("ICEBERG_CATALOG"))


def _iceberg_table() -> str:
    catalog = os.environ["ICEBERG_CATALOG"]
    db = os.environ["GLUE_DATABASE_GOLD"]
    return f"{catalog}.{db}.schema_migrations"


def _delta_path() -> str:
    bucket = os.environ.get("LAKEHOUSE_BUCKET", "")
    if bucket.startswith("s3://"):
        return f"{bucket.rstrip('/')}/_migrations/schema_migrations"
    if bucket:
        # Treat plain bucket names as s3:// (matches AWS workflow env style).
        return f"s3://{bucket}/_migrations/schema_migrations"
    # Last-resort local fallback for dry runs without any cloud config.
    return "/tmp/pulsetrack-migrations/schema_migrations"  # nosec B108


# ── Public API ──────────────────────────────────────────────────────────────


class MigrationState:
    """Manages the ``schema_migrations`` ledger.

    All methods are no-ops in ``dry_run=True`` mode — they read from the
    ledger if it exists but never write.
    """

    def __init__(self, spark: "SparkSession", dry_run: bool = False) -> None:
        self.spark = spark
        self.dry_run = dry_run
        self._table_ref = (
            _iceberg_table() if _is_iceberg() else None
        )  # set when iceberg, else None
        self._delta_path = None if _is_iceberg() else _delta_path()

    # ── Initialisation ─────────────────────────────────────────────────

    def ensure_initialized(self) -> None:
        """Create the ledger if it does not yet exist. Idempotent."""
        if self.dry_run:
            return
        if _is_iceberg():
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
        if _is_iceberg():
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
        if _is_iceberg():
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
            if _is_iceberg()
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
        return self._table_ref if _is_iceberg() else f"delta:{self._delta_path}"


# ── Helpers ─────────────────────────────────────────────────────────────────


def _sql_lit(value: Optional[str]) -> str:
    """Escape a string for inline SQL literal use. Doubles single quotes."""
    if value is None:
        return ""
    return value.replace("'", "''")
