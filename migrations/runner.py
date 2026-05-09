"""
Migration runner — executes forward / rollback SQL via Spark.

The runner orchestrates one migration at a time:

  1. Render env-var placeholders.
  2. Split the file into individual statements.
  3. Hand each statement to ``spark.sql`` sequentially.
  4. Time the whole migration and let the caller record state.

The runner does **not** open a transaction — Iceberg/Delta DDL is
auto-committed per statement. If a statement in the middle of a file
fails, anything that already succeeded stays applied. The state row is
only written when *all* statements succeed, so a failed migration shows
as pending on retry.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from .validator import Migration, render_sql

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


def split_statements(sql: str) -> list[str]:
    """Split a multi-statement SQL string into individual statements.

    Linear scanner that splits on ``;`` outside single-quoted string literals
    and outside comments. Apostrophes inside ``-- ...`` line comments and
    ``/* ... */`` block comments would otherwise toggle the string state and
    swallow subsequent semicolons (real bug found in V002 where a comment
    containing "transform's" caused everything after to read as one
    statement).

    Spark's ``spark.sql`` parser only accepts one statement at a time, so we
    split before dispatching.
    """
    stmts: list[str] = []
    current: list[str] = []
    in_string = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_line_comment:
            current.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            current.append(ch)
            if ch == "*" and nxt == "/":
                current.append(nxt)
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue
        if not in_string and ch == "-" and nxt == "-":
            in_line_comment = True
            current.append(ch)
            current.append(nxt)
            i += 2
            continue
        if not in_string and ch == "/" and nxt == "*":
            in_block_comment = True
            current.append(ch)
            current.append(nxt)
            i += 2
            continue

        if ch == "'":
            in_string = not in_string
            current.append(ch)
        elif ch == ";" and not in_string:
            stmt = "".join(current).strip()
            if stmt:
                stmts.append(stmt)
            current = []
        else:
            current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        stmts.append(tail)
    return stmts


# ── Runner ──────────────────────────────────────────────────────────────────


class MigrationRunner:
    """Executes a single migration's SQL through Spark."""

    def __init__(
        self,
        spark: "SparkSession",
        env: dict[str, str] | None = None,
        dry_run: bool = False,
    ) -> None:
        self.spark = spark
        self.env = env
        self.dry_run = dry_run

    def apply(self, migration: Migration) -> int:
        """Apply ``migration``'s forward script. Returns elapsed milliseconds."""
        return self._execute(migration.raw_sql, label=f"apply {migration.version}")

    def rollback(self, migration: Migration) -> int:
        """Apply ``migration``'s ``__down`` script. Errors if missing."""
        if not migration.has_rollback:
            raise FileNotFoundError(
                f"no rollback available for {migration.version} "
                f"({migration.filename}) — expected {migration.version}__"
                f"{migration.name}__down.sql"
            )
        sql = migration.down_path.read_text(encoding="utf-8")  # type: ignore[union-attr]
        return self._execute(sql, label=f"rollback {migration.version}")

    # ── Internals ─────────────────────────────────────────────────────

    def _execute(self, raw_sql: str, label: str) -> int:
        rendered = render_sql(raw_sql, self.env)
        statements = split_statements(rendered)
        start = time.perf_counter()
        for stmt in statements:
            if self.dry_run:
                print(f"[dry-run] {label}: would execute:\n{stmt}\n")
            else:
                self.spark.sql(stmt)
        return int((time.perf_counter() - start) * 1000)
