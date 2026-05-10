"""
Migration CLI — argparse entrypoint for the framework (Glacierbase-style).

Subcommands::

    python migrations/cli.py run                     # apply all pending
    python migrations/cli.py run --version V003      # apply through V003 inclusive
    python migrations/cli.py pending                 # list unapplied migrations
    python migrations/cli.py dry-run                 # show plan, no writes
    python migrations/cli.py validate                # hash + dependency + conflict checks
    python migrations/cli.py status                  # applied + pending listing
    python migrations/cli.py rollback --version V003
    python migrations/cli.py create --name "<descriptive_name>" --author "<author>"

All subcommands accept a top-level ``--catalog <name>`` flag (default
``glue_iceberg``) — the named catalog's YAML in ``migrations/catalogs/``
provides the SparkSession config, template variables, state-table
location, and DynamoDB lock table. This mirrors WHOOP Glacierbase's
catalog-scoped CLI.

The command always discovers migrations from ``migrations/versions/``
relative to this file, so the cwd does not matter.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Allow execution both as ``python migrations/cli.py`` and ``python -m migrations.cli``.
_PKG_DIR = Path(__file__).resolve().parent
if __package__ in (None, ""):
    sys.path.insert(0, str(_PKG_DIR.parent))
    from migrations import lock as lock_mod  # type: ignore[no-redef]
    from migrations.catalog_config import (  # type: ignore[no-redef]
        CatalogConfig,
        list_catalogs,
        load_catalog,
    )
    from migrations.runner import MigrationRunner  # type: ignore[no-redef]
    from migrations.state import MigrationState  # type: ignore[no-redef]
    from migrations.validator import (  # type: ignore[no-redef]
        Migration,
        detect_conflicts,
        discover,
        render_sql,
        topo_sort,
        verify_hash,
    )
else:  # pragma: no cover
    from . import lock as lock_mod
    from .catalog_config import CatalogConfig, list_catalogs, load_catalog
    from .runner import MigrationRunner
    from .state import MigrationState
    from .validator import (
        Migration,
        detect_conflicts,
        discover,
        render_sql,
        topo_sort,
        verify_hash,
    )

VERSIONS_DIR = _PKG_DIR / "versions"
CATALOGS_DIR = _PKG_DIR / "catalogs"
DEFAULT_CATALOG = "glue_iceberg"


# ── Catalog config + Spark bootstrap ────────────────────────────────────────


def _load_catalog_or_die(catalog_name: str) -> CatalogConfig:
    try:
        return load_catalog(catalog_name, CATALOGS_DIR)
    except FileNotFoundError:
        existing = list_catalogs(CATALOGS_DIR)
        print(
            f"ERROR: catalog config not found: {catalog_name}.yaml. "
            f"Existing: {existing or '(none)'}",
            file=sys.stderr,
        )
        sys.exit(2)


def _build_spark(cfg: CatalogConfig):
    """Build the SparkSession from the catalog YAML's ``migrationExecutor.conf``.

    Each ``sparkConf`` key/value becomes a ``builder.config(k, v)`` call;
    each ``dependencies`` entry becomes a ``--packages`` Maven coord (joined
    via ``spark.jars.packages`` so it works whether you invoke this CLI via
    ``python -m`` or via ``spark-submit``).
    """
    from pyspark.sql import SparkSession  # imported lazily for `--help`

    builder = SparkSession.builder.appName(f"PulseTrack-Glacierbase-{cfg.catalog}")
    for key, value in cfg.spark_conf.items():
        builder = builder.config(key, value)
    if cfg.dependencies:
        builder = builder.config("spark.jars.packages", ",".join(cfg.dependencies))
    return builder.getOrCreate()


# ── Plan helpers ────────────────────────────────────────────────────────────


def _load_plan() -> list[Migration]:
    """Discover, validate, topo-sort. Raises on missing/circular/duplicate deps."""
    found = discover(VERSIONS_DIR)
    if not found:
        return []
    return topo_sort(found)


def _verify_applied_hashes(
    migrations: list[Migration], state: MigrationState
) -> dict[str, Any]:
    applied = state.list_applied()
    for m in migrations:
        if m.version in applied:
            verify_hash(m, applied[m.version].sha256)
    return applied


def _filter_to_target(
    migrations: list[Migration], target_version: str | None
) -> list[Migration]:
    if target_version is None:
        return migrations
    target = target_version.upper()
    out: list[Migration] = []
    for m in migrations:
        out.append(m)
        if m.version == target:
            return out
    raise ValueError(f"target version {target} not found among migrations")


# ── Lock helpers ────────────────────────────────────────────────────────────


class _LockGuard:
    """Context manager that acquires the catalog lock for the duration of a
    write operation (``run`` or ``rollback``). Read-only operations
    (``status``, ``pending``, ``validate``, ``dry-run``) skip locking.

    If the catalog YAML doesn't define a ``lock:`` block, this is a no-op
    and nothing is acquired/released. That's the right default for the
    local-dev / no-AWS path."""

    def __init__(self, cfg: CatalogConfig):
        self.cfg = cfg
        self._lock_cfg: lock_mod.LockConfig | None = None
        self._handle: lock_mod.LockHandle | None = None

    def __enter__(self) -> "_LockGuard":
        if not self.cfg.lock:
            return self
        if self.cfg.lock.get("type") != "dynamodb":
            return self
        self._lock_cfg = lock_mod.LockConfig.from_dict(self.cfg.lock)
        self._handle = lock_mod.acquire(self.cfg.catalog, self._lock_cfg)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle and self._lock_cfg:
            lock_mod.release(self._handle, self._lock_cfg)


# ── Subcommand handlers ─────────────────────────────────────────────────────


def cmd_validate(args: argparse.Namespace) -> int:
    plan = _load_plan()
    if not plan:
        print("No migrations to validate.")
        return 0

    cfg = _load_catalog_or_die(args.catalog)

    for m in plan:
        try:
            render_sql(m.raw_sql, variables=cfg.variables, env=dict(os.environ))
        except KeyError as exc:
            print(f"ERROR: {m.version}: {exc}")
            return 1
        if m.has_rollback:
            try:
                render_sql(
                    m.down_path.read_text(encoding="utf-8"),  # type: ignore[union-attr]
                    variables=cfg.variables,
                    env=dict(os.environ),
                )
            except KeyError as exc:
                print(f"ERROR: {m.version} rollback: {exc}")
                return 1

    conflicts = detect_conflicts(plan)
    if conflicts:
        for a, b, table in conflicts:
            print(f"ERROR: {a} and {b} both target table {table}")
        return 1

    print(f"Validated {len(plan)} migration(s) against catalog {cfg.catalog}.")
    for m in plan:
        deps = ",".join(m.depends_on) if m.depends_on else "-"
        rb = "yes" if m.has_rollback else "no"
        author = f" by {m.author}" if m.author else ""
        print(f"  {m.version}  {m.name:<40}  deps={deps}  rollback={rb}{author}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    plan = _load_plan()
    cfg = _load_catalog_or_die(args.catalog)
    spark = _build_spark(cfg)
    state = MigrationState(spark, state_table=cfg.state_table, dry_run=False)
    state.ensure_initialized()
    applied = state.list_applied()

    print(f"Catalog:       {cfg.catalog}")
    print(f"State backend: {state.location}")
    print(f"Discovered {len(plan)} migration(s).\n")
    print(f"  {'Version':<8} {'Name':<42} {'Status':<10} {'Applied At':<20}")
    print(f"  {'-'*8} {'-'*42} {'-'*10} {'-'*20}")
    for m in plan:
        if m.version in applied:
            status = "applied"
            ts = applied[m.version].applied_at
            ts_s = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else ""
        else:
            status = "pending"
            ts_s = ""
        print(f"  {m.version:<8} {m.name:<42} {status:<10} {ts_s:<20}")
    return 0


def cmd_pending(args: argparse.Namespace) -> int:
    """List unapplied migrations (matches WHOOP Glacierbase ``pending``)."""
    plan = _load_plan()
    cfg = _load_catalog_or_die(args.catalog)
    spark = _build_spark(cfg)
    state = MigrationState(spark, state_table=cfg.state_table, dry_run=False)
    state.ensure_initialized()
    applied = state.list_applied()

    pending = [m for m in plan if m.version not in applied]
    print(f"Catalog: {cfg.catalog}")
    print(f"Pending migrations: {len(pending)}")
    for m in pending:
        desc = f" — {m.description}" if m.description else ""
        print(f"  {m.version}  {m.name}{desc}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    plan = _load_plan()
    if not plan:
        print("No migrations discovered.")
        return 0

    conflicts = detect_conflicts(plan)
    if conflicts:
        print("Refusing to run with unresolved conflicts:")
        for a, b, table in conflicts:
            print(f"  {a} and {b} both target {table}")
        return 1

    cfg = _load_catalog_or_die(args.catalog)
    with _LockGuard(cfg):
        spark = _build_spark(cfg)
        state = MigrationState(spark, state_table=cfg.state_table, dry_run=False)
        state.ensure_initialized()
        applied = _verify_applied_hashes(plan, state)

        pending = [m for m in plan if m.version not in applied]
        pending = _filter_to_target(pending, args.version)

        if not pending:
            print("Nothing to apply — all migrations are up to date.")
            return 0

        runner = MigrationRunner(
            spark,
            variables=cfg.variables,
            env=dict(os.environ),
            dry_run=False,
        )
        for m in pending:
            print(f"Applying {m.version} ({m.name}) ...")
            ms = runner.apply(m)
            state.record_applied(m.version, m.name, m.sha256, duration_ms=ms)
            print(f"  -> applied in {ms} ms")
    return 0


def cmd_dry_run(args: argparse.Namespace) -> int:
    plan = _load_plan()
    if not plan:
        print("No migrations discovered.")
        return 0

    conflicts = detect_conflicts(plan)
    if conflicts:
        for a, b, table in conflicts:
            print(f"WARN: {a} and {b} both target {table}")

    cfg = _load_catalog_or_die(args.catalog)

    # Best-effort state read so dry-run only shows truly pending work. If
    # Spark/Glue/Delta aren't configured in this environment (e.g. PR-checks
    # CI) we treat the ledger as empty and print the full plan.
    applied: dict = {}
    spark = None
    try:
        spark = _build_spark(cfg)
        state = MigrationState(spark, state_table=cfg.state_table, dry_run=True)
        applied = state.list_applied()
    except Exception as exc:
        print(f"(state unavailable, treating all migrations as pending: {exc})")

    pending = [m for m in plan if m.version not in applied]
    pending = _filter_to_target(pending, args.version)

    print(f"Plan ({len(pending)} migration(s) would apply against {cfg.catalog}):")
    runner = MigrationRunner(
        spark,
        variables=cfg.variables,
        env=dict(os.environ),
        dry_run=True,
    )
    for m in pending:
        print(f"\n=== {m.version} :: {m.name} ===")
        runner.apply(m)
    print(f"\nDry-run complete. {len(pending)} migration(s) would have been applied.")
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    plan = _load_plan()
    target = args.version.upper()
    by_version = {m.version: m for m in plan}
    if target not in by_version:
        print(f"ERROR: {target} not found among migrations")
        return 1
    migration = by_version[target]

    if not migration.has_rollback:
        print(f"No rollback available for {target} ({migration.filename}). Aborting.")
        return 1

    cfg = _load_catalog_or_die(args.catalog)
    with _LockGuard(cfg):
        spark = _build_spark(cfg)
        state = MigrationState(spark, state_table=cfg.state_table, dry_run=False)
        state.ensure_initialized()
        applied = state.list_applied()
        if target not in applied:
            print(f"{target} is not applied — nothing to roll back.")
            return 0

        runner = MigrationRunner(
            spark,
            variables=cfg.variables,
            env=dict(os.environ),
            dry_run=False,
        )
        print(f"Rolling back {target} ({migration.name}) ...")
        runner.rollback(migration)
        state.mark_rolled_back(target)
        print(f"  -> rolled back {target}")
    return 0


def cmd_create(args: argparse.Namespace) -> int:
    name = args.name.strip().lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name).strip("_")
    if not name:
        print("ERROR: --name produced an empty slug after sanitisation")
        return 1

    existing = sorted(p for p in VERSIONS_DIR.glob("V*.sql"))
    last_n = 0
    for p in existing:
        m = re.match(r"^V(\d{3,})__", p.name)
        if m:
            last_n = max(last_n, int(m.group(1)))
    next_v = f"V{last_n + 1:03d}"

    forward = VERSIONS_DIR / f"{next_v}__{name}.sql"
    down = VERSIONS_DIR / f"{next_v}__{name}__down.sql"
    if forward.exists() or down.exists():
        print(f"ERROR: {next_v} already exists ({forward.name} or {down.name})")
        return 1

    today = datetime.utcnow().strftime("%Y-%m-%d")
    author = args.author or os.environ.get("USER") or "unknown"
    forward.write_text(
        f"-- MIGRATION_DESCRIPTION: {args.name}\n"
        f"-- MIGRATION_AUTHOR: {author}\n"
        f"-- Created: {today}\n"
        f"-- depends_on: \n"
        f"\n"
        f"-- TODO: write the forward migration here.\n"
        f"-- Reference templated variables as {{{{ .variables.X.Y.Z }}}} (resolved\n"
        f"-- from migrations/catalogs/<catalog>.yaml).\n",
        encoding="utf-8",
    )
    down.write_text(
        f"-- MIGRATION_DESCRIPTION: rollback for '{args.name}'\n"
        f"-- MIGRATION_AUTHOR: {author}\n"
        f"-- depends_on: \n"
        f"\n"
        f"-- TODO: write the rollback here, or delete this file if irreversible.\n",
        encoding="utf-8",
    )
    print(f"Scaffolded {forward.name} and {down.name}")
    return 0


# ── argparse wiring ────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="migrations",
        description=(
            "PulseTrack SQL migration framework (Glacierbase-style: catalog-"
            "scoped YAML config, Go-template variable injection, DynamoDB lock)."
        ),
    )
    p.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG,
        help=f"Catalog name (loads migrations/catalogs/<name>.yaml). Default: {DEFAULT_CATALOG}",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="apply all pending migrations")
    p_run.add_argument("--version", help="apply up to (and including) this version")
    p_run.set_defaults(func=cmd_run)

    p_pen = sub.add_parser("pending", help="list unapplied migrations")
    p_pen.set_defaults(func=cmd_pending)

    p_dry = sub.add_parser("dry-run", help="print the plan without writing")
    p_dry.add_argument("--version", help="dry-run up to (and including) this version")
    p_dry.set_defaults(func=cmd_dry_run)

    p_val = sub.add_parser("validate", help="hash + dependency + conflict checks")
    p_val.set_defaults(func=cmd_validate)

    p_st = sub.add_parser("status", help="list applied + pending migrations")
    p_st.set_defaults(func=cmd_status)

    p_rb = sub.add_parser("rollback", help="run a migration's __down.sql")
    p_rb.add_argument("--version", required=True, help="version to roll back, e.g. V003")
    p_rb.set_defaults(func=cmd_rollback)

    p_cr = sub.add_parser("create", help="scaffold a new migration pair")
    p_cr.add_argument("--name", required=True, help="descriptive name for the migration")
    p_cr.add_argument(
        "--author",
        help="author identity (defaults to $USER); written to MIGRATION_AUTHOR header",
    )
    p_cr.set_defaults(func=cmd_create)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (FileNotFoundError, ValueError, KeyError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except lock_mod.LockAcquisitionError as exc:
        print(f"LOCK: {exc}", file=sys.stderr)
        return 75  # EX_TEMPFAIL — retryable


if __name__ == "__main__":
    sys.exit(main())
