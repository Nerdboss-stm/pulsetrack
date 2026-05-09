"""
Migration CLI — argparse entrypoint for the framework.

Subcommands::

    python migrations/cli.py run                  # apply all pending
    python migrations/cli.py run --version V003   # apply through V003 inclusive
    python migrations/cli.py dry-run              # show plan, no writes
    python migrations/cli.py validate             # hash + dependency + conflict checks
    python migrations/cli.py status               # applied + pending listing
    python migrations/cli.py rollback --version V003
    python migrations/cli.py create --name "<descriptive_name>"

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

# Allow execution both as ``python migrations/cli.py`` and ``python -m migrations.cli``.
_PKG_DIR = Path(__file__).resolve().parent
if __package__ in (None, ""):
    sys.path.insert(0, str(_PKG_DIR.parent))
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


# ── Spark bootstrap ─────────────────────────────────────────────────────────


def _build_spark():
    """Build the SparkSession used by every subcommand that needs one.

    Iceberg + Glue settings are read from env vars so the same code runs on a
    laptop (no Iceberg) and on EMR (full catalog wiring).
    """
    from pyspark.sql import SparkSession  # imported lazily for `--help`

    builder = SparkSession.builder.appName("PulseTrack-Migrations")

    # Delta is always wired (state table fallback).
    builder = builder.config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension,"
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    builder = builder.config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )

    if os.environ.get("ICEBERG_CATALOG"):
        cat = os.environ["ICEBERG_CATALOG"]
        bucket = os.environ.get("LAKEHOUSE_BUCKET", "")
        warehouse = os.environ.get(
            "ICEBERG_WAREHOUSE",
            f"s3://{bucket}/iceberg/warehouse/" if bucket else "/tmp/iceberg-wh/",
        )
        builder = (
            builder.config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{cat}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{cat}.warehouse", warehouse)
            .config(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        )

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
) -> dict[str, "object"]:
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


# ── Subcommand handlers ─────────────────────────────────────────────────────


def cmd_validate(args: argparse.Namespace) -> int:
    plan = _load_plan()
    if not plan:
        print("No migrations to validate.")
        return 0

    # Render each file with current env to surface missing vars early.
    env = dict(os.environ)
    for m in plan:
        try:
            render_sql(m.raw_sql, env)
        except KeyError as exc:
            print(f"ERROR: {m.version}: {exc}")
            return 1
        if m.has_rollback:
            try:
                render_sql(
                    m.down_path.read_text(encoding="utf-8"),  # type: ignore[union-attr]
                    env,
                )
            except KeyError as exc:
                print(f"ERROR: {m.version} rollback: {exc}")
                return 1

    conflicts = detect_conflicts(plan)
    if conflicts:
        for a, b, table in conflicts:
            print(f"ERROR: {a} and {b} both target table {table}")
        return 1

    print(f"Validated {len(plan)} migration(s).")
    for m in plan:
        deps = ",".join(m.depends_on) if m.depends_on else "-"
        rb = "yes" if m.has_rollback else "no"
        print(f"  {m.version}  {m.name:<40}  deps={deps}  rollback={rb}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    plan = _load_plan()
    spark = _build_spark()
    state = MigrationState(spark, dry_run=False)
    state.ensure_initialized()
    applied = state.list_applied()

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

    spark = _build_spark()
    state = MigrationState(spark, dry_run=False)
    state.ensure_initialized()
    applied = _verify_applied_hashes(plan, state)

    pending = [m for m in plan if m.version not in applied]
    pending = _filter_to_target(pending, args.version)

    if not pending:
        print("Nothing to apply — all migrations are up to date.")
        return 0

    runner = MigrationRunner(spark, env=dict(os.environ), dry_run=False)
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

    # Best-effort state read so dry-run only shows truly pending work. If
    # Spark/Glue/Delta aren't configured in this environment (e.g. PR-checks
    # CI) we treat the ledger as empty and print the full plan.
    applied: dict = {}
    try:
        spark = _build_spark()
        state = MigrationState(spark, dry_run=True)
        applied = state.list_applied()
    except Exception as exc:
        print(f"(state unavailable, treating all migrations as pending: {exc})")
        spark = None

    pending = [m for m in plan if m.version not in applied]
    pending = _filter_to_target(pending, args.version)

    print(f"Plan ({len(pending)} migration(s) would apply):")
    runner = MigrationRunner(spark, env=dict(os.environ), dry_run=True)
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

    spark = _build_spark()
    state = MigrationState(spark, dry_run=False)
    state.ensure_initialized()
    applied = state.list_applied()
    if target not in applied:
        print(f"{target} is not applied — nothing to roll back.")
        return 0

    runner = MigrationRunner(spark, env=dict(os.environ), dry_run=False)
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
    forward.write_text(
        f"-- {next_v}: {args.name}\n"
        f"-- Created: {today}\n"
        f"-- depends_on: \n"
        f"-- TODO: write the forward migration here.\n",
        encoding="utf-8",
    )
    down.write_text(
        f"-- {next_v} (down): rollback for '{args.name}'\n"
        f"-- depends_on: \n"
        f"-- TODO: write the rollback here, or delete this file if irreversible.\n",
        encoding="utf-8",
    )
    print(f"Scaffolded {forward.name} and {down.name}")
    return 0


# ── argparse wiring ────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="migrations",
        description="PulseTrack SQL migration framework (Glacierbase-inspired).",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="apply all pending migrations")
    p_run.add_argument("--version", help="apply up to (and including) this version")
    p_run.set_defaults(func=cmd_run)

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


if __name__ == "__main__":
    sys.exit(main())
