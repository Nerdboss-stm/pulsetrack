"""Unit tests for migrations.state — the migration ledger backend.

Mocks SparkSession so tests don't need a JVM. Validates:
- _classify_state_table dispatch (Iceberg FQN vs Delta path vs default)
- AppliedMigration dataclass shape
- MigrationState constructor with explicit state_table
- MigrationState constructor falling back to env vars
- ensure_initialized() CREATE TABLE behavior, idempotency, dry-run
- list_applied() filters out rolled-back migrations
- record_applied() / mark_rolled_back() SQL composition
- _sql_lit() escapes single quotes
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from migrations.state import (
    AppliedMigration,
    MigrationState,
    _classify_state_table,
    _sql_lit,
)


# ── _classify_state_table ───────────────────────────────────────────────


def test_classify_iceberg_fqn():
    is_ice, fqn, path = _classify_state_table("glue_iceberg.gold.schema_migrations")
    assert is_ice is True
    assert fqn == "glue_iceberg.gold.schema_migrations"
    assert path is None


def test_classify_s3_path():
    is_ice, fqn, path = _classify_state_table("s3://bucket/_migrations/state")
    assert is_ice is False
    assert path == "s3://bucket/_migrations/state"


def test_classify_local_path():
    is_ice, fqn, path = _classify_state_table("/tmp/test-migrations")
    assert is_ice is False
    assert path == "/tmp/test-migrations"


def test_classify_bare_bucket_name_prepends_s3():
    is_ice, fqn, path = _classify_state_table("my-bucket")
    assert is_ice is False
    assert path == "s3://my-bucket"


def test_classify_empty_string_uses_default():
    is_ice, fqn, path = _classify_state_table("")
    assert is_ice is False
    assert "pulsetrack-migrations" in path


# ── AppliedMigration dataclass ──────────────────────────────────────────


def test_applied_migration_dataclass():
    a = AppliedMigration(
        version="V001",
        name="create_tables",
        sha256="abc123",
        applied_at=datetime(2026, 1, 1),
        applied_by="alice",
        duration_ms=1234,
        rolled_back_at=None,
    )
    assert a.version == "V001"
    assert a.duration_ms == 1234


def test_applied_migration_with_rollback():
    a = AppliedMigration(
        version="V003",
        name="bad_migration",
        sha256="def",
        applied_at=datetime(2026, 1, 1),
        applied_by="bob",
        duration_ms=500,
        rolled_back_at=datetime(2026, 1, 2),
    )
    assert a.rolled_back_at is not None


# ── _sql_lit ────────────────────────────────────────────────────────────


def test_sql_lit_escapes_single_quote():
    assert _sql_lit("O'Brien") == "O''Brien"


def test_sql_lit_handles_none():
    assert _sql_lit(None) == ""


def test_sql_lit_passes_through_plain_text():
    assert _sql_lit("plain text") == "plain text"


def test_sql_lit_handles_empty_string():
    assert _sql_lit("") == ""


# ── MigrationState constructor ──────────────────────────────────────────


def test_state_init_with_explicit_iceberg_table():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.schema_migrations")
    assert state._is_iceberg is True
    assert state._table_ref == "glue.gold.schema_migrations"


def test_state_init_with_explicit_delta_path():
    spark = MagicMock()
    state = MigrationState(spark, state_table="s3://test-bucket/_migrations/state")
    assert state._is_iceberg is False
    assert state._delta_path == "s3://test-bucket/_migrations/state"


def test_state_init_env_var_iceberg_path(monkeypatch):
    """When state_table=None + ICEBERG_CATALOG set → Iceberg path."""
    monkeypatch.setenv("ICEBERG_CATALOG", "glue_iceberg")
    monkeypatch.setenv("GLUE_DATABASE_GOLD", "pulsetrack_gold_dev")
    spark = MagicMock()
    state = MigrationState(spark)
    assert state._is_iceberg is True
    assert "glue_iceberg" in state._table_ref
    assert "schema_migrations" in state._table_ref


def test_state_init_env_var_delta_path_with_bucket(monkeypatch):
    """When state_table=None + LAKEHOUSE_BUCKET set + no ICEBERG_CATALOG → Delta path."""
    monkeypatch.delenv("ICEBERG_CATALOG", raising=False)
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "test-bucket")
    spark = MagicMock()
    state = MigrationState(spark)
    assert state._is_iceberg is False
    assert "test-bucket" in state._delta_path
    assert "s3://" in state._delta_path


def test_state_init_env_var_default_local(monkeypatch):
    """No env vars → local /tmp default."""
    monkeypatch.delenv("ICEBERG_CATALOG", raising=False)
    monkeypatch.delenv("LAKEHOUSE_BUCKET", raising=False)
    spark = MagicMock()
    state = MigrationState(spark)
    assert state._delta_path.startswith("/tmp/")


# ── ensure_initialized ──────────────────────────────────────────────────


def test_ensure_initialized_iceberg_executes_create():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.schema_migrations")
    state.ensure_initialized()
    sql = spark.sql.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "USING iceberg" in sql
    assert "glue.gold.schema_migrations" in sql


def test_ensure_initialized_delta_executes_create():
    spark = MagicMock()
    state = MigrationState(spark, state_table="s3://x/y/state")
    state.ensure_initialized()
    sql = spark.sql.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS delta.`s3://x/y/state`" in sql
    assert "USING delta" in sql


def test_ensure_initialized_dry_run_is_noop():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.t", dry_run=True)
    state.ensure_initialized()
    spark.sql.assert_not_called()


# ── list_applied ────────────────────────────────────────────────────────


def test_list_applied_empty_returns_empty_dict():
    spark = MagicMock()
    spark.table.side_effect = Exception("not found")
    state = MigrationState(spark, state_table="glue.gold.t")
    assert state.list_applied() == {}


def test_list_applied_filters_rolled_back():
    spark = MagicMock()
    row1 = MagicMock()
    row1.asDict.return_value = {
        "version": "V001",
        "name": "create",
        "sha256": "a",
        "applied_at": datetime(2026, 1, 1),
        "applied_by": "me",
        "duration_ms": 100,
        "rolled_back_at": None,  # active
    }
    row2 = MagicMock()
    row2.asDict.return_value = {
        "version": "V002",
        "name": "bad",
        "sha256": "b",
        "applied_at": datetime(2026, 1, 2),
        "applied_by": "me",
        "duration_ms": 50,
        "rolled_back_at": datetime(2026, 1, 3),  # rolled back → should be filtered
    }
    df = MagicMock()
    df.collect.return_value = [row1, row2]
    spark.table.return_value = df

    state = MigrationState(spark, state_table="glue.gold.t")
    applied = state.list_applied()
    assert "V001" in applied
    assert "V002" not in applied  # filtered


# ── record_applied ──────────────────────────────────────────────────────


def test_record_applied_iceberg_emits_insert():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.schema_migrations")
    state.record_applied("V001", "create", "sha-abc", 1500, applied_by="alice")
    sql = spark.sql.call_args[0][0]
    assert "INSERT INTO glue.gold.schema_migrations" in sql
    assert "V001" in sql
    assert "sha-abc" in sql
    assert "1500" in sql
    assert "alice" in sql


def test_record_applied_delta_uses_delta_path_syntax():
    spark = MagicMock()
    state = MigrationState(spark, state_table="s3://x/y/state")
    state.record_applied("V001", "create", "sha", 100, applied_by="me")
    sql = spark.sql.call_args[0][0]
    assert "INSERT INTO delta.`s3://x/y/state`" in sql


def test_record_applied_dry_run_is_noop():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.t", dry_run=True)
    state.record_applied("V001", "x", "y", 100)
    spark.sql.assert_not_called()


def test_record_applied_escapes_single_quotes():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.t")
    state.record_applied("V001", "O'Brien's migration", "sha", 100, applied_by="me")
    sql = spark.sql.call_args[0][0]
    assert "O''Brien''s migration" in sql


def test_record_applied_default_applied_by_uses_env(monkeypatch):
    monkeypatch.setenv("USER", "envuser")
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.t")
    state.record_applied("V001", "x", "y", 100)
    sql = spark.sql.call_args[0][0]
    assert "envuser" in sql


# ── mark_rolled_back ───────────────────────────────────────────────────


def test_mark_rolled_back_iceberg():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.t")
    state.mark_rolled_back("V001")
    sql = spark.sql.call_args[0][0]
    assert "UPDATE glue.gold.t" in sql
    assert "SET rolled_back_at" in sql
    assert "WHERE version = 'V001'" in sql


def test_mark_rolled_back_delta():
    spark = MagicMock()
    state = MigrationState(spark, state_table="s3://x/y/state")
    state.mark_rolled_back("V002")
    sql = spark.sql.call_args[0][0]
    assert "UPDATE delta.`s3://x/y/state`" in sql


def test_mark_rolled_back_dry_run_is_noop():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.t", dry_run=True)
    state.mark_rolled_back("V001")
    spark.sql.assert_not_called()


# ── location property ──────────────────────────────────────────────────


def test_location_iceberg():
    spark = MagicMock()
    state = MigrationState(spark, state_table="glue.gold.t")
    assert state.location == "glue.gold.t"


def test_location_delta():
    spark = MagicMock()
    state = MigrationState(spark, state_table="s3://x/y/state")
    assert state.location == "delta:s3://x/y/state"
