"""Pure-Python tests for migrations.validator.

These tests must run without Spark or any cloud creds. Anything that needs
Spark belongs in integration tests, not here.
"""

from __future__ import annotations

import hashlib
import textwrap
from pathlib import Path

import pytest

from migrations.validator import (
    Migration,
    detect_conflicts,
    discover,
    render_env_in_obj,
    render_env_in_str,
    render_sql,
    render_template,
    topo_sort,
    verify_hash,
)


# ── Helpers ────────────────────────────────────────────────────────────────


def _write(tmp: Path, name: str, body: str) -> Path:
    p = tmp / name
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return p


def _make(version: str, name: str, deps=(), targets=()) -> Migration:
    return Migration(
        version=version,
        name=name,
        path=Path(f"/tmp/{version}__{name}.sql"),
        down_path=None,
        sha256="0" * 64,
        raw_sql="",
        depends_on=list(deps),
        targets=set(targets),
    )


# ── render_sql ─────────────────────────────────────────────────────────────


def test_render_sql_env_only_substitutes_known_vars():
    sql = "CREATE TABLE ${CAT}.${DB}.t (x INT)"
    out = render_sql(sql, env={"CAT": "glue", "DB": "gold"})
    assert out == "CREATE TABLE glue.gold.t (x INT)"


def test_render_sql_env_only_raises_on_missing_vars():
    with pytest.raises(KeyError) as exc:
        render_sql("CREATE TABLE ${CAT}.${DB}.t (x INT)", env={"CAT": "glue"})
    assert "DB" in str(exc.value)


# ── render_template (Go-template style, matches WHOOP Glacierbase) ─────────


def test_render_template_replaces_simple_path():
    out = render_template(
        "SELECT * FROM {{ .variables.iceberg.catalog }}.t",
        {"iceberg": {"catalog": "glue_iceberg"}},
    )
    assert out == "SELECT * FROM glue_iceberg.t"


def test_render_template_handles_nested_path():
    sql = (
        "{{ .variables.iceberg.catalog }}."
        "{{ .variables.glue.database.gold }}.fact_x"
    )
    variables = {
        "iceberg": {"catalog": "glue_iceberg"},
        "glue": {"database": {"gold": "pt_gold_dev"}},
    }
    assert render_template(sql, variables) == "glue_iceberg.pt_gold_dev.fact_x"


def test_render_template_unresolved_path_raises():
    with pytest.raises(KeyError, match="unresolved template variable"):
        render_template("{{ .variables.nope }}", {"other": "x"})


def test_render_template_int_value_coerces_to_string():
    out = render_template(
        "bucket({{ .variables.bucketSize }}, id)",
        {"bucketSize": 16},
    )
    assert out == "bucket(16, id)"


def test_render_sql_template_then_env_fallback():
    """The combined render_sql does ``{{ .variables.X }}`` first, ``${VAR}`` second."""
    sql = "{{ .variables.x }}-${ENV_FALLBACK}"
    out = render_sql(
        sql, variables={"x": "from_template"}, env={"ENV_FALLBACK": "from_env"}
    )
    assert out == "from_template-from_env"


# ── render_env_in_str / render_env_in_obj (used by catalog config loader) ──


def test_render_env_default_value_when_unset():
    assert render_env_in_str("${UNSET:-fallback}", {}) == "fallback"


def test_render_env_default_value_ignored_when_set():
    assert render_env_in_str("${X:-fb}", {"X": "real"}) == "real"


def test_render_env_unset_without_default_raises():
    with pytest.raises(KeyError, match="unset env var"):
        render_env_in_str("${MISSING}", {})


def test_render_env_in_obj_recurses_through_nested_structures():
    cfg = {"top": {"a": "${X}", "b": "literal"}, "list": ["${X}", 42, None]}
    out = render_env_in_obj(cfg, {"X": "v"})
    assert out == {"top": {"a": "v", "b": "literal"}, "list": ["v", 42, None]}


# ── WHOOP-style migration headers (MIGRATION_DESCRIPTION / MIGRATION_AUTHOR) ─


def test_discover_extracts_whoop_headers(tmp_path: Path):
    _write(
        tmp_path,
        "V001__example.sql",
        """\
        -- MIGRATION_DESCRIPTION: Add the new Iceberg gold tables
        -- MIGRATION_AUTHOR: PulseTrack Data Platform
        -- depends_on:
        CREATE TABLE foo (id BIGINT) USING iceberg;
        """,
    )
    found = discover(tmp_path)
    assert len(found) == 1
    m = found[0]
    assert m.description == "Add the new Iceberg gold tables"
    assert m.author == "PulseTrack Data Platform"


def test_discover_missing_headers_default_to_empty(tmp_path: Path):
    _write(tmp_path, "V001__no_headers.sql", "CREATE TABLE foo (id INT);")
    m = discover(tmp_path)[0]
    assert m.description == ""
    assert m.author == ""


def test_discover_header_keys_case_insensitive(tmp_path: Path):
    _write(
        tmp_path,
        "V001__case.sql",
        """\
        -- migration_description: lowercase variant
        -- Migration_Author: Mixed Case Author
        CREATE TABLE foo (id INT);
        """,
    )
    m = discover(tmp_path)[0]
    assert m.description == "lowercase variant"
    assert m.author == "Mixed Case Author"


# ── verify_hash ────────────────────────────────────────────────────────────


def test_verify_hash_passes_when_unchanged():
    body = "CREATE TABLE foo (x INT);"
    sha = hashlib.sha256(body.encode()).hexdigest()
    m = _make("V001", "x")
    m.sha256 = sha
    verify_hash(m, sha)  # no exception


def test_verify_hash_raises_on_tampering():
    m = _make("V001", "x")
    m.sha256 = "a" * 64
    with pytest.raises(RuntimeError, match="tampering detected"):
        verify_hash(m, "b" * 64)


# ── topo_sort ──────────────────────────────────────────────────────────────


def test_topo_sort_respects_explicit_dependencies():
    a = _make("V001", "a")
    b = _make("V002", "b", deps=["V003"])  # b depends on c
    c = _make("V003", "c", deps=["V001"])
    out = [m.version for m in topo_sort([a, b, c])]
    assert out.index("V001") < out.index("V003") < out.index("V002")


def test_topo_sort_default_to_file_order_without_deps():
    a = _make("V001", "a")
    b = _make("V002", "b")
    c = _make("V003", "c")
    out = [m.version for m in topo_sort([a, b, c])]
    assert out == ["V001", "V002", "V003"]


def test_topo_sort_detects_missing_dependency():
    a = _make("V001", "a", deps=["V999"])
    with pytest.raises(ValueError, match="V999"):
        topo_sort([a])


def test_topo_sort_detects_circular_dependency():
    a = _make("V001", "a", deps=["V002"])
    b = _make("V002", "b", deps=["V001"])
    with pytest.raises(ValueError, match="circular"):
        topo_sort([a, b])


# ── detect_conflicts ───────────────────────────────────────────────────────


def test_detect_conflicts_finds_shared_table():
    a = _make("V001", "a", targets={"db.t1"})
    b = _make("V002", "b", targets={"db.t1", "db.t2"})
    out = detect_conflicts([a, b])
    assert ("V001", "V002", "db.t1") in out


def test_detect_conflicts_empty_when_disjoint():
    a = _make("V001", "a", targets={"db.t1"})
    b = _make("V002", "b", targets={"db.t2"})
    assert detect_conflicts([a, b]) == []


def test_detect_conflicts_ignores_declared_dependency():
    a = _make("V001", "a", targets={"db.t1"})
    b = _make("V002", "b", deps=["V001"], targets={"db.t1"})
    # b explicitly depends on a → ordering is declared, not a conflict.
    assert detect_conflicts([a, b]) == []


# ── discover (filesystem) ──────────────────────────────────────────────────


def test_discover_picks_up_pair_and_parses_directives(tmp_path: Path):
    _write(
        tmp_path,
        "V001__create.sql",
        """\
        -- depends_on:
        CREATE TABLE ${CAT}.${DB}.t1 (x INT);
        """,
    )
    _write(
        tmp_path,
        "V001__create__down.sql",
        """\
        DROP TABLE ${CAT}.${DB}.t1;
        """,
    )
    _write(
        tmp_path,
        "V002__alter.sql",
        """\
        -- depends_on: V001
        ALTER TABLE ${CAT}.${DB}.t1 ADD COLUMN y STRING;
        """,
    )

    found = discover(tmp_path)
    assert [m.version for m in found] == ["V001", "V002"]
    v002 = found[1]
    assert v002.depends_on == ["V001"]
    assert any(t.endswith("t1") for t in v002.targets)
    assert found[0].has_rollback is True
    assert found[1].has_rollback is False


def test_discover_rejects_duplicate_versions(tmp_path: Path):
    _write(tmp_path, "V001__a.sql", "SELECT 1;")
    _write(tmp_path, "V001__b.sql", "SELECT 2;")
    with pytest.raises(ValueError, match="duplicate"):
        discover(tmp_path)


def test_discover_rejects_bad_filename(tmp_path: Path):
    _write(tmp_path, "create_iceberg.sql", "SELECT 1;")
    with pytest.raises(ValueError, match="filename"):
        discover(tmp_path)
