"""Unit tests for ``migrations.runner.split_statements``.

These tests are pure-Python — no Spark, no S3, no Glue — so CI can run them
on every PR. The regression cases below correspond to bugs we found while
running V001-V004 end-to-end against the production cluster.
"""

from __future__ import annotations

import pytest

from migrations.runner import split_statements


class TestSplitStatementsBasic:
    """Baseline behavior — splits on top-level ``;``, trims whitespace."""

    def test_single_statement_no_terminator(self) -> None:
        assert split_statements("SELECT 1") == ["SELECT 1"]

    def test_single_statement_with_terminator(self) -> None:
        assert split_statements("SELECT 1;") == ["SELECT 1"]

    def test_two_statements(self) -> None:
        sql = "SELECT 1; SELECT 2"
        assert split_statements(sql) == ["SELECT 1", "SELECT 2"]

    def test_trailing_semicolon_is_not_an_empty_statement(self) -> None:
        assert split_statements("SELECT 1;\n\n") == ["SELECT 1"]

    def test_blank_input(self) -> None:
        assert split_statements("") == []
        assert split_statements("   \n   ") == []
        assert split_statements(";;;") == []


class TestSplitStatementsStringLiterals:
    """A ``;`` inside a single-quoted string is part of the literal — never split."""

    def test_semicolon_inside_string_literal_is_preserved(self) -> None:
        sql = "INSERT INTO t VALUES ('a;b'); SELECT 1"
        out = split_statements(sql)
        assert out == ["INSERT INTO t VALUES ('a;b')", "SELECT 1"]

    def test_two_string_literals_in_one_statement(self) -> None:
        sql = "SELECT 'foo;', 'bar;'"
        assert split_statements(sql) == ["SELECT 'foo;', 'bar;'"]


class TestSplitStatementsComments:
    """Regression tests for the apostrophe-in-comment bug found in V002.

    Before the fix, a ``-- foo's bar`` comment toggled the in-string flag and
    every ``;`` afterward was swallowed, collapsing multi-statement files
    into a single Spark parse error.
    """

    def test_line_comment_with_apostrophe_does_not_swallow_semicolons(self) -> None:
        sql = (
            "-- streaming Gold transform's projected schema\n"
            "ALTER TABLE t ADD COLUMN x STRING;\n"
            "ALTER TABLE u ADD COLUMN y STRING;\n"
        )
        out = split_statements(sql)
        assert len(out) == 2
        assert out[0].endswith("ADD COLUMN x STRING")
        assert out[1].endswith("ADD COLUMN y STRING")

    def test_line_comment_with_semicolon_inside(self) -> None:
        # A ``;`` inside a line comment must NOT split the surrounding stmt.
        sql = "SELECT 1 -- this; that\nFROM dual; SELECT 2"
        out = split_statements(sql)
        assert len(out) == 2
        assert "SELECT 1" in out[0] and "FROM dual" in out[0]
        assert out[1] == "SELECT 2"

    def test_block_comment_with_apostrophes_and_semicolons(self) -> None:
        sql = (
            "/* this is a multi-line\n"
            "   block comment with don't and ; in it */\n"
            "SELECT 1;\n"
            "SELECT 2;\n"
        )
        out = split_statements(sql)
        assert len(out) == 2
        assert "SELECT 1" in out[0]
        assert out[1].strip() == "SELECT 2"

    def test_block_comment_at_end_of_line(self) -> None:
        sql = "SELECT 1 /* trailing */; SELECT 2"
        out = split_statements(sql)
        assert len(out) == 2

    def test_dash_in_string_literal_is_not_a_comment(self) -> None:
        # ``--`` inside quoted text isn't a comment — must NOT enter
        # comment mode and skip the trailing ``;``.
        sql = "SELECT 'a--b'; SELECT 2"
        out = split_statements(sql)
        assert out == ["SELECT 'a--b'", "SELECT 2"]


class TestSplitStatementsRealMigrationFiles:
    """Smoke tests against shapes that match the V001-V004 SQL files."""

    def test_v002_shape_with_apostrophe_comment(self) -> None:
        # Mirrors the actual V002 migration — the apostrophe in
        # "transform's" was the original bug.
        sql = (
            "-- depends_on: V001\n"
            "-- V002: Add source_type to remaining fact tables.\n"
            "-- ----------------------------------------------------------------------------\n"
            "-- fact_vital_reading already has source_type from V001 (it is part of the\n"
            "-- streaming Gold transform's projected schema). The daily summary and lab\n"
            "-- result facts predate the WHOOP integration.\n"
            "-- ----------------------------------------------------------------------------\n"
            "\n"
            "ALTER TABLE catalog.db.fact_vital_daily_summary ADD COLUMN source_type STRING;\n"
            "\n"
            "ALTER TABLE catalog.db.fact_lab_result ADD COLUMN source_type STRING;\n"
        )
        out = split_statements(sql)
        assert len(out) == 2
        assert "fact_vital_daily_summary ADD COLUMN source_type" in out[0]
        assert "fact_lab_result ADD COLUMN source_type" in out[1]

    def test_v004_shape_with_three_alters(self) -> None:
        sql = (
            "-- depends_on: V001\n"
            "ALTER TABLE t DROP PARTITION FIELD days(event_timestamp);\n"
            "ALTER TABLE t ADD PARTITION FIELD bucket(16, patient_key);\n"
            "ALTER TABLE t ADD PARTITION FIELD days(event_timestamp);\n"
        )
        out = split_statements(sql)
        assert len(out) == 3
        assert "DROP PARTITION FIELD" in out[0]
        assert "bucket(16, patient_key)" in out[1]
        assert "days(event_timestamp)" in out[2]


class TestSplitStatementsEdgeCases:
    @pytest.mark.parametrize(
        "sql,expected_count",
        [
            ("SELECT 1; SELECT 2; SELECT 3", 3),
            ("CREATE TABLE t (a INT, b STRING);", 1),
            (
                "CREATE TABLE t (a INT) USING iceberg "
                "TBLPROPERTIES ('write.parquet.compression-codec'='zstd')",
                1,
            ),
        ],
    )
    def test_param_split_count(self, sql: str, expected_count: int) -> None:
        assert len(split_statements(sql)) == expected_count

    def test_unterminated_string_literal_is_not_silently_swallowed(self) -> None:
        # An unmatched apostrophe is always a SQL syntax error — but the
        # splitter shouldn't lose the rest of the buffer either. The current
        # behavior is "collect everything until end of input as one
        # statement" which lets Spark surface the real error message to the
        # operator instead of failing with a confusing "0 statements" output.
        sql = "SELECT 'unterminated; SELECT 2"
        out = split_statements(sql)
        assert len(out) == 1
        assert "unterminated" in out[0]
