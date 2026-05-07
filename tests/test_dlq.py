"""Dead Letter Queue: schema, batch publish, idempotent flush behavior."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.mark.usefixtures("tmp_lakehouse")
def test_dlq_schema_constants_match_user_spec():
    from streaming.dlq import DLQ_SCHEMA

    fields = {f.name: str(f.dataType) for f in DLQ_SCHEMA.fields}
    expected_cols = {
        "original_topic",
        "original_partition",
        "original_offset",
        "original_key",
        "original_value",
        "error_type",
        "error_message",
        "stack_trace",
        "failed_at",
        "retry_count",
    }
    assert set(fields) == expected_cols


@pytest.mark.usefixtures("tmp_lakehouse")
def test_dlq_record_dataclass_defaults():
    from streaming.dlq import DLQRecord

    rec = DLQRecord(error_type="parse_error", error_message="boom")
    assert rec.error_type == "parse_error"
    assert rec.error_message == "boom"
    assert rec.retry_count == 0
    assert rec.failed_at.tzinfo is not None  # always UTC


@pytest.mark.usefixtures("tmp_lakehouse")
def test_publish_dataframe_writes_envelope_columns(spark, tmp_lakehouse):
    from streaming.dlq import DLQHandler

    src = spark.createDataFrame(
        [
            ("vitals", 0, 100, "deviceA", "raw-bytes-1"),
            ("vitals", 1, 101, "deviceB", "raw-bytes-2"),
        ],
        ["topic", "partition", "offset", "key", "value"],
    )
    h = DLQHandler(spark)
    n = h.publish_dataframe(src, error_type="avro_parse", error_message="bad payload")
    assert n == 2

    out = spark.read.format("delta").load(str(tmp_lakehouse / "dlq"))
    assert out.count() == 2
    cols = set(out.columns)
    assert {
        "original_topic",
        "original_partition",
        "original_offset",
        "original_key",
        "original_value",
        "error_type",
        "error_message",
        "failed_at",
        "retry_count",
    } <= cols
    rows = out.collect()
    assert all(r["error_type"] == "avro_parse" for r in rows)


@pytest.mark.usefixtures("tmp_lakehouse")
def test_publish_dataframe_handles_missing_envelope_cols(spark, tmp_lakehouse):
    """A DataFrame with no kafka envelope still writes — extras get NULL."""
    from streaming.dlq import DLQHandler

    src = spark.createDataFrame(
        [("payload-1",), ("payload-2",)],
        ["payload"],
    )
    h = DLQHandler(spark)
    n = h.publish_dataframe(src, error_type="schema_drift", error_message="missing fields")
    assert n == 2
    out = spark.read.format("delta").load(str(tmp_lakehouse / "dlq"))
    assert out.count() == 2
    # original_topic/partition/offset should all be NULL
    nulls = out.filter(out.original_topic.isNull()).count()
    assert nulls == 2


def test_publish_dataframe_no_spark_raises():
    from streaming.dlq import DLQHandler

    h = DLQHandler(spark=None)
    with pytest.raises(RuntimeError):
        h.publish_dataframe(None, "x", "y")
