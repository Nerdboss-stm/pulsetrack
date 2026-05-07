"""Structured JSON logger emits well-formed records."""

from __future__ import annotations

import json
import logging

from logger import JSONFormatter, get_logger


def _make_record(level: int, msg: str, name: str, **extra) -> logging.LogRecord:
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=42,
        msg=msg,
        args=(),
        exc_info=None,
    )
    if extra:
        record.extra_data = extra
    return record


def test_formatter_emits_required_fields():
    fmt = JSONFormatter()
    payload = json.loads(fmt.format(_make_record(logging.INFO, "hello", "x")))
    for field in ("timestamp", "level", "logger", "message", "module", "function", "line"):
        assert field in payload
    assert payload["level"] == "INFO"
    assert payload["message"] == "hello"


def test_formatter_includes_extra_data_at_top_level():
    fmt = JSONFormatter()
    rec = _make_record(logging.INFO, "x", "x", batch_id=7, valid=99)
    payload = json.loads(fmt.format(rec))
    assert payload["batch_id"] == 7
    assert payload["valid"] == 99


def test_get_logger_idempotent_does_not_duplicate_handlers():
    a = get_logger("test_logger_idem")
    b = get_logger("test_logger_idem")
    assert a is b
    assert len(a.handlers) == 1


def test_logger_outputs_valid_json(capsys):
    log = get_logger("test_emit")
    log.info("test message", extra={"extra_data": {"key": "value"}})
    captured = capsys.readouterr()
    # The logger writes to stdout; pytest captures it
    last_line = [ln for ln in captured.out.splitlines() if ln.strip()][-1]
    payload = json.loads(last_line)
    assert payload["message"] == "test message"
    assert payload["key"] == "value"
