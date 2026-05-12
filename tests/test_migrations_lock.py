"""Unit tests for migrations.lock — DynamoDB-based catalog locking."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from migrations.lock import (
    LockAcquisitionError,
    LockConfig,
    LockHandle,
    _holder_id,
    acquire,
    release,
)


# ── LockConfig ──────────────────────────────────────────────────────────


def test_lock_config_from_dict_minimal():
    cfg = LockConfig.from_dict({"tableName": "test-lock"})
    assert cfg.table_name == "test-lock"
    assert cfg.region == "us-east-1"
    assert cfg.ttl_seconds == 1800


def test_lock_config_from_dict_full():
    cfg = LockConfig.from_dict({
        "tableName": "glacierbase-lock",
        "region": "us-west-2",
        "ttlSeconds": 600,
    })
    assert cfg.table_name == "glacierbase-lock"
    assert cfg.region == "us-west-2"
    assert cfg.ttl_seconds == 600


def test_lock_config_ttl_coerced_to_int():
    """ttlSeconds may come in as a string from YAML; coerce to int."""
    cfg = LockConfig.from_dict({"tableName": "t", "ttlSeconds": "1200"})
    assert cfg.ttl_seconds == 1200
    assert isinstance(cfg.ttl_seconds, int)


# ── _holder_id ──────────────────────────────────────────────────────────


def test_holder_id_format(monkeypatch):
    monkeypatch.setenv("USER", "alice")
    holder = _holder_id()
    assert "alice@" in holder
    assert ":" in holder


def test_holder_id_falls_back_to_unknown(monkeypatch):
    monkeypatch.delenv("USER", raising=False)
    monkeypatch.delenv("USERNAME", raising=False)
    holder = _holder_id()
    assert holder.startswith("unknown@") or "unknown" in holder


# ── acquire ─────────────────────────────────────────────────────────────


def test_acquire_success_returns_handle():
    """put_item succeeds → returns a LockHandle."""
    cfg = LockConfig(table_name="test-lock")
    fake_ddb = MagicMock()
    fake_ddb.exceptions.ConditionalCheckFailedException = type(
        "CCFE", (Exception,), {}
    )
    fake_ddb.put_item.return_value = {}
    with patch("boto3.client", return_value=fake_ddb):
        handle = acquire("test_catalog", cfg, holder="me")
    assert handle.catalog == "test_catalog"
    assert handle.holder == "me"
    assert isinstance(handle.acquired_at, int)


def test_acquire_put_item_includes_required_fields():
    cfg = LockConfig(table_name="test-lock", ttl_seconds=900)
    fake_ddb = MagicMock()
    fake_ddb.exceptions.ConditionalCheckFailedException = type(
        "CCFE", (Exception,), {}
    )
    with patch("boto3.client", return_value=fake_ddb):
        acquire("test_catalog", cfg, holder="me")
    call_args = fake_ddb.put_item.call_args
    item = call_args.kwargs["Item"]
    assert item["catalog"]["S"] == "test_catalog"
    assert item["holder"]["S"] == "me"
    assert "acquired_at" in item
    assert "expires_at" in item
    # TTL = acquired_at + ttl_seconds
    acquired = int(item["acquired_at"]["N"])
    expires = int(item["expires_at"]["N"])
    assert expires == acquired + 900


def test_acquire_conditional_failure_raises_lock_acquisition_error():
    """If another holder owns the lock → LockAcquisitionError with their name."""
    cfg = LockConfig(table_name="test-lock")
    fake_ddb = MagicMock()

    class CCFE(Exception):
        pass

    fake_ddb.exceptions.ConditionalCheckFailedException = CCFE
    fake_ddb.put_item.side_effect = CCFE("Conditional check failed")
    fake_ddb.get_item.return_value = {
        "Item": {
            "holder": {"S": "bob@host:5678"},
            "acquired_at": {"N": "1700000000"},
        }
    }
    with patch("boto3.client", return_value=fake_ddb):
        with pytest.raises(LockAcquisitionError, match="bob@host:5678"):
            acquire("test_catalog", cfg)


def test_acquire_diagnostic_get_failure_falls_back():
    """If get_item also fails → still raise LockAcquisitionError but with <unknown>."""
    cfg = LockConfig(table_name="test-lock")
    fake_ddb = MagicMock()

    class CCFE(Exception):
        pass

    fake_ddb.exceptions.ConditionalCheckFailedException = CCFE
    fake_ddb.put_item.side_effect = CCFE("locked")
    fake_ddb.get_item.side_effect = Exception("dynamodb unreachable")
    with patch("boto3.client", return_value=fake_ddb):
        with pytest.raises(LockAcquisitionError, match="<unknown>"):
            acquire("test_catalog", cfg)


def test_acquire_uses_provided_holder():
    cfg = LockConfig(table_name="test-lock")
    fake_ddb = MagicMock()
    fake_ddb.exceptions.ConditionalCheckFailedException = type(
        "CCFE", (Exception,), {}
    )
    with patch("boto3.client", return_value=fake_ddb):
        h = acquire("test_catalog", cfg, holder="custom-holder-id")
    assert h.holder == "custom-holder-id"


def test_acquire_default_holder_uses_holder_id(monkeypatch):
    """When holder=None, _holder_id() is used."""
    monkeypatch.setenv("USER", "test_user")
    cfg = LockConfig(table_name="test-lock")
    fake_ddb = MagicMock()
    fake_ddb.exceptions.ConditionalCheckFailedException = type(
        "CCFE", (Exception,), {}
    )
    with patch("boto3.client", return_value=fake_ddb):
        h = acquire("test_catalog", cfg)
    assert "test_user@" in h.holder


# ── release ─────────────────────────────────────────────────────────────


def test_release_success():
    cfg = LockConfig(table_name="test-lock")
    handle = LockHandle(catalog="test_catalog", holder="me", acquired_at=1700000000)
    fake_ddb = MagicMock()
    fake_ddb.exceptions.ConditionalCheckFailedException = type(
        "CCFE", (Exception,), {}
    )
    with patch("boto3.client", return_value=fake_ddb):
        release(handle, cfg)
    fake_ddb.delete_item.assert_called_once()


def test_release_lock_stolen_or_expired_does_not_raise():
    """If the lock is no longer ours (TTL'd or stolen) → silent return."""
    cfg = LockConfig(table_name="test-lock")
    handle = LockHandle(catalog="test_catalog", holder="me", acquired_at=1700000000)
    fake_ddb = MagicMock()

    class CCFE(Exception):
        pass

    fake_ddb.exceptions.ConditionalCheckFailedException = CCFE
    fake_ddb.delete_item.side_effect = CCFE("not ours")
    with patch("boto3.client", return_value=fake_ddb):
        # Should NOT raise — silent skip is the documented behavior
        release(handle, cfg)


def test_release_conditional_expression_matches_holder():
    cfg = LockConfig(table_name="test-lock")
    handle = LockHandle(catalog="test_catalog", holder="alice@host:1234", acquired_at=1700000000)
    fake_ddb = MagicMock()
    fake_ddb.exceptions.ConditionalCheckFailedException = type(
        "CCFE", (Exception,), {}
    )
    with patch("boto3.client", return_value=fake_ddb):
        release(handle, cfg)
    call = fake_ddb.delete_item.call_args
    # The ConditionExpression locks the delete to our holder
    cond_expr = call.kwargs.get("ConditionExpression", "")
    assert "holder" in cond_expr
    holder_val = call.kwargs["ExpressionAttributeValues"][":holder"]["S"]
    assert holder_val == "alice@host:1234"


# ── LockHandle dataclass ────────────────────────────────────────────────


def test_lock_handle_is_dataclass():
    h = LockHandle(catalog="x", holder="y", acquired_at=123)
    assert h.catalog == "x"
    assert h.holder == "y"
    assert h.acquired_at == 123
