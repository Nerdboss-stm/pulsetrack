"""WHOOP OAuth auth tests — token persistence, refresh, expiry detection."""

from __future__ import annotations

import json
import time
from unittest.mock import patch

import pytest


@pytest.fixture
def fake_token_path(tmp_path, monkeypatch):
    path = tmp_path / "tokens.json"
    monkeypatch.setenv("PT_WHOOP_TOKEN_PATH", str(path))
    monkeypatch.setenv("PT_WHOOP_CLIENT_ID", "test-client")
    monkeypatch.setenv("PT_WHOOP_CLIENT_SECRET", "test-secret")
    # Reload so the module-level Settings instance picks up our env vars.
    import importlib

    import config

    importlib.reload(config)
    import data_generators.whoop_api.auth as auth_module

    importlib.reload(auth_module)
    return str(path)


def test_is_expired_true_when_past(fake_token_path):
    from data_generators.whoop_api.auth import _is_expired

    assert _is_expired({"expires_at": int(time.time()) - 100}) is True


def test_is_expired_false_when_well_in_future(fake_token_path):
    from data_generators.whoop_api.auth import _is_expired

    assert _is_expired({"expires_at": int(time.time()) + 3600}) is False


def test_is_expired_within_leeway(fake_token_path):
    from data_generators.whoop_api.auth import REFRESH_LEEWAY_SECONDS, _is_expired

    # Refresh leeway: anything expiring within 5 minutes is "expired" for refresh purposes.
    near = int(time.time()) + REFRESH_LEEWAY_SECONDS - 1
    assert _is_expired({"expires_at": near}) is True


def test_write_then_read_tokens_roundtrip(fake_token_path):
    from data_generators.whoop_api.auth import _read_tokens, _write_tokens

    _write_tokens({"access_token": "a", "refresh_token": "r", "expires_in": 3600})
    tokens = _read_tokens()
    assert tokens["access_token"] == "a"
    assert tokens["refresh_token"] == "r"
    # _write_tokens should derive expires_at from expires_in.
    assert tokens["expires_at"] >= int(time.time())


def test_get_access_token_uses_refresh_when_expired(fake_token_path):
    from data_generators.whoop_api.auth import _write_tokens, get_access_token

    _write_tokens(
        {"access_token": "old", "refresh_token": "r1", "expires_at": int(time.time()) - 1}
    )

    new_payload = {"access_token": "new", "refresh_token": "r2", "expires_in": 3600}
    with patch("data_generators.whoop_api.auth._refresh_tokens", return_value=new_payload):
        token = get_access_token()

    assert token == "new"
    with open(fake_token_path) as f:
        on_disk = json.load(f)
    assert on_disk["access_token"] == "new"
    assert on_disk["refresh_token"] == "r2"


def test_get_access_token_preserves_refresh_when_not_returned(fake_token_path):
    from data_generators.whoop_api.auth import _write_tokens, get_access_token

    _write_tokens(
        {"access_token": "old", "refresh_token": "r1", "expires_at": int(time.time()) - 1}
    )
    new_payload = {"access_token": "new", "expires_in": 3600}  # no new refresh_token
    with patch("data_generators.whoop_api.auth._refresh_tokens", return_value=new_payload):
        get_access_token()

    with open(fake_token_path) as f:
        on_disk = json.load(f)
    assert on_disk["refresh_token"] == "r1"


def test_get_access_token_returns_existing_when_valid(fake_token_path):
    from data_generators.whoop_api.auth import _write_tokens, get_access_token

    _write_tokens(
        {"access_token": "valid", "refresh_token": "r", "expires_at": int(time.time()) + 3600}
    )
    with patch("data_generators.whoop_api.auth._refresh_tokens") as refresh:
        token = get_access_token()
    assert token == "valid"
    refresh.assert_not_called()
