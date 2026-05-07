"""Exponential-backoff retry decorator."""

from __future__ import annotations

import time

import pytest

from utils.retry import retry


def test_no_retry_on_success():
    calls = []

    @retry(max_retries=3, backoff_factor=0.0)
    def f():
        calls.append(1)
        return "ok"

    assert f() == "ok"
    assert len(calls) == 1


def test_retries_and_succeeds(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)
    calls = []

    @retry(max_retries=3, backoff_factor=0.0)
    def f():
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("flaky")
        return "ok"

    assert f() == "ok"
    assert len(calls) == 3  # 2 failures + 1 success


def test_raises_after_exhaustion(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)

    @retry(max_retries=2, backoff_factor=0.0)
    def f():
        raise RuntimeError("permanent")

    with pytest.raises(RuntimeError, match="permanent"):
        f()


def test_only_specified_exceptions_are_retried(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)
    calls = []

    @retry(max_retries=3, backoff_factor=0.0, exceptions=(ValueError,))
    def f():
        calls.append(1)
        raise TypeError("not retried")

    with pytest.raises(TypeError):
        f()
    assert len(calls) == 1


def test_backoff_grows_exponentially(monkeypatch):
    waits = []
    monkeypatch.setattr(time, "sleep", lambda s: waits.append(s))

    @retry(max_retries=3, backoff_factor=2.0)
    def f():
        raise ValueError("flaky")

    with pytest.raises(ValueError):
        f()
    # 4 attempts, 3 sleeps in between (powers 0,1,2 → 1, 2, 4)
    assert waits == [1.0, 2.0, 4.0]
