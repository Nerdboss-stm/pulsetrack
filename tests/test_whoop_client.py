"""WHOOP client tests — pagination, rate-limit handling, retries."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _mock_response(status_code: int, json_data: dict | None = None, headers: dict | None = None):
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers or {}
    response.json.return_value = json_data or {}
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        from requests import HTTPError

        response.raise_for_status.side_effect = HTTPError(f"{status_code}")
    return response


def test_paginate_follows_next_token():
    from data_generators.whoop_api.client import WhoopClient

    page1 = {"records": [{"id": 1}, {"id": 2}], "next_token": "TOKEN1"}
    page2 = {"records": [{"id": 3}], "next_token": None}

    with (
        patch("data_generators.whoop_api.client.get_access_token", return_value="t"),
        patch(
            "data_generators.whoop_api.client.requests.get",
            side_effect=[_mock_response(200, page1), _mock_response(200, page2)],
        ),
    ):
        client = WhoopClient(base_url="https://api.test")
        out = list(client.list_recovery("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"))
    assert [r["id"] for r in out] == [1, 2, 3]


def test_paginate_stops_on_empty_records():
    from data_generators.whoop_api.client import WhoopClient

    page1 = {"records": [], "next_token": "WOULD_BE_NEXT"}

    with (
        patch("data_generators.whoop_api.client.get_access_token", return_value="t"),
        patch(
            "data_generators.whoop_api.client.requests.get",
            side_effect=[_mock_response(200, page1)],
        ),
    ):
        client = WhoopClient(base_url="https://api.test")
        out = list(client.list_recovery("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"))
    assert out == []


def test_429_triggers_retry_after_sleep():
    from data_generators.whoop_api.client import WhoopClient

    rate_limited = _mock_response(429, headers={"Retry-After": "1"})
    success = _mock_response(200, {"records": [], "next_token": None})

    with (
        patch("data_generators.whoop_api.client.get_access_token", return_value="t"),
        patch("data_generators.whoop_api.client.time.sleep") as sleep_mock,
        patch(
            "data_generators.whoop_api.client.requests.get",
            side_effect=[rate_limited, success],
        ),
    ):
        client = WhoopClient(base_url="https://api.test")
        list(client.list_recovery("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"))
    sleep_mock.assert_called_once_with(1)


def test_get_body_measurement_returns_payload():
    from data_generators.whoop_api.client import WhoopClient

    payload = {"height_meter": 1.78, "weight_kilogram": 75.0, "max_heart_rate": 195}
    with (
        patch("data_generators.whoop_api.client.get_access_token", return_value="t"),
        patch(
            "data_generators.whoop_api.client.requests.get",
            return_value=_mock_response(200, payload),
        ),
    ):
        client = WhoopClient(base_url="https://api.test")
        body = client.get_body_measurement()
    assert body["height_meter"] == 1.78
    assert body["max_heart_rate"] == 195
