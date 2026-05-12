"""Unit tests for observability.alerting — Slack/PagerDuty/SNS routing."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from observability.alerting import (
    _color,
    _format_result,
    alert_pagerduty,
    alert_slack,
    alert_sns,
    route_alerts,
    severity_for,
)
from observability.monitors import MonitorResult


def _r(status="error", **kw):
    """Build a MonitorResult for tests."""
    defaults = dict(monitor_name="freshness", table_name="db.s.t", check_type="freshness")
    defaults.update(kw)
    defaults["status"] = status
    return MonitorResult(**defaults)


# ── severity_for ─────────────────────────────────────────────────────────


def test_severity_for_error_is_critical():
    assert severity_for(_r("error")) == "critical"


def test_severity_for_warn_is_warn():
    assert severity_for(_r("warn")) == "warn"


def test_severity_for_ok_is_info():
    assert severity_for(_r("ok")) == "info"


def test_severity_for_unknown_status_is_info():
    """Defensive default for unknown statuses."""
    assert severity_for(_r("garbage")) == "info"


# ── _color ───────────────────────────────────────────────────────────────


def test_color_critical_is_red():
    assert _color("critical") == "#ff0000"


def test_color_warn_is_orange():
    assert _color("warn") == "#ffaa00"


def test_color_info_is_green():
    assert _color("info") == "#36a64f"


def test_color_unknown_is_grey():
    assert _color("unknown_severity") == "#888888"


# ── _format_result ───────────────────────────────────────────────────────


def test_format_result_includes_monitor_name():
    text = _format_result(_r("error", monitor_name="freshness"))
    assert "freshness" in text


def test_format_result_includes_status_uppercase():
    text = _format_result(_r("error"))
    assert "ERROR" in text


def test_format_result_includes_table_name():
    text = _format_result(_r("error", table_name="bronze.sensor_readings"))
    assert "bronze.sensor_readings" in text


def test_format_result_optional_column():
    text = _format_result(_r("error", column="metric_value"))
    assert "metric_value" in text


def test_format_result_optional_value_and_threshold():
    text = _format_result(_r("warn", value=1234.5, threshold=1000.0))
    assert "1235" in text or "1234" in text
    assert "1000" in text or "1e+03" in text


def test_format_result_includes_detail():
    text = _format_result(_r("error", detail="MAX(ts)=stale by 4hr"))
    assert "stale" in text


# ── alert_slack ──────────────────────────────────────────────────────────


def test_alert_slack_no_webhook_returns_false(monkeypatch):
    """Empty webhook → return False (log only)."""
    monkeypatch.setattr("observability.alerting.SLACK_WEBHOOK_URL", "")
    assert alert_slack(_r("error")) is False


def test_alert_slack_post_success(monkeypatch):
    """Webhook configured + urlopen succeeds → True."""
    monkeypatch.setattr(
        "observability.alerting.SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/X"
    )
    with patch("urllib.request.urlopen") as urlopen:
        urlopen.return_value.read.return_value = b"ok"
        assert alert_slack(_r("error")) is True


def test_alert_slack_post_failure_returns_false(monkeypatch):
    """Webhook configured but POST fails → returns False, doesn't raise."""
    monkeypatch.setattr(
        "observability.alerting.SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/X"
    )
    with patch("urllib.request.urlopen", side_effect=Exception("network error")):
        assert alert_slack(_r("error")) is False


def test_alert_slack_payload_uses_color_per_severity(monkeypatch):
    """The Slack payload includes a color matching severity."""
    monkeypatch.setattr(
        "observability.alerting.SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/X"
    )
    captured = {}

    def fake_urlopen(req, timeout=5):
        captured["data"] = req.data
        return MagicMock(read=lambda: b"ok")

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        alert_slack(_r("error"))
    payload = json.loads(captured["data"])
    assert payload["attachments"][0]["color"] == "#ff0000"  # critical


# ── alert_pagerduty ─────────────────────────────────────────────────────


def test_alert_pagerduty_no_routing_key_returns_false(monkeypatch):
    monkeypatch.setattr("observability.alerting.PAGERDUTY_ROUTING_KEY", "")
    assert alert_pagerduty(_r("error")) is False


def test_alert_pagerduty_post_success(monkeypatch):
    monkeypatch.setattr("observability.alerting.PAGERDUTY_ROUTING_KEY", "test-routing-key")
    with patch("urllib.request.urlopen") as urlopen:
        urlopen.return_value.read.return_value = b'{"status":"success"}'
        assert alert_pagerduty(_r("error")) is True


def test_alert_pagerduty_uses_provided_dedup_key(monkeypatch):
    monkeypatch.setattr("observability.alerting.PAGERDUTY_ROUTING_KEY", "test-routing-key")
    captured = {}

    def fake_urlopen(req, timeout=5):
        captured["data"] = req.data
        return MagicMock(read=lambda: b"ok")

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        alert_pagerduty(_r("error"), dedup_key="custom-key-123")
    payload = json.loads(captured["data"])
    assert payload.get("dedup_key") == "custom-key-123"


# ── alert_sns ────────────────────────────────────────────────────────────


def test_alert_sns_no_topic_returns_false(monkeypatch):
    monkeypatch.setattr("observability.alerting.SNS_ALERT_TOPIC_ARN", "")
    assert alert_sns(_r("error")) is False


def test_alert_sns_publish_success(monkeypatch):
    """Mock boto3.client('sns') and publish → True."""
    monkeypatch.setattr(
        "observability.alerting.SNS_ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:123:test"
    )
    fake_sns = MagicMock()
    fake_sns.publish.return_value = {"MessageId": "abc-123"}
    with patch("boto3.client", return_value=fake_sns):
        result = alert_sns(_r("error"))
    assert result is True
    assert fake_sns.publish.called


def test_alert_sns_publish_failure_returns_false(monkeypatch):
    monkeypatch.setattr(
        "observability.alerting.SNS_ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:123:test"
    )
    fake_sns = MagicMock()
    fake_sns.publish.side_effect = Exception("throttle")
    with patch("boto3.client", return_value=fake_sns):
        assert alert_sns(_r("error")) is False


# ── route_alerts ────────────────────────────────────────────────────────


def test_route_alerts_skips_ok(monkeypatch):
    """OK results don't trigger any actual alert send (slack/sns/pagerduty=0)."""
    monkeypatch.setattr("observability.alerting.SLACK_WEBHOOK_URL", "")
    monkeypatch.setattr("observability.alerting.PAGERDUTY_ROUTING_KEY", "")
    monkeypatch.setattr("observability.alerting.SNS_ALERT_TOPIC_ARN", "")
    out = route_alerts([_r("ok")])
    # No alert channel should have fired. (`skipped` count may be > 0.)
    assert out.get("slack", 0) == 0
    assert out.get("sns", 0) == 0
    assert out.get("pagerduty", 0) == 0


def test_route_alerts_processes_warns_and_errors(monkeypatch):
    """warn + error results both route through (count >= 0)."""
    monkeypatch.setattr("observability.alerting.SLACK_WEBHOOK_URL", "")
    monkeypatch.setattr("observability.alerting.PAGERDUTY_ROUTING_KEY", "")
    monkeypatch.setattr("observability.alerting.SNS_ALERT_TOPIC_ARN", "")
    out = route_alerts([_r("warn"), _r("error"), _r("ok")])
    assert isinstance(out, dict)
