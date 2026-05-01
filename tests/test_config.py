"""Settings load order, env-var overrides, computed paths."""
from __future__ import annotations


def test_default_paths(fresh_settings):
    s = fresh_settings()
    assert s.lakehouse_base == "/tmp/pulsetrack-lakehouse"
    assert s.bronze_sensor.endswith("/bronze/sensor_readings")
    assert s.silver_sensor.endswith("/silver/sensor_readings")
    assert s.gold_fact_vital_daily.endswith("/gold/fact_vital_daily_summary")


def test_env_override_lakehouse_base(fresh_settings, monkeypatch):
    monkeypatch.setenv("PT_LAKEHOUSE_BASE", "/data/pt")
    s = fresh_settings()
    assert s.lakehouse_base == "/data/pt"
    assert s.bronze_sensor == "/data/pt/bronze/sensor_readings"
    assert s.gold_fact_vital_daily == "/data/pt/gold/fact_vital_daily_summary"


def test_env_override_kafka(fresh_settings, monkeypatch):
    monkeypatch.setenv("PT_KAFKA_BOOTSTRAP", "broker:29092")
    monkeypatch.setenv("PT_KAFKA_TOPIC_SENSOR", "vitals_v2")
    s = fresh_settings()
    assert s.kafka_bootstrap == "broker:29092"
    assert s.kafka_topic_sensor == "vitals_v2"


def test_dlq_quarantine_paths_under_lakehouse(fresh_settings):
    s = fresh_settings()
    assert s.dlq.startswith(s.lakehouse_base)
    assert s.quarantine.startswith(s.lakehouse_base)
    assert s.checkpoint_base.startswith(s.lakehouse_base)


def test_silver_paths_match_bronze_root(fresh_settings):
    s = fresh_settings()
    assert s.silver_sensor.startswith(s.silver_base)
    assert s.silver_ehr_conditions.startswith(s.silver_base)
    assert s.silver_identity_bridge.startswith(s.silver_base)


def test_threshold_defaults(fresh_settings):
    s = fresh_settings()
    assert s.late_arrival_threshold_seconds == 7200
    assert s.max_offsets_per_trigger == 10000
    assert s.trigger_interval == "30 seconds"
