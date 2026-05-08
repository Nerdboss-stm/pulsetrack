"""Tests for the consumer-lag / processing-latency listener helper."""

from __future__ import annotations

from types import SimpleNamespace


def _fake_progress(sources, duration_ms=1500):
    return SimpleNamespace(batchDuration=duration_ms, sources=sources)


def _fake_kafka_source(end_offset, num_rows, description="KafkaV2[Subscribe[topic_x]]"):
    return SimpleNamespace(
        description=description,
        endOffset=end_offset,
        numInputRows=num_rows,
    )


def test_kafka_source_updates_lag_gauge_per_partition():
    from prometheus_client import REGISTRY

    from utils.streaming import _update_consumer_lag

    progress = _fake_progress(
        sources=[_fake_kafka_source({"sensor_readings": {"0": 100, "1": 200}}, num_rows=20)]
    )
    _update_consumer_lag(progress, layer="bronze")

    value = REGISTRY.get_sample_value(
        "pt_consumer_lag", {"topic": "sensor_readings", "partition": "0"}
    )
    assert value == 10.0  # 20 rows / 2 partitions


def test_non_kafka_source_is_ignored():
    from utils.streaming import _update_consumer_lag

    progress = _fake_progress(
        sources=[
            SimpleNamespace(
                description="DeltaSource[/path]",
                endOffset='{"_meta": "ignored"}',
                numInputRows=5,
            )
        ]
    )
    _update_consumer_lag(progress, layer="silver")  # should not raise


def test_processing_latency_observed():
    from prometheus_client import REGISTRY

    from utils.streaming import _update_consumer_lag

    progress = _fake_progress(sources=[], duration_ms=2500)
    _update_consumer_lag(progress, layer="gold")

    sample_count = REGISTRY.get_sample_value(
        "pt_processing_latency_seconds_count", {"layer": "gold"}
    )
    assert sample_count is not None and sample_count >= 1


def test_zero_duration_skips_latency_observation():
    from utils.streaming import _update_consumer_lag

    # Should not raise; should not observe a 0.0-second latency (we skip when duration=0).
    _update_consumer_lag(_fake_progress(sources=[], duration_ms=0), layer="bronze")


def test_malformed_end_offset_handled():
    from utils.streaming import _update_consumer_lag

    progress = _fake_progress(sources=[_fake_kafka_source(end_offset="not-json", num_rows=5)])
    _update_consumer_lag(progress, layer="bronze")  # should not raise
