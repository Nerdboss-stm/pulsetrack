"""
Tests for PulseTrack wearable generator.

Run: pytest tests/test_wearable_generator.py -v
"""

import sys, os, json
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_generators.wearable_generator import generate_reading, USERS, DEVICE_METRICS


def parse_ts(ts_str):
    return datetime.fromisoformat(ts_str.replace("Z", ""))


def get_users_with_device_type(dtype):
    """Return (user, device) pairs for a given device type."""
    return [
        (user, device)
        for user in USERS
        for device in user["devices"]
        if device["device_type"] == dtype
    ]


class TestWearableEventStructure:

    def test_has_required_fields(self):
        """Every event must have reading_id, device_id, device_type,
        metrics, event_timestamp, sync_timestamp."""
        user = USERS[0]
        device = user["devices"][0]
        result = generate_reading(user, device)
        if result is None:
            return  # Sleep ring during daytime — skip
        event, _ = result
        required = ["reading_id", "device_id", "device_type", "metrics",
                    "event_timestamp", "sync_timestamp"]
        for field in required:
            assert field in event, f"Missing: {field}"

    def test_different_metrics_per_device_type(self):
        """Smartwatch has heart_rate_bpm; glucose_monitor has blood_glucose_mgdl."""
        # Find a smartwatch event
        sw_event = None
        for user, device in get_users_with_device_type("smartwatch"):
            result = generate_reading(user, device)
            if result:
                sw_event, _ = result
                break

        # Find a glucose_monitor event
        gm_event = None
        for user, device in get_users_with_device_type("glucose_monitor"):
            result = generate_reading(user, device)
            if result:
                gm_event, _ = result
                break

        assert sw_event is not None, "Could not find a smartwatch user"
        assert gm_event is not None, "Could not find a glucose_monitor user"

        assert "heart_rate_bpm" in sw_event["metrics"], \
            "Smartwatch must have heart_rate_bpm"
        assert "blood_glucose_mgdl" not in sw_event["metrics"], \
            "Smartwatch should NOT have blood_glucose_mgdl"

        assert "blood_glucose_mgdl" in gm_event["metrics"], \
            "Glucose monitor must have blood_glucose_mgdl"
        assert "heart_rate_bpm" not in gm_event["metrics"], \
            "Glucose monitor should NOT have heart_rate_bpm"

    def test_sync_after_event(self):
        """sync_timestamp must always be >= event_timestamp."""
        for user in USERS:
            for device in user["devices"]:
                result = generate_reading(user, device)
                if result is None:
                    continue
                event, _ = result
                event_ts = parse_ts(event["event_timestamp"])
                sync_ts  = parse_ts(event["sync_timestamp"])
                assert sync_ts >= event_ts, (
                    f"sync_timestamp {event['sync_timestamp']} is before "
                    f"event_timestamp {event['event_timestamp']}"
                )

    def test_some_late_events(self):
        """~30% of events should have sync_timestamp > 10 min after event_timestamp
        (batch sync simulation)."""
        late_count = 0
        total = 0
        for _ in range(200):
            user   = USERS[_ % len(USERS)]
            device = user["devices"][0]
            result = generate_reading(user, device)
            if result is None:
                continue
            event, _ = result
            total += 1
            diff = (parse_ts(event["sync_timestamp"]) - parse_ts(event["event_timestamp"])).total_seconds()
            if diff > 600:  # > 10 minutes
                late_count += 1

        assert late_count > 0, \
            f"Expected some late events (sync > 10 min after event) in {total} samples"

    def test_json_serializable(self):
        """Every event must be JSON-serializable for Kafka."""
        for i, user in enumerate(USERS[:20]):
            for device in user["devices"]:
                result = generate_reading(user, device)
                if result is None:
                    continue
                event, _ = result
                try:
                    parsed = json.loads(json.dumps(event))
                    assert parsed["reading_id"] == event["reading_id"]
                except (TypeError, ValueError) as e:
                    assert False, f"Event not JSON-serializable: {e}\nEvent: {event}"


class TestWearableDataQuality:

    def test_duplicate_rate_approximately_correct(self):
        """~2% of events should be duplicates (Bluetooth retry simulation)."""
        dup_count = 0
        total = 0
        for _ in range(500):
            user   = USERS[_ % len(USERS)]
            device = user["devices"][0]
            result = generate_reading(user, device)
            if result is None:
                continue
            total += 1
            _, is_dup = result
            if is_dup:
                dup_count += 1

        dup_rate = dup_count / total if total > 0 else 0
        assert 0.001 < dup_rate < 0.10, \
            f"Duplicate rate {dup_rate:.1%} outside expected range. Expected ~2%."

    def test_some_null_metric_values_exist(self):
        """~1% of readings should have a null metric value (sensor malfunction)."""
        null_count = 0
        for _ in range(500):
            user   = USERS[_ % len(USERS)]
            device = user["devices"][0]
            result = generate_reading(user, device)
            if result is None:
                continue
            event, _ = result
            if any(v is None for v in event["metrics"].values()):
                null_count += 1

        assert null_count > 0, "Expected some null metric values in 500 samples"

    def test_metrics_dict_not_empty(self):
        """Every event must have at least one metric."""
        for user in USERS[:10]:
            for device in user["devices"]:
                result = generate_reading(user, device)
                if result is None:
                    continue
                event, _ = result
                assert len(event["metrics"]) > 0, \
                    f"Metrics dict is empty for device type {event['device_type']}"

    def test_device_type_matches_metric_schema(self):
        """Each device type must only produce metrics defined in DEVICE_METRICS."""
        for user in USERS[:20]:
            for device in user["devices"]:
                result = generate_reading(user, device)
                if result is None:
                    continue
                event, _ = result
                expected_keys = set(DEVICE_METRICS[event["device_type"]].keys())
                actual_keys   = set(event["metrics"].keys())
                assert actual_keys <= expected_keys, (
                    f"{event['device_type']} produced unexpected metrics: "
                    f"{actual_keys - expected_keys}"
                )
