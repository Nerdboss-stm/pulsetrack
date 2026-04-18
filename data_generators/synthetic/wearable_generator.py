"""
PulseTrack Wearable Sensor Generator
=====================================
Simulates 4 device types with different metric schemas, batch sync behavior,
and realistic physiological patterns.

KEY COMPLEXITY:
- 4 device types with DIFFERENT metric sets (schema variation)
- Batch sync: events have event_timestamp (on-body) and sync_timestamp (received)
- Sleep ring only sends during sleep hours (10pm-7am)
- Glucose monitor sends every 5 min (lower frequency than other devices)
- Same user can have multiple devices (identity resolution: device → user → patient)

DATA QUALITY ISSUES:
- Batch sync creates "late" events (event_ts hours before sync_ts)
- 2% duplicate readings (Bluetooth retry)
- 1% null metric values (sensor malfunction)
- 0.3% impossible values (HR=0, SpO2=150) — firmware bug simulation

PDF Reference: DM pages 32-40 (event modelling), page 63 (dual timestamps)
"""

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta

from faker import Faker
from kafka import KafkaProducer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger(__name__)
fake = Faker()

# ── USERS AND THEIR DEVICES ──────────────────────
USERS = []
for i in range(100):
    email = fake.email()
    user = {
        "account_id": f"acct_{random.randint(10000, 99999)}",
        "email": email,
        "name": fake.name(),
        "age": random.randint(22, 75),
        "devices": []
    }

    # Each user has 1-3 devices
    device_types = random.sample(
        ["smartwatch", "chest_strap", "sleep_ring", "glucose_monitor"],
        k=random.randint(1, 3)
    )
    for dtype in device_types:
        prefix = {"smartwatch": "SW", "chest_strap": "CS", "sleep_ring": "SR", "glucose_monitor": "GM"}
        device_id = f"{prefix[dtype]}-{fake.bothify('???-#####').upper()}"
        user["devices"].append({
            "device_id": device_id,
            "device_type": dtype,
            "firmware_version": f"{random.randint(2,4)}.{random.randint(0,9)}.{random.randint(0,9)}"
        })

    USERS.append(user)

# ── METRIC DEFINITIONS PER DEVICE TYPE ────────────
DEVICE_METRICS = {
    "smartwatch": {
        "heart_rate_bpm": {"min": 45, "max": 180, "resting": (55, 80)},
        "spo2_pct": {"min": 90, "max": 100, "resting": (95, 99)},
        "steps_since_last": {"min": 0, "max": 200, "resting": (0, 5)},
        "skin_temp_celsius": {"min": 30, "max": 38, "resting": (33, 35)},
        "hrv_ms": {"min": 10, "max": 120, "resting": (30, 70)},
    },
    "chest_strap": {
        "heart_rate_bpm": {"min": 45, "max": 200, "resting": (55, 80)},
        "hrv_ms": {"min": 10, "max": 120, "resting": (30, 70)},
        "respiration_rate": {"min": 8, "max": 30, "resting": (12, 18)},
    },
    "sleep_ring": {
        "heart_rate_bpm": {"min": 40, "max": 100, "resting": (48, 65)},
        "spo2_pct": {"min": 88, "max": 100, "resting": (94, 99)},
        "skin_temp_celsius": {"min": 31, "max": 37, "resting": (34, 36)},
        "sleep_stage": {"values": ["awake", "light", "deep", "rem"]},
    },
    "glucose_monitor": {
        "blood_glucose_mgdl": {"min": 40, "max": 400, "resting": (70, 130)},
    },
}

# Track per-user baseline for realistic values
user_baselines = {}


def get_user_baseline(account_id, metric_name, metric_config):
    """Get or create a personal baseline for this user+metric."""
    key = f"{account_id}_{metric_name}"
    if key not in user_baselines:
        if "resting" in metric_config:
            low, high = metric_config["resting"]
            user_baselines[key] = random.uniform(low, high)
        else:
            user_baselines[key] = None
    return user_baselines[key]


def generate_reading(user, device):
    """Generate one sensor reading with realistic physiological values."""
    device_type = device["device_type"]
    metrics = DEVICE_METRICS[device_type]
    now = datetime.utcnow()

    # Sleep ring only sends during sleep (10pm - 7am)
    current_hour = now.hour
    if device_type == "sleep_ring" and 7 <= current_hour <= 22:
        return None  # Ring is "off" during daytime

    # Generate metric values
    reading_metrics = {}
    for metric_name, config in metrics.items():
        if metric_name == "sleep_stage":
            reading_metrics[metric_name] = random.choice(config["values"])
            continue

        baseline = get_user_baseline(user["account_id"], metric_name, config)
        if baseline:
            # Drift from personal baseline
            value = baseline + random.gauss(0, (config["max"] - config["min"]) * 0.02)
            value = max(config["min"], min(config["max"], round(value, 1)))
        else:
            value = round(random.uniform(config["min"], config["max"]), 1)

        reading_metrics[metric_name] = value

    # ── SIMULATE BATCH SYNC (the late-data challenge!) ──
    # With 30% probability, this reading is from the past (device was offline)
    sync_delay_seconds = 0
    if random.random() < 0.30:
        # Reading happened 10 min to 8 hours ago, syncing now
        sync_delay_seconds = random.randint(600, 28800)

    event_time = now - timedelta(seconds=sync_delay_seconds)

    event = {
        "reading_id": str(uuid.uuid4()),
        "device_id": device["device_id"],
        "device_type": device_type,
        "user_device_account_id": user["account_id"],
        "metrics": reading_metrics,
        "firmware_version": device["firmware_version"],
        "battery_pct": random.randint(5, 100),
        "event_timestamp": event_time.isoformat() + "Z",
        "sync_timestamp": now.isoformat() + "Z",
    }

    # ── INJECT DATA QUALITY ISSUES ──
    # 1% null metric value
    if random.random() < 0.01 and reading_metrics:
        null_metric = random.choice(list(reading_metrics.keys()))
        event["metrics"][null_metric] = None

    # 0.3% impossible value (firmware bug)
    if random.random() < 0.003 and reading_metrics:
        bug_metric = random.choice([m for m in reading_metrics if m != "sleep_stage"])
        event["metrics"][bug_metric] = random.choice([0, -1, 999, 150.0])

    # 2% duplicate
    is_duplicate = random.random() < 0.02

    return event, is_duplicate


def main():
    total_devices = sum(len(u["devices"]) for u in USERS)
    log.info(
        "PulseTrack Wearable Generator starting",
        extra={"extra_data": {
            "kafka_bootstrap": settings.kafka_bootstrap,
            "topic": settings.kafka_topic_sensor,
            "users": len(USERS),
            "devices": total_devices,
            "events_per_second": settings.wearable_events_per_second,
        }},
    )

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
    )

    event_count = 0
    late_count = 0

    try:
        while True:
            user = random.choice(USERS)
            device = random.choice(user["devices"])

            result = generate_reading(user, device)
            if result is None:
                continue  # Sleep ring during daytime — skip

            event, is_duplicate = result
            key = event["device_id"]

            # Check if this is a "late" event
            if event["event_timestamp"] != event["sync_timestamp"]:
                late_count += 1

            producer.send(settings.kafka_topic_sensor, key=key, value=event)
            event_count += 1

            if is_duplicate:
                producer.send(settings.kafka_topic_sensor, key=key, value=event)
                event_count += 1

            if event_count % 100 == 0:
                log.info(
                    "Generator progress",
                    extra={"extra_data": {
                        "events": event_count,
                        "late": late_count,
                        "device_type": device["device_type"],
                        "metric_sample": list(event["metrics"].keys())[:2],
                    }},
                )

            time.sleep(1.0 / settings.wearable_events_per_second)

    except KeyboardInterrupt:
        log.info(
            "Wearable generator stopped",
            extra={"extra_data": {"total_events": event_count, "late_sync": late_count}},
        )
        producer.close()


if __name__ == "__main__":
    main()
