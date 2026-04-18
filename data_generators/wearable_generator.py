"""
PulseTrack Wearable Generator (real-data variant).

Produces medically-accurate sensor readings using the physiological model in
:mod:`data_generators.vitals_model`, serializes them with Avro through the
Schema Registry, and publishes to Kafka topic `sensor_readings`.

Each simulated patient maintains an ongoing activity state (Markov chain) so
that values evolve plausibly over time rather than jittering randomly.

For an offline / no-registry alternative, see ``data_generators/synthetic/``.
"""
from __future__ import annotations

import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from faker import Faker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import settings  # noqa: E402
from data_generators.vitals_model import (  # noqa: E402
    ActivityState,
    PatientProfile,
    blood_pressure,
    generate_reading,
    respiration_rate,
    sleep_stage_index,
    steps_for_state,
)
from logger import get_logger  # noqa: E402
from metrics import (  # noqa: E402
    records_failed,
    records_processed,
    start_metrics_server,
)
from schemas.registry import (  # noqa: E402
    get_avro_serializer,
    register_all_schemas,
)

log = get_logger(__name__)
fake = Faker()

# Device → metric subset. Keys must align with sensor_reading.avsc DeviceType
# enum: smartwatch, chest_strap, sleep_ring, blood_pressure_cuff.
DEVICE_METRICS: dict[str, list[str]] = {
    "smartwatch":          ["heart_rate_bpm", "spo2_pct", "hrv_ms", "skin_temp_celsius", "steps_since_last"],
    "chest_strap":         ["heart_rate_bpm", "hrv_ms", "respiration_rate"],
    "sleep_ring":          ["heart_rate_bpm", "spo2_pct", "skin_temp_celsius", "sleep_stage"],
    "blood_pressure_cuff": ["bp_systolic_mmhg", "bp_diastolic_mmhg"],
}

DEVICE_PREFIX = {
    "smartwatch": "SW",
    "chest_strap": "CS",
    "sleep_ring": "SR",
    "blood_pressure_cuff": "BP",
}


# ── User & device factory ─────────────────────────────────────────────────────
def make_user(account_id: str) -> dict:
    profile = PatientProfile.random(patient_id=account_id)
    chosen = random.sample(list(DEVICE_METRICS.keys()), k=random.randint(1, 3))
    devices = [
        {
            "device_id": f"{DEVICE_PREFIX[t]}-{fake.bothify('???-#####').upper()}",
            "device_type": t,
            "firmware_version": f"{random.randint(2, 4)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
        }
        for t in chosen
    ]
    return {
        "account_id": account_id,
        "email": fake.email(),
        "profile": profile,
        "devices": devices,
        "last_reading_ts": datetime.utcnow(),
    }


def device_metrics_for(user: dict, device: dict, ts: datetime) -> dict | None:
    """Generate metrics for `device` at `ts`. Returns None if device is silent."""
    # Sleep ring only emits during 22:00–07:00
    if device["device_type"] == "sleep_ring" and not (ts.hour >= 22 or ts.hour < 7):
        return None
    # BP cuff is intermittent (real cuffs are taken once or twice a day)
    if device["device_type"] == "blood_pressure_cuff" and random.random() > 0.02:
        return None

    reading = generate_reading(user["profile"], ts)
    state = ActivityState(reading["activity_state"])
    interval = max(1.0, (ts - user["last_reading_ts"]).total_seconds())

    out: dict[str, float] = {}
    for m in DEVICE_METRICS[device["device_type"]]:
        if m == "respiration_rate":
            out[m] = float(respiration_rate(state))
        elif m == "steps_since_last":
            out[m] = float(steps_for_state(state, interval))
        elif m == "sleep_stage":
            out[m] = float(sleep_stage_index(ts))
        elif m in ("bp_systolic_mmhg", "bp_diastolic_mmhg"):
            sys_v, dia_v = blood_pressure(user["profile"], state)
            out["bp_systolic_mmhg"] = float(sys_v)
            out["bp_diastolic_mmhg"] = float(dia_v)
            break
        else:
            out[m] = float(reading[m])
    return out


# ── Avro event construction ───────────────────────────────────────────────────
def build_event(user: dict, device: dict, metrics: dict, ts: datetime) -> dict:
    sync_delay = random.randint(600, 28800) if random.random() < 0.30 else 0
    event_ts = ts - timedelta(seconds=sync_delay)
    return {
        "reading_id": str(uuid.uuid4()),
        "device_id": device["device_id"],
        "device_type": device["device_type"],
        "user_device_account_id": user["account_id"],
        "patient_email": user["email"],
        "metrics": metrics,
        "firmware_version": device["firmware_version"],
        "battery_pct": random.randint(5, 100),
        "event_timestamp": int(event_ts.timestamp() * 1000),
        "sync_timestamp": int(ts.timestamp() * 1000),
    }


def _on_delivery(err, msg):
    if err is not None:
        records_failed.labels(layer="bronze", source="wearable", reason="delivery_error").inc()
        log.error(
            "Kafka delivery failed",
            extra={"extra_data": {"topic": msg.topic(), "error": str(err)}},
        )


# ── Main loop ─────────────────────────────────────────────────────────────────
def main(num_users: int = 100, metrics_port: int = 8000):
    start_metrics_server(metrics_port)

    register_all_schemas()
    subject = f"{settings.kafka_topic_sensor}-value"
    serializer = get_avro_serializer(subject)
    key_serializer = StringSerializer()

    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 50,
    })

    users = [make_user(f"acct_{i:05d}") for i in range(num_users)]
    total_devices = sum(len(u["devices"]) for u in users)
    log.info(
        "Wearable generator (real) started",
        extra={"extra_data": {
            "users": len(users),
            "devices": total_devices,
            "topic": settings.kafka_topic_sensor,
            "schema_registry": settings.schema_registry_url,
            "events_per_second": settings.wearable_events_per_second,
            "metrics_port": metrics_port,
        }},
    )

    sent = 0
    errors = 0
    last_log_at = time.time()

    try:
        while True:
            user = random.choice(users)
            device = random.choice(user["devices"])
            ts = datetime.utcnow()
            metrics_payload = device_metrics_for(user, device, ts)
            user["last_reading_ts"] = ts
            if metrics_payload is None:
                continue

            event = build_event(user, device, metrics_payload, ts)
            try:
                value_bytes = serializer(
                    event,
                    SerializationContext(settings.kafka_topic_sensor, MessageField.VALUE),
                )
                producer.produce(
                    topic=settings.kafka_topic_sensor,
                    key=key_serializer(event["device_id"]),
                    value=value_bytes,
                    on_delivery=_on_delivery,
                )
                sent += 1
                records_processed.labels(layer="bronze", source="wearable").inc()
            except Exception:
                errors += 1
                records_failed.labels(layer="bronze", source="wearable", reason="serialize_error").inc()
                log.error("Serialize/produce failed", exc_info=True)

            producer.poll(0)

            now = time.time()
            if now - last_log_at >= 5.0:
                rate = sent / (now - last_log_at) if (now - last_log_at) > 0 else 0
                log.info(
                    "Producer progress",
                    extra={"extra_data": {
                        "sent": sent,
                        "errors": errors,
                        "events_per_second": round(rate, 2),
                    }},
                )
                last_log_at = now
                sent = 0  # rate-window counter

            time.sleep(1.0 / settings.wearable_events_per_second)

    except KeyboardInterrupt:
        producer.flush(10)
        log.info(
            "Wearable generator stopped",
            extra={"extra_data": {"final_window_sent": sent, "errors": errors}},
        )


if __name__ == "__main__":
    main()
