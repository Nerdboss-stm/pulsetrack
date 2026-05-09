"""
Push synthetic SensorReading records into MSK Serverless via OAUTHBEARER.

Why this script exists:
    The project's `data_generators/whoop_api/producer.py` uses the Java JAAS
    config from `streaming/kafka_helpers.py` (sasl.mechanism=AWS_MSK_IAM,
    sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule). librdkafka
    (which confluent-kafka-python wraps) does NOT support AWS_MSK_IAM as a
    mechanism. It only speaks OAUTHBEARER.

    For the cloud end-to-end demo we need *something* on the topic. This
    builds Avro-encoded records matching `schemas/sensor_reading.avsc` and
    publishes them with the same OAUTHBEARER pattern verify_msk_iam.py
    proved out earlier.

Run locally with the `pulsetrack` AWS profile:
    AWS_PROFILE=pulsetrack AWS_DEFAULT_REGION=us-east-1 \\
        python3 scripts/produce_sensor_records.py \\
            --brokers boot-feh0wbx9.c3.kafka-serverless.us-east-1.amazonaws.com:9098 \\
            --topic   sensor_readings \\
            --count   200
"""

from __future__ import annotations

import argparse
import io
import random
import socket
import struct
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fastavro
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

REGION = "us-east-1"
SCHEMA_FILE = (
    Path(__file__).resolve().parent.parent / "schemas" / "sensor_reading.avsc"
)


def oauth_cb(_oauth_config: str) -> tuple[str, float]:
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return token, time.time() + expiry_ms / 1000.0


def base_config(brokers: str) -> dict:
    return {
        "bootstrap.servers": brokers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
        "client.id": socket.gethostname(),
    }


def ensure_topic(brokers: str, topic: str) -> None:
    admin = AdminClient(base_config(brokers))
    # AdminClient has no background poll thread — drive oauth_cb manually.
    for _ in range(10):
        admin.poll(0.5)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=3)]
    )
    for name, future in futures.items():
        deadline = time.time() + 60
        while not future.done() and time.time() < deadline:
            admin.poll(0.5)
        try:
            future.result(timeout=5)
            print(f"[producer] Created topic: {name}", flush=True)
        except KafkaException as exc:
            if "already exists" in str(exc).lower():
                print(f"[producer] Topic {name} already exists (OK)", flush=True)
            else:
                raise


N_USERS = 50
N_DAYS = 30


def make_record(seq: int, base_ts: datetime) -> dict:
    """Synthesize one SensorReading. Wider user/day spread than the smoke
    test so Gold (grouped by patient_key × metric × date_key) has thousands
    of rows, matching the volume the local pipeline carries."""
    user_idx = seq % N_USERS
    device_idx = seq % 3
    device_types = ["smartwatch", "chest_strap", "sleep_ring"]
    # Stagger event_timestamp across N_DAYS days × 24 hours so daily
    # aggregations have meaningful grain.
    event_ts = base_ts - timedelta(hours=seq % (24 * N_DAYS))
    sync_ts = event_ts + timedelta(seconds=30)
    return {
        "reading_id": str(uuid.uuid4()),
        "device_id": f"device_{device_idx:02d}",
        "device_type": device_types[device_idx],
        "user_device_account_id": f"acct_{user_idx:05d}",
        "patient_email": f"user{user_idx}@example.com",
        # Metric names must match the gold/silver vocabulary in
        # data_quality.expectations.silver_sensor_suite (value_set check).
        # Wrong names → MERGE is skipped at the quality gate.
        "metrics": {
            "heart_rate_bpm": 60.0 + random.uniform(-5, 35),
            "hrv_ms": 30.0 + random.uniform(-10, 50),
            "spo2_pct": 95.0 + random.uniform(0, 5),
            "skin_temp_celsius": 32.0 + random.uniform(-1.5, 1.5),
        },
        "firmware_version": "1.2.3",
        "battery_pct": random.randint(20, 100),
        "event_timestamp": int(event_ts.timestamp() * 1000),
        "sync_timestamp": int(sync_ts.timestamp() * 1000),
        "source_type": "simulator",
    }


def encode_avro(record: dict, schema: dict) -> bytes:
    """Confluent wire format: 1 magic byte (0) + 4 schema-id bytes + Avro binary.

    The bronze_ingestion decoder strips bytes 0..5 before from_avro, so the
    schema-id we put here is never validated. Use 1 — matches what most
    Confluent Schema Registry clients emit for a fresh subject.
    """
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    avro_payload = buf.getvalue()
    return b"\x00" + struct.pack(">I", 1) + avro_payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", required=True)
    parser.add_argument("--topic", default="sensor_readings")
    parser.add_argument("--count", type=int, default=200)
    args = parser.parse_args()

    schema = fastavro.schema.load_schema(str(SCHEMA_FILE))
    parsed_schema = fastavro.parse_schema(schema)

    print(f"[producer] brokers={args.brokers}", flush=True)
    print(f"[producer] topic={args.topic}", flush=True)
    print(f"[producer] count={args.count}", flush=True)

    ensure_topic(args.brokers, args.topic)

    producer = Producer(base_config(args.brokers))
    base_ts = datetime.now(timezone.utc)

    delivered = 0
    failed = 0

    def cb(err, msg):
        nonlocal delivered, failed
        if err:
            failed += 1
            print(f"[producer] DELIVERY FAILED: {err}", file=sys.stderr, flush=True)
        else:
            delivered += 1

    for seq in range(args.count):
        rec = make_record(seq, base_ts)
        payload = encode_avro(rec, parsed_schema)
        producer.produce(
            args.topic,
            key=rec["user_device_account_id"].encode(),
            value=payload,
            callback=cb,
        )
        # Pump the queue periodically so oauth_cb fires + memory stays bounded.
        if seq % 50 == 0:
            producer.poll(0)

    remaining = producer.flush(60)
    if remaining > 0:
        raise RuntimeError(f"producer.flush left {remaining} unsent")

    print(
        f"[producer] DONE — delivered={delivered} failed={failed} "
        f"(total submitted={args.count})",
        flush=True,
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
