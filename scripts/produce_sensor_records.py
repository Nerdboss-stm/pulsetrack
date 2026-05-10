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
DEVICE_TYPES = ["smartwatch", "chest_strap", "sleep_ring"]


def make_record(seq: int, base_ts: datetime) -> dict:
    """Synthesize one SensorReading.

    device_id is per-(user, device_type) — same WHOOP-style format
    ``WT-A01-12345`` per the bronze GX regex. With ``N_USERS=50`` and
    3 device types this produces 150 distinct devices, so dim_device
    has the SCD2 cardinality the resume claim implies (one row per
    (device, firmware) version). Reusing device_id_00/01/02 across all
    users (the prior implementation) collapsed dim_device to 3 rows.
    """
    user_idx = seq % N_USERS
    device_idx = seq % len(DEVICE_TYPES)
    device_type = DEVICE_TYPES[device_idx]
    # Per-(user, device_type) globally-unique id matching the bronze
    # GX regex ``^[A-Z]{2}-[A-Z0-9]{3}-\d{5}$``. Letter prefix encodes
    # device family (WT smartwatch, CS chest strap, SR sleep ring);
    # numeric suffix encodes the user.
    type_prefix = {"smartwatch": "WT", "chest_strap": "CS", "sleep_ring": "SR"}[device_type]
    device_id = f"{type_prefix}-A{device_idx:02d}-{user_idx:05d}"
    # Stagger event_timestamp across N_DAYS days × 24 hours so daily
    # aggregations have meaningful grain.
    event_ts = base_ts - timedelta(hours=seq % (24 * N_DAYS))
    sync_ts = event_ts + timedelta(seconds=30)
    return {
        "reading_id": str(uuid.uuid4()),
        "device_id": device_id,
        "device_type": device_type,
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
    parser.add_argument(
        "--count",
        type=int,
        default=200,
        help=(
            "Total records when --mode=once. In --mode=continuous, the count "
            "per cycle (the producer cycles forever)."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=["once", "continuous"],
        default="once",
        help=(
            "once: emit COUNT records and exit (the original behavior). "
            "continuous: emit COUNT records per cycle, sleep INTERVAL, repeat. "
            "Use continuous to keep the streaming bronze->silver->gold "
            "queries fed for an end-to-end live demo."
        ),
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=10.0,
        help="Sleep between cycles in --mode=continuous.",
    )
    args = parser.parse_args()

    schema = fastavro.schema.load_schema(str(SCHEMA_FILE))
    parsed_schema = fastavro.parse_schema(schema)

    print(f"[producer] brokers={args.brokers}", flush=True)
    print(f"[producer] topic={args.topic}", flush=True)
    print(
        f"[producer] mode={args.mode} count={args.count} "
        f"interval={args.interval_seconds}s",
        flush=True,
    )

    ensure_topic(args.brokers, args.topic)
    producer = Producer(base_config(args.brokers))

    cycle = 0
    while True:
        cycle += 1
        # Each cycle uses a new ``base_ts`` so timestamps fan out correctly.
        # ``seq_offset`` keeps reading_id uniqueness across cycles (uuid4
        # already handles this, but using the offset for the user/device
        # spread keeps the per-cycle data shape varied across cycles too).
        seq_offset = (cycle - 1) * args.count
        base_ts = datetime.now(timezone.utc)

        delivered = 0
        failed = 0

        def cb(err, msg):
            nonlocal delivered, failed
            if err:
                failed += 1
                print(
                    f"[producer] DELIVERY FAILED: {err}",
                    file=sys.stderr,
                    flush=True,
                )
            else:
                delivered += 1

        for offset in range(args.count):
            seq = seq_offset + offset
            rec = make_record(seq, base_ts)
            payload = encode_avro(rec, parsed_schema)
            producer.produce(
                args.topic,
                key=rec["user_device_account_id"].encode(),
                value=payload,
                callback=cb,
            )
            # Pump the queue periodically so oauth_cb fires + memory stays bounded.
            if offset % 50 == 0:
                producer.poll(0)

        remaining = producer.flush(60)
        if remaining > 0:
            raise RuntimeError(f"producer.flush left {remaining} unsent")

        print(
            f"[producer] cycle={cycle} delivered={delivered} failed={failed} "
            f"(submitted={args.count}, total_delivered={cycle * args.count - failed})",
            flush=True,
        )
        if failed:
            sys.exit(1)
        if args.mode == "once":
            break
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
