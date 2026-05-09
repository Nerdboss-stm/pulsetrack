"""
PulseTrack MSK Serverless — IAM auth end-to-end verification.

Runs on an EMR EC2 node where the instance profile carries
kafka-cluster:Connect / *Topic* / WriteData / ReadData permissions
(see infrastructure/modules/iam/main.tf, msk_iam_auth policy).

Steps:
  1. Generate IAM SASL/OAUTHBEARER token via aws-msk-iam-sasl-signer-python.
  2. AdminClient.create_topics — idempotent, MSK Serverless requires RF=3.
  3. Producer → flush, capture broker-assigned offset.
  4. Consumer → poll until we see the exact value we just produced.
  5. Print PASS/FAIL with offsets so the assertion is operator-visible.

Why this matters:
  * Proves the IAM policy is sufficient (not over- or under-scoped).
  * Proves the MSK SG ingress on 9098 from EMR resolves at runtime.
  * Proves IMDSv2 → instance role → SASL signer wiring works on the node.

Usage on EMR master (after `pip install aws-msk-iam-sasl-signer-python`):

    python3 verify_msk_iam.py <brokers> <topic>

Exit code 0 = pass.
"""

from __future__ import annotations

import socket
import sys
import time
import uuid

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

REGION = "us-east-1"
PRODUCE_TIMEOUT_S = 30
CONSUME_TIMEOUT_S = 30


def oauth_cb(_oauth_config: str) -> tuple[str, float]:
    """SASL/OAUTHBEARER token provider for MSK IAM.

    confluent-kafka expects (token, expiry_unix_seconds).
    The signer returns (token, expiry_ms_from_now).
    """
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
    """Idempotent topic create for MSK Serverless (RF=3 mandatory).

    Note: confluent-kafka's AdminClient does NOT have a background polling
    thread, so OAUTHBEARER's `oauth_cb` is never invoked unless we manually
    pump the event loop with `poll()`. Without this, the SASL handshake
    stalls forever and create_topics times out with no useful error.
    """
    admin = AdminClient(base_config(brokers))
    # Drive the event loop so oauth_cb gets called for the initial SASL handshake.
    for _ in range(10):
        admin.poll(0.5)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=3)]
    )
    for name, future in futures.items():
        # Pump the loop while the create is in flight too.
        deadline = time.time() + 60
        while not future.done() and time.time() < deadline:
            admin.poll(0.5)
        try:
            future.result(timeout=5)
            print(f"[msk] Created topic: {name}", flush=True)
        except KafkaException as exc:
            msg = str(exc).lower()
            if "already exists" in msg or "topic_already_exists" in msg:
                print(f"[msk] Topic {name} already exists (OK)", flush=True)
            else:
                raise


def produce_one(brokers: str, topic: str, value: str) -> int:
    deliveries: list = []

    def cb(err, msg):
        deliveries.append((err, msg.offset() if msg else None))

    producer = Producer(base_config(brokers))
    producer.produce(topic, value=value.encode(), callback=cb)
    remaining = producer.flush(PRODUCE_TIMEOUT_S)
    assert remaining == 0, f"producer.flush left {remaining} unsent"
    err, offset = deliveries[0]
    assert err is None, f"producer error: {err}"
    return offset


def consume_until(brokers: str, topic: str, expected_value: str) -> int:
    cfg = {
        **base_config(brokers),
        "group.id": f"pulsetrack-smoke-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(cfg)
    consumer.subscribe([topic])
    try:
        deadline = time.time() + CONSUME_TIMEOUT_S
        while time.time() < deadline:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(f"consumer error: {msg.error()}")
            if msg.value().decode() == expected_value:
                return msg.offset()
        raise AssertionError(
            f"did not consume {expected_value!r} within {CONSUME_TIMEOUT_S}s"
        )
    finally:
        consumer.close()


def main(brokers: str, topic: str) -> None:
    print(f"[msk] region={REGION}", flush=True)
    print(f"[msk] brokers={brokers}", flush=True)
    print(f"[msk] topic={topic}", flush=True)

    ensure_topic(brokers, topic)

    test_value = f"pulsetrack-smoke-{uuid.uuid4().hex[:12]}"
    produce_offset = produce_one(brokers, topic, test_value)
    print(
        f"[msk] Produced offset={produce_offset} value={test_value}", flush=True
    )

    consume_offset = consume_until(brokers, topic, test_value)
    print(
        f"[msk] Consumed offset={consume_offset} value={test_value}", flush=True
    )

    assert produce_offset == consume_offset, (
        f"offset mismatch produced={produce_offset} consumed={consume_offset}"
    )
    print(
        "[msk] ALL CHECKS PASSED ✓ "
        "(Connect + CreateTopic + WriteData + ReadData via IAM)",
        flush=True,
    )


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: verify_msk_iam.py <brokers> <topic>", file=sys.stderr)
        sys.exit(2)
    main(sys.argv[1], sys.argv[2])
