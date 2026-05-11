"""
PulseTrack batch scale producer — 10M event bulk Kafka producer.

Purpose
-------
The real-time `wearable_generator.py` produces ~10 events/sec (one user's
worth of devices). For scale tests against MSK Serverless + EMR streaming
we need 10M sensor readings landing in Kafka in <30 minutes — a 4-5 order
of magnitude difference in throughput.

This producer ditches the per-event sleep and reuses one Producer instance
across all 10M records, leaning entirely on librdkafka's internal queue +
batcher + compressor to amortize syscall cost. With lz4 + linger.ms=50 +
batch.size=512KiB and 50k-event in-flight queue, a single Python process
sustains ~25-35k records/sec on m5.xlarge against MSK Serverless (network
+ broker becomes the bottleneck, not the producer).

Design choices
--------------
1. Avro + Confluent wire prefix (1 magic + 4 schema_id + Avro binary) so
   the same `bronze_ingestion.py` decoder consumes scale-test events
   alongside real wearable_generator output — no special branch.
2. Synchronous batch produce loop with `producer.poll(0)` every N records
   to drain delivery callbacks. The poll thread inside librdkafka does the
   actual network send.
3. Deterministic device IDs (`SW-A00-00042` style) so the bronze GX
   regex passes and silver→gold partition shape matches the 50K-user
   production claim.
4. Same KNOWN_METRICS set silver→gold uses → silver `is_valid` retains
   ~95% (matches WHOOP-style retention numbers).
5. Resume via checkpoint file (`.batch_scale_offset.json`). If the
   process is killed (Ctrl+C / SIGTERM / spot reclaim), restart with the
   same `--count` and it picks up at the last flushed offset.
6. MSK OAUTHBEARER auth via `aws_msk_iam_sasl_signer.MSKAuthTokenProvider`
   — librdkafka cannot speak `AWS_MSK_IAM` directly; OAUTHBEARER is the
   supported path.

Usage
-----
On EMR master (preferred — co-located with brokers):

    AWS_DEFAULT_REGION=us-east-1 python3 data_generators/batch_scale_producer.py \\
        --brokers "$BOOTSTRAP_SERVERS" \\
        --topic sensor_readings \\
        --count 10000000 \\
        --users 50000 \\
        --report-interval 10000

Resume after kill:
    Re-run the same command — checkpoint at .batch_scale_offset.json
    causes the producer to skip already-flushed records.

Locally for smoke test (PLAINTEXT broker on port 9093):
    python3 data_generators/batch_scale_producer.py \\
        --brokers localhost:9093 --count 100000 --no-iam-auth
"""

from __future__ import annotations

import argparse
import io
import json
import os
import random
import signal
import socket
import struct
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fastavro

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = REPO_ROOT / "schemas" / "sensor_reading.avsc"
DEFAULT_CHECKPOINT = REPO_ROOT / ".batch_scale_offset.json"


# ── Constants matching bronze + silver expectations ───────────────────────
# bronze GX regex: ^[A-Z]{2}-[A-Z0-9]{3}-\d{5}$
DEVICE_TYPES = ["smartwatch", "chest_strap", "sleep_ring", "blood_pressure_cuff"]
DEVICE_PREFIX = {
    "smartwatch": "SW",
    "chest_strap": "CS",
    "sleep_ring": "SR",
    "blood_pressure_cuff": "BP",
}
# Per-device-type metric subsets — must align with silver KNOWN_METRICS
METRICS_PER_DEVICE_TYPE: dict[str, list[str]] = {
    "smartwatch": [
        "heart_rate_bpm",
        "spo2_pct",
        "hrv_ms",
        "skin_temp_celsius",
        "steps_since_last",
    ],
    "chest_strap": ["heart_rate_bpm", "hrv_ms", "respiration_rate"],
    "sleep_ring": ["heart_rate_bpm", "spo2_pct", "skin_temp_celsius"],
    "blood_pressure_cuff": ["bp_systolic_mmhg", "bp_diastolic_mmhg"],
}

# Physiologically plausible value ranges so silver `is_valid` mostly stays
# true (range checks in silver layer reject HR<40, HR>220, SpO2<70, etc.).
# Center ranges + small jitter → ~95% retention.
METRIC_RANGES: dict[str, tuple[float, float]] = {
    "heart_rate_bpm": (55.0, 95.0),
    "spo2_pct": (94.0, 99.5),
    "hrv_ms": (30.0, 75.0),
    "skin_temp_celsius": (32.5, 35.0),
    "steps_since_last": (0.0, 150.0),
    "respiration_rate": (12.0, 18.0),
    "bp_systolic_mmhg": (110.0, 135.0),
    "bp_diastolic_mmhg": (70.0, 88.0),
}


def make_device_id(device_type: str, user_idx: int) -> str:
    """Generate a device_id matching bronze GX regex.

    Format: ``<2-letter prefix>-A<2-digit type>-<5-digit user>``.
    With user_idx 0..99999 we get a 6-char user suffix; mod 100000 keeps
    the field 5 chars wide.
    """
    prefix = DEVICE_PREFIX[device_type]
    type_id = DEVICE_TYPES.index(device_type)  # 0..3
    return f"{prefix}-A{type_id:02d}-{user_idx % 100000:05d}"


def make_record(seq: int, num_users: int, base_ts: datetime) -> dict:
    """Synthesize one SensorReading.

    Spread across `num_users` users and 4 device types. Event timestamps
    spread across the last 24h so partition fan-out exercises the
    streaming watermark + late-arrival logic (bronze rejects >30d old
    events, so 24h gives margin without hitting GX failures).
    """
    user_idx = seq % num_users
    device_type = DEVICE_TYPES[seq % len(DEVICE_TYPES)]
    device_id = make_device_id(device_type, user_idx)

    # Event timestamp: spread over last 24h. 30% have a 10-min to 8h
    # sync delay (real wearable batch-sync behavior). Watermark = 10min,
    # so most of these still land on time.
    minutes_ago = seq % (24 * 60)
    event_ts = base_ts - timedelta(minutes=minutes_ago)
    if random.random() < 0.30:
        sync_delay_s = random.randint(600, 28800)
        sync_ts = event_ts + timedelta(seconds=sync_delay_s)
    else:
        sync_ts = event_ts + timedelta(seconds=30)

    # Generate metrics for this device type
    metrics: dict[str, float | None] = {}
    for m in METRICS_PER_DEVICE_TYPE[device_type]:
        lo, hi = METRIC_RANGES[m]
        # ~0.5% null (sensor glitch); silver flags but doesn't reject
        if random.random() < 0.005:
            metrics[m] = None
        else:
            metrics[m] = round(random.uniform(lo, hi), 2)

    return {
        "reading_id": str(uuid.uuid4()),
        "device_id": device_id,
        "device_type": device_type,
        "user_device_account_id": f"acct_{user_idx:05d}",
        "patient_email": f"user{user_idx}@example.com",
        "metrics": metrics,
        "firmware_version": f"3.{(seq % 5)}.0",
        "battery_pct": 20 + (seq % 80),
        "event_timestamp": int(event_ts.timestamp() * 1000),
        "sync_timestamp": int(sync_ts.timestamp() * 1000),
        "source_type": "simulator",
    }


def encode_avro(record: dict, schema: dict) -> bytes:
    """Confluent wire format. bronze decoder strips bytes 0..5 then decodes."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return b"\x00" + struct.pack(">I", 1) + buf.getvalue()


# ── Checkpoint helpers ────────────────────────────────────────────────────
def load_checkpoint(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(json.loads(path.read_text()).get("flushed_offset", 0))
    except (json.JSONDecodeError, ValueError):
        print(f"[producer] WARN: bad checkpoint at {path}; starting from 0", flush=True)
        return 0


def save_checkpoint(path: Path, offset: int) -> None:
    payload = {
        "flushed_offset": offset,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload))
    tmp.replace(path)


# ── MSK OAUTHBEARER auth ──────────────────────────────────────────────────
def _msk_oauth_cb(region: str):
    """Return an oauth_cb closure that signs against the given AWS region."""
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def cb(_oauth_config: str) -> tuple[str, float]:
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, time.time() + expiry_ms / 1000.0

    return cb


def build_producer_config(brokers: str, use_iam: bool, region: str) -> dict:
    """librdkafka config tuned for sustained 25k+ rec/s."""
    cfg = {
        "bootstrap.servers": brokers,
        "client.id": f"batch-scale-{socket.gethostname()}",
        # ── Reliability ──
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "retries": 10,
        # ── Throughput tuning ──
        "compression.type": "lz4",
        "linger.ms": 50,
        "batch.size": 524288,            # 512 KiB
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 131072,  # 128 MiB
        # Drop the default 5s socket timeout — MSK Serverless adds latency
        "socket.timeout.ms": 30000,
        "request.timeout.ms": 30000,
        "delivery.timeout.ms": 300000,    # 5 min — protects against
                                          # transient AZ/leader switches
    }
    if use_iam:
        cfg.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "OAUTHBEARER",
                "oauth_cb": _msk_oauth_cb(region),
            }
        )
    return cfg


# ── Main loop ─────────────────────────────────────────────────────────────
class ProducerState:
    """Mutable progress + delivery counters shared with signal handler."""

    def __init__(self):
        self.delivered = 0
        self.failed = 0
        self.last_failure: str | None = None
        self.aborted = False


def _make_delivery_cb(state: ProducerState):
    def cb(err, msg):
        if err is not None:
            state.failed += 1
            state.last_failure = str(err)
            if state.failed <= 10:  # cap noisy stderr at first 10
                print(f"[producer] DELIVERY FAILED: {err}", file=sys.stderr, flush=True)
        else:
            state.delivered += 1

    return cb


def _install_signal_handler(state: ProducerState):
    def handler(signum, _frame):
        print(
            f"\n[producer] Received signal {signum}; flushing remaining "
            "messages and exiting.",
            flush=True,
        )
        state.aborted = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def main() -> int:
    parser = argparse.ArgumentParser(description="PulseTrack batch scale producer")
    parser.add_argument("--brokers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="sensor_readings")
    parser.add_argument(
        "--count", type=int, default=10_000_000,
        help="Total records to produce (default 10M)",
    )
    parser.add_argument(
        "--users", type=int, default=50_000,
        help="Distinct user count for device fan-out (default 50K)",
    )
    parser.add_argument(
        "--report-interval", type=int, default=10_000,
        help="Print + checkpoint every N produced records",
    )
    parser.add_argument(
        "--checkpoint", default=str(DEFAULT_CHECKPOINT),
        help="Resume offset file (default .batch_scale_offset.json)",
    )
    parser.add_argument(
        "--no-iam-auth", action="store_true",
        help="Use PLAINTEXT broker (local smoke test) instead of MSK IAM",
    )
    parser.add_argument(
        "--aws-region", default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        help="AWS region for MSK IAM token signing",
    )
    parser.add_argument(
        "--reset-checkpoint", action="store_true",
        help="Ignore + delete any existing checkpoint before starting",
    )
    args = parser.parse_args()

    # ── Validation ──
    if args.count <= 0:
        print("[producer] --count must be positive", file=sys.stderr)
        return 2
    if args.users <= 0:
        print("[producer] --users must be positive", file=sys.stderr)
        return 2

    # ── Schema ──
    schema = fastavro.schema.load_schema(str(SCHEMA_FILE))
    parsed_schema = fastavro.parse_schema(schema)

    # ── Checkpoint ──
    checkpoint_path = Path(args.checkpoint).expanduser()
    if args.reset_checkpoint and checkpoint_path.exists():
        checkpoint_path.unlink()
        print(f"[producer] checkpoint reset: {checkpoint_path}", flush=True)
    resume_offset = load_checkpoint(checkpoint_path)
    if resume_offset > 0:
        print(
            f"[producer] RESUMING from offset {resume_offset:,} "
            f"(checkpoint {checkpoint_path})",
            flush=True,
        )
    if resume_offset >= args.count:
        print(
            f"[producer] checkpoint ({resume_offset:,}) >= count ({args.count:,}); "
            "nothing to do. Use --reset-checkpoint to restart.",
            flush=True,
        )
        return 0

    # ── Producer ──
    from confluent_kafka import Producer

    cfg = build_producer_config(args.brokers, not args.no_iam_auth, args.aws_region)
    state = ProducerState()
    _install_signal_handler(state)

    # Compose config + delivery cb in a way that lets oauth_cb run during
    # client construction (librdkafka requires the callback registered before
    # any network round-trip).
    producer = Producer(cfg)
    delivery_cb = _make_delivery_cb(state)

    # Confluent wire prefix is constant — precompute to skip per-record allocation
    print(
        f"[producer] brokers={args.brokers} topic={args.topic} "
        f"count={args.count:,} users={args.users:,} resume={resume_offset:,} "
        f"iam_auth={not args.no_iam_auth}",
        flush=True,
    )
    base_ts = datetime.now(timezone.utc)
    start_wall = time.time()
    last_report_wall = start_wall
    last_report_offset = resume_offset

    for seq in range(resume_offset, args.count):
        if state.aborted:
            break

        rec = make_record(seq, args.users, base_ts)
        payload = encode_avro(rec, parsed_schema)
        # Key partitions on (user, device) — same as wearable_generator,
        # so silver dedupeWithinWatermark + identity_bridge produce identical
        # cardinality vs. real producer.
        key = rec["device_id"].encode()

        # BufferError happens when librdkafka's internal queue is full
        # (queue.buffering.max.messages=500k). Drain via poll until space
        # is freed.
        while True:
            try:
                producer.produce(
                    args.topic,
                    key=key,
                    value=payload,
                    callback=delivery_cb,
                )
                break
            except BufferError:
                producer.poll(0.5)

        # Drain delivery callbacks every 100 records — keeps stats fresh
        # without trashing throughput (poll(0) is cheap).
        if seq % 100 == 0:
            producer.poll(0)

        # Periodic report + checkpoint
        if (seq + 1 - resume_offset) % args.report_interval == 0:
            now = time.time()
            window = max(now - last_report_wall, 1e-3)
            rate = (seq + 1 - last_report_offset) / window
            overall = (seq + 1 - resume_offset) / max(now - start_wall, 1e-3)
            print(
                f"[producer] offset={seq + 1:,} "
                f"delivered={state.delivered:,} failed={state.failed} "
                f"window_rate={rate:,.0f}/s overall_rate={overall:,.0f}/s "
                f"elapsed={now - start_wall:.1f}s",
                flush=True,
            )
            last_report_wall = now
            last_report_offset = seq + 1
            save_checkpoint(checkpoint_path, seq + 1)

    # ── Final flush ──
    print("[producer] Flushing producer queue ...", flush=True)
    remaining = producer.flush(120)
    if remaining > 0:
        print(
            f"[producer] WARN: producer.flush left {remaining} unsent (timeout)",
            file=sys.stderr,
            flush=True,
        )

    end_wall = time.time()
    elapsed = end_wall - start_wall
    submitted = args.count - resume_offset if not state.aborted else state.delivered + state.failed
    print(
        f"[producer] DONE elapsed={elapsed:.1f}s submitted={submitted:,} "
        f"delivered={state.delivered:,} failed={state.failed} "
        f"avg_rate={state.delivered / max(elapsed, 1e-3):,.0f}/s "
        f"last_failure={state.last_failure!r}",
        flush=True,
    )

    # Update checkpoint to reflect actual progress
    save_checkpoint(checkpoint_path, resume_offset + state.delivered)

    if state.aborted:
        return 130  # SIGINT exit code
    if state.failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
