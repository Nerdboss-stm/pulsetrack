"""Unit tests for schemas/registry.py + schemas/glue_registry.py.

Covers:
- load_schema_str() from filesystem + zip-aware fallback
- SCHEMA_FILES dict completeness
- Glue wire-format encode + decode roundtrip
- Glue header parsing edge cases
- Dispatch via register_schemas_for_environment
"""

from __future__ import annotations

import struct
import uuid
from unittest.mock import MagicMock, patch

import pytest

from schemas.glue_registry import (
    GLUE_SR_HEADER_SIZE,
    GLUE_SR_HEADER_VERSION,
    GLUE_SR_NO_COMPRESSION,
    decode_glue_wire_format,
    encode_glue_wire_format,
)
from schemas.registry import SCHEMA_FILES, load_schema_str


# ── load_schema_str ──────────────────────────────────────────────────────


def test_load_schema_str_sensor_reading():
    """sensor_reading.avsc loads as valid JSON-shaped Avro text."""
    s = load_schema_str("sensor_reading.avsc")
    assert "SensorReading" in s or '"name"' in s
    assert len(s) > 100


def test_load_schema_str_pharmacy_event():
    s = load_schema_str("pharmacy_event.avsc")
    assert "PharmacyEvent" in s or '"name"' in s
    assert len(s) > 100


def test_load_schema_str_missing_file_raises():
    with pytest.raises((FileNotFoundError, Exception)):
        load_schema_str("does_not_exist.avsc")


# ── SCHEMA_FILES mapping ────────────────────────────────────────────────


def test_schema_files_has_sensor_subject():
    """At least one entry maps to sensor_reading.avsc."""
    avsc_files = list(SCHEMA_FILES.values())
    assert "sensor_reading.avsc" in avsc_files


def test_schema_files_has_pharmacy_subject():
    avsc_files = list(SCHEMA_FILES.values())
    assert "pharmacy_event.avsc" in avsc_files


def test_schema_files_subjects_end_with_value():
    """Confluent convention: subject = <topic>-value."""
    for subject in SCHEMA_FILES:
        assert subject.endswith("-value"), f"non-conforming subject: {subject}"


# ── Glue wire format: encode ────────────────────────────────────────────


def test_glue_encode_header_size():
    """Encoded payload has the 18-byte Glue SR header prepended."""
    uuid_str = "12345678-1234-5678-1234-567812345678"
    payload = b"some-avro-data"
    encoded = encode_glue_wire_format(uuid_str, payload)
    assert len(encoded) == GLUE_SR_HEADER_SIZE + len(payload)


def test_glue_encode_first_byte_is_header_version():
    encoded = encode_glue_wire_format("00000000-0000-0000-0000-000000000000", b"x")
    assert encoded[0] == GLUE_SR_HEADER_VERSION


def test_glue_encode_compression_byte_is_zero():
    encoded = encode_glue_wire_format("00000000-0000-0000-0000-000000000000", b"x")
    assert encoded[1] == GLUE_SR_NO_COMPRESSION


def test_glue_encode_uuid_bytes_match_input():
    uuid_str = "deadbeef-1234-5678-9abc-def012345678"
    encoded = encode_glue_wire_format(uuid_str, b"")
    assert encoded[2:18] == uuid.UUID(uuid_str).bytes


def test_glue_encode_payload_unchanged():
    payload = b"\x01\x02\x03\xff"
    encoded = encode_glue_wire_format("00000000-0000-0000-0000-000000000000", payload)
    assert encoded[GLUE_SR_HEADER_SIZE:] == payload


# ── Glue wire format: decode ────────────────────────────────────────────


def test_glue_decode_roundtrip():
    """encode → decode returns the original UUID and bytes."""
    uuid_str = "12345678-1234-1234-1234-123456789012"
    avro_bytes = b"original avro payload"
    encoded = encode_glue_wire_format(uuid_str, avro_bytes)
    decoded_uuid, decoded_payload = decode_glue_wire_format(encoded)
    assert decoded_uuid == uuid_str
    assert decoded_payload == avro_bytes


def test_glue_decode_too_short_raises():
    """Payload shorter than the header → ValueError."""
    with pytest.raises(ValueError, match="payload too short"):
        decode_glue_wire_format(b"\x03\x00")


def test_glue_decode_wrong_header_version_raises():
    """First byte != 3 → ValueError."""
    bad = bytes([0xFF]) + bytes(17) + b"data"
    with pytest.raises(ValueError, match="header version"):
        decode_glue_wire_format(bad)


def test_glue_decode_compressed_not_implemented():
    """Compression flag != 0 → NotImplementedError."""
    bad = bytes([GLUE_SR_HEADER_VERSION, 0x05]) + bytes(16) + b"data"
    with pytest.raises(NotImplementedError, match="compressed"):
        decode_glue_wire_format(bad)


def test_glue_decode_empty_payload():
    """Decoded payload can be empty (header-only)."""
    encoded = encode_glue_wire_format("00000000-0000-0000-0000-000000000000", b"")
    _, payload = decode_glue_wire_format(encoded)
    assert payload == b""


# ── Differentiates from Confluent wire format ───────────────────────────


def test_glue_header_first_byte_differs_from_confluent_magic():
    """Confluent uses \\x00 magic byte; Glue uses \\x03. The bronze decoder
    sniffs byte 0 to pick the wire format — these must differ."""
    confluent_magic = b"\x00"
    glue = encode_glue_wire_format("00000000-0000-0000-0000-000000000000", b"")
    assert glue[:1] != confluent_magic
    assert glue[0] == 3


# ── Dispatch ────────────────────────────────────────────────────────────


def test_register_schemas_dispatch_local(monkeypatch):
    """When settings.environment != 'cloud', dispatches to Confluent path."""
    import schemas.registry as reg

    # Stub out the inner functions so we just verify the dispatch call.
    confluent_called = []
    glue_called = []
    monkeypatch.setattr(
        reg,
        "register_all_schemas",
        lambda: confluent_called.append(True) or {"x": 1},
    )
    monkeypatch.setattr(
        reg,
        "register_all_schemas_glue",
        lambda: glue_called.append(True) or {"x": "uuid"},
    )

    from config import settings as cfg
    monkeypatch.setattr(cfg, "environment", "local")
    reg.register_schemas_for_environment()
    assert confluent_called == [True]
    assert glue_called == []


def test_register_schemas_dispatch_cloud(monkeypatch):
    """When settings.environment == 'cloud', dispatches to Glue path."""
    import schemas.registry as reg

    confluent_called = []
    glue_called = []
    monkeypatch.setattr(
        reg,
        "register_all_schemas",
        lambda: confluent_called.append(True) or {"x": 1},
    )
    monkeypatch.setattr(
        reg,
        "register_all_schemas_glue",
        lambda: glue_called.append(True) or {"x": "uuid"},
    )

    from config import settings as cfg
    monkeypatch.setattr(cfg, "environment", "cloud")
    reg.register_schemas_for_environment()
    assert confluent_called == []
    assert glue_called == [True]
