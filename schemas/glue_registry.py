"""AWS Glue Schema Registry client.

Wraps boto3.glue calls so the rest of the pipeline doesn't import boto3 directly.
Supports BOTH wire-format prefixes the project uses depending on environment:

  Local (Confluent SR, docker-compose):
    \\x00 + 4-byte big-endian schema_id + Avro payload

  Cloud (AWS Glue SR):
    1 byte header_version (3) + 1 byte compression (0=none, 5=zlib) +
    16 byte schema_version_uuid + Avro payload

The bronze decoder (`schemas/registry.py`) sniffs the first byte and routes
to the right wire-format decoder.

References:
- Glue Schema Registry SerDe wire format (Java reference; no AWS docs):
  https://github.com/awslabs/aws-glue-schema-registry/blob/master/serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/serializers/SerializerDataParser.java
- Header layout:
  byte 0    = 0x03   (current header version)
  byte 1    = 0x00   (no compression) or 0x05 (zlib)
  bytes 2-17 = schema_version_id (UUID, big-endian)
  bytes 18+ = Avro-encoded payload
"""

from __future__ import annotations

import io
import struct
from functools import lru_cache
from typing import Optional
from uuid import UUID

# 18-byte fixed Glue SR header.
GLUE_SR_HEADER_VERSION = 3
GLUE_SR_NO_COMPRESSION = 0
GLUE_SR_HEADER_SIZE = 18


def encode_glue_wire_format(schema_version_id: str, avro_bytes: bytes) -> bytes:
    """Prepend the Glue SR header to an Avro payload.

    Args:
        schema_version_id: UUID string as returned by RegisterSchemaVersion /
            GetSchemaByDefinition. Example: ``"12345678-1234-1234-1234-1234567890ab"``.
        avro_bytes: Avro-encoded record (single-object encoding, no fingerprint).

    Returns:
        Wire-format bytes ready to publish to Kafka.
    """
    uuid_bytes = UUID(schema_version_id).bytes  # 16 bytes big-endian
    header = struct.pack("BB", GLUE_SR_HEADER_VERSION, GLUE_SR_NO_COMPRESSION) + uuid_bytes
    if len(header) != GLUE_SR_HEADER_SIZE:
        raise ValueError(f"header build error: got {len(header)} bytes, expected {GLUE_SR_HEADER_SIZE}")
    return header + avro_bytes


def decode_glue_wire_format(payload: bytes) -> tuple[str, bytes]:
    """Strip the Glue SR header from a wire payload.

    Returns:
        (schema_version_id_uuid_str, avro_bytes)
    Raises:
        ValueError if the header bytes don't match expected version/compression.
    """
    if len(payload) < GLUE_SR_HEADER_SIZE:
        raise ValueError(
            f"payload too short for Glue SR header: {len(payload)} < {GLUE_SR_HEADER_SIZE}"
        )
    header_version, compression = struct.unpack("BB", payload[:2])
    if header_version != GLUE_SR_HEADER_VERSION:
        raise ValueError(
            f"unexpected Glue SR header version: {header_version} (expected {GLUE_SR_HEADER_VERSION})"
        )
    if compression != GLUE_SR_NO_COMPRESSION:
        raise NotImplementedError(
            f"compressed Glue SR payloads not yet supported (got compression={compression})"
        )
    schema_version_id = str(UUID(bytes=payload[2:18]))
    return schema_version_id, payload[GLUE_SR_HEADER_SIZE:]


class GlueSchemaRegistryClient:
    """Thin client over boto3.glue for the operations the pipeline needs.

    Operations:
        register_or_get_schema(name, definition, data_format='AVRO', compatibility='BACKWARD')
        get_schema_version(schema_version_id)
        list_schemas()

    All calls are cached with lru_cache (per-process) — schema version IDs are
    immutable so the cache never goes stale.
    """

    def __init__(self, registry_name: str, region: str = "us-east-1"):
        # Lazy boto3 import — keeps the test suite from needing boto3 unless
        # this client is actually used.
        import boto3  # noqa: F401, intentionally lazy

        self.registry_name = registry_name
        self.region = region
        self._glue = boto3.client("glue", region_name=region)

    @lru_cache(maxsize=256)
    def register_or_get_schema(
        self,
        schema_name: str,
        definition: str,
        *,
        data_format: str = "AVRO",
        compatibility: str = "BACKWARD",
    ) -> str:
        """Idempotent register-or-get. Returns the schema_version_id (UUID str).

        Tries ``GetSchemaByDefinition`` first (no write needed for known schemas).
        If unknown, falls back to ``RegisterSchemaVersion`` for an existing
        schema name, or ``CreateSchema`` for a new name.
        """
        try:
            resp = self._glue.get_schema_by_definition(
                SchemaId={"SchemaName": schema_name, "RegistryName": self.registry_name},
                SchemaDefinition=definition,
            )
            return resp["SchemaVersionId"]
        except self._glue.exceptions.EntityNotFoundException:
            pass

        # Schema name may exist but this definition is new → register new version.
        try:
            resp = self._glue.register_schema_version(
                SchemaId={"SchemaName": schema_name, "RegistryName": self.registry_name},
                SchemaDefinition=definition,
            )
            return resp["SchemaVersionId"]
        except self._glue.exceptions.EntityNotFoundException:
            # Schema name doesn't exist either → create it.
            resp = self._glue.create_schema(
                RegistryId={"RegistryName": self.registry_name},
                SchemaName=schema_name,
                DataFormat=data_format,
                Compatibility=compatibility,
                SchemaDefinition=definition,
            )
            return resp["SchemaVersionId"]

    @lru_cache(maxsize=1024)
    def get_schema_version(self, schema_version_id: str) -> str:
        """Look up an Avro schema definition by its UUID version id."""
        resp = self._glue.get_schema_version(SchemaVersionId=schema_version_id)
        return resp["SchemaDefinition"]


def get_default_client(
    registry_name: Optional[str] = None,
    region: Optional[str] = None,
) -> GlueSchemaRegistryClient:
    """Convenience factory that resolves from settings if args omitted."""
    from config import settings

    return GlueSchemaRegistryClient(
        registry_name=registry_name or getattr(settings, "glue_registry_name", "pulsetrack-dev-schemas"),
        region=region or getattr(settings, "aws_default_region", "us-east-1"),
    )
