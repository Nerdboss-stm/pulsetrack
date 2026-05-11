"""
PulseTrack — Schema Registry helper.

Loads .avsc files from this directory, registers them against the configured
Schema Registry, and builds Avro serializers/deserializers for the
confluent-kafka client.

Subjects follow the standard `<topic>-value` naming convention.
Run `python schemas/registry.py` to register all schemas at startup.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

# Lazy-imported in functions that use them. The Confluent Schema Registry
# client transitively depends on httpx (added in confluent-kafka>=2.6.x),
# which isn't installed everywhere on EMR YARN containers. Streaming
# drivers that only need ``load_schema_str(filename)`` (reading the local
# .avsc file as text) don't need the registry client at all, so we
# defer the heavy import.
if TYPE_CHECKING:
    from confluent_kafka.schema_registry import SchemaRegistryClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import settings  # noqa: E402
from logger import get_logger  # noqa: E402

log = get_logger(__name__)

SCHEMAS_DIR = Path(__file__).parent

# Subject (Confluent default: <topic>-value) → schema file in this directory.
SCHEMA_FILES: dict[str, str] = {
    f"{settings.kafka_topic_sensor}-value": "sensor_reading.avsc",
    f"{settings.kafka_topic_pharmacy}-value": "pharmacy_event.avsc",
}


def get_schema_registry_client() -> "SchemaRegistryClient":
    # Lazy import — see module docstring.
    from confluent_kafka.schema_registry import SchemaRegistryClient

    config: dict[str, str] = {"url": settings.schema_registry_url}
    if settings.schema_registry_api_key:
        config["basic.auth.user.info"] = (
            f"{settings.schema_registry_api_key}:{settings.schema_registry_api_secret}"
        )
    return SchemaRegistryClient(config)


def load_schema_str(filename: str) -> str:
    """Read an .avsc file from the schemas/ package.

    Zip-safe: handles both local-filesystem paths and PySpark cluster mode
    where this module lives inside ``pulsetrack-deps.zip`` (--py-files).
    Plain ``pathlib.Path.read_text()`` fails with NotADirectoryError on the
    zip path; ``importlib.resources.files()`` works in both cases.
    """
    # Fast path: real filesystem
    path = SCHEMAS_DIR / filename
    try:
        return path.read_text()
    except (NotADirectoryError, FileNotFoundError):
        pass
    # Fallback: zip-aware resource loader
    from importlib.resources import files

    return files("schemas").joinpath(filename).read_text()


def register_all_schemas() -> dict[str, int]:
    """Idempotently register every schema. Returns subject → schema_id."""
    from confluent_kafka.schema_registry import Schema  # lazy

    client = get_schema_registry_client()
    registered: dict[str, int] = {}
    for subject, filename in SCHEMA_FILES.items():
        schema_str = load_schema_str(filename)
        schema = Schema(schema_str, schema_type="AVRO")
        schema_id = client.register_schema(subject, schema)
        registered[subject] = schema_id
        log.info(
            "Registered Avro schema",
            extra={
                "extra_data": {
                    "subject": subject,
                    "schema_id": schema_id,
                    "file": filename,
                }
            },
        )
    return registered


def get_avro_serializer(
    subject: str,
    to_dict: Optional[Callable] = None,
):
    """Build an AvroSerializer for `subject`.

    `to_dict` maps a Python object to a dict matching the schema. Pass None
    when producing dicts directly.
    """
    from confluent_kafka.schema_registry.avro import AvroSerializer  # lazy

    if subject not in SCHEMA_FILES:
        raise KeyError(f"Unknown subject: {subject}. Known: {sorted(SCHEMA_FILES)}")
    client = get_schema_registry_client()
    schema_str = load_schema_str(SCHEMA_FILES[subject])
    return AvroSerializer(client, schema_str, to_dict=to_dict)


def get_avro_deserializer(
    subject: str,
    from_dict: Optional[Callable] = None,
):
    """Build an AvroDeserializer for `subject`.

    `from_dict` maps a dict back to a Python object. Pass None to receive
    plain dicts in your consumer.
    """
    from confluent_kafka.schema_registry.avro import AvroDeserializer  # lazy

    if subject not in SCHEMA_FILES:
        raise KeyError(f"Unknown subject: {subject}. Known: {sorted(SCHEMA_FILES)}")
    client = get_schema_registry_client()
    schema_str = load_schema_str(SCHEMA_FILES[subject])
    return AvroDeserializer(client, schema_str, from_dict=from_dict)


def register_all_schemas_glue() -> dict[str, str]:
    """Cloud path: register every schema against AWS Glue Schema Registry.

    Returns subject → schema_version_id (UUID string).

    Why a parallel function instead of overloading register_all_schemas():
      - Confluent SR returns int schema_id; Glue returns UUID schema_version_id.
        Different types in the wire-format prefix → callers need to know which.
      - Confluent SR uses HTTP; Glue uses boto3 + IAM. Different failure modes.
      - Keep the local-dev (`register_all_schemas`) and cloud (`register_all_schemas_glue`)
        paths separable so unit tests can stub each independently.

    Both functions are idempotent — calling them twice has no effect (Glue's
    GetSchemaByDefinition short-circuits the create).
    """
    from schemas.glue_registry import get_default_client  # lazy

    client = get_default_client()
    registered: dict[str, str] = {}
    for subject, filename in SCHEMA_FILES.items():
        # Glue's schema name doesn't include the "-value" suffix; strip it.
        schema_name = subject.removesuffix("-value")
        schema_str = load_schema_str(filename)
        version_id = client.register_or_get_schema(schema_name, schema_str)
        registered[subject] = version_id
        log.info(
            "Registered Avro schema with Glue Schema Registry",
            extra={
                "extra_data": {
                    "subject": subject,
                    "glue_schema_name": schema_name,
                    "schema_version_id": version_id,
                    "file": filename,
                }
            },
        )
    return registered


def register_schemas_for_environment() -> dict[str, object]:
    """Dispatch to the right registry based on settings.environment.

    Local: Confluent Schema Registry (returns int schema_ids).
    Cloud: AWS Glue Schema Registry (returns UUID schema_version_ids).

    Returns subject → schema_id-or-version-id (heterogeneous).
    """
    if settings.environment == "cloud":
        return register_all_schemas_glue()
    return register_all_schemas()


if __name__ == "__main__":
    register_schemas_for_environment()
