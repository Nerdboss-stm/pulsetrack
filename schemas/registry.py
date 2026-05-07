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
from typing import Callable, Optional

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

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


def get_schema_registry_client() -> SchemaRegistryClient:
    config: dict[str, str] = {"url": settings.schema_registry_url}
    if settings.schema_registry_api_key:
        config["basic.auth.user.info"] = (
            f"{settings.schema_registry_api_key}:{settings.schema_registry_api_secret}"
        )
    return SchemaRegistryClient(config)


def load_schema_str(filename: str) -> str:
    path = SCHEMAS_DIR / filename
    return path.read_text()


def register_all_schemas() -> dict[str, int]:
    """Idempotently register every schema. Returns subject → schema_id."""
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
) -> AvroSerializer:
    """Build an AvroSerializer for `subject`.

    `to_dict` maps a Python object to a dict matching the schema. Pass None
    when producing dicts directly.
    """
    if subject not in SCHEMA_FILES:
        raise KeyError(f"Unknown subject: {subject}. Known: {sorted(SCHEMA_FILES)}")
    client = get_schema_registry_client()
    schema_str = load_schema_str(SCHEMA_FILES[subject])
    return AvroSerializer(client, schema_str, to_dict=to_dict)


def get_avro_deserializer(
    subject: str,
    from_dict: Optional[Callable] = None,
) -> AvroDeserializer:
    """Build an AvroDeserializer for `subject`.

    `from_dict` maps a dict back to a Python object. Pass None to receive
    plain dicts in your consumer.
    """
    if subject not in SCHEMA_FILES:
        raise KeyError(f"Unknown subject: {subject}. Known: {sorted(SCHEMA_FILES)}")
    client = get_schema_registry_client()
    schema_str = load_schema_str(SCHEMA_FILES[subject])
    return AvroDeserializer(client, schema_str, from_dict=from_dict)


if __name__ == "__main__":
    register_all_schemas()
