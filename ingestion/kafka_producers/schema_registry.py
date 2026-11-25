"""Schema Registry integration: compatibility policy, registration, compatibility checks."""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from data_generator.events import TOPICS
from pipelines.common.logs import get_logger
from schemas.contracts import avro_schema

log = get_logger(__name__)
COMPATIBILITY = "BACKWARD"


def subject_for(topic: str) -> str:
    """TopicNameStrategy: one subject per topic value."""
    return f"{topic}-value"


def registry_client(url: str) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url})


class IncompatibleSchemaError(RuntimeError):
    pass


def ensure_schema(client: SchemaRegistryClient, entity: str, schema: dict[str, Any] | None = None) -> int:
    """Register (or confirm) the schema for an entity, enforcing BACKWARD compatibility.

    Returns the schema id. Raises IncompatibleSchemaError if the new version
    would break existing consumers - the contract change must be redesigned.
    """
    topic = TOPICS[entity]
    subject = subject_for(topic)
    candidate = Schema(json.dumps(schema or avro_schema(entity)), schema_type="AVRO")
    try:
        client.set_compatibility(subject_name=subject, level=COMPATIBILITY)
    except SchemaRegistryError as exc:  # pragma: no cover - registry without per-subject config
        log.warning("could not set compatibility", extra={"subject": subject, "error": str(exc)})
    try:
        existing = client.get_latest_version(subject)
    except SchemaRegistryError:
        existing = None
    if existing is not None and not client.test_compatibility(subject, candidate):
        raise IncompatibleSchemaError(
            f"Schema for {subject} is not {COMPATIBILITY}-compatible with version {existing.version}"
        )
    schema_id = int(client.register_schema(subject, candidate))
    log.info("schema registered", extra={"subject": subject, "schema_id": schema_id})
    return schema_id


def latest_schema(client: SchemaRegistryClient, entity: str) -> tuple[int, dict[str, Any]]:
    registered = client.get_latest_version(subject_for(TOPICS[entity]))
    return int(registered.schema_id), json.loads(registered.schema.schema_str)
