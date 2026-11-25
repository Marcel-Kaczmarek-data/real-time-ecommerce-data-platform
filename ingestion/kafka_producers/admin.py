"""Kafka administration: create topics from the catalogue and register event contracts.

python -m ingestion.kafka_producers.admin bootstrap        # idempotent
python -m ingestion.kafka_producers.admin describe
python -m ingestion.kafka_producers.admin check-compat     # CI / pre-deploy gate
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from data_generator.events import TOPIC_PREFIX, TOPICS
from ingestion.kafka_producers.schema_registry import (
    IncompatibleSchemaError,
    ensure_schema,
    registry_client,
    subject_for,
)
from pipelines.common.config import load_config, load_topics
from pipelines.common.logs import get_logger
from schemas.contracts import PAYLOADS, avro_schema

log = get_logger("kafka-admin")


def admin_client() -> AdminClient:
    cfg = load_config()
    return AdminClient({"bootstrap.servers": cfg.get_path("kafka.bootstrap_servers")})


def wait_for_kafka(client: AdminClient, timeout_s: int = 120) -> None:
    deadline = time.time() + timeout_s
    while True:
        try:
            client.list_topics(timeout=5)
            return
        except Exception as exc:
            if time.time() > deadline:
                raise RuntimeError("Kafka not reachable") from exc
            time.sleep(2)


def create_topics(
    client: AdminClient, full_scale: bool = False, partitions_override: int | None = None
) -> list[str]:
    existing = set(client.list_topics(timeout=10).topics)
    new_topics = []
    for spec in load_topics():
        t = {**spec, "name": f"{TOPIC_PREFIX}{spec['name']}"}
        if t["name"] in existing:
            continue
        partitions = partitions_override or int(t["partitions_full"] if full_scale else t["partitions"])
        new_topics.append(
            NewTopic(
                t["name"],
                num_partitions=partitions,
                replication_factor=int(t["replication_factor"]),
                config={
                    "retention.ms": str(t["retention_ms"]),
                    "cleanup.policy": t.get("cleanup_policy", "delete"),
                    "min.insync.replicas": str(t["min_insync_replicas"]),
                    "compression.type": t.get("compression_type", "producer"),
                },
            )
        )
    created = []
    if new_topics:
        for name, future in client.create_topics(new_topics, operation_timeout=30).items():
            future.result()
            created.append(name)
            log.info("topic created", extra={"topic": name})
    return created


def register_schemas() -> dict[str, int]:
    cfg = load_config()
    client = registry_client(str(cfg.get_path("kafka.schema_registry_url")))
    return {entity: ensure_schema(client, entity) for entity in PAYLOADS}


def check_compatibility() -> int:
    """Return non-zero if any local contract is incompatible with the registered version."""
    from confluent_kafka.schema_registry import Schema
    from confluent_kafka.schema_registry.error import SchemaRegistryError

    cfg = load_config()
    client = registry_client(str(cfg.get_path("kafka.schema_registry_url")))
    failures = 0
    for entity in PAYLOADS:
        subject = subject_for(TOPICS[entity])
        try:
            ok = client.test_compatibility(subject, Schema(json.dumps(avro_schema(entity)), "AVRO"))
        except SchemaRegistryError:
            ok = True  # subject not registered yet
        print(f"{subject:40s} {'compatible' if ok else 'INCOMPATIBLE'}")
        failures += 0 if ok else 1
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("command", choices=["bootstrap", "describe", "check-compat"])
    parser.add_argument("--full-scale", action="store_true", help="use partitions_full from the catalogue")
    parser.add_argument("--partitions", type=int, default=None, help="override partitions for every topic")
    args = parser.parse_args(argv)
    client = admin_client()
    wait_for_kafka(client)
    if args.command == "bootstrap":
        created = create_topics(client, args.full_scale, args.partitions)
        try:
            ids = register_schemas()
        except IncompatibleSchemaError as exc:
            log.error("schema compatibility check failed", extra={"error": str(exc)})
            return 2
        print(f"topics created: {created or 'none (already present)'}; schemas: {ids}")
        return 0
    if args.command == "check-compat":
        return check_compatibility()
    md = client.list_topics(timeout=10)
    for name in sorted(md.topics):
        if name.startswith("ecommerce."):
            print(f"{name:28s} partitions={len(md.topics[name].partitions)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
