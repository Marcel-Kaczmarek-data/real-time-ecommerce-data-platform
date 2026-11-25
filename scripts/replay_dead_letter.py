"""Replay events from the dead-letter topic after the root cause was fixed.

    python scripts/replay_dead_letter.py --dry-run
    python scripts/replay_dead_letter.py --stage delivery --stage producer_validation

Reads ``ecommerce.dead-letter`` from the beginning (its own consumer group, so it
can be re-run). Each event is re-validated against the CURRENT contract and
republished to its original topic if it is now valid: for example after a
broker outage (stage ``delivery``) or after registering a missing schema
version. Still-invalid events are reported and left in the DLQ. Republished
events keep their original ``event_id``, so Silver deduplicates any that had
already arrived.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
import uuid
from collections import Counter
from datetime import datetime

from confluent_kafka import Consumer

from data_generator.events import DEAD_LETTER_TOPIC, TOPICS, Event
from ingestion.kafka_producers.producer import ContractValidator, EventPublisher
from pipelines.common.config import load_config

ENTITY_BY_TOPIC = {topic: entity for entity, topic in TOPICS.items()}


def to_event(record: dict) -> Event | None:  # type: ignore[type-arg]
    entity = ENTITY_BY_TOPIC.get(record.get("original_topic", ""))
    env = record.get("event")
    if entity is None or not isinstance(env, dict):
        return None
    payload = dict(env.get("payload") or {})
    for key, value in payload.items():  # timestamps were serialised as ISO strings
        if isinstance(value, str) and len(value) >= 19 and value[4] == "-" and "T" in value:
            with contextlib.suppress(ValueError):
                payload[key] = datetime.fromisoformat(value)
    return Event(
        entity=entity,
        event_type=env["event_type"],
        event_id=env["event_id"],
        event_timestamp=datetime.fromisoformat(env["event_timestamp"]),
        correlation_id=env["correlation_id"],
        payload=payload,
        event_version=int(env.get("event_version", 1)),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--stage", action="append", help="only replay these stages (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-messages", type=int, default=100_000)
    args = parser.parse_args(argv)
    cfg = load_config()
    consumer = Consumer(
        {
            "bootstrap.servers": cfg.get_path("kafka.bootstrap_servers"),
            "group.id": f"dlq-replay-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([DEAD_LETTER_TOPIC])
    validator = ContractValidator()
    publisher = None if args.dry_run else EventPublisher()
    stats: Counter[str] = Counter()
    empty_polls = 0
    while stats["read"] < args.max_messages and empty_polls < 5:
        msg = consumer.poll(2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        empty_polls = 0
        stats["read"] += 1
        record = json.loads(msg.value())
        if args.stage and record.get("stage") not in args.stage:
            stats["skipped_stage"] += 1
            continue
        event = to_event(record)
        if event is None:
            stats["not_replayable"] += 1  # e.g. bronze_decode records carry raw bytes only
            continue
        error = validator.validate(event)
        if error:
            stats["still_invalid"] += 1
            continue
        stats["replayable"] += 1
        if publisher is not None:
            publisher.publish(event)
    consumer.close()
    if publisher is not None:
        publisher.flush(60)
        stats["republished"] = publisher.acked
    print(json.dumps(dict(stats), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
