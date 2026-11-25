"""Confluent wire-format Avro decoding for Spark (executor side).

Wire format: magic byte 0x00 | 4-byte big-endian schema id | Avro binary.

The *writer* schema is fetched from Schema Registry by id (cached per Python
worker). The *reader* schema is the current contract, so fastavro performs
Avro schema resolution: an older or newer BACKWARD-compatible event still
decodes into the current shape (schema evolution). Anything that cannot be
decoded is returned with ``decode_error`` set instead of failing the batch.
"""

from __future__ import annotations

import io
import json
import os
import struct
from collections.abc import Iterator
from datetime import date, datetime
from typing import Any

import pandas as pd

from data_generator.events import TOPICS

TOPIC_ENTITY = {topic: entity for entity, topic in TOPICS.items()}

DECODED_COLUMNS = [
    "event_id",
    "event_type",
    "event_version",
    "event_timestamp_ms",
    "source_system",
    "correlation_id",
    "raw_payload",
    "schema_id",
    "decode_error",
]

_writer_cache: dict[int, Any] = {}
_reader_cache: dict[str, Any] = {}


def _registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


def writer_schema(schema_id: int) -> Any:
    if schema_id not in _writer_cache:
        import requests
        from fastavro import parse_schema

        resp = requests.get(f"{_registry_url()}/schemas/ids/{schema_id}", timeout=10)
        resp.raise_for_status()
        _writer_cache[schema_id] = parse_schema(json.loads(resp.json()["schema"]))
    return _writer_cache[schema_id]


def reader_schema(entity: str) -> Any:
    if entity not in _reader_cache:
        from fastavro import parse_schema

        from schemas.contracts import avro_schema

        _reader_cache[entity] = parse_schema(avro_schema(entity))
    return _reader_cache[entity]


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(type(value))


def decode_message(value: bytes | None, topic: str) -> dict[str, Any]:
    from fastavro import schemaless_reader

    out: dict[str, Any] = dict.fromkeys(DECODED_COLUMNS)
    try:
        if value is None or len(value) < 6:
            raise ValueError("message too short for Confluent wire format")
        magic, schema_id = struct.unpack(">bI", value[:5])
        if magic != 0:
            raise ValueError(f"unknown magic byte {magic}")
        out["schema_id"] = schema_id
        entity = TOPIC_ENTITY[topic]
        record: dict[str, Any] = schemaless_reader(
            io.BytesIO(value[5:]), writer_schema(schema_id), reader_schema(entity)
        )
        ts = record["event_timestamp"]
        out.update(
            event_id=record["event_id"],
            event_type=record["event_type"],
            event_version=int(record["event_version"]),
            event_timestamp_ms=int(ts.timestamp() * 1000) if isinstance(ts, datetime) else int(ts),
            source_system=record["source_system"],
            correlation_id=record["correlation_id"],
            raw_payload=json.dumps(record["payload"], default=_json_default, separators=(",", ":")),
        )
    except Exception as exc:  # corrupt record -> quarantine, never fail the batch
        out["decode_error"] = f"{type(exc).__name__}: {exc}"[:1000]
    return out


def decode_batches(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """mapInPandas function: (value, topic, ...) -> original columns + decoded columns."""
    for pdf in batches:
        decoded = [decode_message(v, t) for v, t in zip(pdf["value"], pdf["topic"], strict=True)]
        dec = pd.DataFrame(decoded, columns=DECODED_COLUMNS)
        dec["event_version"] = dec["event_version"].astype("Int32")
        dec["schema_id"] = dec["schema_id"].astype("Int32")
        dec["event_timestamp_ms"] = dec["event_timestamp_ms"].astype("Int64")
        yield pd.concat([pdf.reset_index(drop=True), dec], axis=1)
