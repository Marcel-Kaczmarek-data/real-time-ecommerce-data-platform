"""Structured JSON logging with correlation identifiers.

Every log line is a single JSON object so it can be shipped to CloudWatch /
Loki / Elasticsearch without parsing rules. A correlation id (pipeline run id,
Airflow run id or event correlation id) is propagated through a context
variable and attached to every record emitted in that context.
"""

from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import os
import sys
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

_correlation_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("correlation_id", default=None)
_pipeline: contextvars.ContextVar[str | None] = contextvars.ContextVar("pipeline", default=None)

_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if cid := _correlation_id.get():
            payload["correlation_id"] = cid
        if pipeline := _pipeline.get():
            payload["pipeline"] = pipeline
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


_state = {"configured": False}


def configure_logging(level: str | None = None) -> None:
    if _state["configured"]:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    resolved: str = level or os.environ.get("LOG_LEVEL") or "INFO"
    root.setLevel(resolved.upper())
    # Quieten very chatty third-party loggers.
    for noisy in ("py4j", "botocore", "urllib3", "s3transfer", "faker", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _state["configured"] = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)


def new_correlation_id() -> str:
    return str(uuid.uuid4())


@contextlib.contextmanager
def log_context(pipeline: str | None = None, correlation_id: str | None = None) -> Iterator[str]:
    """Bind a pipeline name and correlation id to all log records in the block."""
    cid = correlation_id or new_correlation_id()
    t1 = _correlation_id.set(cid)
    t2 = _pipeline.set(pipeline) if pipeline else None
    try:
        yield cid
    finally:
        _correlation_id.reset(t1)
        if t2 is not None:
            _pipeline.reset(t2)
