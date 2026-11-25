"""Reference-data REST API (stand-in for an FX provider and the marketing platform).

    GET /health
    GET /v1/fx-rates?date=YYYY-MM-DD        -> daily rates (units per USD)
    GET /v1/campaigns?date=YYYY-MM-DD       -> active paid campaigns with daily spend

Requires the header ``X-API-Key``. Values are deterministic functions of the
date, so they always agree with the prices used in the event stream. Set
``REFERENCE_API_FAILURE_RATE`` to inject HTTP 503 errors and exercise client retries.
"""

from __future__ import annotations

import json
import os
import random
from datetime import date
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from data_generator.reference import CURRENCIES, campaigns_for_date, fx_rate
from pipelines.common.logs import get_logger

log = get_logger("reference-api")


def fx_payload(d: date) -> dict[str, object]:
    return {
        "base": "USD",
        "date": d.isoformat(),
        "provider": "synthetic-fx",
        "rates": {cur: fx_rate(cur, d) for cur in CURRENCIES},
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "ReferenceAPI/1.0"

    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/health":
            self._send(HTTPStatus.OK, {"status": "ok"})
            return
        if self.headers.get("X-API-Key") != os.environ.get("REFERENCE_API_KEY", "local-dev-key"):
            self._send(HTTPStatus.UNAUTHORIZED, {"error": "invalid API key"})
            return
        if random.random() < float(os.environ.get("REFERENCE_API_FAILURE_RATE", "0")):
            self._send(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "temporarily unavailable"})
            return
        try:
            d = date.fromisoformat(parse_qs(url.query)["date"][0])
        except (KeyError, ValueError):
            self._send(HTTPStatus.BAD_REQUEST, {"error": "query parameter 'date' (YYYY-MM-DD) required"})
            return
        if url.path == "/v1/fx-rates":
            self._send(HTTPStatus.OK, fx_payload(d))
        elif url.path == "/v1/campaigns":
            self._send(HTTPStatus.OK, {"date": d.isoformat(), "campaigns": campaigns_for_date(d)})
        else:
            self._send(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def log_message(self, fmt: str, *args: object) -> None:
        log.debug("request", extra={"line": fmt % args})


def main() -> None:
    port = int(os.environ.get("REFERENCE_API_PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)  # noqa: S104 - container service
    log.info("reference API listening", extra={"port": port})
    server.serve_forever()


if __name__ == "__main__":
    main()
