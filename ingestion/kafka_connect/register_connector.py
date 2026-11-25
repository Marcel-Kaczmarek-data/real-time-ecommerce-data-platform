"""Register (or update) the Debezium PostgreSQL CDC connector with Kafka Connect.

    docker compose --profile cdc up -d kafka-connect
    python -m ingestion.kafka_connect.register_connector [--connect-url http://kafka-connect:8083]

Optional alternative to the watermark-based PostgreSQL extracts. CDC captures every change
(including deletes) from the WAL, without polling or clock-skew risk. The topics
``cdc.ops.<table>`` carry flattened rows (``ExtractNewRecordState``) plus ``__op`` / ``__source_ts_ms``.
Credentials are resolved by Kafka Connect from its environment (``${env:...}``) and never sent in clear text.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

CONFIG = Path(__file__).with_name("debezium-postgres-ops.json")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--connect-url", default="http://kafka-connect:8083")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args(argv)
    spec = json.loads(CONFIG.read_text(encoding="utf-8"))
    deadline = time.time() + args.timeout
    while True:
        try:
            requests.get(f"{args.connect_url}/connectors", timeout=5).raise_for_status()
            break
        except requests.RequestException:
            if time.time() > deadline:
                print("Kafka Connect not reachable", file=sys.stderr)
                return 1
            time.sleep(3)
    # PUT /config is idempotent: creates the connector or updates its configuration.
    resp = requests.put(
        f"{args.connect_url}/connectors/{spec['name']}/config", json=spec["config"], timeout=30
    )
    resp.raise_for_status()
    status = requests.get(f"{args.connect_url}/connectors/{spec['name']}/status", timeout=10).json()
    print(json.dumps(status, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
