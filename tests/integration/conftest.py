"""Integration fixtures: these tests talk to the docker-compose services.

Run inside the platform container so service host names resolve:
    docker compose run --rm platform pytest tests/integration -m integration
"""

from __future__ import annotations

import socket

import pytest


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session", autouse=True)
def require_services(request: pytest.FixtureRequest) -> None:
    if request.node.get_closest_marker("integration") is None and not any(
        item.get_closest_marker("integration") for item in request.session.items
    ):
        return
    missing = [
        name
        for name, host, port in [
            ("kafka", "kafka", 29092),
            ("schema-registry", "schema-registry", 8081),
            ("postgres", "postgres", 5432),
            ("object-storage", "object-storage", 9000),
        ]
        if not _reachable(host, port)
    ]
    if missing:
        pytest.skip(f"services not reachable: {missing} (start them with `make up`)")
