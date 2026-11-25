"""Streaming failure recovery: checkpoint restart and crash-between-write-and-commit.

Scenario: Spark commits micro-batch N to Delta, then crashes before it records
batch N in the checkpoint's ``commits/`` log. On restart Spark replays batch
N from the same offsets. Delta's ``txnAppId``/``txnVersion`` must make
that replay a no-op, so Bronze row counts stay unchanged (exactly-once per offset).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

CHECKPOINT = Path("/opt/checkpoints/bronze_kafka_ingest")


def _drain() -> None:
    subprocess.run(
        [sys.executable, "-m", "pipelines.bronze.stream_kafka_to_bronze", "--once", "--no-realtime"],
        check=True,
        timeout=1800,
    )


def _bronze_counts() -> dict[str, int]:
    from deltalake import DeltaTable

    from pipelines.bronze.avro_decoder import TOPIC_ENTITY
    from pipelines.common.storage import delta_rs_storage_options, to_s3_uri
    from pipelines.common.tables import table

    out = {}
    for entity in TOPIC_ENTITY.values():
        dt = DeltaTable(
            to_s3_uri(table(f"bronze.{entity}_events").path), storage_options=delta_rs_storage_options()
        )
        out[entity] = sum(int(a["num_records"]) for a in dt.get_add_actions(flatten=True).to_pylist())
    return out


@pytest.mark.timeout(3600)
def test_restart_and_replayed_batch_do_not_duplicate() -> None:
    _drain()  # bring Bronze up to date
    before = _bronze_counts()
    commits = sorted(
        (CHECKPOINT / "commits").iterdir(), key=lambda p: int(p.name) if p.name.isdigit() else -1
    )
    last = [c for c in commits if c.name.isdigit()][-1]
    # Simulate the crash window: forget that the last batch was committed.
    last.unlink()
    crc = CHECKPOINT / "commits" / f".{last.name}.crc"
    if crc.exists():
        crc.unlink()
    _drain()  # Spark re-executes batch N; Delta skips it via txnVersion
    after = _bronze_counts()
    assert after == before, f"replayed micro-batch duplicated rows: {before} -> {after}"
    assert (CHECKPOINT / "commits" / last.name).exists(), "the replayed batch is committed again"
