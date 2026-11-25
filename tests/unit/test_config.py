from __future__ import annotations

import pytest

from pipelines.common import config as cfg


def test_placeholder_resolution_uses_env_then_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOME_HOST", "broker:9092")
    monkeypatch.delenv("MISSING_VAR", raising=False)
    resolved = cfg._resolve({"a": "${SOME_HOST}", "b": ["${MISSING_VAR:-fallback}", "x-${MISSING_VAR}-y"]})
    assert resolved == {"a": "broker:9092", "b": ["fallback", "x--y"]}


def test_empty_env_value_falls_back_to_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMPTY_VAR", "")
    assert cfg._resolve("${EMPTY_VAR:-dflt}") == "dflt"


@pytest.mark.parametrize("env", ["local", "cloud"])
def test_environment_configs_load(env: str) -> None:
    cfg.load_config.cache_clear()
    conf = cfg.load_config(env)
    assert conf.env == env
    assert conf.get_path("kafka.producer.acks") == "all"
    assert conf.get_path("kafka.producer")["enable.idempotence"] is True
    assert conf.get_path("lakehouse.layers") == ["bronze", "silver", "gold", "audit", "quarantine"]
    assert conf.get_path("does.not.exist", "d") == "d"


def test_topic_catalogue_is_consistent() -> None:
    topics = cfg.load_topics()
    names = {t["name"] for t in topics}
    assert "ecommerce.dead-letter" in names
    assert len(names) == len(topics) == 10
    for topic in topics:
        assert topic["partitions"] >= 1
        assert topic["partitions_full"] >= topic["partitions"]
        assert topic["replication_factor"] >= 1
        if topic["entity"] != "dead_letter":
            assert topic["key"], f"{topic['name']} must declare a partition key"
            assert topic["event_types"], f"{topic['name']} must declare event types"
    assert cfg.topic_for_entity("orders")["key"] == "order_id"
    with pytest.raises(KeyError):
        cfg.topic_for_entity("nope")
