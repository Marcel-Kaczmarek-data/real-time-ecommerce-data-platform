"""Environment-aware configuration loading.

Configuration lives in ``config/<env>.yaml``. Values may contain shell-style
placeholders (``${VAR}`` or ``${VAR:-default}``) that are resolved from the
process environment, so secrets never live in the repository.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"

_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _resolve(value: Any) -> Any:
    """Recursively substitute ``${VAR:-default}`` placeholders."""
    if isinstance(value, dict):
        return {k: _resolve(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve(v) for v in value]
    if isinstance(value, str):

        def repl(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            env_value = os.environ.get(name)
            if env_value:
                return env_value
            return default if default is not None else ""

        return _PLACEHOLDER.sub(repl, value)
    return value


class Config(dict[str, Any]):
    """Dictionary with dotted-path access (``cfg.get_path("kafka.bootstrap_servers")``)."""

    def get_path(self, dotted: str, default: Any = None) -> Any:
        node: Any = self
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    @property
    def env(self) -> str:
        return str(self.get("environment", "local"))


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@lru_cache(maxsize=8)
def load_config(env: str | None = None) -> Config:
    """Load ``config/<env>.yaml`` (env defaults to ``$PLATFORM_ENV`` or ``local``)."""
    env = env or os.environ.get("PLATFORM_ENV", "local")
    path = CONFIG_DIR / f"{env}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"No configuration file for environment '{env}': {path}")
    return Config(_resolve(load_yaml(path)))


@lru_cache(maxsize=1)
def load_topics() -> list[dict[str, Any]]:
    """Return the Kafka topic catalogue with defaults applied."""
    raw = load_yaml(CONFIG_DIR / "kafka-topics.yaml")
    defaults = raw.get("defaults", {})
    return [{**defaults, **topic} for topic in raw["topics"]]


def topic_for_entity(entity: str) -> dict[str, Any]:
    for topic in load_topics():
        if topic["entity"] == entity:
            return topic
    raise KeyError(f"No Kafka topic configured for entity '{entity}'")


def env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
