from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import yaml


class ConfigError(ValueError):
    """Raised when Market Discovery configuration is invalid."""


@dataclass(frozen=True)
class MarketDiscoveryConfig:
    gamma_base_url: str
    request_timeout_seconds: float
    max_retries: int
    retry_delay_seconds: float
    version: int = 1


TOP_LEVEL_KEYS = frozenset({"version", "market_discovery"})
MARKET_DISCOVERY_KEYS = frozenset(
    {
        "gamma_base_url",
        "request_timeout_seconds",
        "max_retries",
        "retry_delay_seconds",
    }
)


def load_config(path: Path) -> MarketDiscoveryConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ConfigError("configuration must be a YAML mapping")

    _reject_unknown_keys(raw, TOP_LEVEL_KEYS, "top-level")
    version = _required_int(raw, "version", "top-level")
    if version != 1:
        raise ConfigError("version must be 1")

    section = _required_mapping(raw, "market_discovery", "top-level")
    _reject_unknown_keys(section, MARKET_DISCOVERY_KEYS, "market_discovery")

    gamma_base_url = _required_str(section, "gamma_base_url", "market_discovery").rstrip("/")
    _validate_https_url(gamma_base_url)

    request_timeout_seconds = _positive_number(
        section,
        "request_timeout_seconds",
        "market_discovery",
    )
    max_retries = _required_int(section, "max_retries", "market_discovery")
    if max_retries < 0:
        raise ConfigError("market_discovery.max_retries must be non-negative")

    retry_delay_seconds = _non_negative_number(
        section,
        "retry_delay_seconds",
        "market_discovery",
    )

    return MarketDiscoveryConfig(
        version=version,
        gamma_base_url=gamma_base_url,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
    )


load_market_discovery_config = load_config


def _validate_https_url(value: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme != "https":
        raise ConfigError("market_discovery.gamma_base_url must use HTTPS")
    if not parsed.netloc:
        raise ConfigError("market_discovery.gamma_base_url must include a host")


def _reject_unknown_keys(raw: Mapping[str, Any], allowed: frozenset[str], section: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ConfigError(f"unknown {section} key: {unknown[0]}")


def _required_mapping(raw: Mapping[str, Any], key: str, section: str) -> Mapping[str, Any]:
    value = raw.get(key)
    if not isinstance(value, Mapping):
        raise ConfigError(f"{section}.{key} must be a mapping")
    return value


def _required_str(raw: Mapping[str, Any], key: str, section: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{section}.{key} must be a non-empty string")
    return value.strip()


def _required_int(raw: Mapping[str, Any], key: str, section: str) -> int:
    value = raw.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ConfigError(f"{section}.{key} must be an integer")
    return value


def _positive_number(raw: Mapping[str, Any], key: str, section: str) -> float:
    value = raw.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigError(f"{section}.{key} must be a number")
    if value <= 0:
        raise ConfigError(f"{section}.{key} must be positive")
    return float(value)


def _non_negative_number(raw: Mapping[str, Any], key: str, section: str) -> float:
    value = raw.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigError(f"{section}.{key} must be a number")
    if value < 0:
        raise ConfigError(f"{section}.{key} must be non-negative")
    return float(value)
