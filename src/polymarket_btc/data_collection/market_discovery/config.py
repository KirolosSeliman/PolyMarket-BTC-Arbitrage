from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import yaml


class ConfigError(ValueError):
    """Raised when Market Discovery configuration is invalid."""


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    gamma_base_url: str
    slug_template: str
    search_query: str


@dataclass(frozen=True)
class TargetConfig:
    asset: str
    market_family: str
    duration_seconds: int
    expected_outcomes: tuple[str, ...]
    resolution_source: str | None


@dataclass(frozen=True)
class PollingConfig:
    interval_seconds: int
    request_timeout_seconds: int
    max_retries: int
    retry_base_delay_seconds: float
    retry_max_delay_seconds: float
    preload_next_market: bool


@dataclass(frozen=True)
class SelectionConfig:
    require_unique_active_match: bool
    require_condition_id: bool
    require_token_ids: bool
    require_order_book: bool
    reject_missing_timestamps: bool
    reject_unknown_outcomes: bool
    reject_ambiguous_token_mapping: bool
    reject_closed: bool
    reject_archived: bool


@dataclass(frozen=True)
class FailurePolicyConfig:
    no_match: str
    multiple_matches: str
    invalid_candidate: str


@dataclass(frozen=True)
class MarketDiscoveryConfig:
    version: int
    provider: ProviderConfig
    target: TargetConfig
    polling: PollingConfig
    selection: SelectionConfig
    failure_policy: FailurePolicyConfig


TOP_LEVEL_KEYS = frozenset({"version", "market_discovery"})
MARKET_DISCOVERY_KEYS = frozenset(
    {"provider", "target", "polling", "selection", "failure_policy"}
)
PROVIDER_KEYS = frozenset({"name", "gamma_base_url", "slug_template", "search_query"})
TARGET_KEYS = frozenset(
    {"asset", "market_family", "duration_seconds", "expected_outcomes", "resolution_source"}
)
POLLING_KEYS = frozenset(
    {
        "interval_seconds",
        "request_timeout_seconds",
        "max_retries",
        "retry_base_delay_seconds",
        "retry_max_delay_seconds",
        "preload_next_market",
    }
)
SELECTION_KEYS = frozenset(
    {
        "require_unique_active_match",
        "require_condition_id",
        "require_token_ids",
        "require_order_book",
        "reject_missing_timestamps",
        "reject_unknown_outcomes",
        "reject_ambiguous_token_mapping",
        "reject_closed",
        "reject_archived",
    }
)
FAILURE_POLICY_KEYS = frozenset({"no_match", "multiple_matches", "invalid_candidate"})


def load_market_discovery_config(path: Path) -> MarketDiscoveryConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ConfigError("configuration must be a YAML mapping")

    _reject_unknown_keys(raw, TOP_LEVEL_KEYS, "top-level")

    version = _required_int(raw, "version", "top-level")
    if version != 1:
        raise ConfigError("version must be 1")

    section = _required_mapping(raw, "market_discovery", "top-level")
    _reject_unknown_keys(section, MARKET_DISCOVERY_KEYS, "market_discovery")

    provider = _parse_provider(_required_mapping(section, "provider", "market_discovery"))
    target = _parse_target(_required_mapping(section, "target", "market_discovery"))
    polling = _parse_polling(_required_mapping(section, "polling", "market_discovery"))
    selection = _parse_selection(_required_mapping(section, "selection", "market_discovery"))
    failure_policy = _parse_failure_policy(
        _required_mapping(section, "failure_policy", "market_discovery")
    )

    return MarketDiscoveryConfig(
        version=version,
        provider=provider,
        target=target,
        polling=polling,
        selection=selection,
        failure_policy=failure_policy,
    )


def _parse_provider(raw: Mapping[str, Any]) -> ProviderConfig:
    _reject_unknown_keys(raw, PROVIDER_KEYS, "provider")
    name = _required_str(raw, "name", "provider")
    if name != "polymarket":
        raise ConfigError("provider.name must be 'polymarket'")

    gamma_base_url = _required_str(raw, "gamma_base_url", "provider").rstrip("/")
    parsed = urlparse(gamma_base_url)
    if parsed.scheme != "https":
        raise ConfigError("provider.gamma_base_url must use HTTPS")
    if not parsed.netloc:
        raise ConfigError("provider.gamma_base_url must include a host")

    slug_template = _required_str(raw, "slug_template", "provider")
    if "{start_epoch}" not in slug_template:
        raise ConfigError("provider.slug_template must contain {start_epoch}")

    return ProviderConfig(
        name=name,
        gamma_base_url=gamma_base_url,
        slug_template=slug_template,
        search_query=_required_str(raw, "search_query", "provider"),
    )


def _parse_target(raw: Mapping[str, Any]) -> TargetConfig:
    _reject_unknown_keys(raw, TARGET_KEYS, "target")
    expected_outcomes = _required_str_list(raw, "expected_outcomes", "target")
    normalized = tuple(_normalize_outcome_name(value) for value in expected_outcomes)
    if normalized != ("Up", "Down"):
        raise ConfigError("target.expected_outcomes must be exactly ['Up', 'Down']")

    duration_seconds = _required_int(raw, "duration_seconds", "target")
    if duration_seconds != 300:
        raise ConfigError("target.duration_seconds must be 300 for BTC 5m discovery")

    asset = _required_str(raw, "asset", "target").upper()
    if asset != "BTC":
        raise ConfigError("target.asset must be BTC")

    market_family = _required_str(raw, "market_family", "target")
    if market_family != "up_down":
        raise ConfigError("target.market_family must be up_down")

    resolution_source = raw.get("resolution_source")
    if resolution_source is not None and not isinstance(resolution_source, str):
        raise ConfigError("target.resolution_source must be a string when provided")

    return TargetConfig(
        asset=asset,
        market_family=market_family,
        duration_seconds=duration_seconds,
        expected_outcomes=normalized,
        resolution_source=resolution_source,
    )


def _parse_polling(raw: Mapping[str, Any]) -> PollingConfig:
    _reject_unknown_keys(raw, POLLING_KEYS, "polling")
    interval_seconds = _positive_int(raw, "interval_seconds", "polling")
    request_timeout_seconds = _positive_int(raw, "request_timeout_seconds", "polling")
    max_retries = _required_int(raw, "max_retries", "polling")
    if max_retries < 0:
        raise ConfigError("polling.max_retries must be non-negative")

    retry_base_delay_seconds = _positive_number(raw, "retry_base_delay_seconds", "polling")
    retry_max_delay_seconds = _positive_number(raw, "retry_max_delay_seconds", "polling")
    if retry_base_delay_seconds > retry_max_delay_seconds:
        raise ConfigError("polling.retry_base_delay_seconds must not exceed retry_max_delay_seconds")

    return PollingConfig(
        interval_seconds=interval_seconds,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        retry_base_delay_seconds=retry_base_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        preload_next_market=_required_bool(raw, "preload_next_market", "polling"),
    )


def _parse_selection(raw: Mapping[str, Any]) -> SelectionConfig:
    _reject_unknown_keys(raw, SELECTION_KEYS, "selection")
    values = {
        key: _required_bool(raw, key, "selection")
        for key in SELECTION_KEYS
    }
    return SelectionConfig(**values)


def _parse_failure_policy(raw: Mapping[str, Any]) -> FailurePolicyConfig:
    _reject_unknown_keys(raw, FAILURE_POLICY_KEYS, "failure_policy")
    no_match = _required_str(raw, "no_match", "failure_policy")
    multiple_matches = _required_str(raw, "multiple_matches", "failure_policy")
    invalid_candidate = _required_str(raw, "invalid_candidate", "failure_policy")
    for key, value in {
        "no_match": no_match,
        "multiple_matches": multiple_matches,
        "invalid_candidate": invalid_candidate,
    }.items():
        if value not in {"retry", "reject"}:
            raise ConfigError(f"failure_policy.{key} must be 'retry' or 'reject'")

    return FailurePolicyConfig(
        no_match=no_match,
        multiple_matches=multiple_matches,
        invalid_candidate=invalid_candidate,
    )


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


def _positive_int(raw: Mapping[str, Any], key: str, section: str) -> int:
    value = _required_int(raw, key, section)
    if value <= 0:
        raise ConfigError(f"{section}.{key} must be positive")
    return value


def _positive_number(raw: Mapping[str, Any], key: str, section: str) -> float:
    value = raw.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigError(f"{section}.{key} must be a number")
    if value <= 0:
        raise ConfigError(f"{section}.{key} must be positive")
    return float(value)


def _required_bool(raw: Mapping[str, Any], key: str, section: str) -> bool:
    value = raw.get(key)
    if not isinstance(value, bool):
        raise ConfigError(f"{section}.{key} must be a boolean")
    return value


def _required_str_list(raw: Mapping[str, Any], key: str, section: str) -> tuple[str, ...]:
    value = raw.get(key)
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{section}.{key} must be a non-empty list")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise ConfigError(f"{section}.{key} must contain only non-empty strings")
    return tuple(item.strip() for item in value)


def _normalize_outcome_name(value: str) -> str:
    lowered = value.strip().lower()
    if lowered == "up":
        return "Up"
    if lowered == "down":
        return "Down"
    return value.strip()
