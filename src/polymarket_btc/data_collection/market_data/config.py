"""Strict TOML configuration for the market data gateway."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib
from urllib.parse import urlparse

from .models import ConfigurationError


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    snapshot_interval_ms: int
    shutdown_timeout_seconds: int
    log_level: str


@dataclass(frozen=True, slots=True)
class QueueConfig:
    state_capacity: int
    storage_capacity: int
    market_state_capacity: int
    subscriber_capacity: int
    put_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class RtdsConfig:
    url: str
    heartbeat_seconds: float
    stale_after_ms: int


@dataclass(frozen=True, slots=True)
class BinanceConfig:
    url: str
    symbol: str
    agg_trade_stream: str
    book_ticker_stream: str
    depth_stream: str
    stale_depth_after_ms: int
    stale_trade_after_ms: int
    proactive_reconnect_seconds: int


@dataclass(frozen=True, slots=True)
class ClobConfig:
    url: str
    heartbeat_seconds: float
    subscription_timeout_seconds: float
    unsubscribe_grace_seconds: float


@dataclass(frozen=True, slots=True)
class ReconnectConfig:
    initial_delay_seconds: float
    maximum_delay_seconds: float
    multiplier: float
    jitter_fraction: float


@dataclass(frozen=True, slots=True)
class StorageConfig:
    data_dir: Path
    raw_rotate_seconds: int
    raw_rotate_bytes: int
    raw_flush_interval_ms: int
    raw_flush_events: int
    raw_fsync_interval_ms: int
    snapshot_rotate_seconds: int
    snapshot_rotate_rows: int
    zstd_level: int


@dataclass(frozen=True, slots=True)
class HealthConfig:
    health_file: Path
    update_interval_seconds: float


@dataclass(frozen=True, slots=True)
class MarketDataConfig:
    service: ServiceConfig
    queues: QueueConfig
    rtds: RtdsConfig
    binance: BinanceConfig
    clob: ClobConfig
    reconnect: ReconnectConfig
    storage: StorageConfig
    health: HealthConfig


_SCHEMA = {
    "service": set(ServiceConfig.__annotations__),
    "queues": set(QueueConfig.__annotations__),
    "rtds": set(RtdsConfig.__annotations__),
    "binance": set(BinanceConfig.__annotations__),
    "clob": set(ClobConfig.__annotations__),
    "reconnect": set(ReconnectConfig.__annotations__),
    "storage": set(StorageConfig.__annotations__),
    "health": set(HealthConfig.__annotations__),
}
_HOSTS = {
    "rtds": "ws-live-data.polymarket.com",
    "binance": "stream.binance.com",
    "clob": "ws-subscriptions-clob.polymarket.com",
}


def _positive(value: int | float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ConfigurationError(f"{name} must be positive")


def _validate_url(value: str, section: str, allow_custom: bool) -> None:
    parsed = urlparse(value)
    if parsed.scheme != "wss" or not parsed.hostname:
        raise ConfigurationError(f"{section}.url must be WSS")
    if not allow_custom and parsed.hostname != _HOSTS[section]:
        raise ConfigurationError(f"{section}.url has an unexpected hostname")


def _strict_sections(raw: dict[str, object]) -> None:
    if set(raw) != set(_SCHEMA):
        raise ConfigurationError("configuration sections do not match the schema")
    for section, keys in _SCHEMA.items():
        value = raw.get(section)
        if not isinstance(value, dict) or set(value) != keys:
            raise ConfigurationError(f"{section} keys do not match the schema")


def load_config(
    path: str | Path,
    *,
    allow_custom_endpoints: bool = False,
) -> MarketDataConfig:
    config_path = Path(path).expanduser().resolve()
    try:
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigurationError(f"cannot read configuration: {exc}") from exc
    _strict_sections(raw)

    service_raw = dict(raw["service"])
    queues_raw = dict(raw["queues"])
    rtds_raw = dict(raw["rtds"])
    binance_raw = dict(raw["binance"])
    clob_raw = dict(raw["clob"])
    reconnect_raw = dict(raw["reconnect"])
    storage_raw = dict(raw["storage"])
    health_raw = dict(raw["health"])

    for section in ("rtds", "binance", "clob"):
        section_raw = {"rtds": rtds_raw, "binance": binance_raw, "clob": clob_raw}[section]
        _validate_url(str(section_raw["url"]), section, allow_custom_endpoints)

    if not 50 <= service_raw["snapshot_interval_ms"] <= 5_000:
        raise ConfigurationError("service.snapshot_interval_ms must be between 50 and 5000")
    for name, value in queues_raw.items():
        _positive(value, f"queues.{name}")
    if binance_raw["symbol"] != "btcusdt":
        raise ConfigurationError("binance.symbol must be btcusdt")
    if binance_raw["depth_stream"] != "btcusdt@depth20@100ms":
        raise ConfigurationError("binance.depth_stream must be btcusdt@depth20@100ms")

    for section_name, values in (
        ("service", service_raw),
        ("rtds", rtds_raw),
        ("binance", binance_raw),
        ("clob", clob_raw),
        ("reconnect", reconnect_raw),
        ("storage", storage_raw),
        ("health", health_raw),
    ):
        for name, value in values.items():
            if name in {"url", "symbol", "agg_trade_stream", "book_ticker_stream", "depth_stream", "log_level", "data_dir", "health_file"}:
                continue
            _positive(value, f"{section_name}.{name}")

    root = config_path.parent.parent if config_path.parent.name == "config" else config_path.parent
    storage_raw["data_dir"] = (root / str(storage_raw["data_dir"])).resolve()
    health_raw["health_file"] = (root / str(health_raw["health_file"])).resolve()

    return MarketDataConfig(
        service=ServiceConfig(**service_raw),
        queues=QueueConfig(**queues_raw),
        rtds=RtdsConfig(**rtds_raw),
        binance=BinanceConfig(**binance_raw),
        clob=ClobConfig(**clob_raw),
        reconnect=ReconnectConfig(**reconnect_raw),
        storage=StorageConfig(**storage_raw),
        health=HealthConfig(**health_raw),
    )


__all__ = ["ConfigurationError", "MarketDataConfig", "load_config"]
