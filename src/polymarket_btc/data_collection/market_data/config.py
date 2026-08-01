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
    snapshot_storage_capacity: int


@dataclass(frozen=True, slots=True)
class RtdsConfig:
    url: str
    heartbeat_seconds: float
    stale_after_ms: int


@dataclass(frozen=True, slots=True)
class BinanceConfig:
    url: str
    full_depth_url: str
    symbol: str
    agg_trade_stream: str
    book_ticker_stream: str
    depth_stream: str
    kline_stream: str
    ticker_stream: str
    stale_depth_after_ms: int
    stale_trade_after_ms: int
    stale_full_depth_after_ms: int
    proactive_reconnect_seconds: int


@dataclass(frozen=True, slots=True)
class BinanceFuturesConfig:
    depth_url: str
    mark_price_url: str
    liquidations_url: str
    agg_trade_url: str
    kline_url: str
    ticker_url: str
    symbol: str
    depth_stream: str
    mark_price_stream: str
    agg_trade_stream: str
    kline_stream: str
    ticker_stream: str
    stale_depth_after_ms: int
    stale_mark_price_after_ms: int
    proactive_reconnect_seconds: int
    rest_poll_interval_seconds: float
    mark_price_poll_interval_seconds: float
    kline_poll_interval_seconds: float
    ticker_poll_interval_seconds: float
    agg_trade_poll_interval_seconds: float


@dataclass(frozen=True, slots=True)
class DomConfig:
    bucket_size: float
    price_range: float
    snapshot_interval_ms: int


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
    binance_futures: BinanceFuturesConfig
    dom: DomConfig
    clob: ClobConfig
    reconnect: ReconnectConfig
    storage: StorageConfig
    health: HealthConfig


_SCHEMA = {
    "service": set(ServiceConfig.__annotations__),
    "queues": set(QueueConfig.__annotations__),
    "rtds": set(RtdsConfig.__annotations__),
    "binance": set(BinanceConfig.__annotations__),
    "binance_futures": set(BinanceFuturesConfig.__annotations__),
    "dom": set(DomConfig.__annotations__),
    "clob": set(ClobConfig.__annotations__),
    "reconnect": set(ReconnectConfig.__annotations__),
    "storage": set(StorageConfig.__annotations__),
    "health": set(HealthConfig.__annotations__),
}
_HOSTS = {
    "rtds": "ws-live-data.polymarket.com",
    "binance": "stream.binance.com",
    "binance_futures": "fstream.binance.com",
    "clob": "ws-subscriptions-clob.polymarket.com",
}


def _positive(value: int | float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ConfigurationError(f"{name} must be positive")


def _validate_url(value: str, section: str, allow_custom: bool, field: str = "url") -> None:
    parsed = urlparse(value)
    if parsed.scheme != "wss" or not parsed.hostname:
        raise ConfigurationError(f"{section}.{field} must be WSS")
    if not allow_custom and parsed.hostname != _HOSTS[section]:
        raise ConfigurationError(f"{section}.{field} has an unexpected hostname")


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
    binance_futures_raw = dict(raw["binance_futures"])
    dom_raw = dict(raw["dom"])
    clob_raw = dict(raw["clob"])
    reconnect_raw = dict(raw["reconnect"])
    storage_raw = dict(raw["storage"])
    health_raw = dict(raw["health"])

    for section in ("rtds", "binance", "clob"):
        section_raw = {"rtds": rtds_raw, "binance": binance_raw, "clob": clob_raw}[section]
        _validate_url(str(section_raw["url"]), section, allow_custom_endpoints)
    _validate_url(str(binance_raw["full_depth_url"]), "binance", allow_custom_endpoints, field="full_depth_url")
    for futures_url_field in (
        "depth_url", "mark_price_url", "liquidations_url", "agg_trade_url", "kline_url", "ticker_url",
    ):
        _validate_url(
            str(binance_futures_raw[futures_url_field]),
            "binance_futures",
            allow_custom_endpoints,
            field=futures_url_field,
        )

    if not 50 <= service_raw["snapshot_interval_ms"] <= 5_000:
        raise ConfigurationError("service.snapshot_interval_ms must be between 50 and 5000")
    for name, value in queues_raw.items():
        _positive(value, f"queues.{name}")
    if binance_raw["symbol"] != "btcusdt":
        raise ConfigurationError("binance.symbol must be btcusdt")
    if binance_raw["depth_stream"] != "btcusdt@depth20@100ms":
        raise ConfigurationError("binance.depth_stream must be btcusdt@depth20@100ms")
    if binance_raw["kline_stream"] != "btcusdt@kline_1m":
        raise ConfigurationError("binance.kline_stream must be btcusdt@kline_1m")
    if binance_raw["ticker_stream"] != "btcusdt@ticker":
        raise ConfigurationError("binance.ticker_stream must be btcusdt@ticker")
    if binance_futures_raw["symbol"] != "btcusdt":
        raise ConfigurationError("binance_futures.symbol must be btcusdt")
    if binance_futures_raw["depth_stream"] != "btcusdt@depth@100ms":
        raise ConfigurationError("binance_futures.depth_stream must be btcusdt@depth@100ms")
    if binance_futures_raw["mark_price_stream"] != "btcusdt@markPrice@1s":
        raise ConfigurationError("binance_futures.mark_price_stream must be btcusdt@markPrice@1s")
    if binance_futures_raw["agg_trade_stream"] != "btcusdt@aggTrade":
        raise ConfigurationError("binance_futures.agg_trade_stream must be btcusdt@aggTrade")
    if binance_futures_raw["kline_stream"] != "btcusdt@kline_1m":
        raise ConfigurationError("binance_futures.kline_stream must be btcusdt@kline_1m")
    if binance_futures_raw["ticker_stream"] != "btcusdt@ticker":
        raise ConfigurationError("binance_futures.ticker_stream must be btcusdt@ticker")

    for section_name, values in (
        ("service", service_raw),
        ("rtds", rtds_raw),
        ("binance", binance_raw),
        ("binance_futures", binance_futures_raw),
        ("dom", dom_raw),
        ("clob", clob_raw),
        ("reconnect", reconnect_raw),
        ("storage", storage_raw),
        ("health", health_raw),
    ):
        for name, value in values.items():
            if name in {
                "url", "symbol", "agg_trade_stream", "book_ticker_stream", "depth_stream",
                "log_level", "data_dir", "health_file", "depth_url", "mark_price_url",
                "liquidations_url", "mark_price_stream", "full_depth_url",
                "kline_stream", "ticker_stream", "agg_trade_url", "kline_url", "ticker_url",
            }:
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
        binance_futures=BinanceFuturesConfig(**binance_futures_raw),
        dom=DomConfig(**dom_raw),
        clob=ClobConfig(**clob_raw),
        reconnect=ReconnectConfig(**reconnect_raw),
        storage=StorageConfig(**storage_raw),
        health=HealthConfig(**health_raw),
    )


__all__ = ["ConfigurationError", "MarketDataConfig", "load_config"]
