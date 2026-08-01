"""Immutable canonical models for the market data gateway."""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
import json
from pathlib import Path
from typing import TypeAlias

from polymarket_btc.data_collection.market_discovery import Timeframe


class MarketDataError(Exception):
    """Base class for market data failures."""


class ConfigurationError(MarketDataError):
    """Configuration is invalid."""


class SourceConnectionError(MarketDataError):
    """A source connection failed."""


class SourceProtocolError(MarketDataError):
    """A provider violated its documented protocol."""


class InvalidEventError(MarketDataError):
    """An external event failed validation."""


class BackpressureFatalError(MarketDataError):
    """A required bounded queue could not accept data."""


class StorageFatalError(MarketDataError):
    """Durable storage cannot continue safely."""


class ReplayIntegrityError(MarketDataError):
    """Replay input failed an integrity check."""


class GatewayInvariantError(MarketDataError):
    """An internal gateway invariant was violated."""


class ShutdownTimeoutError(MarketDataError):
    """The gateway did not shut down before its deadline."""


class EventSource(str, Enum):
    MARKET_DISCOVERY = "market_discovery"
    CHAINLINK_RTDS = "chainlink_rtds"
    BINANCE_SPOT = "binance_spot"
    POLYMARKET_CLOB = "polymarket_clob"
    BINANCE_SPOT_FULL_DEPTH = "binance_spot_full_depth"
    BINANCE_FUTURES_DEPTH = "binance_futures_depth"
    BINANCE_FUTURES_MARK_PRICE = "binance_futures_mark_price"
    BINANCE_FUTURES_LIQUIDATIONS = "binance_futures_liquidations"
    BINANCE_FUTURES_REST = "binance_futures_rest"
    BINANCE_FUTURES_TRADE = "binance_futures_trade"
    BINANCE_FUTURES_KLINE = "binance_futures_kline"
    BINANCE_FUTURES_TICKER = "binance_futures_ticker"


class EventStream(str, Enum):
    MARKET_WINDOW_STATE = "market_window_state"
    SOURCE_STATUS = "source_status"
    SNAPSHOT_TICK = "snapshot_tick"
    CHAINLINK_PRICE = "chainlink_price"
    BINANCE_AGG_TRADE = "binance_agg_trade"
    BINANCE_BOOK_TICKER = "binance_book_ticker"
    BINANCE_DEPTH20 = "binance_depth20"
    POLYMARKET_BOOK = "polymarket_book"
    POLYMARKET_PRICE_CHANGE = "polymarket_price_change"
    POLYMARKET_BEST_BID_ASK = "polymarket_best_bid_ask"
    POLYMARKET_LAST_TRADE = "polymarket_last_trade"
    POLYMARKET_TICK_SIZE_CHANGE = "polymarket_tick_size_change"
    POLYMARKET_MARKET_RESOLVED = "polymarket_market_resolved"
    BINANCE_SPOT_DOM_SNAPSHOT = "binance_spot_dom_snapshot"
    BINANCE_FUTURES_DOM_SNAPSHOT = "binance_futures_dom_snapshot"
    BINANCE_FUTURES_MARK_PRICE = "binance_futures_mark_price"
    BINANCE_FUTURES_LIQUIDATION = "binance_futures_liquidation"
    BINANCE_FUTURES_OPEN_INTEREST = "binance_futures_open_interest"
    BINANCE_FUTURES_LONG_SHORT_RATIO = "binance_futures_long_short_ratio"
    BINANCE_FUTURES_AGG_TRADE = "binance_futures_agg_trade"
    BINANCE_KLINE = "binance_kline"
    BINANCE_TICKER_24H = "binance_ticker_24h"


class TakerSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class Outcome(str, Enum):
    UP = "up"
    DOWN = "down"


def parse_decimal(
    value: str | Decimal,
    field_name: str,
    *,
    allow_zero: bool = False,
    strictly_positive: bool = False,
) -> Decimal:
    if not isinstance(value, (str, Decimal)) or isinstance(value, bool):
        raise ValueError(f"{field_name} must be a decimal string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} is not a valid decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if parsed < 0:
        raise ValueError(f"{field_name} cannot be negative")
    if strictly_positive and parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    if parsed == 0 and not allow_zero and not strictly_positive:
        return parsed
    return parsed


def parse_signed_decimal(value: str, field_name: str) -> Decimal:
    """Like parse_decimal, but permits negative values (funding rate, price
    change, and price-change-percent can all be negative)."""
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a decimal string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} is not a valid decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return parsed


@dataclass(frozen=True, slots=True)
class PriceLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        parse_decimal(self.price, "price", strictly_positive=True)
        parse_decimal(self.quantity, "quantity", allow_zero=True)


@dataclass(frozen=True, slots=True)
class ChainlinkPricePayload:
    symbol: str
    price: Decimal


@dataclass(frozen=True, slots=True)
class BinanceAggTradePayload:
    symbol: str
    aggregate_trade_id: int
    price: Decimal
    quantity: Decimal
    first_trade_id: int
    last_trade_id: int
    trade_timestamp_ns: int
    taker_side: TakerSide


@dataclass(frozen=True, slots=True)
class BinanceBookTickerPayload:
    symbol: str
    update_id: int
    best_bid_price: Decimal
    best_bid_quantity: Decimal
    best_ask_price: Decimal
    best_ask_quantity: Decimal


@dataclass(frozen=True, slots=True)
class BinanceDepth20Payload:
    symbol: str
    last_update_id: int
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]


@dataclass(frozen=True, slots=True)
class PolymarketBookPayload:
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    book_hash: str | None


@dataclass(frozen=True, slots=True)
class PolymarketPriceChangePayload:
    side: str
    price: Decimal
    quantity: Decimal
    best_bid: Decimal | None
    best_ask: Decimal | None
    change_hash: str | None


@dataclass(frozen=True, slots=True)
class PolymarketBestBidAskPayload:
    best_bid: Decimal | None
    best_ask: Decimal | None
    spread: Decimal | None


@dataclass(frozen=True, slots=True)
class PolymarketLastTradePayload:
    price: Decimal
    quantity: Decimal
    side: str
    fee_rate_bps: Decimal | None


@dataclass(frozen=True, slots=True)
class PolymarketTickSizePayload:
    old_tick_size: Decimal
    new_tick_size: Decimal


@dataclass(frozen=True, slots=True)
class PolymarketResolvedPayload:
    winning_asset_id: str | None
    winning_outcome: Outcome | None
    affected_asset_ids: tuple[str, ...] = ()
    market_id: str | None = None
    condition_id: str | None = None


@dataclass(frozen=True, slots=True)
class DomBucketPayload:
    price: Decimal  # bucket floor price
    bid_quantity: Decimal
    ask_quantity: Decimal


@dataclass(frozen=True, slots=True)
class BinanceDomSnapshotPayload:
    market: str  # "spot" or "futures"
    mid_price: Decimal
    bucket_size: Decimal
    buckets: tuple[DomBucketPayload, ...]


@dataclass(frozen=True, slots=True)
class BinanceFuturesMarkPricePayload:
    mark_price: Decimal
    index_price: Decimal
    funding_rate: Decimal
    next_funding_time_ns: int


@dataclass(frozen=True, slots=True)
class BinanceFuturesLiquidationPayload:
    symbol: str
    side: str
    price: Decimal
    quantity: Decimal
    average_price: Decimal
    order_status: str


@dataclass(frozen=True, slots=True)
class BinanceFuturesOpenInterestPayload:
    open_interest: Decimal


@dataclass(frozen=True, slots=True)
class BinanceFuturesLongShortRatioPayload:
    long_account_ratio: Decimal
    short_account_ratio: Decimal
    long_short_ratio: Decimal


@dataclass(frozen=True, slots=True)
class BinanceKlinePayload:
    market: str  # "spot" or "futures"
    interval: str
    open_time_ns: int
    close_time_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    quote_volume: Decimal
    trade_count: int
    is_closed: bool


@dataclass(frozen=True, slots=True)
class BinanceTicker24hPayload:
    market: str  # "spot" or "futures"
    price_change: Decimal
    price_change_percent: Decimal
    weighted_avg_price: Decimal
    last_price: Decimal
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    base_volume: Decimal
    quote_volume: Decimal


@dataclass(frozen=True, slots=True)
class SourceHealthSnapshot:
    connected: bool
    stale: bool
    current_session_id: str | None
    last_message_timestamp_ns: int | None
    age_ms: int | None
    reconnect_count: int
    invalid_count: int
    duplicate_count: int
    stale_session_count: int
    divergence_count: int
    protocol_error_count: int


@dataclass(frozen=True, slots=True)
class SourceStatusPayload:
    source: EventSource
    connected: bool
    session_id: str | None
    reconnect_count: int
    reason: str | None


@dataclass(frozen=True, slots=True)
class SnapshotTickPayload:
    snapshot_sequence: int
    scheduled_timestamp_ns: int
    health: tuple[tuple[EventSource, SourceHealthSnapshot], ...]


@dataclass(frozen=True, slots=True)
class MarketWindowPayload:
    timeframe: Timeframe
    market_id: str
    condition_id: str
    slug: str
    start_timestamp_ns: int
    end_timestamp_ns: int
    up_token_id: str
    down_token_id: str
    resolution_source: str


@dataclass(frozen=True, slots=True)
class MarketWindowStatePayload:
    state: str
    timeframe: Timeframe
    current_market: MarketWindowPayload | None
    next_market: MarketWindowPayload | None
    expected_transition_timestamp_ns: int | None
    updated_at_timestamp_ns: int
    attempt_count: int
    last_error: str | None


EventPayload: TypeAlias = (
    ChainlinkPricePayload
    | BinanceAggTradePayload
    | BinanceBookTickerPayload
    | BinanceDepth20Payload
    | PolymarketBookPayload
    | PolymarketPriceChangePayload
    | PolymarketBestBidAskPayload
    | PolymarketLastTradePayload
    | PolymarketTickSizePayload
    | PolymarketResolvedPayload
    | MarketWindowStatePayload
    | SourceStatusPayload
    | SnapshotTickPayload
    | BinanceDomSnapshotPayload
    | BinanceFuturesMarkPricePayload
    | BinanceFuturesLiquidationPayload
    | BinanceFuturesOpenInterestPayload
    | BinanceFuturesLongShortRatioPayload
    | BinanceKlinePayload
    | BinanceTicker24hPayload
)


@dataclass(frozen=True, slots=True)
class MarketDataEvent:
    schema_version: int
    ingest_sequence: int
    event_id: str
    source: EventSource
    stream: EventStream
    instrument: str
    source_timestamp_ns: int | None
    server_timestamp_ns: int | None
    received_wall_timestamp_ns: int
    received_monotonic_ns: int
    source_sequence: str | None
    timeframe: Timeframe | None
    market_id: str | None
    condition_id: str | None
    asset_id: str | None
    outcome: Outcome | None
    payload: EventPayload
    source_session_id: str | None = None


@dataclass(frozen=True, slots=True)
class ChainlinkSnapshot:
    price: Decimal | None
    source_timestamp_ns: int | None
    received_timestamp_ns: int | None
    age_ms: int | None


@dataclass(frozen=True, slots=True)
class BinanceSnapshot:
    last_price: Decimal | None
    taker_side: TakerSide | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    best_bid_quantity: Decimal | None
    best_ask_quantity: Decimal | None
    depth_bids: tuple[PriceLevel, ...]
    depth_asks: tuple[PriceLevel, ...]
    mid_price: Decimal | None
    spread: Decimal | None
    spread_bps: Decimal | None
    microprice: Decimal | None
    top1_imbalance: Decimal | None
    top20_bid_notional: Decimal | None
    top20_ask_notional: Decimal | None
    top20_depth_imbalance: Decimal | None
    rolling_windows: tuple["RollingWindowSnapshot", ...]


@dataclass(frozen=True, slots=True)
class RollingWindowSnapshot:
    window_seconds: int
    buy_volume: Decimal
    sell_volume: Decimal
    vwap: Decimal | None


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    asset_id: str
    market_id: str
    condition_id: str
    outcome: Outcome
    source_session_id: str | None
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    best_bid: Decimal | None
    best_ask: Decimal | None
    last_trade_price: Decimal | None
    tick_size: Decimal | None
    initialized: bool
    coherent: bool
    resolved: bool
    last_event_timestamp_ns: int | None


@dataclass(frozen=True, slots=True)
class PolymarketTimeframeSnapshot:
    timeframe: Timeframe
    market_id: str | None
    condition_id: str | None
    start_timestamp_ns: int | None
    end_timestamp_ns: int | None
    remaining_ms: int | None
    up: OrderBookSnapshot | None
    down: OrderBookSnapshot | None
    resolved: bool
    ready: bool
    not_ready_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PolymarketSnapshot:
    five_minutes: PolymarketTimeframeSnapshot | None
    fifteen_minutes: PolymarketTimeframeSnapshot | None


@dataclass(frozen=True, slots=True)
class MarketDataSnapshot:
    schema_version: int
    snapshot_sequence: int
    snapshot_timestamp_ns: int
    market_5m: PolymarketTimeframeSnapshot | None
    market_15m: PolymarketTimeframeSnapshot | None
    chainlink: ChainlinkSnapshot
    binance: BinanceSnapshot
    polymarket: PolymarketSnapshot
    health: tuple[tuple[EventSource, SourceHealthSnapshot], ...]
    ready_for_strategy: bool
    not_ready_reasons: tuple[str, ...]
    spot_dom: BinanceDomSnapshotPayload | None = None
    futures_dom: BinanceDomSnapshotPayload | None = None
    futures_mark_price: BinanceFuturesMarkPricePayload | None = None
    futures_open_interest: BinanceFuturesOpenInterestPayload | None = None
    futures_long_short_ratio: BinanceFuturesLongShortRatioPayload | None = None
    futures_last_liquidation: BinanceFuturesLiquidationPayload | None = None
    futures_last_trade: BinanceAggTradePayload | None = None
    spot_recent_trades: tuple[BinanceAggTradePayload, ...] = ()
    futures_recent_trades: tuple[BinanceAggTradePayload, ...] = ()
    spot_kline: BinanceKlinePayload | None = None
    futures_kline: BinanceKlinePayload | None = None
    spot_ticker_24h: BinanceTicker24hPayload | None = None
    futures_ticker_24h: BinanceTicker24hPayload | None = None


def _jsonable(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {
            field.name: _jsonable(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def json_dumps(value: object) -> str:
    return json.dumps(
        _jsonable(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _level_from_dict(value: dict[str, object]) -> PriceLevel:
    return PriceLevel(Decimal(str(value["price"])), Decimal(str(value["quantity"])))


def _source_health_from_dict(value: dict[str, object]) -> SourceHealthSnapshot:
    return SourceHealthSnapshot(
        connected=bool(value["connected"]),
        stale=bool(value["stale"]),
        current_session_id=(
            value.get("current_session_id")
            if isinstance(value.get("current_session_id"), str)
            else None
        ),
        last_message_timestamp_ns=(
            None
            if value.get("last_message_timestamp_ns") is None
            else int(value["last_message_timestamp_ns"])
        ),
        age_ms=None if value.get("age_ms") is None else int(value["age_ms"]),
        reconnect_count=int(value["reconnect_count"]),
        invalid_count=int(value["invalid_count"]),
        duplicate_count=int(value["duplicate_count"]),
        stale_session_count=int(value.get("stale_session_count", 0)),
        divergence_count=int(value.get("divergence_count", 0)),
        protocol_error_count=int(value.get("protocol_error_count", 0)),
    )


def _market_window_from_dict(value: object) -> MarketWindowPayload | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("market window must be an object")
    return MarketWindowPayload(
        timeframe=Timeframe(str(value["timeframe"])),
        market_id=str(value["market_id"]),
        condition_id=str(value["condition_id"]),
        slug=str(value["slug"]),
        start_timestamp_ns=int(value["start_timestamp_ns"]),
        end_timestamp_ns=int(value["end_timestamp_ns"]),
        up_token_id=str(value["up_token_id"]),
        down_token_id=str(value["down_token_id"]),
        resolution_source=str(value["resolution_source"]),
    )


def event_from_dict(value: dict[str, object]) -> MarketDataEvent:
    schema_version = int(value["schema_version"])
    if schema_version not in {1, 2}:
        raise ReplayIntegrityError(
            f"unsupported market data event schema version: {schema_version}"
        )
    stream = EventStream(str(value["stream"]))
    raw_payload = value["payload"]
    if not isinstance(raw_payload, dict):
        raise ValueError("payload must be an object")
    payload: EventPayload
    if stream is EventStream.CHAINLINK_PRICE:
        payload = ChainlinkPricePayload(
            str(raw_payload["symbol"]),
            Decimal(str(raw_payload["price"])),
        )
    elif stream in (EventStream.BINANCE_AGG_TRADE, EventStream.BINANCE_FUTURES_AGG_TRADE):
        payload = BinanceAggTradePayload(
            str(raw_payload["symbol"]),
            int(raw_payload["aggregate_trade_id"]),
            Decimal(str(raw_payload["price"])),
            Decimal(str(raw_payload["quantity"])),
            int(raw_payload["first_trade_id"]),
            int(raw_payload["last_trade_id"]),
            int(raw_payload["trade_timestamp_ns"]),
            TakerSide(str(raw_payload["taker_side"])),
        )
    elif stream is EventStream.BINANCE_KLINE:
        payload = BinanceKlinePayload(
            str(raw_payload["market"]),
            str(raw_payload["interval"]),
            int(raw_payload["open_time_ns"]),
            int(raw_payload["close_time_ns"]),
            Decimal(str(raw_payload["open"])),
            Decimal(str(raw_payload["high"])),
            Decimal(str(raw_payload["low"])),
            Decimal(str(raw_payload["close"])),
            Decimal(str(raw_payload["base_volume"])),
            Decimal(str(raw_payload["quote_volume"])),
            int(raw_payload["trade_count"]),
            bool(raw_payload["is_closed"]),
        )
    elif stream is EventStream.BINANCE_TICKER_24H:
        payload = BinanceTicker24hPayload(
            str(raw_payload["market"]),
            Decimal(str(raw_payload["price_change"])),
            Decimal(str(raw_payload["price_change_percent"])),
            Decimal(str(raw_payload["weighted_avg_price"])),
            Decimal(str(raw_payload["last_price"])),
            Decimal(str(raw_payload["open_price"])),
            Decimal(str(raw_payload["high_price"])),
            Decimal(str(raw_payload["low_price"])),
            Decimal(str(raw_payload["base_volume"])),
            Decimal(str(raw_payload["quote_volume"])),
        )
    elif stream in (EventStream.BINANCE_SPOT_DOM_SNAPSHOT, EventStream.BINANCE_FUTURES_DOM_SNAPSHOT):
        payload = BinanceDomSnapshotPayload(
            str(raw_payload["market"]),
            Decimal(str(raw_payload["mid_price"])),
            Decimal(str(raw_payload["bucket_size"])),
            tuple(
                DomBucketPayload(
                    Decimal(str(bucket["price"])),
                    Decimal(str(bucket["bid_quantity"])),
                    Decimal(str(bucket["ask_quantity"])),
                )
                for bucket in raw_payload["buckets"]  # type: ignore[union-attr]
            ),
        )
    elif stream is EventStream.BINANCE_FUTURES_MARK_PRICE:
        payload = BinanceFuturesMarkPricePayload(
            Decimal(str(raw_payload["mark_price"])),
            Decimal(str(raw_payload["index_price"])),
            Decimal(str(raw_payload["funding_rate"])),
            int(raw_payload["next_funding_time_ns"]),
        )
    elif stream is EventStream.BINANCE_FUTURES_LIQUIDATION:
        payload = BinanceFuturesLiquidationPayload(
            str(raw_payload["symbol"]),
            str(raw_payload["side"]),
            Decimal(str(raw_payload["price"])),
            Decimal(str(raw_payload["quantity"])),
            Decimal(str(raw_payload["average_price"])),
            str(raw_payload["order_status"]),
        )
    elif stream is EventStream.BINANCE_FUTURES_OPEN_INTEREST:
        payload = BinanceFuturesOpenInterestPayload(
            Decimal(str(raw_payload["open_interest"])),
        )
    elif stream is EventStream.BINANCE_FUTURES_LONG_SHORT_RATIO:
        payload = BinanceFuturesLongShortRatioPayload(
            Decimal(str(raw_payload["long_account_ratio"])),
            Decimal(str(raw_payload["short_account_ratio"])),
            Decimal(str(raw_payload["long_short_ratio"])),
        )
    elif stream is EventStream.BINANCE_BOOK_TICKER:
        payload = BinanceBookTickerPayload(
            str(raw_payload["symbol"]),
            int(raw_payload["update_id"]),
            Decimal(str(raw_payload["best_bid_price"])),
            Decimal(str(raw_payload["best_bid_quantity"])),
            Decimal(str(raw_payload["best_ask_price"])),
            Decimal(str(raw_payload["best_ask_quantity"])),
        )
    elif stream is EventStream.BINANCE_DEPTH20:
        payload = BinanceDepth20Payload(
            str(raw_payload["symbol"]),
            int(raw_payload["last_update_id"]),
            tuple(_level_from_dict(item) for item in raw_payload["bids"]),  # type: ignore[union-attr]
            tuple(_level_from_dict(item) for item in raw_payload["asks"]),  # type: ignore[union-attr]
        )
    elif stream is EventStream.POLYMARKET_BOOK:
        payload = PolymarketBookPayload(
            tuple(_level_from_dict(item) for item in raw_payload["bids"]),  # type: ignore[union-attr]
            tuple(_level_from_dict(item) for item in raw_payload["asks"]),  # type: ignore[union-attr]
            raw_payload.get("book_hash") if isinstance(raw_payload.get("book_hash"), str) else None,
        )
    elif stream is EventStream.POLYMARKET_PRICE_CHANGE:
        payload = PolymarketPriceChangePayload(
            str(raw_payload["side"]),
            Decimal(str(raw_payload["price"])),
            Decimal(str(raw_payload["quantity"])),
            None if raw_payload.get("best_bid") is None else Decimal(str(raw_payload["best_bid"])),
            None if raw_payload.get("best_ask") is None else Decimal(str(raw_payload["best_ask"])),
            raw_payload.get("change_hash") if isinstance(raw_payload.get("change_hash"), str) else None,
        )
    elif stream is EventStream.POLYMARKET_BEST_BID_ASK:
        payload = PolymarketBestBidAskPayload(
            None if raw_payload.get("best_bid") is None else Decimal(str(raw_payload["best_bid"])),
            None if raw_payload.get("best_ask") is None else Decimal(str(raw_payload["best_ask"])),
            None if raw_payload.get("spread") is None else Decimal(str(raw_payload["spread"])),
        )
    elif stream is EventStream.POLYMARKET_LAST_TRADE:
        payload = PolymarketLastTradePayload(
            Decimal(str(raw_payload["price"])),
            Decimal(str(raw_payload["quantity"])),
            str(raw_payload["side"]),
            None if raw_payload.get("fee_rate_bps") is None else Decimal(str(raw_payload["fee_rate_bps"])),
        )
    elif stream is EventStream.POLYMARKET_TICK_SIZE_CHANGE:
        payload = PolymarketTickSizePayload(
            Decimal(str(raw_payload["old_tick_size"])),
            Decimal(str(raw_payload["new_tick_size"])),
        )
    elif stream is EventStream.POLYMARKET_MARKET_RESOLVED:
        payload = PolymarketResolvedPayload(
            raw_payload.get("winning_asset_id") if isinstance(raw_payload.get("winning_asset_id"), str) else None,
            None if raw_payload.get("winning_outcome") is None else Outcome(str(raw_payload["winning_outcome"])),
            tuple(str(item) for item in raw_payload.get("affected_asset_ids", ())),
            raw_payload.get("market_id") if isinstance(raw_payload.get("market_id"), str) else None,
            raw_payload.get("condition_id") if isinstance(raw_payload.get("condition_id"), str) else None,
        )
    elif stream is EventStream.SOURCE_STATUS:
        payload = SourceStatusPayload(
            EventSource(str(raw_payload["source"])),
            bool(raw_payload["connected"]),
            (
                raw_payload.get("session_id")
                if isinstance(raw_payload.get("session_id"), str)
                else None
            ),
            int(raw_payload["reconnect_count"]),
            raw_payload.get("reason") if isinstance(raw_payload.get("reason"), str) else None,
        )
    elif stream is EventStream.SNAPSHOT_TICK:
        raw_health = raw_payload.get("health")
        if not isinstance(raw_health, list):
            raise ValueError("snapshot tick health must be a list")
        health: list[tuple[EventSource, SourceHealthSnapshot]] = []
        for row in raw_health:
            if (
                not isinstance(row, list)
                or len(row) != 2
                or not isinstance(row[1], dict)
            ):
                raise ValueError("snapshot tick health row is invalid")
            health.append(
                (EventSource(str(row[0])), _source_health_from_dict(row[1]))
            )
        payload = SnapshotTickPayload(
            int(raw_payload["snapshot_sequence"]),
            int(raw_payload["scheduled_timestamp_ns"]),
            tuple(health),
        )
    else:
        timeframe = (
            Timeframe(str(raw_payload["timeframe"]))
            if schema_version == 2
            else Timeframe(str(value["timeframe"]))
        )
        if schema_version == 2:
            payload = MarketWindowStatePayload(
                str(raw_payload["state"]),
                timeframe,
                _market_window_from_dict(raw_payload.get("current_market")),
                _market_window_from_dict(raw_payload.get("next_market")),
                (
                    None
                    if raw_payload.get("expected_transition_timestamp_ns") is None
                    else int(raw_payload["expected_transition_timestamp_ns"])
                ),
                int(raw_payload["updated_at_timestamp_ns"]),
                int(raw_payload["attempt_count"]),
                (
                    raw_payload.get("last_error")
                    if isinstance(raw_payload.get("last_error"), str)
                    else None
                ),
            )
        else:
            legacy_market = None
            if (
                value.get("market_id") is not None
                and value.get("condition_id") is not None
                and raw_payload.get("start_timestamp_ns") is not None
                and raw_payload.get("end_timestamp_ns") is not None
                and raw_payload.get("up_token_id") is not None
                and raw_payload.get("down_token_id") is not None
            ):
                legacy_market = MarketWindowPayload(
                    timeframe,
                    str(value["market_id"]),
                    str(value["condition_id"]),
                    str(value["market_id"]),
                    int(raw_payload["start_timestamp_ns"]),
                    int(raw_payload["end_timestamp_ns"]),
                    str(raw_payload["up_token_id"]),
                    str(raw_payload["down_token_id"]),
                    "",
                )
            payload = MarketWindowStatePayload(
                str(raw_payload["state"]),
                timeframe,
                legacy_market,
                None,
                None,
                int(value["received_wall_timestamp_ns"]),
                0,
                None,
            )
    return MarketDataEvent(
        schema_version=schema_version,
        ingest_sequence=int(value["ingest_sequence"]),
        event_id=str(value["event_id"]),
        source=EventSource(str(value["source"])),
        stream=stream,
        instrument=str(value["instrument"]),
        source_timestamp_ns=None if value.get("source_timestamp_ns") is None else int(value["source_timestamp_ns"]),
        server_timestamp_ns=None if value.get("server_timestamp_ns") is None else int(value["server_timestamp_ns"]),
        received_wall_timestamp_ns=int(value["received_wall_timestamp_ns"]),
        received_monotonic_ns=int(value["received_monotonic_ns"]),
        source_sequence=value.get("source_sequence") if isinstance(value.get("source_sequence"), str) else None,
        timeframe=None if value.get("timeframe") is None else Timeframe(str(value["timeframe"])),
        market_id=value.get("market_id") if isinstance(value.get("market_id"), str) else None,
        condition_id=value.get("condition_id") if isinstance(value.get("condition_id"), str) else None,
        asset_id=value.get("asset_id") if isinstance(value.get("asset_id"), str) else None,
        outcome=None if value.get("outcome") is None else Outcome(str(value["outcome"])),
        payload=payload,
        source_session_id=(
            value.get("source_session_id")
            if schema_version == 2 and isinstance(value.get("source_session_id"), str)
            else None
        ),
    )
