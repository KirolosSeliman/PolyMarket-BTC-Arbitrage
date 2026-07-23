"""Binance Spot combined-stream parsing and collection."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from decimal import Decimal

from websockets.asyncio.client import connect

from ..config import MarketDataConfig
from ..models import (
    BinanceAggTradePayload,
    BinanceBookTickerPayload,
    BinanceDepth20Payload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    PriceLevel,
    TakerSide,
    parse_decimal,
)
from .base import ExponentialBackoff

STREAMS = (
    "btcusdt@aggTrade",
    "btcusdt@bookTicker",
    "btcusdt@depth20@100ms",
)


def build_combined_url(base_url: str, microseconds: bool = True) -> str:
    suffix = f"?streams={'/'.join(STREAMS)}"
    if microseconds:
        suffix += "&timeUnit=MICROSECOND"
    return base_url + suffix


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field} must be a positive integer")
    return value


def _levels(value: object, side: str) -> tuple[PriceLevel, ...]:
    if not isinstance(value, list) or not 1 <= len(value) <= 20:
        raise InvalidEventError(f"{side} must contain one to twenty levels")
    levels: list[PriceLevel] = []
    prices: set[Decimal] = set()
    for row in value:
        if not isinstance(row, list) or len(row) != 2:
            raise InvalidEventError(f"invalid {side} level")
        try:
            price = parse_decimal(str(row[0]), f"{side}.price", strictly_positive=True)
            quantity = parse_decimal(str(row[1]), f"{side}.quantity", strictly_positive=True)
        except ValueError as exc:
            raise InvalidEventError(str(exc)) from exc
        if price in prices:
            raise InvalidEventError(f"duplicate {side} price")
        prices.add(price)
        levels.append(PriceLevel(price, quantity))
    expected = sorted(levels, key=lambda level: level.price, reverse=side == "bids")
    if levels != expected:
        raise InvalidEventError(f"{side} are not sorted")
    return tuple(levels)


class BinanceMessageParser:
    def __init__(self, connection_session_id: str, *, timestamp_unit: str) -> None:
        if timestamp_unit not in {"microsecond", "millisecond"}:
            raise ValueError("timestamp_unit must be microsecond or millisecond")
        self.session_id = connection_session_id
        self.timestamp_multiplier = 1_000 if timestamp_unit == "microsecond" else 1_000_000
        self.last_agg_trade_id: int | None = None
        self.last_book_update_id: int | None = None
        self.last_depth_update_id: int | None = None

    def parse(
        self,
        message: object,
        *,
        received_wall_timestamp_ns: int,
        received_monotonic_ns: int,
        ingest_sequence: int,
        now_ns: int,
    ) -> MarketDataEvent:
        if not isinstance(message, dict) or not isinstance(message.get("data"), dict):
            raise InvalidEventError("Binance combined message must contain data")
        stream = message.get("stream")
        data = message["data"]
        common = {
            "schema_version": 1,
            "ingest_sequence": ingest_sequence,
            "source": EventSource.BINANCE_SPOT,
            "instrument": "BTCUSDT",
            "received_wall_timestamp_ns": received_wall_timestamp_ns,
            "received_monotonic_ns": received_monotonic_ns,
            "timeframe": None,
            "market_id": None,
            "condition_id": None,
            "asset_id": None,
            "outcome": None,
        }
        if stream == STREAMS[0]:
            return self._agg_trade(data, now_ns, common)
        if stream == STREAMS[1]:
            return self._book_ticker(data, common)
        if stream == STREAMS[2]:
            return self._depth(data, common)
        raise InvalidEventError("unexpected Binance stream")

    def _agg_trade(self, data: dict[str, object], now_ns: int, common: dict[str, object]) -> MarketDataEvent:
        if data.get("e") != "aggTrade" or data.get("s") != "BTCUSDT":
            raise InvalidEventError("invalid aggregate trade identity")
        aggregate_id = _positive_int(data.get("a"), "a")
        if self.last_agg_trade_id is not None and aggregate_id <= self.last_agg_trade_id:
            raise InvalidEventError("aggregate trade ID is not increasing")
        first_id = _positive_int(data.get("f"), "f")
        last_id = _positive_int(data.get("l"), "l")
        if first_id > last_id:
            raise InvalidEventError("first trade ID exceeds last trade ID")
        event_time = _positive_int(data.get("E"), "E")
        trade_time = _positive_int(data.get("T"), "T")
        source_ns = trade_time * self.timestamp_multiplier
        if source_ns > now_ns + 5_000_000_000:
            raise InvalidEventError("trade timestamp is too far in the future")
        if not isinstance(data.get("m"), bool):
            raise InvalidEventError("m must be boolean")
        try:
            price = parse_decimal(str(data.get("p")), "p", strictly_positive=True)
            quantity = parse_decimal(str(data.get("q")), "q", strictly_positive=True)
        except ValueError as exc:
            raise InvalidEventError(str(exc)) from exc
        self.last_agg_trade_id = aggregate_id
        return MarketDataEvent(
            **common,
            event_id=f"binance:aggTrade:BTCUSDT:{aggregate_id}",
            stream=EventStream.BINANCE_AGG_TRADE,
            source_timestamp_ns=source_ns,
            server_timestamp_ns=event_time * self.timestamp_multiplier,
            source_sequence=str(aggregate_id),
            payload=BinanceAggTradePayload(
                "BTCUSDT",
                aggregate_id,
                price,
                quantity,
                first_id,
                last_id,
                source_ns,
                TakerSide.SELL if data["m"] else TakerSide.BUY,
            ),
        )

    def _book_ticker(self, data: dict[str, object], common: dict[str, object]) -> MarketDataEvent:
        if data.get("s") != "BTCUSDT":
            raise InvalidEventError("invalid book ticker symbol")
        update_id = _positive_int(data.get("u"), "u")
        if self.last_book_update_id is not None and update_id <= self.last_book_update_id:
            raise InvalidEventError("book ticker ID is not increasing")
        try:
            bid = parse_decimal(str(data.get("b")), "b", strictly_positive=True)
            ask = parse_decimal(str(data.get("a")), "a", strictly_positive=True)
            bid_quantity = parse_decimal(str(data.get("B")), "B", allow_zero=True)
            ask_quantity = parse_decimal(str(data.get("A")), "A", allow_zero=True)
        except ValueError as exc:
            raise InvalidEventError(str(exc)) from exc
        if bid >= ask:
            raise InvalidEventError("book ticker is crossed")
        self.last_book_update_id = update_id
        return MarketDataEvent(
            **common,
            event_id=f"binance:bookTicker:BTCUSDT:{self.session_id}:{update_id}",
            stream=EventStream.BINANCE_BOOK_TICKER,
            source_timestamp_ns=None,
            server_timestamp_ns=None,
            source_sequence=str(update_id),
            payload=BinanceBookTickerPayload(
                "BTCUSDT", update_id, bid, bid_quantity, ask, ask_quantity
            ),
        )

    def _depth(self, data: dict[str, object], common: dict[str, object]) -> MarketDataEvent:
        update_id = _positive_int(data.get("lastUpdateId"), "lastUpdateId")
        if self.last_depth_update_id is not None and update_id <= self.last_depth_update_id:
            raise InvalidEventError("depth update ID is not increasing")
        bids = _levels(data.get("bids"), "bids")
        asks = _levels(data.get("asks"), "asks")
        if bids[0].price >= asks[0].price:
            raise InvalidEventError("depth snapshot is crossed")
        self.last_depth_update_id = update_id
        return MarketDataEvent(
            **common,
            event_id=f"binance:depth20:BTCUSDT:{self.session_id}:{update_id}",
            stream=EventStream.BINANCE_DEPTH20,
            source_timestamp_ns=None,
            server_timestamp_ns=None,
            source_sequence=str(update_id),
            payload=BinanceDepth20Payload("BTCUSDT", update_id, bids, asks),
        )


class BinanceSpotSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
    ) -> None:
        self._config = config
        self._publish = publish
        self._next_sequence = next_sequence
        self._stop = asyncio.Event()
        self.reconnect_count = 0
        self.invalid_count = 0
        self.connected = False

    async def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        microseconds = True
        backoff = ExponentialBackoff(
            self._config.reconnect.initial_delay_seconds,
            self._config.reconnect.maximum_delay_seconds,
            self._config.reconnect.multiplier,
            self._config.reconnect.jitter_fraction,
        )
        while not self._stop.is_set():
            try:
                parser = BinanceMessageParser(
                    uuid.uuid4().hex,
                    timestamp_unit="microsecond" if microseconds else "millisecond",
                )
                reconnect_after = self._config.binance.proactive_reconnect_seconds
                reconnect_after += uuid.uuid4().int % 31
                async with connect(
                    build_combined_url(self._config.binance.url, microseconds),
                    max_size=4 * 1024 * 1024,
                ) as websocket:
                    self.connected = True
                    deadline = time.monotonic() + reconnect_after
                    while not self._stop.is_set() and time.monotonic() < deadline:
                        timeout = max(0.1, deadline - time.monotonic())
                        raw = await asyncio.wait_for(websocket.recv(), timeout)
                        wall_ns, monotonic_ns = time.time_ns(), time.monotonic_ns()
                        try:
                            event = parser.parse(
                                json.loads(raw),
                                received_wall_timestamp_ns=wall_ns,
                                received_monotonic_ns=monotonic_ns,
                                ingest_sequence=self._next_sequence(),
                                now_ns=wall_ns,
                            )
                        except (json.JSONDecodeError, InvalidEventError):
                            self.invalid_count += 1
                            continue
                        await self._publish(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.reconnect_count += 1
                if self._stop.is_set():
                    break
                await asyncio.sleep(backoff.next_delay())
            finally:
                self.connected = False
