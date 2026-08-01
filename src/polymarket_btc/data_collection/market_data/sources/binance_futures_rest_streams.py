"""REST-polling fallback for the four USDT-M Futures feeds whose WebSocket
push (aggTrade, markPrice/funding, kline, 24h ticker) Binance geo-restricts
in some jurisdictions while leaving the equivalent REST endpoints open --
confirmed for at least one CA-egress IP: `depth`/`bookTicker` WebSocket
streams deliver normally, these four deliver zero messages indefinitely,
and the REST endpoints below answer normally from the same IP.

This mirrors BinanceFuturesRestPoller's open-interest/long-short pattern:
same error handling shape, same "connected means last poll succeeded"
health semantics. aggTrade uses an incremental `fromId` cursor so a poll
only ever reprocesses trades it hasn't already published.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from collections.abc import Awaitable, Callable

from ..config import MarketDataConfig
from ..health import HealthRegistry
from ..models import (
    BinanceAggTradePayload,
    BinanceFuturesMarkPricePayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    TakerSide,
    parse_decimal,
    parse_signed_decimal,
)
from .binance_kline_ticker import parse_kline_data, parse_ticker_24h_data

def premium_index_url(symbol: str) -> str:
    return f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"


def kline_url(symbol: str) -> str:
    return f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=2"


def ticker_url(symbol: str) -> str:
    return f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}"


def agg_trades_url(symbol: str) -> str:
    return f"https://fapi.binance.com/fapi/v1/aggTrades?symbol={symbol}&limit=1"


def agg_trades_from_id_url(symbol: str, from_id: int) -> str:
    return f"https://fapi.binance.com/fapi/v1/aggTrades?symbol={symbol}&limit=1000&fromId={from_id}"


# Kept as the BTC defaults -- nothing outside this module referenced these
# directly, but the plain module-level constants remain the simplest form
# for the (overwhelmingly common) single-symbol case.
PREMIUM_INDEX_URL = premium_index_url("BTCUSDT")
KLINE_URL = kline_url("BTCUSDT")
TICKER_URL = ticker_url("BTCUSDT")
AGG_TRADES_URL = agg_trades_url("BTCUSDT")


def _get_json_sync(url: str, timeout: float) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": "polymarket-btc-dom-collector"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


async def _get_json(url: str, *, timeout: float = 10.0) -> object:
    return await asyncio.to_thread(_get_json_sync, url, timeout)


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field} must be a positive integer")
    return value


def parse_premium_index(
    data: object, *, ingest_sequence: int, now_ns: int, symbol: str = "BTCUSDT",
) -> MarketDataEvent:
    if not isinstance(data, dict) or data.get("symbol") != symbol:
        raise InvalidEventError("unexpected premiumIndex response shape")
    next_funding_ms = _positive_int(data.get("nextFundingTime"), "nextFundingTime")
    try:
        mark_price = parse_decimal(str(data.get("markPrice")), "markPrice", strictly_positive=True)
        index_price = parse_decimal(str(data.get("indexPrice")), "indexPrice", strictly_positive=True)
        funding_rate = parse_signed_decimal(str(data.get("lastFundingRate")), "lastFundingRate")
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_rest:premiumIndex:{symbol}:{now_ns}",
        source=EventSource.BINANCE_FUTURES_MARK_PRICE,
        stream=EventStream.BINANCE_FUTURES_MARK_PRICE,
        instrument=symbol,
        source_timestamp_ns=None,
        server_timestamp_ns=None,
        received_wall_timestamp_ns=now_ns,
        received_monotonic_ns=time.monotonic_ns(),
        source_sequence=None,
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=BinanceFuturesMarkPricePayload(
            mark_price=mark_price,
            index_price=index_price,
            funding_rate=funding_rate,
            next_funding_time_ns=next_funding_ms * 1_000_000,
        ),
    )


def parse_rest_klines(
    data: object, *, ingest_sequence: int, now_ns: int, symbol: str = "BTCUSDT",
) -> MarketDataEvent:
    if not isinstance(data, list) or not data:
        raise InvalidEventError("klines response must be a non-empty array")
    latest = data[-1]
    if not isinstance(latest, list) or len(latest) < 11:
        raise InvalidEventError("kline row is malformed")
    message = {
        "e": "kline", "s": symbol,
        "k": {
            "t": latest[0], "T": latest[6], "s": symbol, "i": "1m",
            "o": latest[1], "h": latest[2], "l": latest[3], "c": latest[4],
            "v": latest[5], "q": latest[7], "n": latest[8],
            "x": now_ns >= int(latest[6]) * 1_000_000,  # REST has no closed flag; derive it
        },
    }
    payload = parse_kline_data(message, market="futures", symbol=symbol, interval="1m")
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_rest:kline:{symbol}:1m:{payload.open_time_ns}:{now_ns}",
        source=EventSource.BINANCE_FUTURES_KLINE,
        stream=EventStream.BINANCE_KLINE,
        instrument=symbol,
        source_timestamp_ns=payload.close_time_ns,
        server_timestamp_ns=payload.close_time_ns,
        received_wall_timestamp_ns=now_ns,
        received_monotonic_ns=time.monotonic_ns(),
        source_sequence=None,
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=payload,
    )


def parse_rest_ticker_24h(
    data: object, *, ingest_sequence: int, now_ns: int, symbol: str = "BTCUSDT",
) -> MarketDataEvent:
    if not isinstance(data, dict) or data.get("symbol") != symbol:
        raise InvalidEventError("unexpected ticker/24hr response shape")
    message = {
        "e": "24hrTicker", "s": symbol,
        "p": data.get("priceChange"), "P": data.get("priceChangePercent"),
        "w": data.get("weightedAvgPrice"), "c": data.get("lastPrice"),
        "o": data.get("openPrice"), "h": data.get("highPrice"), "l": data.get("lowPrice"),
        "v": data.get("volume"), "q": data.get("quoteVolume"),
    }
    payload = parse_ticker_24h_data(message, market="futures", symbol=symbol)
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_rest:ticker24h:{symbol}:{now_ns}",
        source=EventSource.BINANCE_FUTURES_TICKER,
        stream=EventStream.BINANCE_TICKER_24H,
        instrument=symbol,
        source_timestamp_ns=None,
        server_timestamp_ns=None,
        received_wall_timestamp_ns=now_ns,
        received_monotonic_ns=time.monotonic_ns(),
        source_sequence=None,
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=payload,
    )


def parse_rest_agg_trades(
    data: object, *, next_sequence: Callable[[], int], now_ns: int, symbol: str = "BTCUSDT",
) -> list[MarketDataEvent]:
    if not isinstance(data, list):
        raise InvalidEventError("aggTrades response must be an array")
    events: list[MarketDataEvent] = []
    for row in data:
        if not isinstance(row, dict):
            raise InvalidEventError("aggTrades row must be an object")
        aggregate_id = _positive_int(row.get("a"), "a")
        first_id = _positive_int(row.get("f"), "f")
        last_id = _positive_int(row.get("l"), "l")
        trade_time_ms = _positive_int(row.get("T"), "T")
        if not isinstance(row.get("m"), bool):
            raise InvalidEventError("m must be boolean")
        try:
            price = parse_decimal(str(row.get("p")), "p", strictly_positive=True)
            quantity = parse_decimal(str(row.get("q")), "q", strictly_positive=True)
        except ValueError as exc:
            raise InvalidEventError(str(exc)) from exc
        trade_ns = trade_time_ms * 1_000_000
        events.append(MarketDataEvent(
            schema_version=2,
            ingest_sequence=next_sequence(),
            event_id=f"binance_futures_rest:aggTrade:{symbol}:{aggregate_id}",
            source=EventSource.BINANCE_FUTURES_TRADE,
            stream=EventStream.BINANCE_FUTURES_AGG_TRADE,
            instrument=symbol,
            source_timestamp_ns=trade_ns,
            server_timestamp_ns=None,
            received_wall_timestamp_ns=now_ns,
            received_monotonic_ns=time.monotonic_ns(),
            source_sequence=str(aggregate_id),
            timeframe=None,
            market_id=None,
            condition_id=None,
            asset_id=None,
            outcome=None,
            payload=BinanceAggTradePayload(
                symbol, aggregate_id, price, quantity, first_id, last_id, trade_ns,
                TakerSide.SELL if row["m"] else TakerSide.BUY,
            ),
        ))
    return events


class _RestPollSource:
    """Generic REST poll loop: same connect/health/backoff shape as the
    WebSocket sources, just polling instead of streaming."""

    def __init__(
        self,
        *,
        event_source: EventSource,
        interval_seconds: float,
        poll_once: Callable[[], Awaitable[list[MarketDataEvent]]],
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        health_registry: HealthRegistry,
        on_connection: Callable[[bool], None],
        instrument: str = "BTC",
    ) -> None:
        self.event_source = event_source
        self._interval_seconds = interval_seconds
        self._poll_once = poll_once
        self._publish = publish
        self.health_registry = health_registry
        self._on_connection = on_connection
        self._instrument = instrument
        self._stop = asyncio.Event()

    def _set_connected(self, connected: bool) -> None:
        if connected:
            self.health_registry.record_connection(
                self.event_source, None, time.time_ns(), instrument=self._instrument,
            )
        else:
            self.health_registry.record_disconnection(
                self.event_source, None, "poll_failed", instrument=self._instrument,
            )
        self._on_connection(connected)

    @property
    def connected(self) -> bool:
        return self.health_registry.source_snapshot(
            self.event_source, time.time_ns(), instrument=self._instrument,
        ).connected

    @property
    def reconnect_count(self) -> int:
        return self.health_registry.source_snapshot(
            self.event_source, time.time_ns(), instrument=self._instrument,
        ).reconnect_count

    @property
    def invalid_count(self) -> int:
        return self.health_registry.source_snapshot(
            self.event_source, time.time_ns(), instrument=self._instrument,
        ).invalid_count

    async def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                events = await self._poll_once()
                for event in events:
                    await self._publish(event)
                self._set_connected(True)
            except asyncio.CancelledError:
                raise
            except (InvalidEventError, OSError, ValueError, json.JSONDecodeError):
                self.health_registry.record_invalid(self.event_source, instrument=self._instrument)
                self._set_connected(False)
                self.health_registry.record_reconnect(self.event_source, instrument=self._instrument)
            except Exception:
                self.health_registry.record_invalid(self.event_source, instrument=self._instrument)
                self._set_connected(False)
                self.health_registry.record_reconnect(self.event_source, instrument=self._instrument)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass


class BinanceFuturesMarkPriceRestSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
        health_registry: HealthRegistry | None = None,
        *,
        symbol: str | None = None,
    ) -> None:
        resolved_symbol = (symbol or config.binance_futures.symbol).upper()
        health_instrument = resolved_symbol if symbol is not None else None
        url = premium_index_url(resolved_symbol)

        async def poll_once() -> list[MarketDataEvent]:
            data = await _get_json(url)
            return [parse_premium_index(
                data, ingest_sequence=next_sequence(), now_ns=time.time_ns(), symbol=resolved_symbol,
            )]

        self._inner = _RestPollSource(
            event_source=EventSource.BINANCE_FUTURES_MARK_PRICE,
            interval_seconds=config.binance_futures.mark_price_poll_interval_seconds,
            poll_once=poll_once,
            publish=publish,
            health_registry=health_registry or HealthRegistry(),
            on_connection=on_connection or (lambda _connected: None),
            instrument=health_instrument,
        )
        self.health_registry = self._inner.health_registry

    @property
    def connected(self) -> bool:
        return self._inner.connected

    @property
    def reconnect_count(self) -> int:
        return self._inner.reconnect_count

    @property
    def invalid_count(self) -> int:
        return self._inner.invalid_count

    async def stop(self) -> None:
        await self._inner.stop()

    async def run(self) -> None:
        await self._inner.run()


class BinanceFuturesKlineRestSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
        health_registry: HealthRegistry | None = None,
        *,
        symbol: str | None = None,
    ) -> None:
        resolved_symbol = (symbol or config.binance_futures.symbol).upper()
        health_instrument = resolved_symbol if symbol is not None else None
        url = kline_url(resolved_symbol)

        async def poll_once() -> list[MarketDataEvent]:
            data = await _get_json(url)
            return [parse_rest_klines(
                data, ingest_sequence=next_sequence(), now_ns=time.time_ns(), symbol=resolved_symbol,
            )]

        self._inner = _RestPollSource(
            event_source=EventSource.BINANCE_FUTURES_KLINE,
            interval_seconds=config.binance_futures.kline_poll_interval_seconds,
            poll_once=poll_once,
            publish=publish,
            health_registry=health_registry or HealthRegistry(),
            on_connection=on_connection or (lambda _connected: None),
            instrument=health_instrument,
        )
        self.health_registry = self._inner.health_registry

    @property
    def connected(self) -> bool:
        return self._inner.connected

    @property
    def reconnect_count(self) -> int:
        return self._inner.reconnect_count

    @property
    def invalid_count(self) -> int:
        return self._inner.invalid_count

    async def stop(self) -> None:
        await self._inner.stop()

    async def run(self) -> None:
        await self._inner.run()


class BinanceFuturesTicker24hRestSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
        health_registry: HealthRegistry | None = None,
        *,
        symbol: str | None = None,
    ) -> None:
        resolved_symbol = (symbol or config.binance_futures.symbol).upper()
        health_instrument = resolved_symbol if symbol is not None else None
        url = ticker_url(resolved_symbol)

        async def poll_once() -> list[MarketDataEvent]:
            data = await _get_json(url)
            return [parse_rest_ticker_24h(
                data, ingest_sequence=next_sequence(), now_ns=time.time_ns(), symbol=resolved_symbol,
            )]

        self._inner = _RestPollSource(
            event_source=EventSource.BINANCE_FUTURES_TICKER,
            interval_seconds=config.binance_futures.ticker_poll_interval_seconds,
            poll_once=poll_once,
            publish=publish,
            health_registry=health_registry or HealthRegistry(),
            on_connection=on_connection or (lambda _connected: None),
            instrument=health_instrument,
        )
        self.health_registry = self._inner.health_registry

    @property
    def connected(self) -> bool:
        return self._inner.connected

    @property
    def reconnect_count(self) -> int:
        return self._inner.reconnect_count

    @property
    def invalid_count(self) -> int:
        return self._inner.invalid_count

    async def stop(self) -> None:
        await self._inner.stop()

    async def run(self) -> None:
        await self._inner.run()


class BinanceFuturesTradeRestSource:
    """Incrementally polls /fapi/v1/aggTrades with a `fromId` cursor so a
    poll only returns trades this source hasn't already published."""

    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
        health_registry: HealthRegistry | None = None,
        *,
        symbol: str | None = None,
    ) -> None:
        resolved_symbol = (symbol or config.binance_futures.symbol).upper()
        health_instrument = resolved_symbol if symbol is not None else None
        self._last_id: int | None = None

        async def poll_once() -> list[MarketDataEvent]:
            url = (
                agg_trades_url(resolved_symbol)
                if self._last_id is None
                else agg_trades_from_id_url(resolved_symbol, self._last_id + 1)
            )
            data = await _get_json(url)
            events = parse_rest_agg_trades(
                data, next_sequence=next_sequence, now_ns=time.time_ns(), symbol=resolved_symbol,
            )
            if events:
                self._last_id = int(events[-1].source_sequence)  # type: ignore[arg-type]
            return events

        self._inner = _RestPollSource(
            event_source=EventSource.BINANCE_FUTURES_TRADE,
            interval_seconds=config.binance_futures.agg_trade_poll_interval_seconds,
            poll_once=poll_once,
            publish=publish,
            health_registry=health_registry or HealthRegistry(),
            on_connection=on_connection or (lambda _connected: None),
            instrument=health_instrument,
        )
        self.health_registry = self._inner.health_registry

    @property
    def connected(self) -> bool:
        return self._inner.connected

    @property
    def reconnect_count(self) -> int:
        return self._inner.reconnect_count

    @property
    def invalid_count(self) -> int:
        return self._inner.invalid_count

    async def stop(self) -> None:
        await self._inner.stop()

    async def run(self) -> None:
        await self._inner.run()


__all__ = [
    "BinanceFuturesKlineRestSource",
    "BinanceFuturesMarkPriceRestSource",
    "BinanceFuturesTicker24hRestSource",
    "BinanceFuturesTradeRestSource",
    "parse_premium_index",
    "parse_rest_agg_trades",
    "parse_rest_klines",
    "parse_rest_ticker_24h",
]
