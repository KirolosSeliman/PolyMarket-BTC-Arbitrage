"""Binance USDT-M Futures forced-liquidation stream (!forceOrder@arr).

This is an all-symbols stream; every message is filtered down to BTCUSDT
here. Each message is a single forced order Binance's engine just
executed -- useful context for DOM analysis, since a cluster of
liquidations can precede price punching through a thin zone in the book.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable

from websockets.asyncio.client import connect

from ..config import MarketDataConfig
from ..health import HealthRegistry
from ..models import (
    BinanceFuturesLiquidationPayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    parse_decimal,
)
from .base import ExponentialBackoff

_VALID_SIDES = {"BUY", "SELL"}


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field} must be a positive integer")
    return value


def _matching_symbol(message: object, symbol_filters: frozenset[str]) -> str | None:
    """Cheap, non-raising peek at a raw forceOrder message's symbol, without
    validating/consuming anything -- lets the caller decide whether this
    message is even worth turning into an event before it spends a sequence
    number on it (see the run() loop's comment for why that matters)."""
    if not isinstance(message, dict):
        return None
    order = message.get("o")
    if not isinstance(order, dict):
        return None
    symbol = order.get("s")
    if isinstance(symbol, str) and symbol in symbol_filters:
        return symbol
    return None


def parse_liquidation_message(
    message: object,
    *,
    received_wall_timestamp_ns: int,
    received_monotonic_ns: int,
    ingest_sequence: int,
    symbol_filter: str = "BTCUSDT",
) -> MarketDataEvent | None:
    """Returns None (not an error) when the message is a valid liquidation
    for a symbol other than symbol_filter -- expected most of the time on
    this all-symbols stream."""
    if not isinstance(message, dict) or message.get("e") != "forceOrder":
        raise InvalidEventError("unexpected forceOrder message shape")
    order = message.get("o")
    if not isinstance(order, dict):
        raise InvalidEventError("forceOrder message missing order object")
    symbol = order.get("s")
    if not isinstance(symbol, str):
        raise InvalidEventError("forceOrder order missing symbol")
    if symbol != symbol_filter:
        return None
    side = order.get("S")
    if side not in _VALID_SIDES:
        raise InvalidEventError("forceOrder side must be BUY or SELL")
    order_status = order.get("X")
    if not isinstance(order_status, str) or not order_status:
        raise InvalidEventError("forceOrder missing order status")
    trade_time_ms = _positive_int(order.get("T"), "T")
    try:
        price = parse_decimal(str(order.get("p")), "p", strictly_positive=True)
        quantity = parse_decimal(str(order.get("q")), "q", strictly_positive=True)
        average_price = parse_decimal(str(order.get("ap")), "ap", strictly_positive=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures:forceOrder:{symbol}:{trade_time_ms}:{price}:{quantity}",
        source=EventSource.BINANCE_FUTURES_LIQUIDATIONS,
        stream=EventStream.BINANCE_FUTURES_LIQUIDATION,
        instrument=symbol,
        source_timestamp_ns=trade_time_ms * 1_000_000,
        server_timestamp_ns=trade_time_ms * 1_000_000,
        received_wall_timestamp_ns=received_wall_timestamp_ns,
        received_monotonic_ns=received_monotonic_ns,
        source_sequence=str(trade_time_ms),
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=BinanceFuturesLiquidationPayload(
            symbol=symbol,
            side=side,
            price=price,
            quantity=quantity,
            average_price=average_price,
            order_status=order_status,
        ),
    )


class BinanceFuturesLiquidationSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
        health_registry: HealthRegistry | None = None,
        symbol_filter: str = "BTCUSDT",
        *,
        symbol_filters: frozenset[str] | None = None,
    ) -> None:
        self._config = config
        self._publish = publish
        self._next_sequence = next_sequence
        self._stop = asyncio.Event()
        self.health_registry = health_registry or HealthRegistry()
        self._on_connection = on_connection or (lambda _connected: None)
        # symbol_filters (a set) supersedes symbol_filter (a single string,
        # kept for backward compat) when given -- one shared connection can
        # match against several symbols since !forceOrder@arr is already an
        # unfiltered all-symbols stream. Health is recorded once per matched
        # symbol below (each with its own instrument key), except BTC's own
        # default single-symbol construction, which stays keyed exactly as
        # before (instrument=None -> health.py's own "BTC" default).
        self._symbol_filters = symbol_filters or frozenset({symbol_filter})
        self._is_default = symbol_filters is None and symbol_filter == "BTCUSDT"

    def _health_instruments(self) -> tuple[str | None, ...]:
        if self._is_default:
            return (None,)
        return tuple(self._symbol_filters)

    def _set_connected(self, connected: bool) -> None:
        for instrument in self._health_instruments():
            if connected:
                self.health_registry.record_connection(
                    EventSource.BINANCE_FUTURES_LIQUIDATIONS, None, time.time_ns(), instrument=instrument,
                )
            else:
                self.health_registry.record_disconnection(
                    EventSource.BINANCE_FUTURES_LIQUIDATIONS, None, "connection_closed", instrument=instrument,
                )
        self._on_connection(connected)

    @property
    def connected(self) -> bool:
        return self.health_registry.source_snapshot(
            EventSource.BINANCE_FUTURES_LIQUIDATIONS, time.time_ns(), instrument=self._health_instruments()[0],
        ).connected

    @property
    def reconnect_count(self) -> int:
        return self.health_registry.source_snapshot(
            EventSource.BINANCE_FUTURES_LIQUIDATIONS, time.time_ns(), instrument=self._health_instruments()[0],
        ).reconnect_count

    @property
    def invalid_count(self) -> int:
        return self.health_registry.source_snapshot(
            EventSource.BINANCE_FUTURES_LIQUIDATIONS, time.time_ns(), instrument=self._health_instruments()[0],
        ).invalid_count

    async def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = ExponentialBackoff(
            self._config.reconnect.initial_delay_seconds,
            self._config.reconnect.maximum_delay_seconds,
            self._config.reconnect.multiplier,
            self._config.reconnect.jitter_fraction,
        )
        while not self._stop.is_set():
            try:
                async with connect(self._config.binance_futures.liquidations_url, max_size=1024 * 1024) as websocket:
                    self._set_connected(True)
                    backoff.reset()
                    while not self._stop.is_set():
                        raw = await asyncio.wait_for(websocket.recv(), timeout=60)
                        wall_ns, monotonic_ns = time.time_ns(), time.monotonic_ns()
                        try:
                            message = json.loads(raw)
                            # !forceOrder@arr is unfiltered: almost every message
                            # is for a symbol nobody asked to track. Peek at it
                            # before spending a sequence number -- next_sequence()
                            # is a single counter shared by every source in this
                            # gateway, and replay's integrity check requires it to
                            # be gapless once merged, so a number spent on a
                            # message that's never published would corrupt replay
                            # for every OTHER source sharing that counter too.
                            matched_symbol = _matching_symbol(message, self._symbol_filters)
                            if matched_symbol is None:
                                continue
                            event = parse_liquidation_message(
                                message,
                                received_wall_timestamp_ns=wall_ns,
                                received_monotonic_ns=monotonic_ns,
                                ingest_sequence=self._next_sequence(),
                                symbol_filter=matched_symbol,
                            )
                        except (json.JSONDecodeError, InvalidEventError):
                            for instrument in self._health_instruments():
                                self.health_registry.record_invalid(
                                    EventSource.BINANCE_FUTURES_LIQUIDATIONS, instrument=instrument,
                                )
                            continue
                        if event is not None:
                            await self._publish(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                for instrument in self._health_instruments():
                    self.health_registry.record_reconnect(
                        EventSource.BINANCE_FUTURES_LIQUIDATIONS, instrument=instrument,
                    )
                if self._stop.is_set():
                    break
                await asyncio.sleep(backoff.next_delay())
            finally:
                self._set_connected(False)
