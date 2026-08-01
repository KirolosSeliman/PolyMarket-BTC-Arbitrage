"""Binance USDT-M Futures mark price + funding rate stream (markPrice@1s).

This single stream gives both the mark price and the live funding rate --
Binance recomputes the funding rate continuously here, unlike the 8-hour
settlement boundary that applies to historical funding *payments*.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from decimal import Decimal, InvalidOperation

from websockets.asyncio.client import connect

from ..config import MarketDataConfig
from ..health import HealthRegistry
from ..models import (
    BinanceFuturesMarkPricePayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    parse_decimal,
)
from .base import ExponentialBackoff


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field} must be a positive integer")
    return value


def _signed_decimal(value: object, field: str) -> Decimal:
    """Like parse_decimal, but permits negative values -- funding rate can be negative."""
    if not isinstance(value, str):
        raise InvalidEventError(f"{field} must be a decimal string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise InvalidEventError(f"{field} is not a valid decimal") from exc
    if not parsed.is_finite():
        raise InvalidEventError(f"{field} must be finite")
    return parsed


def parse_mark_price_message(
    message: object,
    *,
    received_wall_timestamp_ns: int,
    received_monotonic_ns: int,
    ingest_sequence: int,
) -> MarketDataEvent:
    if not isinstance(message, dict):
        raise InvalidEventError("markPrice message must be an object")
    if message.get("e") != "markPriceUpdate" or message.get("s") != "BTCUSDT":
        raise InvalidEventError("unexpected markPrice message identity")
    event_time_ms = _positive_int(message.get("E"), "E")
    next_funding_ms = _positive_int(message.get("T"), "T")
    try:
        mark_price = parse_decimal(str(message.get("p")), "p", strictly_positive=True)
        index_price = parse_decimal(str(message.get("i")), "i", strictly_positive=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    funding_rate = _signed_decimal(message.get("r"), "r")
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures:markPrice:BTCUSDT:{event_time_ms}",
        source=EventSource.BINANCE_FUTURES_MARK_PRICE,
        stream=EventStream.BINANCE_FUTURES_MARK_PRICE,
        instrument="BTCUSDT",
        source_timestamp_ns=event_time_ms * 1_000_000,
        server_timestamp_ns=event_time_ms * 1_000_000,
        received_wall_timestamp_ns=received_wall_timestamp_ns,
        received_monotonic_ns=received_monotonic_ns,
        source_sequence=str(event_time_ms),
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


class BinanceFuturesMarkPriceSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
        health_registry: HealthRegistry | None = None,
    ) -> None:
        self._config = config
        self._publish = publish
        self._next_sequence = next_sequence
        self._stop = asyncio.Event()
        self.health_registry = health_registry or HealthRegistry()
        self._on_connection = on_connection or (lambda _connected: None)

    def _set_connected(self, connected: bool) -> None:
        if connected:
            self.health_registry.record_connection(
                EventSource.BINANCE_FUTURES_MARK_PRICE, None, time.time_ns()
            )
        else:
            self.health_registry.record_disconnection(
                EventSource.BINANCE_FUTURES_MARK_PRICE, None, "connection_closed"
            )
        self._on_connection(connected)

    @property
    def connected(self) -> bool:
        return self.health_registry.source_snapshot(
            EventSource.BINANCE_FUTURES_MARK_PRICE, time.time_ns()
        ).connected

    @property
    def reconnect_count(self) -> int:
        return self.health_registry.source_snapshot(
            EventSource.BINANCE_FUTURES_MARK_PRICE, time.time_ns()
        ).reconnect_count

    @property
    def invalid_count(self) -> int:
        return self.health_registry.source_snapshot(
            EventSource.BINANCE_FUTURES_MARK_PRICE, time.time_ns()
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
                async with connect(self._config.binance_futures.mark_price_url, max_size=1024 * 1024) as websocket:
                    self._set_connected(True)
                    backoff.reset()
                    while not self._stop.is_set():
                        raw = await asyncio.wait_for(websocket.recv(), timeout=30)
                        wall_ns, monotonic_ns = time.time_ns(), time.monotonic_ns()
                        try:
                            event = parse_mark_price_message(
                                json.loads(raw),
                                received_wall_timestamp_ns=wall_ns,
                                received_monotonic_ns=monotonic_ns,
                                ingest_sequence=self._next_sequence(),
                            )
                        except (json.JSONDecodeError, InvalidEventError):
                            self.health_registry.record_invalid(EventSource.BINANCE_FUTURES_MARK_PRICE)
                            continue
                        await self._publish(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.health_registry.record_reconnect(EventSource.BINANCE_FUTURES_MARK_PRICE)
                if self._stop.is_set():
                    break
                await asyncio.sleep(backoff.next_delay())
            finally:
                self._set_connected(False)
