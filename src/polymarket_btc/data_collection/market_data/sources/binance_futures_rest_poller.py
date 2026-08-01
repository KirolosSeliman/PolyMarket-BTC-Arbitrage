"""Poller for the futures data Binance only exposes over REST: open
interest and the top-trader long/short account ratio. Neither has a
WebSocket push -- this is the only way to get them.

Binance itself only recomputes these every 5 minutes server-side
(period=5m is the finest granularity offered), so polling faster than
that does not produce fresher data -- config.binance_futures's
rest_poll_interval_seconds defaults to 300 to match that real cadence.
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
    BinanceFuturesLongShortRatioPayload,
    BinanceFuturesOpenInterestPayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    parse_decimal,
)

OPEN_INTEREST_URL = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
LONG_SHORT_URL = (
    "https://fapi.binance.com/futures/data/topLongShortAccountRatio"
    "?symbol=BTCUSDT&period=5m&limit=1"
)


def _get_json_sync(url: str, timeout: float) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": "polymarket-btc-dom-collector"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


async def _get_json(url: str, *, timeout: float = 10.0) -> object:
    return await asyncio.to_thread(_get_json_sync, url, timeout)


def parse_open_interest(data: object, *, ingest_sequence: int, now_ns: int) -> MarketDataEvent:
    if not isinstance(data, dict) or data.get("symbol") != "BTCUSDT":
        raise InvalidEventError("unexpected openInterest response shape")
    try:
        open_interest = parse_decimal(str(data.get("openInterest")), "openInterest", allow_zero=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_rest:openInterest:BTCUSDT:{now_ns}",
        source=EventSource.BINANCE_FUTURES_REST,
        stream=EventStream.BINANCE_FUTURES_OPEN_INTEREST,
        instrument="BTCUSDT",
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
        payload=BinanceFuturesOpenInterestPayload(open_interest=open_interest),
    )


def parse_long_short_ratio(data: object, *, ingest_sequence: int, now_ns: int) -> MarketDataEvent | None:
    if not isinstance(data, list) or not data:
        return None  # valid empty response, nothing new yet
    latest = data[-1]
    if not isinstance(latest, dict) or latest.get("symbol") != "BTCUSDT":
        raise InvalidEventError("unexpected topLongShortAccountRatio response shape")
    try:
        long_account = parse_decimal(str(latest.get("longAccount")), "longAccount", allow_zero=True)
        short_account = parse_decimal(str(latest.get("shortAccount")), "shortAccount", allow_zero=True)
        ratio = parse_decimal(str(latest.get("longShortRatio")), "longShortRatio", allow_zero=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_rest:longShortRatio:BTCUSDT:{now_ns}",
        source=EventSource.BINANCE_FUTURES_REST,
        stream=EventStream.BINANCE_FUTURES_LONG_SHORT_RATIO,
        instrument="BTCUSDT",
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
        payload=BinanceFuturesLongShortRatioPayload(
            long_account_ratio=long_account,
            short_account_ratio=short_account,
            long_short_ratio=ratio,
        ),
    )


class BinanceFuturesRestPoller:
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
            self.health_registry.record_connection(EventSource.BINANCE_FUTURES_REST, None, time.time_ns())
        else:
            self.health_registry.record_disconnection(
                EventSource.BINANCE_FUTURES_REST, None, "poll_failed"
            )
        self._on_connection(connected)

    @property
    def connected(self) -> bool:
        return self.health_registry.source_snapshot(EventSource.BINANCE_FUTURES_REST, time.time_ns()).connected

    @property
    def reconnect_count(self) -> int:
        return self.health_registry.source_snapshot(
            EventSource.BINANCE_FUTURES_REST, time.time_ns()
        ).reconnect_count

    @property
    def invalid_count(self) -> int:
        return self.health_registry.source_snapshot(EventSource.BINANCE_FUTURES_REST, time.time_ns()).invalid_count

    async def stop(self) -> None:
        self._stop.set()

    async def _poll_once(self) -> None:
        now_ns = time.time_ns()
        try:
            oi_data = await _get_json(OPEN_INTEREST_URL)
            event = parse_open_interest(oi_data, ingest_sequence=self._next_sequence(), now_ns=now_ns)
            await self._publish(event)
        except (InvalidEventError, OSError, ValueError) as exc:
            self.health_registry.record_invalid(EventSource.BINANCE_FUTURES_REST)
            self._set_connected(False)
            raise ConnectionError("open interest poll failed") from exc

        try:
            ls_data = await _get_json(LONG_SHORT_URL)
            ls_event = parse_long_short_ratio(ls_data, ingest_sequence=self._next_sequence(), now_ns=now_ns)
            if ls_event is not None:
                await self._publish(ls_event)
        except (InvalidEventError, OSError, ValueError) as exc:
            self.health_registry.record_invalid(EventSource.BINANCE_FUTURES_REST)
            self._set_connected(False)
            raise ConnectionError("long/short ratio poll failed") from exc

        self._set_connected(True)

    async def run(self) -> None:
        interval = self._config.binance_futures.rest_poll_interval_seconds
        while not self._stop.is_set():
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.health_registry.record_reconnect(EventSource.BINANCE_FUTURES_REST)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except TimeoutError:
                pass
