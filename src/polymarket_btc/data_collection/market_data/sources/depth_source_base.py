"""Shared reconnect/resync/periodic-emit loop for full-depth order book
sources. Spot and USDT-M Futures depth streams follow the identical
snapshot + diff protocol (see full_depth_reconstruction.py) -- only the
WebSocket/REST endpoints, EventSource, and instrument label differ between
the two concrete subclasses.

Unlike the raw per-message streams elsewhere in this package, an individual
depth diff message is not published as its own MarketDataEvent (that would
be ~10-20 events/second per market just for book maintenance). Instead this
loop reconstructs the book internally and periodically publishes a single
BinanceDomSnapshotPayload -- the bucketed ladder -- at config.dom's
snapshot_interval_ms cadence. That's the artifact downstream DOM analysis
actually consumes.
"""
from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from collections.abc import Awaitable, Callable
from decimal import Decimal

from websockets.asyncio.client import connect

from ..config import MarketDataConfig
from ..health import HealthRegistry
from ..models import EventSource, MarketDataEvent
from .base import ExponentialBackoff
from .dom_bucketing import build_dom_snapshot
from .full_depth_reconstruction import FullDepthOrderBook, ResyncRequired


def _get_json_sync(url: str, timeout: float) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": "polymarket-btc-dom-collector"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


async def _get_json(url: str, *, timeout: float = 10.0) -> dict:
    return await asyncio.to_thread(_get_json_sync, url, timeout)


class BaseFullDepthSource:
    event_source: EventSource
    dom_stream: object  # EventStream, set by subclass
    market_label: str = ""

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
        self._config = config
        self._publish = publish
        self._next_sequence = next_sequence
        self._stop = asyncio.Event()
        self.health_registry = health_registry or HealthRegistry()
        self._on_connection = on_connection or (lambda _connected: None)
        # None (health.py's own default) unless a real symbol override was
        # given -- keeps BTC's own health keyed exactly as before.
        self._symbol_override = symbol.upper() if symbol else None
        self.book = FullDepthOrderBook(symbol=self.instrument)
        self.resync_count = 0

    @property
    def instrument(self) -> str:
        raise NotImplementedError

    def _ws_url(self) -> str:
        raise NotImplementedError

    def _rest_url(self) -> str:
        raise NotImplementedError

    def _set_connected(self, connected: bool) -> None:
        if connected:
            self.health_registry.record_connection(
                self.event_source, None, time.time_ns(), instrument=self._symbol_override,
            )
        else:
            self.health_registry.record_disconnection(
                self.event_source, None, "connection_closed", instrument=self._symbol_override,
            )
        self._on_connection(connected)

    @property
    def connected(self) -> bool:
        return self.health_registry.source_snapshot(
            self.event_source, time.time_ns(), instrument=self._symbol_override,
        ).connected

    @property
    def reconnect_count(self) -> int:
        return self.health_registry.source_snapshot(
            self.event_source, time.time_ns(), instrument=self._symbol_override,
        ).reconnect_count

    @property
    def invalid_count(self) -> int:
        return self.health_registry.source_snapshot(
            self.event_source, time.time_ns(), instrument=self._symbol_override,
        ).invalid_count

    async def stop(self) -> None:
        self._stop.set()

    async def _emit_dom_snapshot(self, now_ns: int) -> None:
        payload = build_dom_snapshot(
            self.book,
            self.market_label,
            bucket_size=Decimal(str(self._config.dom.bucket_size)),
            price_range=Decimal(str(self._config.dom.price_range)),
        )
        if payload is None:
            return
        event = MarketDataEvent(
            schema_version=2,
            ingest_sequence=self._next_sequence(),
            event_id=f"{self.event_source.value}:dom:{self.book.last_update_id}:{now_ns}",
            source=self.event_source,
            stream=self.dom_stream,
            instrument=self.instrument,
            source_timestamp_ns=None,
            server_timestamp_ns=None,
            received_wall_timestamp_ns=now_ns,
            received_monotonic_ns=time.monotonic_ns(),
            source_sequence=str(self.book.last_update_id),
            timeframe=None,
            market_id=None,
            condition_id=None,
            asset_id=None,
            outcome=None,
            payload=payload,
        )
        await self._publish(event)

    async def run(self) -> None:
        backoff = ExponentialBackoff(
            self._config.reconnect.initial_delay_seconds,
            self._config.reconnect.maximum_delay_seconds,
            self._config.reconnect.multiplier,
            self._config.reconnect.jitter_fraction,
        )
        emit_interval = self._config.dom.snapshot_interval_ms / 1_000
        while not self._stop.is_set():
            try:
                async with connect(self._ws_url(), max_size=4 * 1024 * 1024) as websocket:
                    self._set_connected(True)
                    backoff.reset()
                    self.book.reset()
                    snapshot_task = asyncio.create_task(_get_json(self._rest_url()))
                    synced = False
                    next_emit = time.monotonic() + emit_interval
                    while not self._stop.is_set():
                        raw = await asyncio.wait_for(websocket.recv(), timeout=30)
                        event = json.loads(raw)
                        if not synced:
                            self.book.buffer_event(event)
                            if snapshot_task.done():
                                self.book.apply_snapshot(snapshot_task.result())
                                self.resync_count += 1
                                for buffered in self.book.drain_buffer():
                                    try:
                                        self.book.apply_event(buffered)
                                    except ResyncRequired:
                                        break
                                synced = self.book.ready
                            continue
                        try:
                            self.book.apply_event(event)
                        except ResyncRequired:
                            self.health_registry.record_invalid(self.event_source, instrument=self._symbol_override)
                            synced = False
                            self.book.reset()
                            snapshot_task = asyncio.create_task(_get_json(self._rest_url()))
                            continue
                        if time.monotonic() >= next_emit:
                            await self._emit_dom_snapshot(time.time_ns())
                            next_emit = time.monotonic() + emit_interval
            except asyncio.CancelledError:
                raise
            except Exception:
                self.health_registry.record_reconnect(self.event_source, instrument=self._symbol_override)
                if self._stop.is_set():
                    break
                await asyncio.sleep(backoff.next_delay())
            finally:
                self._set_connected(False)
