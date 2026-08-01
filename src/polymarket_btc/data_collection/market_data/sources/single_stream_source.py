"""Generic single-endpoint WebSocket source.

Connect, hand each decoded JSON message to a caller-supplied parser, publish
the resulting event, and manage reconnect/backoff/health tracking. Factors out
the scaffolding that would otherwise be re-duplicated for every new
single-purpose Binance Futures feed (trades, klines, 24h ticker); existing
sources predate this helper and are left untouched.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable

from websockets.asyncio.client import connect

from ..config import MarketDataConfig
from ..health import HealthRegistry
from ..models import EventSource, InvalidEventError, MarketDataEvent
from .base import ExponentialBackoff

MessageParser = Callable[..., MarketDataEvent]


class SingleStreamSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
        health_registry: HealthRegistry | None = None,
        *,
        event_source: EventSource,
        url: str,
        parse_message: MessageParser,
        recv_timeout: float = 30.0,
        max_size: int = 1024 * 1024,
    ) -> None:
        self._config = config
        self._publish = publish
        self._next_sequence = next_sequence
        self._stop = asyncio.Event()
        self.health_registry = health_registry or HealthRegistry()
        self._on_connection = on_connection or (lambda _connected: None)
        self.event_source = event_source
        self._url = url
        self._parse_message = parse_message
        self._recv_timeout = recv_timeout
        self._max_size = max_size

    def _set_connected(self, connected: bool) -> None:
        if connected:
            self.health_registry.record_connection(self.event_source, None, time.time_ns())
        else:
            self.health_registry.record_disconnection(self.event_source, None, "connection_closed")
        self._on_connection(connected)

    @property
    def connected(self) -> bool:
        return self.health_registry.source_snapshot(self.event_source, time.time_ns()).connected

    @property
    def reconnect_count(self) -> int:
        return self.health_registry.source_snapshot(self.event_source, time.time_ns()).reconnect_count

    @property
    def invalid_count(self) -> int:
        return self.health_registry.source_snapshot(self.event_source, time.time_ns()).invalid_count

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
                async with connect(self._url, max_size=self._max_size) as websocket:
                    self._set_connected(True)
                    backoff.reset()
                    while not self._stop.is_set():
                        raw = await asyncio.wait_for(websocket.recv(), timeout=self._recv_timeout)
                        wall_ns, monotonic_ns = time.time_ns(), time.monotonic_ns()
                        try:
                            event = self._parse_message(
                                json.loads(raw),
                                received_wall_timestamp_ns=wall_ns,
                                received_monotonic_ns=monotonic_ns,
                                ingest_sequence=self._next_sequence(),
                            )
                        except (json.JSONDecodeError, InvalidEventError):
                            self.health_registry.record_invalid(self.event_source)
                            continue
                        await self._publish(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.health_registry.record_reconnect(self.event_source)
                if self._stop.is_set():
                    break
                await asyncio.sleep(backoff.next_delay())
            finally:
                self._set_connected(False)


__all__ = ["SingleStreamSource"]
