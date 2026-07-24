"""Polymarket RTDS Chainlink parsing and collection."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable

from websockets.asyncio.client import connect

from ..config import MarketDataConfig
from ..health import HealthRegistry
from ..models import (
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    SourceConnectionError,
    parse_decimal,
)
from .base import ExponentialBackoff

SUBSCRIPTION = {
    "action": "subscribe",
    "subscriptions": [{
        "topic": "crypto_prices_chainlink",
        "type": "*",
        "filters": '{"symbol":"btc/usd"}',
    }],
}


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field} must be a positive integer")
    return value


def parse_chainlink_message(
    message: object,
    *,
    received_wall_timestamp_ns: int,
    received_monotonic_ns: int,
    ingest_sequence: int,
    now_ns: int,
    last_source_timestamp_ns: int | None = None,
) -> MarketDataEvent:
    if not isinstance(message, dict):
        raise InvalidEventError("RTDS message must be an object")
    if message.get("topic") != "crypto_prices_chainlink":
        raise InvalidEventError("unexpected RTDS topic")
    if message.get("type") != "update":
        raise InvalidEventError("unexpected RTDS message type")
    payload = message.get("payload")
    if not isinstance(payload, dict):
        raise InvalidEventError("RTDS payload must be an object")
    if payload.get("symbol") != "btc/usd":
        raise InvalidEventError("unexpected Chainlink symbol")
    payload_ms = _integer(payload.get("timestamp"), "payload.timestamp")
    server_ms = _integer(message.get("timestamp"), "timestamp")
    source_ns = payload_ms * 1_000_000
    if source_ns > now_ns + 5_000_000_000:
        raise InvalidEventError("Chainlink timestamp is too far in the future")
    if last_source_timestamp_ns is not None and source_ns < last_source_timestamp_ns:
        raise InvalidEventError("Chainlink timestamp is out of order")
    if "value" not in payload or payload["value"] is None:
        raise InvalidEventError("Chainlink price is missing")
    try:
        price = parse_decimal(str(payload["value"]), "payload.value", strictly_positive=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"chainlink:{payload_ms}:{server_ms}",
        source=EventSource.CHAINLINK_RTDS,
        stream=EventStream.CHAINLINK_PRICE,
        instrument="BTC/USD",
        source_timestamp_ns=source_ns,
        server_timestamp_ns=server_ms * 1_000_000,
        received_wall_timestamp_ns=received_wall_timestamp_ns,
        received_monotonic_ns=received_monotonic_ns,
        source_sequence=str(payload_ms),
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=ChainlinkPricePayload(symbol="btc/usd", price=price),
    )


class ChainlinkRtdsSource:
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
        self._last_source_ns: int | None = None
        self.health_registry = health_registry or HealthRegistry()
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._on_connection = on_connection or (lambda _connected: None)

    def _set_connected(self, connected: bool) -> None:
        if connected:
            self.health_registry.record_connection(EventSource.CHAINLINK_RTDS, None, time.time_ns())
        else:
            self.health_registry.record_disconnection(EventSource.CHAINLINK_RTDS, None, "connection_closed")
        self._on_connection(connected)

    @property
    def connected(self) -> bool:
        return self.health_registry.source_snapshot(EventSource.CHAINLINK_RTDS, time.time_ns()).connected

    @property
    def reconnect_count(self) -> int:
        return self.health_registry.source_snapshot(EventSource.CHAINLINK_RTDS, time.time_ns()).reconnect_count

    @property
    def invalid_count(self) -> int:
        return self.health_registry.source_snapshot(EventSource.CHAINLINK_RTDS, time.time_ns()).invalid_count

    async def stop(self) -> None:
        self._stop.set()

    async def _heartbeat(self, websocket: object) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self._config.rtds.heartbeat_seconds)
            await websocket.send("PING")  # type: ignore[attr-defined]

    async def run(self) -> None:
        backoff = ExponentialBackoff(
            self._config.reconnect.initial_delay_seconds,
            self._config.reconnect.maximum_delay_seconds,
            self._config.reconnect.multiplier,
            self._config.reconnect.jitter_fraction,
        )
        while not self._stop.is_set():
            heartbeat: asyncio.Task[None] | None = None
            try:
                async with connect(
                    self._config.rtds.url,
                    max_size=4 * 1024 * 1024,
                    ping_interval=None,
                ) as websocket:
                    self._set_connected(True)
                    await websocket.send(json.dumps(SUBSCRIPTION, separators=(",", ":")))
                    heartbeat = asyncio.create_task(self._heartbeat(websocket))
                    connected_at = time.monotonic()
                    async for raw in websocket:
                        wall_ns, monotonic_ns = time.time_ns(), time.monotonic_ns()
                        if self._stop.is_set():
                            break
                        try:
                            message = json.loads(raw)
                            event = parse_chainlink_message(
                                message,
                                received_wall_timestamp_ns=wall_ns,
                                received_monotonic_ns=monotonic_ns,
                                ingest_sequence=self._next_sequence(),
                                now_ns=wall_ns,
                                last_source_timestamp_ns=self._last_source_ns,
                            )
                        except (json.JSONDecodeError, InvalidEventError):
                            self.health_registry.record_invalid(EventSource.CHAINLINK_RTDS)
                            logging.getLogger(__name__).warning("invalid Chainlink message")
                            continue
                        if event.event_id in self._seen:
                            self.health_registry.record_duplicate(EventSource.CHAINLINK_RTDS)
                            continue
                        self._seen[event.event_id] = None
                        while len(self._seen) > 4096:
                            self._seen.popitem(last=False)
                        self._last_source_ns = event.source_timestamp_ns
                        await self._publish(event)
                    if time.monotonic() - connected_at >= 30:
                        backoff.reset()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health_registry.record_reconnect(EventSource.CHAINLINK_RTDS)
                if self._stop.is_set():
                    break
                await asyncio.sleep(backoff.next_delay())
            finally:
                self._set_connected(False)
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
