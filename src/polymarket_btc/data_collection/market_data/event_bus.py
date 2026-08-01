"""Bounded in-process event fanout with fatal backpressure."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
import time

from .models import BackpressureFatalError, MarketDataEvent


class EventBus:
    def __init__(
        self,
        state_capacity: int,
        storage_capacity: int,
        market_state_capacity: int,
        *,
        put_timeout_seconds: float,
    ) -> None:
        self.state_queue: asyncio.Queue[MarketDataEvent] = asyncio.Queue(state_capacity)
        self.storage_queue: asyncio.Queue[MarketDataEvent] = asyncio.Queue(storage_capacity)
        self.market_state_queue: asyncio.Queue[object] = asyncio.Queue(market_state_capacity)
        self.put_timeout_seconds = put_timeout_seconds
        self.high_water = {"state": 0, "storage": 0, "market_state": 0}
        self._publish_lock = asyncio.Lock()
        self.closed = False

    async def publish(
        self,
        event: MarketDataEvent,
        sequence_allocator: Callable[[], int] | None = None,
    ) -> bool:
        if self.closed:
            raise BackpressureFatalError("event bus is closed")
        async with self._publish_lock:
            if sequence_allocator is not None:
                event = replace(event, ingest_sequence=sequence_allocator())
            deadline = time.monotonic() + self.put_timeout_seconds
            while self.state_queue.full() or self.storage_queue.full():
                if time.monotonic() >= deadline:
                    raise BackpressureFatalError(
                        "state or storage queue remained full past the deadline"
                    )
                await asyncio.sleep(0.001)
            self.state_queue.put_nowait(event)
            self.storage_queue.put_nowait(event)
            self.high_water["state"] = max(
                self.high_water["state"], self.state_queue.qsize()
            )
            self.high_water["storage"] = max(
                self.high_water["storage"], self.storage_queue.qsize()
            )
            return True

    async def publish_storage_only(
        self,
        event: MarketDataEvent,
        sequence_allocator: Callable[[], int] | None = None,
    ) -> bool:
        """Like publish(), but skips state_queue entirely -- for events whose
        symbol the state reducer isn't built to represent (see StateStore):
        they still get archived, they just never reach the BTC composite
        snapshot pipeline, which would otherwise silently overwrite its
        single-symbol fields with whichever event arrived last."""
        if self.closed:
            raise BackpressureFatalError("event bus is closed")
        async with self._publish_lock:
            if sequence_allocator is not None:
                event = replace(event, ingest_sequence=sequence_allocator())
            deadline = time.monotonic() + self.put_timeout_seconds
            while self.storage_queue.full():
                if time.monotonic() >= deadline:
                    raise BackpressureFatalError(
                        "storage queue remained full past the deadline"
                    )
                await asyncio.sleep(0.001)
            self.storage_queue.put_nowait(event)
            self.high_water["storage"] = max(
                self.high_water["storage"], self.storage_queue.qsize()
            )
            return True

    def publish_market_state_nowait(self, snapshot: object) -> None:
        if self.closed:
            raise BackpressureFatalError("event bus is closed")
        try:
            self.market_state_queue.put_nowait(snapshot)
            self.high_water["market_state"] = max(
                self.high_water["market_state"], self.market_state_queue.qsize()
            )
        except asyncio.QueueFull as exc:
            raise BackpressureFatalError("market state queue is full") from exc

    def close(self) -> None:
        self.closed = True
