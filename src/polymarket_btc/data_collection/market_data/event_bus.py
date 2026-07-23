"""Bounded in-process event fanout with fatal backpressure."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import replace
import time

from .models import BackpressureFatalError, EventStream, MarketDataEvent


class EventBus:
    def __init__(
        self,
        state_capacity: int,
        storage_capacity: int,
        market_state_capacity: int,
        *,
        put_timeout_seconds: float,
        deduplication_capacity: int = 100_000,
    ) -> None:
        self.state_queue: asyncio.Queue[MarketDataEvent] = asyncio.Queue(state_capacity)
        self.storage_queue: asyncio.Queue[MarketDataEvent] = asyncio.Queue(storage_capacity)
        self.market_state_queue: asyncio.Queue[object] = asyncio.Queue(market_state_capacity)
        self.put_timeout_seconds = put_timeout_seconds
        self.deduplication_capacity = deduplication_capacity
        self.duplicate_count = 0
        self.high_water = {"state": 0, "storage": 0, "market_state": 0}
        self._seen: dict[EventStream, OrderedDict[str, None]] = {}
        self._publish_lock = asyncio.Lock()
        self.closed = False

    def _is_duplicate(self, event: MarketDataEvent) -> bool:
        seen = self._seen.setdefault(event.stream, OrderedDict())
        if event.event_id in seen:
            seen.move_to_end(event.event_id)
            self.duplicate_count += 1
            return True
        seen[event.event_id] = None
        while len(seen) > self.deduplication_capacity:
            seen.popitem(last=False)
        return False

    async def publish(
        self,
        event: MarketDataEvent,
        sequence_allocator: Callable[[], int] | None = None,
    ) -> bool:
        if self.closed:
            raise BackpressureFatalError("event bus is closed")
        async with self._publish_lock:
            if self._is_duplicate(event):
                return False
            if sequence_allocator is not None:
                event = replace(event, ingest_sequence=sequence_allocator())
            deadline = time.monotonic() + self.put_timeout_seconds
            while self.state_queue.full() or self.storage_queue.full():
                if time.monotonic() >= deadline:
                    self._seen[event.stream].pop(event.event_id, None)
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
