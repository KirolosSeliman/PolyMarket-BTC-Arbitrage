"""Monotonic 250 ms snapshot publisher with bounded subscribers."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from .models import MarketDataSnapshot


class SnapshotPublisher:
    def __init__(
        self,
        *,
        subscriber_capacity: int,
    ) -> None:
        self.subscriber_capacity = subscriber_capacity
        self._subscribers: set[asyncio.Queue[MarketDataSnapshot | None]] = set()
        self._stop = asyncio.Event()
        self.slow_subscribers_removed = 0

    def subscribe(self) -> asyncio.Queue[MarketDataSnapshot | None]:
        if self._stop.is_set():
            raise RuntimeError("snapshot publisher is stopping")
        queue: asyncio.Queue[MarketDataSnapshot | None] = asyncio.Queue(
            self.subscriber_capacity
        )
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[MarketDataSnapshot | None]) -> None:
        self._subscribers.discard(queue)

    def publish(self, snapshot: MarketDataSnapshot) -> None:
        for queue in tuple(self._subscribers):
            try:
                queue.put_nowait(snapshot)
            except asyncio.QueueFull:
                self._subscribers.discard(queue)
                self.slow_subscribers_removed += 1

    async def stop(self) -> None:
        self._stop.set()
        for queue in tuple(self._subscribers):
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
        self._subscribers.clear()
