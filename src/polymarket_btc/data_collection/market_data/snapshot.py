"""Monotonic 250 ms snapshot publisher with bounded subscribers."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from .models import MarketDataSnapshot
from .state import StateStore


class SnapshotPublisher:
    def __init__(
        self,
        state: StateStore,
        *,
        interval_ms: int,
        subscriber_capacity: int,
        on_snapshot: Callable[[MarketDataSnapshot], None] | None = None,
    ) -> None:
        self.state = state
        self.interval_seconds = interval_ms / 1_000
        self.subscriber_capacity = subscriber_capacity
        self.on_snapshot = on_snapshot
        self._subscribers: set[asyncio.Queue[MarketDataSnapshot | None]] = set()
        self._stop = asyncio.Event()
        self._sequence = 0
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

    def publish_once(self, now_ns: int | None = None) -> MarketDataSnapshot:
        self._sequence += 1
        snapshot = self.state.snapshot(time.time_ns() if now_ns is None else now_ns, self._sequence)
        if self.on_snapshot is not None:
            self.on_snapshot(snapshot)
        for queue in tuple(self._subscribers):
            try:
                queue.put_nowait(snapshot)
            except asyncio.QueueFull:
                self._subscribers.discard(queue)
                self.slow_subscribers_removed += 1
        return snapshot

    async def run(self) -> None:
        next_slot = time.monotonic()
        while not self._stop.is_set():
            next_slot += self.interval_seconds
            delay = next_slot - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    break
                except TimeoutError:
                    pass
            else:
                skipped = int((-delay) // self.interval_seconds) + 1
                next_slot += skipped * self.interval_seconds
            self.publish_once()

    async def stop(self) -> None:
        self._stop.set()
        for queue in tuple(self._subscribers):
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
        self._subscribers.clear()

