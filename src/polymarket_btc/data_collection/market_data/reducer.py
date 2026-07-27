"""Deterministic reducer shared by live ingestion and replay."""

from __future__ import annotations

from dataclasses import replace

from .models import (
    EventStream,
    MarketDataEvent,
    MarketDataSnapshot,
    SnapshotTickPayload,
)
from .state import StateStore


class MarketDataReducer:
    def __init__(self, state: StateStore) -> None:
        self.state = state

    def apply(self, event: MarketDataEvent) -> MarketDataSnapshot | None:
        if event.stream is not EventStream.SNAPSHOT_TICK:
            self.state.apply(event)
            return None
        payload = event.payload
        if not isinstance(payload, SnapshotTickPayload):
            raise ValueError("SNAPSHOT_TICK payload is invalid")
        snapshot = self.state.snapshot(
            payload.scheduled_timestamp_ns,
            payload.snapshot_sequence,
        )
        return replace(snapshot, health=payload.health)
