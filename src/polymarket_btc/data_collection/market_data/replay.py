"""Integrity-checked replay of raw event segments."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import zstandard

from .models import MarketDataEvent, ReplayIntegrityError, event_from_dict


def read_raw_events(raw_dir: Path):
    events: list[MarketDataEvent] = []
    for manifest_path in Path(raw_dir).rglob("*.manifest.json"):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if "event_count" not in manifest:
            continue
        compressed_path = manifest_path.with_name(str(manifest["relative_path"]))
        compressed = compressed_path.read_bytes()
        if hashlib.sha256(compressed).hexdigest() != manifest["sha256"]:
            raise ReplayIntegrityError(f"hash mismatch: {compressed_path}")
        try:
            raw = zstandard.ZstdDecompressor().decompress(compressed)
        except zstandard.ZstdError as exc:
            raise ReplayIntegrityError(f"cannot decompress: {compressed_path}") from exc
        for line in raw.splitlines():
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ReplayIntegrityError("raw event is not an object")
            events.append(event_from_dict(value))
    events.sort(key=lambda event: event.ingest_sequence)
    for previous, current in zip(events, events[1:]):
        if current.ingest_sequence != previous.ingest_sequence + 1:
            raise ReplayIntegrityError(
                f"ingest sequence gap: {previous.ingest_sequence} -> {current.ingest_sequence}"
            )
    yield from events

