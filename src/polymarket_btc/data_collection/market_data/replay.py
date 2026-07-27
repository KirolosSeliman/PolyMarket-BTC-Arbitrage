"""Streaming, integrity-checked replay of raw event segments."""

from __future__ import annotations

import hashlib
import io
import heapq
import json
from pathlib import Path
from collections.abc import Iterable, Iterator

import zstandard

from .models import MarketDataEvent, ReplayIntegrityError, event_from_dict


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class RawSegmentIterator:
    def __init__(self, manifest_path: Path) -> None:
        self.manifest_path = Path(manifest_path)
        try:
            self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ReplayIntegrityError(f"invalid manifest: {manifest_path}") from exc
        if not isinstance(self.manifest, dict):
            raise ReplayIntegrityError(f"manifest is not an object: {manifest_path}")
        self.data_path = self.manifest_path.with_name(str(self.manifest["relative_path"]))

    def __iter__(self) -> Iterator[MarketDataEvent]:
        if not self.data_path.is_file():
            raise ReplayIntegrityError(f"missing raw segment: {self.data_path}")
        if _sha256(self.data_path) != self.manifest.get("sha256"):
            raise ReplayIntegrityError(f"hash mismatch: {self.data_path}")
        count = 0
        first: int | None = None
        last: int | None = None
        try:
            with self.data_path.open("rb") as compressed:
                with zstandard.ZstdDecompressor().stream_reader(compressed) as raw_reader:
                    reader = io.BufferedReader(raw_reader)
                    while line := reader.readline():
                        try:
                            value = json.loads(line)
                            if not isinstance(value, dict):
                                raise ValueError("event is not an object")
                            event = event_from_dict(value)
                        except (ValueError, json.JSONDecodeError) as exc:
                            raise ReplayIntegrityError(
                                f"invalid event in {self.data_path}"
                            ) from exc
                        count += 1
                        first = event.ingest_sequence if first is None else first
                        last = event.ingest_sequence
                        yield event
        except zstandard.ZstdError as exc:
            raise ReplayIntegrityError(f"cannot decompress: {self.data_path}") from exc
        if count != int(self.manifest.get("event_count", -1)):
            raise ReplayIntegrityError(f"event count mismatch: {self.data_path}")
        if first != self.manifest.get("first_ingest_sequence") or last != self.manifest.get("last_ingest_sequence"):
            raise ReplayIntegrityError(f"sequence bounds mismatch: {self.data_path}")


class RawStreamChain:
    def __init__(self, manifests: Iterable[Path]) -> None:
        self.manifests = sorted(
            (Path(path) for path in manifests),
            key=lambda path: int(json.loads(path.read_text(encoding="utf-8"))["first_ingest_sequence"]),
        )

    def __iter__(self) -> Iterator[MarketDataEvent]:
        previous_last: int | None = None
        for manifest in self.manifests:
            current = RawSegmentIterator(manifest)
            first: int | None = None
            last: int | None = None
            for event in current:
                first = event.ingest_sequence if first is None else first
                last = event.ingest_sequence
                if previous_last is not None and event.ingest_sequence <= previous_last:
                    raise ReplayIntegrityError("overlapping raw sequence ranges")
                yield event
            if first is not None:
                previous_last = last


def merge_raw_streams(streams: Iterable[Iterable[MarketDataEvent]]) -> Iterator[MarketDataEvent]:
    heap: list[tuple[int, int, MarketDataEvent, Iterator[MarketDataEvent]]] = []
    for index, stream in enumerate(streams):
        iterator = iter(stream)
        try:
            event = next(iterator)
        except StopIteration:
            continue
        heapq.heappush(heap, (event.ingest_sequence, index, event, iterator))
    expected: int | None = None
    while heap:
        sequence, index, event, iterator = heapq.heappop(heap)
        if expected is None:
            expected = sequence
        if sequence != expected:
            raise ReplayIntegrityError(
                f"ingest sequence gap or duplicate: expected {expected}, got {sequence}"
            )
        yield event
        expected += 1
        try:
            next_event = next(iterator)
        except StopIteration:
            continue
        heapq.heappush(heap, (next_event.ingest_sequence, index, next_event, iterator))


def read_raw_events(raw_dir: Path) -> Iterator[MarketDataEvent]:
    grouped: dict[Path, list[Path]] = {}
    for manifest in Path(raw_dir).rglob("*.manifest.json"):
        try:
            value = json.loads(manifest.read_text(encoding="utf-8"))
            if "event_count" not in value:
                continue
        except (OSError, json.JSONDecodeError) as exc:
            raise ReplayIntegrityError(f"invalid manifest: {manifest}") from exc
        grouped.setdefault(manifest.parent, []).append(manifest)
    return merge_raw_streams(RawStreamChain(paths) for paths in grouped.values())
