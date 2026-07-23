"""Crash-aware raw JSONL/Zstandard and Parquet snapshot storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard

from .models import (
    MarketDataEvent,
    MarketDataSnapshot,
    StorageFatalError,
    json_dumps,
)


def _utc(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1_000_000_000, UTC)


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    partial.replace(path)


@dataclass(slots=True)
class _RawSegment:
    partial_path: Path
    handle: object
    opened_monotonic: float
    event_count: int = 0
    first_sequence: int | None = None
    last_sequence: int | None = None
    first_received_ns: int | None = None
    last_received_ns: int | None = None
    uncompressed_bytes: int = 0


class RawEventStorage:
    def __init__(
        self,
        data_dir: Path,
        *,
        zstd_level: int,
        rotate_seconds: int = 300,
        rotate_bytes: int = 134_217_728,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.zstd_level = zstd_level
        self.rotate_seconds = rotate_seconds
        self.rotate_bytes = rotate_bytes
        self._segments: dict[tuple[str, str], _RawSegment] = {}
        self._manifests: list[Path] = []

    def _open(self, event: MarketDataEvent) -> _RawSegment:
        instant = _utc(event.received_wall_timestamp_ns)
        directory = (
            self.data_dir / "raw" / f"date={instant:%Y-%m-%d}" / f"hour={instant:%H}"
            / f"source={event.source.value}" / f"stream={event.stream.value}"
        )
        directory.mkdir(parents=True, exist_ok=True)
        stem = f"part-{event.received_wall_timestamp_ns}-{uuid.uuid4().hex}"
        partial = directory / f"{stem}.jsonl.partial"
        try:
            handle = partial.open("xb")
        except OSError as exc:
            raise StorageFatalError(f"cannot open raw segment: {exc}") from exc
        return _RawSegment(partial, handle, time.monotonic())

    def write(self, event: MarketDataEvent) -> None:
        key = (event.source.value, event.stream.value)
        segment = self._segments.get(key)
        if segment is None:
            segment = self._segments[key] = self._open(event)
        encoded = (json_dumps(event) + "\n").encode("utf-8")
        if (
            segment.event_count
            and (
                segment.uncompressed_bytes + len(encoded) > self.rotate_bytes
                or time.monotonic() - segment.opened_monotonic >= self.rotate_seconds
            )
        ):
            self._manifests.append(self._finalize(segment))
            segment = self._segments[key] = self._open(event)
        try:
            segment.handle.write(encoded)  # type: ignore[attr-defined]
        except OSError as exc:
            raise StorageFatalError(f"cannot write raw event: {exc}") from exc
        segment.event_count += 1
        segment.first_sequence = segment.first_sequence or event.ingest_sequence
        segment.last_sequence = event.ingest_sequence
        segment.first_received_ns = segment.first_received_ns or event.received_wall_timestamp_ns
        segment.last_received_ns = event.received_wall_timestamp_ns
        segment.uncompressed_bytes += len(encoded)

    def flush(self, *, fsync: bool = False) -> None:
        for segment in self._segments.values():
            segment.handle.flush()  # type: ignore[attr-defined]
            if fsync:
                os.fsync(segment.handle.fileno())  # type: ignore[attr-defined]

    def _finalize(self, segment: _RawSegment) -> Path:
        handle = segment.handle
        handle.flush()  # type: ignore[attr-defined]
        os.fsync(handle.fileno())  # type: ignore[attr-defined]
        handle.close()  # type: ignore[attr-defined]
        jsonl_path = segment.partial_path.with_suffix("")
        segment.partial_path.replace(jsonl_path)
        compressed_path = jsonl_path.with_suffix(".jsonl.zst")
        compressor = zstandard.ZstdCompressor(level=self.zstd_level)
        raw = jsonl_path.read_bytes()
        compressed = compressor.compress(raw)
        compressed_path.write_bytes(compressed)
        try:
            if zstandard.ZstdDecompressor().decompress(compressed) != raw:
                raise StorageFatalError("compressed segment verification failed")
        except zstandard.ZstdError as exc:
            raise StorageFatalError("compressed segment is unreadable") from exc
        digest = hashlib.sha256(compressed).hexdigest()
        now = datetime.now(UTC).isoformat()
        manifest_path = jsonl_path.with_suffix(".manifest.json")
        manifest = {
            "schema_version": 1,
            "relative_path": compressed_path.name,
            "sha256": digest,
            "event_count": segment.event_count,
            "first_ingest_sequence": segment.first_sequence,
            "last_ingest_sequence": segment.last_sequence,
            "first_received_timestamp_ns": segment.first_received_ns,
            "last_received_timestamp_ns": segment.last_received_ns,
            "uncompressed_bytes": len(raw),
            "compressed_bytes": len(compressed),
            "created_at_utc": _utc(segment.first_received_ns or time.time_ns()).isoformat(),
            "finalized_at_utc": now,
        }
        _atomic_json(manifest_path, manifest)
        jsonl_path.unlink()
        return manifest_path

    def close(self) -> list[Path]:
        for key, segment in list(self._segments.items()):
            if segment.event_count:
                self._manifests.append(self._finalize(segment))
            else:
                segment.handle.close()  # type: ignore[attr-defined]
                segment.partial_path.unlink(missing_ok=True)
            del self._segments[key]
        return list(self._manifests)


def recover_partial_files(data_dir: Path, *, zstd_level: int) -> list[Path]:
    manifests: list[Path] = []
    quarantine = Path(data_dir) / "quarantine"
    for partial in (Path(data_dir) / "raw").rglob("*.jsonl.partial"):
        raw = partial.read_bytes()
        last_newline = raw.rfind(b"\n")
        complete = raw[: last_newline + 1] if last_newline >= 0 else b""
        rows: list[dict[str, object]] = []
        invalid = False
        for line in complete.splitlines():
            try:
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError
                rows.append(value)
            except (json.JSONDecodeError, ValueError):
                invalid = True
                break
        if invalid:
            quarantine.mkdir(parents=True, exist_ok=True)
            target = quarantine / partial.name
            if target.exists():
                target = quarantine / f"{uuid.uuid4().hex}-{partial.name}"
            shutil.move(str(partial), target)
            continue
        if not rows:
            partial.unlink()
            continue
        partial.write_bytes(complete)
        first = rows[0]
        last = rows[-1]
        handle = partial.open("ab")
        segment = _RawSegment(
            partial,
            handle,
            time.monotonic(),
            len(rows),
            int(first["ingest_sequence"]),
            int(last["ingest_sequence"]),
            int(first["received_wall_timestamp_ns"]),
            int(last["received_wall_timestamp_ns"]),
            len(complete),
        )
        storage = RawEventStorage(Path(data_dir), zstd_level=zstd_level)
        manifests.append(storage._finalize(segment))
    return manifests


_LEVEL_TYPE = pa.list_(pa.struct([
    pa.field("price", pa.string(), nullable=False),
    pa.field("quantity", pa.string(), nullable=False),
]))
PARQUET_SCHEMA = pa.schema([
    pa.field("schema_version", pa.int32(), nullable=False),
    pa.field("snapshot_sequence", pa.int64(), nullable=False),
    pa.field("snapshot_timestamp_ns", pa.int64(), nullable=False),
    pa.field("ready_for_strategy", pa.bool_(), nullable=False),
    pa.field("not_ready_reasons", pa.list_(pa.string()), nullable=False),
    pa.field("chainlink_price", pa.string()),
    pa.field("binance_last_price", pa.string()),
    pa.field("binance_best_bid", pa.string()),
    pa.field("binance_best_ask", pa.string()),
    pa.field("binance_depth_bids", _LEVEL_TYPE, nullable=False),
    pa.field("binance_depth_asks", _LEVEL_TYPE, nullable=False),
    pa.field("market_5m_up_bids", _LEVEL_TYPE, nullable=False),
    pa.field("market_5m_up_asks", _LEVEL_TYPE, nullable=False),
    pa.field("market_5m_down_bids", _LEVEL_TYPE, nullable=False),
    pa.field("market_5m_down_asks", _LEVEL_TYPE, nullable=False),
    pa.field("market_15m_up_bids", _LEVEL_TYPE, nullable=False),
    pa.field("market_15m_up_asks", _LEVEL_TYPE, nullable=False),
    pa.field("market_15m_down_bids", _LEVEL_TYPE, nullable=False),
    pa.field("market_15m_down_asks", _LEVEL_TYPE, nullable=False),
])


def _levels(levels: object) -> list[dict[str, str]]:
    return [{"price": str(level.price), "quantity": str(level.quantity)} for level in levels]  # type: ignore[union-attr]


def _market_levels(snapshot: object, outcome: str, side: str) -> list[dict[str, str]]:
    if snapshot is None:
        return []
    book = getattr(snapshot, outcome)
    if book is None:
        return []
    return _levels(getattr(book, side))


class ParquetSnapshotWriter:
    def __init__(
        self,
        data_dir: Path,
        *,
        zstd_level: int,
        rotate_seconds: int = 60,
        rotate_rows: int = 10_000,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.zstd_level = zstd_level
        self.rotate_seconds = rotate_seconds
        self.rotate_rows = rotate_rows
        self._rows: list[dict[str, object]] = []
        self._opened = time.monotonic()
        self._manifests: list[Path] = []

    def write(self, snapshot: MarketDataSnapshot) -> None:
        self._rows.append({
            "schema_version": snapshot.schema_version,
            "snapshot_sequence": snapshot.snapshot_sequence,
            "snapshot_timestamp_ns": snapshot.snapshot_timestamp_ns,
            "ready_for_strategy": snapshot.ready_for_strategy,
            "not_ready_reasons": list(snapshot.not_ready_reasons),
            "chainlink_price": None if snapshot.chainlink.price is None else str(snapshot.chainlink.price),
            "binance_last_price": None if snapshot.binance.last_price is None else str(snapshot.binance.last_price),
            "binance_best_bid": None if snapshot.binance.best_bid is None else str(snapshot.binance.best_bid),
            "binance_best_ask": None if snapshot.binance.best_ask is None else str(snapshot.binance.best_ask),
            "binance_depth_bids": _levels(snapshot.binance.depth_bids),
            "binance_depth_asks": _levels(snapshot.binance.depth_asks),
            "market_5m_up_bids": _market_levels(snapshot.market_5m, "up", "bids"),
            "market_5m_up_asks": _market_levels(snapshot.market_5m, "up", "asks"),
            "market_5m_down_bids": _market_levels(snapshot.market_5m, "down", "bids"),
            "market_5m_down_asks": _market_levels(snapshot.market_5m, "down", "asks"),
            "market_15m_up_bids": _market_levels(snapshot.market_15m, "up", "bids"),
            "market_15m_up_asks": _market_levels(snapshot.market_15m, "up", "asks"),
            "market_15m_down_bids": _market_levels(snapshot.market_15m, "down", "bids"),
            "market_15m_down_asks": _market_levels(snapshot.market_15m, "down", "asks"),
        })
        if len(self._rows) >= self.rotate_rows or time.monotonic() - self._opened >= self.rotate_seconds:
            self._manifests.append(self._finalize())

    def _finalize(self) -> Path:
        first_ns = int(self._rows[0]["snapshot_timestamp_ns"])
        instant = _utc(first_ns)
        directory = self.data_dir / "snapshots" / f"date={instant:%Y-%m-%d}" / f"hour={instant:%H}"
        directory.mkdir(parents=True, exist_ok=True)
        stem = f"part-{first_ns}-{uuid.uuid4().hex}"
        partial = directory / f"{stem}.parquet.partial"
        final = directory / f"{stem}.parquet"
        table = pa.Table.from_pylist(self._rows, schema=PARQUET_SCHEMA)
        pq.write_table(table, partial, compression="zstd", compression_level=self.zstd_level)
        partial.replace(final)
        digest = hashlib.sha256(final.read_bytes()).hexdigest()
        manifest_path = directory / f"{stem}.manifest.json"
        manifest = {
            "schema_version": 1,
            "row_count": len(self._rows),
            "first_snapshot_sequence": self._rows[0]["snapshot_sequence"],
            "last_snapshot_sequence": self._rows[-1]["snapshot_sequence"],
            "first_snapshot_timestamp_ns": first_ns,
            "last_snapshot_timestamp_ns": self._rows[-1]["snapshot_timestamp_ns"],
            "sha256": digest,
            "compressed_bytes": final.stat().st_size,
            "finalized_at_utc": datetime.now(UTC).isoformat(),
        }
        _atomic_json(manifest_path, manifest)
        self._rows = []
        self._opened = time.monotonic()
        return manifest_path

    def close(self) -> list[Path]:
        if self._rows:
            self._manifests.append(self._finalize())
        return list(self._manifests)

