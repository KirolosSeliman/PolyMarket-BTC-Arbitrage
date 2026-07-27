"""Crash-aware raw JSONL/Zstandard and Parquet snapshot storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard

from .models import (
    MarketDataEvent,
    PriceLevel,
    MarketDataSnapshot,
    BinanceSnapshot,
    ChainlinkSnapshot,
    OrderBookSnapshot,
    PolymarketSnapshot,
    PolymarketTimeframeSnapshot,
    RollingWindowSnapshot,
    SourceHealthSnapshot,
    EventSource,
    Outcome,
    TakerSide,
    StorageFatalError,
    json_dumps,
    event_from_dict,
)
from polymarket_btc.data_collection.market_discovery import Timeframe


def _utc(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1_000_000_000, UTC)


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    partial.replace(path)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stream_validate_jsonl(path: Path) -> tuple[int, int | None, int | None]:
    count = 0
    first = None
    last = None
    with Path(path).open("rb") as handle:
        for raw_line in handle:
            if not raw_line.endswith(b"\n"):
                raise StorageFatalError(f"JSONL line is not terminated: {path}")
            try:
                value = json.loads(raw_line)
                if not isinstance(value, dict):
                    raise ValueError("event is not an object")
                event = event_from_dict(value)
            except Exception as exc:
                raise StorageFatalError(f"invalid JSONL event: {path}") from exc
            count += 1
            first = event.ingest_sequence if first is None else first
            last = event.ingest_sequence
    return count, first, last


def stream_validate_zstd_jsonl(path: Path) -> tuple[int, int | None, int | None]:
    import io
    count = 0
    first = None
    last = None
    with Path(path).open("rb") as compressed:
        with zstandard.ZstdDecompressor().stream_reader(compressed) as raw_reader:
            reader = io.BufferedReader(raw_reader)
            for raw_line in iter(reader.readline, b""):
                try:
                    value = json.loads(raw_line)
                    if not isinstance(value, dict):
                        raise ValueError("event is not an object")
                    event = event_from_dict(value)
                except Exception as exc:
                    raise StorageFatalError(f"invalid compressed JSONL: {path}") from exc
                count += 1
                first = event.ingest_sequence if first is None else first
                last = event.ingest_sequence
    return count, first, last


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
        compressed_partial = compressed_path.with_suffix(compressed_path.suffix + ".partial")
        compressor = zstandard.ZstdCompressor(level=self.zstd_level)
        with compressed_partial.open("xb") as compressed_handle:
            with compressor.stream_writer(
                compressed_handle,
                closefd=False,
                size=segment.uncompressed_bytes,
            ) as writer:
                with jsonl_path.open("rb") as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        writer.write(chunk)
                writer.flush(zstandard.FLUSH_FRAME)
            compressed_handle.flush()
            os.fsync(compressed_handle.fileno())
        count, first, last = stream_validate_zstd_jsonl(compressed_partial)
        if (count, first, last) != (
            segment.event_count,
            segment.first_sequence,
            segment.last_sequence,
        ):
            raise StorageFatalError("compressed segment metadata mismatch")
        digest = stream_sha256(compressed_partial)
        compressed_partial.replace(compressed_path)
        _fsync_directory(compressed_path.parent)
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
            "uncompressed_bytes": segment.uncompressed_bytes,
            "compressed_bytes": compressed_path.stat().st_size,
            "created_at_utc": _utc(segment.first_received_ns or time.time_ns()).isoformat(),
            "finalized_at_utc": now,
        }
        _atomic_json(manifest_path, manifest)
        _fsync_directory(manifest_path.parent)
        jsonl_path.unlink()
        _fsync_directory(jsonl_path.parent)
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

def _quarantine(path: Path, data_dir: Path, reason: str, exc: BaseException | None = None) -> None:
    quarantine = data_dir / "quarantine"
    quarantine.mkdir(parents=True, exist_ok=True)
    target = quarantine / f"{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}-{uuid.uuid4().hex}-{path.name}"
    try:
        path.replace(target)
        _fsync_directory(quarantine)
        _atomic_json(
            target.with_name(target.name + ".reason.json"),
            {
                "original_path": str(path),
                "reason": reason,
                "exception_type": None if exc is None else type(exc).__name__,
                "detected_at_utc": datetime.now(UTC).isoformat(),
            },
        )
    except OSError as move_error:
        raise StorageFatalError(f"cannot quarantine {path}: {move_error}") from move_error


def _raw_event_metadata(path: Path, *, compressed: bool) -> tuple[int, int | None, int | None, int | None, int | None]:
    count = first = last = first_received = last_received = None
    import io

    def consume(lines: object) -> None:
        nonlocal count, first, last, first_received, last_received
        for raw_line in lines:  # type: ignore[union-attr]
            if not raw_line.endswith(b"\n"):
                raise StorageFatalError(f"JSONL line is not terminated: {path}")
            value = json.loads(raw_line)
            if not isinstance(value, dict):
                raise StorageFatalError(f"event is not an object: {path}")
            event = event_from_dict(value)
            count = 1 if count is None else count + 1
            first = event.ingest_sequence if first is None else first
            last = event.ingest_sequence
            first_received = event.received_wall_timestamp_ns if first_received is None else first_received
            last_received = event.received_wall_timestamp_ns

    if compressed:
        with path.open("rb") as handle:
            with zstandard.ZstdDecompressor().stream_reader(handle) as raw_reader:
                consume(io.BufferedReader(raw_reader))
    else:
        with path.open("rb") as handle:
            consume(handle)
    return int(count or 0), first, last, first_received, last_received


def _raw_manifest(data_path: Path) -> Path:
    compressed = data_path.name.endswith(".jsonl.zst")
    count, first, last, first_received, last_received = _raw_event_metadata(
        data_path, compressed=compressed
    )
    if not count:
        raise StorageFatalError(f"raw segment is empty: {data_path}")
    manifest_path = data_path.with_name(data_path.name.removesuffix(".jsonl.zst") + ".manifest.json")
    if manifest_path.exists():
        return manifest_path
    manifest = {
        "schema_version": 1,
        "relative_path": data_path.name,
        "sha256": stream_sha256(data_path),
        "event_count": count,
        "first_ingest_sequence": first,
        "last_ingest_sequence": last,
        "first_received_timestamp_ns": first_received,
        "last_received_timestamp_ns": last_received,
        "uncompressed_bytes": None if compressed else data_path.stat().st_size,
        "compressed_bytes": data_path.stat().st_size,
        "created_at_utc": _utc(first_received or time.time_ns()).isoformat(),
        "finalized_at_utc": datetime.now(UTC).isoformat(),
    }
    _atomic_json(manifest_path, manifest)
    _fsync_directory(manifest_path.parent)
    return manifest_path


def _validate_parquet(path: Path) -> tuple[int, int, int]:
    table = pq.read_table(path)
    required = set(PARQUET_SCHEMA.names)
    if not required.issubset(table.schema.names) or table.num_rows <= 0:
        raise StorageFatalError(f"invalid Parquet schema or empty file: {path}")
    sequences = table.column("snapshot_sequence").to_pylist()
    timestamps = table.column("snapshot_timestamp_ns").to_pylist()
    return table.num_rows, int(sequences[0]), int(timestamps[0])


def _parquet_manifest(data_path: Path) -> Path:
    row_count, first_sequence, first_timestamp = _validate_parquet(data_path)
    table = pq.read_table(data_path, columns=["snapshot_sequence", "snapshot_timestamp_ns"])
    sequences = table.column("snapshot_sequence").to_pylist()
    timestamps = table.column("snapshot_timestamp_ns").to_pylist()
    manifest_path = data_path.with_suffix(".manifest.json")
    if manifest_path.exists():
        return manifest_path
    manifest = {
        "schema_version": 2,
        "row_count": row_count,
        "first_snapshot_sequence": first_sequence,
        "last_snapshot_sequence": int(sequences[-1]),
        "first_snapshot_timestamp_ns": first_timestamp,
        "last_snapshot_timestamp_ns": int(timestamps[-1]),
        "sha256": stream_sha256(data_path),
        "compressed_bytes": data_path.stat().st_size,
        "finalized_at_utc": datetime.now(UTC).isoformat(),
    }
    _atomic_json(manifest_path, manifest)
    _fsync_directory(manifest_path.parent)
    return manifest_path


def _recover_jsonl_partial(path: Path, data_dir: Path, zstd_level: int) -> Path | None:
    if path.stat().st_size == 0:
        path.unlink()
        return None
    complete_bytes = 0
    try:
        with path.open("rb+") as handle:
            for raw_line in handle:
                if not raw_line.endswith(b"\n"):
                    break
                value = json.loads(raw_line)
                if not isinstance(value, dict):
                    raise ValueError("event is not an object")
                event_from_dict(value)
                complete_bytes += len(raw_line)
            handle.truncate(complete_bytes)
        if complete_bytes == 0:
            path.unlink()
            return None
        count, first, last, first_received, last_received = _raw_event_metadata(path, compressed=False)
        segment = _RawSegment(
            path, path.open("ab"), time.monotonic(), count, first, last,
            first_received, last_received, complete_bytes,
        )
        return RawEventStorage(data_dir, zstd_level=zstd_level)._finalize(segment)
    except Exception as exc:
        _quarantine(path, data_dir, "invalid JSONL partial segment", exc)
        return None


def recover_partial_files(data_dir: Path, *, zstd_level: int) -> list[Path]:
    """Recover every known raw/Parquet crash state without overwriting data."""
    root = Path(data_dir)
    manifests: list[Path] = []
    raw_root = root / "raw"
    snapshot_root = root / "snapshots"
    raw_root.mkdir(parents=True, exist_ok=True)
    snapshot_root.mkdir(parents=True, exist_ok=True)

    for path in sorted(raw_root.rglob("*.jsonl.partial")):
        recovered = _recover_jsonl_partial(path, root, zstd_level)
        if recovered is not None:
            manifests.append(recovered)

    for path in sorted(raw_root.rglob("*.jsonl")):
        manifest = path.with_name(path.name.removesuffix(".jsonl") + ".manifest.json")
        if manifest.exists():
            continue
        try:
            partial = path.with_name(path.name + ".partial")
            path.replace(partial)
            recovered = _recover_jsonl_partial(partial, root, zstd_level)
            if recovered is not None:
                manifests.append(recovered)
        except Exception as exc:
            if path.exists():
                _quarantine(path, root, "orphan JSONL segment", exc)

    for path in sorted(raw_root.rglob("*.jsonl.zst.partial")):
        try:
            _raw_event_metadata(path, compressed=True)
            final = path.with_suffix("")
            path.replace(final)
            manifests.append(_raw_manifest(final))
        except Exception as exc:
            if path.exists():
                _quarantine(path, root, "invalid compressed partial segment", exc)

    for path in sorted(raw_root.rglob("*.jsonl.zst")):
        manifest = path.with_name(path.name.removesuffix(".jsonl.zst") + ".manifest.json")
        if manifest.exists():
            continue
        try:
            manifests.append(_raw_manifest(path))
        except Exception as exc:
            _quarantine(path, root, "invalid compressed segment without manifest", exc)

    for manifest in sorted(raw_root.rglob("*.manifest.json.partial")):
        try:
            value = json.loads(manifest.read_text(encoding="utf-8"))
            data_path = manifest.parent / str(value["relative_path"])
            expected = str(value["sha256"])
            if not data_path.is_file() or stream_sha256(data_path) != expected:
                raise StorageFatalError("manifest does not match data file")
            final = manifest.with_suffix("")
            manifest.replace(final)
            manifests.append(final)
        except Exception as exc:
            if manifest.exists():
                _quarantine(manifest, root, "invalid partial manifest", exc)

    for manifest in sorted(raw_root.rglob("*.manifest.json")):
        try:
            value = json.loads(manifest.read_text(encoding="utf-8"))
            data_path = manifest.parent / str(value["relative_path"])
            if not data_path.is_file():
                raise StorageFatalError("manifest data file is absent")
            if stream_sha256(data_path) != str(value["sha256"]):
                raise StorageFatalError("manifest hash mismatch")
        except Exception as exc:
            _quarantine(manifest, root, "manifest data is missing or corrupt", exc)

    for path in sorted(snapshot_root.rglob("*.parquet.partial")):
        try:
            _validate_parquet(path)
            final = path.with_suffix("")
            path.replace(final)
            manifests.append(_parquet_manifest(final))
        except Exception as exc:
            if path.exists():
                _quarantine(path, root, "invalid Parquet partial", exc)

    for path in sorted(snapshot_root.rglob("*.parquet")):
        manifest = path.with_suffix(".manifest.json")
        if manifest.exists():
            continue
        try:
            manifests.append(_parquet_manifest(path))
        except Exception as exc:
            _quarantine(path, root, "invalid Parquet without manifest", exc)

    for manifest in sorted(snapshot_root.rglob("*.manifest.json.partial")):
        try:
            value = json.loads(manifest.read_text(encoding="utf-8"))
            relative_path = value.get(
                "relative_path", manifest.name.replace(".manifest.json.partial", ".parquet")
            )
            data_path = manifest.parent / str(relative_path)
            if not data_path.is_file() or stream_sha256(data_path) != str(value["sha256"]):
                raise StorageFatalError("Parquet manifest does not match data file")
            final = manifest.with_suffix("")
            manifest.replace(final)
            manifests.append(final)
        except Exception as exc:
            if manifest.exists():
                _quarantine(manifest, root, "invalid Parquet partial manifest", exc)

    for manifest in sorted(snapshot_root.rglob("*.manifest.json")):
        try:
            value = json.loads(manifest.read_text(encoding="utf-8"))
            data_path = manifest.parent / str(value.get("relative_path", manifest.name.replace(".manifest.json", ".parquet")))
            if not data_path.is_file():
                # Snapshot manifests created by ParquetSnapshotWriter use a same-stem parquet file.
                data_path = manifest.with_name(manifest.name.replace(".manifest.json", ".parquet"))
            if not data_path.is_file() or stream_sha256(data_path) != str(value["sha256"]):
                raise StorageFatalError("Parquet manifest data is absent or corrupt")
        except Exception as exc:
            _quarantine(manifest, root, "Parquet manifest data is missing or corrupt", exc)
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
    pa.field("snapshot_json", pa.string(), nullable=False),
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


def snapshot_to_parquet_row(snapshot: MarketDataSnapshot) -> dict[str, object]:
    return {
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
        "snapshot_json": json_dumps(snapshot),
    }


def _decimal(value: object) -> object:
    return None if value is None else Decimal(str(value))


def _levels_from_row(value: object) -> tuple[PriceLevel, ...]:
    return tuple(PriceLevel(Decimal(str(row["price"])), Decimal(str(row["quantity"]))) for row in value)  # type: ignore[index]


def snapshot_from_parquet_row(row: dict[str, object]) -> MarketDataSnapshot:
    value = json.loads(str(row["snapshot_json"]))
    def book(raw: object) -> OrderBookSnapshot | None:
        if raw is None:
            return None
        return OrderBookSnapshot(
            str(raw["asset_id"]), str(raw["market_id"]), str(raw["condition_id"]),
            Outcome(str(raw["outcome"])), raw.get("source_session_id"),
            _levels_from_row(raw["bids"]), _levels_from_row(raw["asks"]),
            _decimal(raw["best_bid"]), _decimal(raw["best_ask"]), _decimal(raw["last_trade_price"]),
            _decimal(raw["tick_size"]), bool(raw["initialized"]), bool(raw["coherent"]),
            bool(raw["resolved"]), raw.get("last_event_timestamp_ns"),
        )
    def market(raw: object) -> PolymarketTimeframeSnapshot | None:
        if raw is None:
            return None
        return PolymarketTimeframeSnapshot(
            Timeframe(str(raw["timeframe"])), raw.get("market_id"), raw.get("condition_id"),
            raw.get("start_timestamp_ns"), raw.get("end_timestamp_ns"), raw.get("remaining_ms"),
            book(raw.get("up")), book(raw.get("down")), bool(raw["resolved"]),
            bool(raw["ready"]), tuple(raw["not_ready_reasons"]),
        )
    chain = value["chainlink"]
    binance = value["binance"]
    return MarketDataSnapshot(
        int(value["schema_version"]), int(value["snapshot_sequence"]), int(value["snapshot_timestamp_ns"]),
        market(value.get("market_5m")), market(value.get("market_15m")),
        ChainlinkSnapshot(_decimal(chain["price"]), chain.get("source_timestamp_ns"), chain.get("received_timestamp_ns"), chain.get("age_ms")),
        BinanceSnapshot(_decimal(binance["last_price"]), None if binance["taker_side"] is None else TakerSide(str(binance["taker_side"])), _decimal(binance["best_bid"]), _decimal(binance["best_ask"]), _decimal(binance["best_bid_quantity"]), _decimal(binance["best_ask_quantity"]), _levels_from_row(binance["depth_bids"]), _levels_from_row(binance["depth_asks"]), _decimal(binance["mid_price"]), _decimal(binance["spread"]), _decimal(binance["spread_bps"]), _decimal(binance["microprice"]), _decimal(binance["top1_imbalance"]), _decimal(binance["top20_bid_notional"]), _decimal(binance["top20_ask_notional"]), _decimal(binance["top20_depth_imbalance"]), tuple(RollingWindowSnapshot(int(w["window_seconds"]), Decimal(str(w["buy_volume"])), Decimal(str(w["sell_volume"])), _decimal(w["vwap"])) for w in binance["rolling_windows"])),
        PolymarketSnapshot(market(value["polymarket"]["five_minutes"]), market(value["polymarket"]["fifteen_minutes"])),
        tuple((EventSource(str(source)), SourceHealthSnapshot(**health)) for source, health in value["health"]),
        bool(value["ready_for_strategy"]), tuple(value["not_ready_reasons"]),
    )


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
        self._rows.append(snapshot_to_parquet_row(snapshot))
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
        digest = stream_sha256(final)
        manifest_path = directory / f"{stem}.manifest.json"
        manifest = {
            "schema_version": 2,
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
