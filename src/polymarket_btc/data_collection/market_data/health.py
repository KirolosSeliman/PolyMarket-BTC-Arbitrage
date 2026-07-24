"""Atomic health-file publication and status inspection."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import time
from dataclasses import asdict, dataclass

from .models import EventSource, SourceHealthSnapshot


@dataclass(slots=True)
class _SourceHealth:
    connected: bool = False
    current_session_id: str | None = None
    last_valid_message_ns: int | None = None
    reconnect_count: int = 0
    invalid_count: int = 0
    duplicate_count: int = 0
    stale_session_count: int = 0
    divergence_count: int = 0
    protocol_error_count: int = 0
    last_disconnect_reason: str | None = None


class HealthRegistry:
    def __init__(self) -> None:
        self._sources = {source: _SourceHealth() for source in EventSource}
        self._runtime: dict[str, object] = {
            "schema_version": 2,
            "process_started_at": datetime.now(UTC).isoformat(),
            "gateway_state": "starting",
            "ready": False,
            "not_ready_reasons": (),
            "last_fatal_error": None,
        }

    def record_connection(self, source: EventSource, session_id: str | None, now_ns: int) -> None:
        row = self._sources[source]
        row.connected = True
        row.current_session_id = session_id
        row.last_valid_message_ns = now_ns
        row.last_disconnect_reason = None

    def record_disconnection(self, source: EventSource, session_id: str | None, reason: str | None) -> None:
        row = self._sources[source]
        row.connected = False
        row.current_session_id = session_id
        row.last_disconnect_reason = reason
        self._runtime["ready"] = False

    def record_message(self, source: EventSource, now_ns: int) -> None:
        self._sources[source].last_valid_message_ns = now_ns

    def record_reconnect(self, source: EventSource) -> None:
        self._sources[source].reconnect_count += 1

    def record_invalid(self, source: EventSource) -> None:
        self._sources[source].invalid_count += 1

    def record_duplicate(self, source: EventSource) -> None:
        self._sources[source].duplicate_count += 1

    def record_stale_session(self, source: EventSource) -> None:
        self._sources[source].stale_session_count += 1

    def record_divergence(self, source: EventSource) -> None:
        self._sources[source].divergence_count += 1

    def record_protocol_error(self, source: EventSource) -> None:
        self._sources[source].protocol_error_count += 1

    def record_fatal(self, exception: BaseException, now_ns: int) -> None:
        self._runtime.update({
            "gateway_state": "failed",
            "ready": False,
            "last_fatal_error": {
                "message": str(exception),
                "exception_type": type(exception).__name__,
                "timestamp_ns": now_ns,
            },
        })

    def update_runtime(self, **values: object) -> None:
        self._runtime.update(values)

    def source_snapshot(self, source: EventSource, now_ns: int) -> SourceHealthSnapshot:
        row = self._sources[source]
        age = None if row.last_valid_message_ns is None else max(
            0, (now_ns - row.last_valid_message_ns) // 1_000_000
        )
        return SourceHealthSnapshot(
            row.connected, not row.connected, row.current_session_id,
            row.last_valid_message_ns, age, row.reconnect_count,
            row.invalid_count, row.duplicate_count, row.stale_session_count,
            row.divergence_count, row.protocol_error_count,
        )

    def all_source_snapshots(self, now_ns: int) -> tuple[tuple[EventSource, SourceHealthSnapshot], ...]:
        return tuple((source, self.source_snapshot(source, now_ns)) for source in EventSource)

    def to_health_file_payload(self, now_ns: int) -> dict[str, object]:
        payload = dict(self._runtime)
        payload["not_ready_reasons"] = list(payload.get("not_ready_reasons", ()))
        payload["sources"] = {
            source.value: asdict(row)
            for source, row in self._sources.items()
        }
        payload["updated_at_ns"] = now_ns
        return payload


def write_health_file(path: Path, payload: dict[str, object]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    value = dict(payload)
    value["updated_at_ns"] = time.time_ns()
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, separators=(",", ":"), sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    partial.replace(path)


def read_status(path: Path, *, now_ns: int | None = None) -> tuple[int, dict[str, object] | None]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return 2, None
    except (OSError, json.JSONDecodeError):
        return 3, None
    if not isinstance(payload, dict) or not isinstance(payload.get("updated_at_ns"), int):
        return 3, None
    current = time.time_ns() if now_ns is None else now_ns
    if current - payload["updated_at_ns"] > 5_000_000_000:
        return 2, payload
    return (0 if payload.get("ready") is True else 1), payload


def initial_health() -> dict[str, object]:
    return {
        "schema_version": 1,
        "process_started_at": datetime.now(UTC).isoformat(),
        "gateway_state": "starting",
        "ready": False,
        "pid": os.getpid(),
        "connections": {"rtds": False, "binance": False, "clob": False},
        "reconnect_count": 0,
        "invalid_message_count": 0,
        "duplicate_count": 0,
        "divergence_count": 0,
        "raw_events_written": 0,
        "snapshots_written": 0,
        "storage_error": None,
        "current_5m_market_id": None,
        "current_15m_market_id": None,
        "active_token_ids": [],
        "last_fatal_error": None,
    }
