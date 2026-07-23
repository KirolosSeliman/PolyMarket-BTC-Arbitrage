"""Atomic health-file publication and status inspection."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import time


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
