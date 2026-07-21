"""Durable append-only JSON Lines transition log."""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from .models import TransitionResult


def default_transition_log_path() -> Path:
    return Path.home() / ".polymarket-btc" / "market_discovery" / "transitions.jsonl"


class TransitionLogError(RuntimeError):
    pass


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class TransitionLogger:
    def __init__(self, path: Path | None = None) -> None:
        selected = default_transition_log_path() if path is None else path
        self._path = selected.expanduser().resolve()

    @property
    def path(self) -> Path:
        return self._path

    def append(self, result: TransitionResult) -> None:
        new_market = result.new_market
        row = {
            "event_type": "market_transition",
            "timeframe": result.timeframe.value,
            "status": "success" if result.success else "failed",
            "expected_start_utc": _timestamp(result.expected_start_utc),
            "search_started_at_utc": _timestamp(result.search_started_at_utc),
            "resolved_at_utc": _timestamp(result.resolved_at_utc),
            "attempt_count": result.attempt_count,
            "transition_delay_ms": result.transition_delay_ms,
            "previous_market_id": result.previous_market.market_id if result.previous_market else None,
            "new_market_id": new_market.market_id if new_market else None,
            "condition_id": new_market.condition_id if new_market else None,
            "up_token_id": new_market.up_token_id if new_market else None,
            "down_token_id": new_market.down_token_id if new_market else None,
            "last_error": result.last_error,
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as exc:
            raise TransitionLogError(
                f"failed to persist transition log at {self._path}: {exc}"
            ) from exc
