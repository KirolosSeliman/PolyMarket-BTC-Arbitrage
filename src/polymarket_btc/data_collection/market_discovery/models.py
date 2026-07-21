"""Immutable public models for BTC market discovery."""

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class Timeframe(str, Enum):
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"

    @property
    def duration_seconds(self) -> int:
        if self is Timeframe.FIVE_MINUTES:
            return 300
        return 900

    @property
    def slug_prefix(self) -> str:
        if self is Timeframe.FIVE_MINUTES:
            return "btc-updown-5m"
        return "btc-updown-15m"


class ResolveOutcome(str, Enum):
    FOUND = "found"
    NOT_FOUND = "not_found"
    ERROR = "error"


class DiscoveryState(str, Enum):
    ACTIVE = "active"
    SEARCHING_NEXT = "searching_next"
    NEXT_READY = "next_ready"
    TRANSITION_FAILED = "transition_failed"


@dataclass(frozen=True)
class MarketWindow:
    timeframe: Timeframe
    market_id: str
    condition_id: str
    slug: str
    start_time_utc: datetime
    end_time_utc: datetime
    up_token_id: str
    down_token_id: str
    resolution_source: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "start_time_utc", _as_utc(self.start_time_utc, "start_time_utc"))
        object.__setattr__(self, "end_time_utc", _as_utc(self.end_time_utc, "end_time_utc"))
        if self.up_token_id == self.down_token_id:
            raise ValueError("up_token_id and down_token_id must differ")


@dataclass(frozen=True)
class ResolveResult:
    outcome: ResolveOutcome
    timeframe: Timeframe
    expected_start_utc: datetime
    market: MarketWindow | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "expected_start_utc", _as_utc(self.expected_start_utc, "expected_start_utc"))
        if self.outcome is ResolveOutcome.FOUND and (self.market is None or self.error is not None):
            raise ValueError("FOUND requires market and forbids error")
        if self.outcome is ResolveOutcome.NOT_FOUND and self.market is not None:
            raise ValueError("NOT_FOUND forbids market")
        if self.outcome is ResolveOutcome.ERROR and (self.market is not None or not self.error):
            raise ValueError("ERROR requires error and forbids market")


@dataclass(frozen=True)
class TimeframeSnapshot:
    timeframe: Timeframe
    state: DiscoveryState
    current_market: MarketWindow | None
    next_market: MarketWindow | None
    expected_transition_utc: datetime | None
    updated_at_utc: datetime
    attempt_count: int
    last_error: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "updated_at_utc", _as_utc(self.updated_at_utc, "updated_at_utc"))
        if self.expected_transition_utc is not None:
            object.__setattr__(
                self,
                "expected_transition_utc",
                _as_utc(self.expected_transition_utc, "expected_transition_utc"),
            )
        if self.attempt_count < 0:
            raise ValueError("attempt_count cannot be negative")


@dataclass(frozen=True)
class TransitionResult:
    success: bool
    timeframe: Timeframe
    previous_market: MarketWindow | None
    new_market: MarketWindow | None
    expected_start_utc: datetime
    search_started_at_utc: datetime
    resolved_at_utc: datetime | None
    attempt_count: int
    transition_delay_ms: int | None
    last_error: str | None

    def __post_init__(self) -> None:
        for name in ("expected_start_utc", "search_started_at_utc"):
            object.__setattr__(self, name, _as_utc(getattr(self, name), name))
        if self.resolved_at_utc is not None:
            object.__setattr__(self, "resolved_at_utc", _as_utc(self.resolved_at_utc, "resolved_at_utc"))
        if self.attempt_count < 0:
            raise ValueError("attempt_count cannot be negative")
        if self.success:
            if self.new_market is None or self.resolved_at_utc is None:
                raise ValueError("successful transition requires market and resolved time")
            if self.transition_delay_ms is None or self.transition_delay_ms < 0 or self.last_error is not None:
                raise ValueError("successful transition requires non-negative delay and no error")
        elif self.new_market is not None or self.resolved_at_utc is not None or self.transition_delay_ms is not None or not self.last_error:
            raise ValueError("failed transition requires only last_error")
