from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping


class DiscoveryStatus(StrEnum):
    SELECTED = "selected"
    NO_MATCH = "no_match"
    AMBIGUOUS = "ambiguous"
    INVALID_RESPONSE = "invalid_response"
    PROVIDER_UNAVAILABLE = "provider_unavailable"


@dataclass(frozen=True)
class OutcomeDescriptor:
    normalized_name: str
    source_name: str
    token_id: str


@dataclass(frozen=True)
class MarketDescriptor:
    provider: str
    event_id: str | None
    market_id: str
    condition_id: str
    question_id: str | None
    slug: str
    question: str
    description: str | None
    resolution_source: str | None
    start_time_utc: datetime
    end_time_utc: datetime
    duration_seconds: int
    active: bool
    closed: bool
    archived: bool
    enable_order_book: bool
    outcomes: tuple[OutcomeDescriptor, ...]
    discovered_at_utc: datetime


@dataclass(frozen=True)
class RejectedCandidate:
    reason: str
    message: str
    market_id: str | None = None
    slug: str | None = None


@dataclass(frozen=True)
class CandidateValidation:
    market: MarketDescriptor | None = None
    rejection: RejectedCandidate | None = None
    raw_payload: Mapping[str, Any] | None = None

    @property
    def is_valid(self) -> bool:
        return self.market is not None and self.rejection is None


@dataclass(frozen=True)
class DiscoveryResult:
    status: DiscoveryStatus
    observed_at_utc: datetime
    selected_market: MarketDescriptor | None
    next_market: MarketDescriptor | None
    candidate_count: int
    valid_candidate_count: int
    rejected_candidates: tuple[RejectedCandidate, ...]
    source_endpoint: str
    request_metadata: Mapping[str, Any]
    transition_detected: bool = False
    transition_from_market_id: str | None = None
    transition_to_market_id: str | None = None
