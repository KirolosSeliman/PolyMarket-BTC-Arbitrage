from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class DiscoveryStatus(str, Enum):
    SELECTED = "selected"
    NO_MATCH = "no_match"
    PROVIDER_UNAVAILABLE = "provider_unavailable"


@dataclass(frozen=True)
class DiscoveredMarket:
    market_id: str
    condition_id: str
    slug: str
    start_time_utc: datetime
    end_time_utc: datetime
    up_token_id: str
    down_token_id: str
    resolution_source: str


@dataclass(frozen=True)
class DiscoveryResult:
    status: DiscoveryStatus
    market: DiscoveredMarket | None = None
    reason: str | None = None
