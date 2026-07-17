from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from polymarket_btc.data_collection.market_discovery.config import MarketDiscoveryConfig
from polymarket_btc.data_collection.market_discovery.models import (
    CandidateValidation,
    DiscoveryResult,
    DiscoveryStatus,
    MarketDescriptor,
    RejectedCandidate,
)


def select_markets(
    candidates: Sequence[CandidateValidation],
    now_utc: datetime,
    config: MarketDiscoveryConfig,
    *,
    source_endpoint: str = "gamma",
    request_metadata: Mapping[str, Any] | None = None,
) -> DiscoveryResult:
    if now_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")

    valid_markets = [candidate.market for candidate in candidates if candidate.market is not None]
    rejections = tuple(
        candidate.rejection for candidate in candidates if candidate.rejection is not None
    )
    current_matches = [
        market
        for market in valid_markets
        if _is_current(market, now_utc)
    ]
    next_market = _find_next(valid_markets, now_utc)

    if len(current_matches) == 1:
        status = DiscoveryStatus.SELECTED
        selected_market = current_matches[0]
    elif len(current_matches) > 1 and config.selection.require_unique_active_match:
        status = DiscoveryStatus.AMBIGUOUS
        selected_market = None
    else:
        status = DiscoveryStatus.NO_MATCH
        selected_market = None

    return DiscoveryResult(
        status=status,
        observed_at_utc=now_utc,
        selected_market=selected_market,
        next_market=next_market,
        candidate_count=len(candidates),
        valid_candidate_count=len(valid_markets),
        rejected_candidates=tuple(rejection for rejection in rejections if rejection is not None),
        source_endpoint=source_endpoint,
        request_metadata=request_metadata or {},
    )


def _is_current(market: MarketDescriptor, now_utc: datetime) -> bool:
    return market.start_time_utc <= now_utc < market.end_time_utc


def _find_next(
    markets: Sequence[MarketDescriptor],
    now_utc: datetime,
) -> MarketDescriptor | None:
    future_markets = [market for market in markets if market.start_time_utc > now_utc]
    if not future_markets:
        return None
    return sorted(future_markets, key=lambda market: market.start_time_utc)[0]
