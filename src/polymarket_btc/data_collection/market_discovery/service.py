from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from polymarket_btc.data_collection.common.time import utc_now
from polymarket_btc.data_collection.market_discovery.config import MarketDiscoveryConfig
from polymarket_btc.data_collection.market_discovery.gamma_client import (
    GammaClient,
    GammaClientError,
)
from polymarket_btc.data_collection.market_discovery.models import (
    CandidateValidation,
    DiscoveryResult,
    DiscoveryStatus,
)
from polymarket_btc.data_collection.market_discovery.normalizer import normalize_gamma_market
from polymarket_btc.data_collection.market_discovery.selector import select_markets


class MarketDiscoveryService:
    def __init__(
        self,
        config: MarketDiscoveryConfig,
        *,
        gamma_client: Any | None = None,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._gamma_client = gamma_client or GammaClient(config)
        self._clock = clock
        self._sleep = sleep
        self._last_selected_market_id: str | None = None

    def discover_once(self, now_utc: datetime | None = None) -> DiscoveryResult:
        observed_at = now_utc or self._clock()
        if observed_at.tzinfo is None:
            raise ValueError("now_utc must be timezone-aware")
        observed_at = observed_at.astimezone(UTC)

        starts = [compute_five_minute_window_start(observed_at)]
        if self._config.polling.preload_next_market:
            starts.append(starts[0] + timedelta(seconds=self._config.target.duration_seconds))

        slugs = [self._slug_for_start(start) for start in starts]
        try:
            payloads = [
                payload
                for slug in slugs
                if (payload := self._gamma_client.fetch_market_by_slug(slug)) is not None
            ]
        except GammaClientError as error:
            return DiscoveryResult(
                status=DiscoveryStatus.PROVIDER_UNAVAILABLE,
                observed_at_utc=observed_at,
                selected_market=None,
                next_market=None,
                candidate_count=0,
                valid_candidate_count=0,
                rejected_candidates=(),
                source_endpoint="gamma:/markets/slug",
                request_metadata={"slugs": slugs, "error": str(error)},
            )

        candidates = [
            normalize_gamma_market(payload, self._config, observed_at)
            for payload in payloads
        ]
        result = select_markets(
            candidates,
            observed_at,
            self._config,
            source_endpoint="gamma:/markets/slug",
            request_metadata={"slugs": slugs},
        )
        return self._with_transition(result)

    def poll(self, iterations: int | None = None) -> Iterator[DiscoveryResult]:
        count = 0
        while iterations is None or count < iterations:
            yield self.discover_once()
            count += 1
            if iterations is not None and count >= iterations:
                break
            self._sleep(self._config.polling.interval_seconds)

    def _slug_for_start(self, start_time_utc: datetime) -> str:
        return self._config.provider.slug_template.format(
            start_epoch=int(start_time_utc.astimezone(UTC).timestamp())
        )

    def _with_transition(self, result: DiscoveryResult) -> DiscoveryResult:
        selected = result.selected_market
        if selected is None:
            return result

        previous = self._last_selected_market_id
        self._last_selected_market_id = selected.market_id
        if previous is None or previous == selected.market_id:
            return result

        return DiscoveryResult(
            status=result.status,
            observed_at_utc=result.observed_at_utc,
            selected_market=result.selected_market,
            next_market=result.next_market,
            candidate_count=result.candidate_count,
            valid_candidate_count=result.valid_candidate_count,
            rejected_candidates=result.rejected_candidates,
            source_endpoint=result.source_endpoint,
            request_metadata=result.request_metadata,
            transition_detected=True,
            transition_from_market_id=previous,
            transition_to_market_id=selected.market_id,
        )


def compute_five_minute_window_start(now_utc: datetime) -> datetime:
    if now_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")
    now = now_utc.astimezone(UTC)
    epoch = int(now.timestamp())
    start_epoch = epoch - (epoch % 300)
    return datetime.fromtimestamp(start_epoch, UTC)
