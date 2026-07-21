"""Deterministic transition scheduling for independent 5m and 15m workers."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from .models import (
    DiscoveryState,
    MarketWindow,
    ResolveOutcome,
    Timeframe,
    TimeframeSnapshot,
    TransitionResult,
)
from .resolver import MarketResolver, ensure_utc, floor_to_window_start
from .transition_log import TransitionLogger


SEARCH_BEFORE_SECONDS = 5.0
SEARCH_AFTER_SECONDS = 5.0
SEARCH_INTERVAL_SECONDS = 0.5


def default_utc_now() -> datetime:
    return datetime.now(UTC)


class TransitionController:
    def __init__(
        self,
        resolver: MarketResolver,
        *,
        now_utc: Callable[[], datetime] = default_utc_now,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._resolver = resolver
        self._now_utc = now_utc
        self._monotonic = monotonic
        self._sleep = sleep
        self._locks = {timeframe: asyncio.Lock() for timeframe in Timeframe}

    async def _wait_until(self, deadline: float) -> None:
        remaining = deadline - self._monotonic()
        if remaining > 0:
            await self._sleep(remaining)

    @staticmethod
    def _live_previous(previous: MarketWindow | None, now: datetime) -> MarketWindow | None:
        if previous is not None and now < previous.end_time_utc:
            return previous
        return None

    async def search_next_market(
        self,
        timeframe: Timeframe,
        previous_market: MarketWindow | None,
        expected_start_utc: datetime,
        on_state: Callable[[TimeframeSnapshot], None],
    ) -> TransitionResult:
        async with self._locks[timeframe]:
            return await self._search(timeframe, previous_market, ensure_utc(expected_start_utc), on_state)

    async def _search(
        self,
        timeframe: Timeframe,
        previous_market: MarketWindow | None,
        expected_start: datetime,
        on_state: Callable[[TimeframeSnapshot], None],
    ) -> TransitionResult:
        search_start = expected_start - timedelta(seconds=SEARCH_BEFORE_SECONDS)
        base_utc = ensure_utc(self._now_utc())
        base_monotonic = self._monotonic()

        def deadline(value: datetime) -> float:
            return base_monotonic + (value - base_utc).total_seconds()

        await self._wait_until(deadline(search_start))
        search_started = ensure_utc(self._now_utc())
        attempts = 0
        last_error: str | None = None

        def snapshot(
            state: DiscoveryState,
            *,
            current: MarketWindow | None,
            next_market: MarketWindow | None = None,
        ) -> None:
            on_state(
                TimeframeSnapshot(
                    timeframe,
                    state,
                    current,
                    next_market,
                    expected_start,
                    ensure_utc(self._now_utc()),
                    attempts,
                    last_error,
                )
            )

        snapshot(
            DiscoveryState.SEARCHING_NEXT,
            current=self._live_previous(previous_market, search_started),
        )
        previous_hidden = previous_market is None or search_started >= expected_start
        slot = 0
        while slot <= 20 and deadline(
            search_start + timedelta(seconds=slot * SEARCH_INTERVAL_SECONDS)
        ) < self._monotonic():
            slot += 1
        while slot <= 20:
            slot_time = search_start + timedelta(seconds=slot * SEARCH_INTERVAL_SECONDS)
            await self._wait_until(deadline(slot_time))
            now = ensure_utc(self._now_utc())
            if now >= expected_start and not previous_hidden:
                snapshot(DiscoveryState.SEARCHING_NEXT, current=None)
                previous_hidden = True
            resolved = await self._resolver.resolve_market(timeframe, expected_start)
            attempts += 1
            resolved_at = ensure_utc(self._now_utc())
            if resolved.outcome is ResolveOutcome.FOUND:
                last_error = None
                market = resolved.market
                if resolved_at < expected_start:
                    snapshot(
                        DiscoveryState.NEXT_READY,
                        current=self._live_previous(previous_market, resolved_at),
                        next_market=market,
                    )
                    await self._wait_until(deadline(expected_start))
                    snapshot(DiscoveryState.ACTIVE, current=market)
                    delay_ms = 0
                else:
                    snapshot(DiscoveryState.ACTIVE, current=market)
                    delay_ms = max(0, int((resolved_at - expected_start).total_seconds() * 1000))
                return TransitionResult(
                    True,
                    timeframe,
                    previous_market,
                    market,
                    expected_start,
                    search_started,
                    resolved_at,
                    attempts,
                    delay_ms,
                    None,
                )
            last_error = resolved.error or "market_not_found"
            if resolved_at >= expected_start and not previous_hidden:
                snapshot(DiscoveryState.SEARCHING_NEXT, current=None)
                previous_hidden = True
            slot += 1
            while slot <= 20 and deadline(
                search_start + timedelta(seconds=slot * SEARCH_INTERVAL_SECONDS)
            ) < self._monotonic():
                slot += 1
        snapshot(DiscoveryState.TRANSITION_FAILED, current=None)
        return TransitionResult(
            False,
            timeframe,
            previous_market,
            None,
            expected_start,
            search_started,
            None,
            attempts,
            None,
            last_error or "market_not_found",
        )


class MarketDiscoveryRunner:
    def __init__(
        self,
        resolver: MarketResolver,
        transition_controller: TransitionController,
        transition_logger: TransitionLogger,
        on_state: Callable[[TimeframeSnapshot], None],
        *,
        now_utc: Callable[[], datetime] = default_utc_now,
    ) -> None:
        self._resolver = resolver
        self._controller = transition_controller
        self._logger = transition_logger
        self._on_state = on_state
        self._now_utc = now_utc
        self._log_lock = asyncio.Lock()

    async def _append_transition(self, result: TransitionResult) -> None:
        async with self._log_lock:
            await asyncio.to_thread(self._logger.append, result)

    async def _worker(self, timeframe: Timeframe) -> None:
        now = ensure_utc(self._now_utc())
        current_result = await self._resolver.resolve_current_market(timeframe, now)
        current = current_result.market if current_result.outcome is ResolveOutcome.FOUND else None
        expected = floor_to_window_start(now, timeframe) + timedelta(seconds=timeframe.duration_seconds)
        self._on_state(
            TimeframeSnapshot(
                timeframe,
                DiscoveryState.ACTIVE if current is not None else DiscoveryState.TRANSITION_FAILED,
                current,
                None,
                expected,
                ensure_utc(self._now_utc()),
                0,
                current_result.error,
            )
        )
        while True:
            result = await self._controller.search_next_market(timeframe, current, expected, self._on_state)
            await self._append_transition(result)
            current = result.new_market if result.success else None
            expected += timedelta(seconds=timeframe.duration_seconds)

    async def run_forever(self) -> None:
        await asyncio.gather(
            self._worker(Timeframe.FIVE_MINUTES),
            self._worker(Timeframe.FIFTEEN_MINUTES),
        )
