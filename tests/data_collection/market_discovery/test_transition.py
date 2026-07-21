import asyncio
import threading
import unittest
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from polymarket_btc.data_collection.market_discovery.models import (
    DiscoveryState,
    ResolveOutcome,
    ResolveResult,
    Timeframe,
    TransitionResult,
)
from polymarket_btc.data_collection.market_discovery.transition import TransitionController
from polymarket_btc.data_collection.market_discovery.transition import MarketDiscoveryRunner
from tests.data_collection.market_discovery.fixtures import market


T = datetime(2026, 7, 21, 10, 5, tzinfo=UTC)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.value = now
        self.ticks = 100.0

    def now_utc(self) -> datetime:
        return self.value

    def monotonic(self) -> float:
        return self.ticks

    async def sleep(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)
        self.ticks += seconds
        await asyncio.sleep(0)

    async def advance(self, seconds: float) -> None:
        await self.sleep(seconds)


class FakeResolver:
    def __init__(self, clock: FakeClock, results: list[ResolveResult], latency: float = 0.0) -> None:
        self.clock = clock
        self.results = list(results)
        self.latency = latency
        self.calls: list[tuple[Timeframe, datetime]] = []
        self.active = defaultdict(int)
        self.overlap = False

    async def resolve_market(self, timeframe: Timeframe, expected: datetime) -> ResolveResult:
        self.calls.append((timeframe, self.clock.now_utc()))
        self.active[timeframe] += 1
        self.overlap |= self.active[timeframe] > 1
        if self.latency:
            await self.clock.advance(self.latency)
        self.active[timeframe] -= 1
        if self.results:
            return self.results.pop(0)
        return not_found(timeframe, expected)


def found(timeframe: Timeframe = Timeframe.FIVE_MINUTES, expected: datetime = T) -> ResolveResult:
    return ResolveResult(ResolveOutcome.FOUND, timeframe, expected, market(timeframe, expected, market_id="new"))


def not_found(timeframe: Timeframe = Timeframe.FIVE_MINUTES, expected: datetime = T, error: str | None = None) -> ResolveResult:
    return ResolveResult(ResolveOutcome.NOT_FOUND, timeframe, expected, error=error)


def provider_error(timeframe: Timeframe = Timeframe.FIVE_MINUTES, expected: datetime = T) -> ResolveResult:
    return ResolveResult(ResolveOutcome.ERROR, timeframe, expected, error="gamma offline")


class TransitionTests(unittest.IsolatedAsyncioTestCase):
    async def run_search(self, clock: FakeClock, resolver: FakeResolver, timeframe: Timeframe = Timeframe.FIVE_MINUTES):
        states = []
        controller = TransitionController(
            resolver,
            now_utc=clock.now_utc,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
        previous_start = T - timedelta(seconds=timeframe.duration_seconds)
        result = await controller.search_next_market(timeframe, market(timeframe, previous_start, market_id="old"), T, states.append)
        return result, states

    async def test_no_request_before_t_minus_five_and_first_at_deadline(self) -> None:
        clock = FakeClock(T - timedelta(seconds=8))
        resolver = FakeResolver(clock, [found()])
        result, states = await self.run_search(clock, resolver)
        self.assertEqual(resolver.calls[0][1], T - timedelta(seconds=5))
        self.assertEqual(states[0].state, DiscoveryState.SEARCHING_NEXT)
        self.assertTrue(result.success)

    async def test_not_found_attempts_every_half_second_at_most_21_times(self) -> None:
        clock = FakeClock(T - timedelta(seconds=5))
        resolver = FakeResolver(clock, [])
        result, states = await self.run_search(clock, resolver)
        times = [called_at for _, called_at in resolver.calls]
        self.assertEqual(len(times), 21)
        self.assertEqual(times, [T - timedelta(seconds=5) + timedelta(seconds=0.5 * i) for i in range(21)])
        self.assertEqual(times[-1], T + timedelta(seconds=5))
        self.assertFalse(result.success)
        self.assertEqual(states[-1].state, DiscoveryState.TRANSITION_FAILED)
        self.assertIsNone(states[-1].current_market)

    async def test_entry_after_t_plus_five_fails_without_request(self) -> None:
        clock = FakeClock(T + timedelta(seconds=5, microseconds=1))
        resolver = FakeResolver(clock, [found()])
        result, states = await self.run_search(clock, resolver)
        self.assertEqual(resolver.calls, [])
        self.assertFalse(result.success)
        self.assertEqual(result.attempt_count, 0)
        self.assertEqual(states[-1].state, DiscoveryState.TRANSITION_FAILED)

    async def test_entry_exactly_at_t_plus_five_may_make_final_attempt(self) -> None:
        clock = FakeClock(T + timedelta(seconds=5))
        resolver = FakeResolver(clock, [found()])
        result, _ = await self.run_search(clock, resolver)
        self.assertEqual(len(resolver.calls), 1)
        self.assertTrue(result.success)

    async def test_found_before_t_is_next_ready_then_active_at_t(self) -> None:
        clock = FakeClock(T - timedelta(seconds=5))
        resolver = FakeResolver(clock, [not_found()] * 4 + [found()])
        result, states = await self.run_search(clock, resolver)
        self.assertEqual(resolver.calls[-1][1], T - timedelta(seconds=3))
        self.assertEqual([state.state for state in states], [DiscoveryState.SEARCHING_NEXT, DiscoveryState.NEXT_READY, DiscoveryState.ACTIVE])
        self.assertEqual(states[1].current_market.market_id, "old")
        self.assertIsNone(states[1].last_error)
        self.assertIsNone(states[-1].last_error)
        self.assertEqual(clock.now_utc(), T)
        self.assertEqual(result.transition_delay_ms, 0)

    async def test_found_at_t_transitions_immediately(self) -> None:
        clock = FakeClock(T - timedelta(seconds=5))
        resolver = FakeResolver(clock, [not_found()] * 10 + [found()])
        result, states = await self.run_search(clock, resolver)
        self.assertEqual(resolver.calls[-1][1], T)
        self.assertEqual(states[-1].state, DiscoveryState.ACTIVE)
        self.assertEqual(result.transition_delay_ms, 0)

    async def test_found_after_t_never_exposes_old_market(self) -> None:
        clock = FakeClock(T - timedelta(seconds=5))
        resolver = FakeResolver(clock, [not_found()] * 14 + [found()])
        result, states = await self.run_search(clock, resolver)
        self.assertEqual(resolver.calls[-1][1], T + timedelta(seconds=2))
        self.assertEqual(result.transition_delay_ms, 2000)
        self.assertEqual(states[-1].current_market.market_id, "new")
        self.assertTrue(all(s.current_market is None for s in states if s.updated_at_utc > T and s.state != DiscoveryState.ACTIVE))

    async def test_errors_continue_and_last_error_is_preserved(self) -> None:
        clock = FakeClock(T - timedelta(seconds=5))
        resolver = FakeResolver(clock, [provider_error()] * 21)
        result, _ = await self.run_search(clock, resolver)
        self.assertFalse(result.success)
        self.assertEqual(result.last_error, "gamma offline")

    async def test_slow_requests_skip_elapsed_slots_without_overlap_or_burst(self) -> None:
        clock = FakeClock(T - timedelta(seconds=5))
        resolver = FakeResolver(clock, [], latency=0.8)
        result, _ = await self.run_search(clock, resolver)
        times = [called_at for _, called_at in resolver.calls]
        self.assertFalse(resolver.overlap)
        self.assertTrue(all((right - left).total_seconds() >= 1.0 for left, right in zip(times, times[1:])))
        self.assertLessEqual(len(times), 11)
        self.assertFalse(result.success)

    async def test_timeframes_may_resolve_concurrently_and_keep_results_independent(self) -> None:
        started: set[Timeframe] = set()
        release = asyncio.Event()

        class ConcurrentResolver:
            async def resolve_market(self, timeframe: Timeframe, expected: datetime) -> ResolveResult:
                started.add(timeframe)
                if len(started) == 2:
                    release.set()
                await release.wait()
                if timeframe is Timeframe.FIVE_MINUTES:
                    return found(timeframe, expected)
                return not_found(timeframe, expected)

        clock = FakeClock(T - timedelta(seconds=5))
        controller = TransitionController(ConcurrentResolver(), now_utc=clock.now_utc, monotonic=clock.monotonic, sleep=clock.sleep)
        five, fifteen = await asyncio.gather(
            controller.search_next_market(Timeframe.FIVE_MINUTES, None, T, lambda _: None),
            controller.search_next_market(Timeframe.FIFTEEN_MINUTES, None, T, lambda _: None),
        )
        self.assertEqual(started, set(Timeframe))
        self.assertTrue(five.success)
        self.assertFalse(fifteen.success)

    async def test_runner_starts_exactly_two_independent_workers(self) -> None:
        started: set[Timeframe] = set()
        both_started = asyncio.Event()

        class ProbeRunner(MarketDiscoveryRunner):
            async def _worker(self, timeframe: Timeframe) -> None:
                started.add(timeframe)
                if len(started) == 2:
                    both_started.set()
                await both_started.wait()

        runner = ProbeRunner(None, None, None, lambda _: None)
        await runner.run_forever()
        self.assertEqual(started, set(Timeframe))

    async def test_worker_advances_to_next_boundary_after_failure(self) -> None:
        expected_calls: list[datetime] = []

        class StartupResolver:
            async def resolve_current_market(self, timeframe: Timeframe, now: datetime) -> ResolveResult:
                return not_found(timeframe, T - timedelta(minutes=5))

        class Controller:
            async def search_next_market(self, timeframe, previous, expected, on_state):
                expected_calls.append(expected)
                if len(expected_calls) == 2:
                    raise asyncio.CancelledError
                return TransitionResult(
                    False, timeframe, previous, None, expected, expected - timedelta(seconds=5),
                    None, 21, None, "market_not_found",
                )

        class Logger:
            def append(self, result: TransitionResult) -> None:
                pass

        runner = MarketDiscoveryRunner(
            StartupResolver(), Controller(), Logger(), lambda _: None, now_utc=lambda: T - timedelta(minutes=1)
        )
        with self.assertRaises(asyncio.CancelledError):
            await runner._worker(Timeframe.FIVE_MINUTES)
        self.assertEqual(len(expected_calls), 2)
        self.assertEqual(expected_calls[1] - expected_calls[0], timedelta(minutes=5))

    async def test_durable_append_does_not_block_event_loop(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class BlockingLogger:
            def append(self, result: object) -> None:
                entered.set()
                release.wait(timeout=2)

        runner = MarketDiscoveryRunner(None, None, BlockingLogger(), lambda _: None)
        task = asyncio.create_task(runner._append_transition(object()))
        for _ in range(100):
            if entered.is_set():
                break
            await asyncio.sleep(0.001)
        self.assertTrue(entered.is_set())
        marker = False
        await asyncio.sleep(0)
        marker = True
        self.assertTrue(marker)
        release.set()
        await task
