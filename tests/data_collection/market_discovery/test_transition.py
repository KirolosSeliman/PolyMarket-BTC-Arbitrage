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
from polymarket_btc.data_collection.market_discovery.transition_log import TransitionLogError
from tests.data_collection.market_discovery.fixtures import market


T = datetime(2026, 7, 21, 10, 5, tzinfo=UTC)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.value = now
        self.ticks = 100.0
        self.sleepers: list[tuple[float, asyncio.Future[None]]] = []
        self.driver_scheduled = False

    def now_utc(self) -> datetime:
        return self.value

    def monotonic(self) -> float:
        return self.ticks

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        future = asyncio.get_running_loop().create_future()
        self.sleepers.append((self.ticks + seconds, future))
        self._schedule_driver()
        await future

    def _schedule_driver(self) -> None:
        if not self.driver_scheduled:
            self.driver_scheduled = True
            asyncio.get_running_loop().call_soon(self._advance_next_deadline)

    def _advance_next_deadline(self) -> None:
        self.driver_scheduled = False
        if not self.sleepers:
            return
        next_deadline = min(deadline for deadline, _ in self.sleepers)
        elapsed = next_deadline - self.ticks
        self.value += timedelta(seconds=elapsed)
        self.ticks = next_deadline
        due = [item for item in self.sleepers if item[0] <= self.ticks]
        self.sleepers = [item for item in self.sleepers if item[0] > self.ticks]
        for _, sleeper in due:
            if not sleeper.done():
                sleeper.set_result(None)
        if self.sleepers:
            self._schedule_driver()

    async def advance(self, seconds: float) -> None:
        await self.sleep(seconds)


class ControlledClock:
    def __init__(self, now: datetime) -> None:
        self.value = now
        self.ticks = 100.0
        self.sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def now_utc(self) -> datetime:
        return self.value

    def monotonic(self) -> float:
        return self.ticks

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        future = asyncio.get_running_loop().create_future()
        self.sleepers.append((self.ticks + seconds, future))
        await future

    async def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)
        self.ticks += seconds
        due = [item for item in self.sleepers if item[0] <= self.ticks]
        self.sleepers = [item for item in self.sleepers if item[0] > self.ticks]
        for _, future in due:
            if not future.done():
                future.set_result(None)
        await asyncio.sleep(0)


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
    async def run_startup_worker(
        self,
        clock: FakeClock,
        results: list[ResolveResult | tuple[ResolveResult, float]],
        timeframe: Timeframe = Timeframe.FIVE_MINUTES,
    ):
        calls: list[datetime] = []
        sleeps: list[float] = []
        expected_calls: list[datetime] = []

        class StartupResolver:
            async def resolve_current_market(self, requested: Timeframe, now: datetime) -> ResolveResult:
                calls.append(now)
                item = results.pop(0)
                if isinstance(item, tuple):
                    resolved, latency = item
                    clock.value += timedelta(seconds=latency)
                    clock.ticks += latency
                    return resolved
                return item

        class Controller:
            async def search_next_market(self, requested, previous, expected, on_state):
                expected_calls.append(expected)
                raise asyncio.CancelledError

        async def startup_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            await clock.sleep(seconds)

        states = []
        runner = MarketDiscoveryRunner(
            StartupResolver(),
            Controller(),
            None,
            states.append,
            now_utc=clock.now_utc,
            sleep=startup_sleep,
        )
        with self.assertRaises(asyncio.CancelledError):
            await runner._worker(timeframe)
        return states, calls, sleeps, expected_calls

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

    async def test_next_ready_does_not_emit_expiry_searching_state_at_t(self) -> None:
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

    async def test_old_market_is_hidden_exactly_at_t_while_request_is_in_flight(self) -> None:
        clock = ControlledClock(T - timedelta(seconds=0.5))
        started = asyncio.Event()
        release = asyncio.Event()

        class BlockingResolver:
            async def resolve_market(self, timeframe: Timeframe, expected: datetime) -> ResolveResult:
                started.set()
                await release.wait()
                return found(timeframe, expected)

        states = []
        controller = TransitionController(
            BlockingResolver(), now_utc=clock.now_utc, monotonic=clock.monotonic, sleep=clock.sleep
        )
        previous = market(Timeframe.FIVE_MINUTES, T - timedelta(minutes=5), market_id="old")
        task = asyncio.create_task(
            controller.search_next_market(Timeframe.FIVE_MINUTES, previous, T, states.append)
        )
        await started.wait()
        await clock.advance(0.5)
        self.assertIsNone(states[-1].current_market)
        self.assertEqual(states[-1].state, DiscoveryState.SEARCHING_NEXT)
        self.assertFalse(task.done())
        release.set()
        result = await task
        self.assertTrue(result.success)
        self.assertEqual(states[-1].state, DiscoveryState.ACTIVE)
        self.assertEqual(states[-1].current_market.market_id, "new")

    async def test_expiry_snapshot_is_emitted_only_once(self) -> None:
        clock = ControlledClock(T - timedelta(seconds=0.5))
        started = asyncio.Event()
        release = asyncio.Event()

        class BlockingResolver:
            async def resolve_market(self, timeframe: Timeframe, expected: datetime) -> ResolveResult:
                started.set()
                await release.wait()
                return found(timeframe, expected)

        states = []
        controller = TransitionController(
            BlockingResolver(), now_utc=clock.now_utc, monotonic=clock.monotonic, sleep=clock.sleep
        )
        previous = market(Timeframe.FIVE_MINUTES, T - timedelta(minutes=5), market_id="old")
        task = asyncio.create_task(
            controller.search_next_market(Timeframe.FIVE_MINUTES, previous, T, states.append)
        )
        await started.wait()
        await clock.advance(0.5)
        await clock.advance(1.0)
        release.set()
        await task
        expiry_states = [
            state for state in states
            if state.state is DiscoveryState.SEARCHING_NEXT and state.current_market is None
        ]
        self.assertEqual(len(expiry_states), 1)

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

    async def test_startup_succeeds_on_first_attempt(self) -> None:
        clock = FakeClock(T - timedelta(minutes=1))
        current = market(Timeframe.FIVE_MINUTES, T - timedelta(minutes=5), market_id="current")
        result = ResolveResult(ResolveOutcome.FOUND, Timeframe.FIVE_MINUTES, current.start_time_utc, current)
        states, calls, sleeps, expected = await self.run_startup_worker(clock, [result])
        self.assertEqual((len(calls), sleeps), (1, []))
        self.assertEqual(states[0].state, DiscoveryState.ACTIVE)
        self.assertEqual(states[0].attempt_count, 1)
        self.assertEqual(expected, [current.end_time_utc])

    async def test_startup_succeeds_on_second_attempt(self) -> None:
        clock = FakeClock(T - timedelta(minutes=1))
        current = market(Timeframe.FIVE_MINUTES, T - timedelta(minutes=5), market_id="current")
        success = ResolveResult(ResolveOutcome.FOUND, Timeframe.FIVE_MINUTES, current.start_time_utc, current)
        states, calls, sleeps, _ = await self.run_startup_worker(
            clock, [not_found(expected=current.start_time_utc), success]
        )
        self.assertEqual((len(calls), sleeps), (2, [0.5]))
        self.assertEqual((states[0].state, states[0].attempt_count), (DiscoveryState.ACTIVE, 2))

    async def test_startup_succeeds_on_third_attempt(self) -> None:
        clock = FakeClock(T - timedelta(minutes=1))
        current = market(Timeframe.FIVE_MINUTES, T - timedelta(minutes=5), market_id="current")
        success = ResolveResult(ResolveOutcome.FOUND, Timeframe.FIVE_MINUTES, current.start_time_utc, current)
        states, calls, sleeps, _ = await self.run_startup_worker(
            clock,
            [not_found(expected=current.start_time_utc), provider_error(expected=current.start_time_utc), success],
        )
        self.assertEqual((len(calls), sleeps), (3, [0.5, 0.5]))
        self.assertEqual((states[0].state, states[0].attempt_count), (DiscoveryState.ACTIVE, 3))

    async def test_startup_stops_after_three_failures(self) -> None:
        clock = FakeClock(T - timedelta(minutes=1))
        expected_start = T - timedelta(minutes=5)
        states, calls, sleeps, _ = await self.run_startup_worker(
            clock,
            [not_found(expected=expected_start), provider_error(expected=expected_start), not_found(expected=expected_start)],
        )
        self.assertEqual((len(calls), sleeps), (3, [0.5, 0.5]))
        self.assertEqual(states[0].state, DiscoveryState.TRANSITION_FAILED)
        self.assertEqual(states[0].attempt_count, 3)
        self.assertEqual(states[0].last_error, "market_not_found")

    async def test_startup_recomputes_current_window_after_boundary(self) -> None:
        clock = FakeClock(T - timedelta(seconds=0.2))
        old_start = T - timedelta(minutes=5)
        new_market = market(Timeframe.FIVE_MINUTES, T, market_id="new-current")
        success = ResolveResult(ResolveOutcome.FOUND, Timeframe.FIVE_MINUTES, T, new_market)
        states, calls, sleeps, expected = await self.run_startup_worker(
            clock, [not_found(expected=old_start), success]
        )
        self.assertLess(calls[0], T)
        self.assertGreaterEqual(calls[1], T)
        self.assertEqual(sleeps, [0.5])
        self.assertEqual(states[0].current_market.market_id, "new-current")
        self.assertEqual(expected, [new_market.end_time_utc])

    async def test_startup_rejects_market_that_expired_during_request(self) -> None:
        clock = FakeClock(T - timedelta(seconds=0.2))
        old_market = market(Timeframe.FIVE_MINUTES, T - timedelta(minutes=5), market_id="expired")
        old_result = ResolveResult(ResolveOutcome.FOUND, Timeframe.FIVE_MINUTES, old_market.start_time_utc, old_market)
        new_market = market(Timeframe.FIVE_MINUTES, T, market_id="new-current")
        new_result = ResolveResult(ResolveOutcome.FOUND, Timeframe.FIVE_MINUTES, T, new_market)
        states, calls, sleeps, _ = await self.run_startup_worker(
            clock, [(old_result, 0.3), new_result]
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, [0.5])
        self.assertEqual(states[0].current_market.market_id, "new-current")

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

        async def no_sleep(seconds: float) -> None:
            pass

        runner = MarketDiscoveryRunner(
            StartupResolver(),
            Controller(),
            Logger(),
            lambda _: None,
            now_utc=lambda: T - timedelta(minutes=1),
            sleep=no_sleep,
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

    async def test_transition_log_failure_stops_both_workers(self) -> None:
        other_started = asyncio.Event()
        other_cancelled = asyncio.Event()

        class FailingLogger:
            def append(self, result: object) -> None:
                raise TransitionLogError("disk unavailable")

        class FailingRunner(MarketDiscoveryRunner):
            async def _worker(self, timeframe: Timeframe) -> None:
                if timeframe is Timeframe.FIVE_MINUTES:
                    await other_started.wait()
                    await self._append_transition(object())
                    return
                other_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    other_cancelled.set()

        runner = FailingRunner(None, None, FailingLogger(), lambda _: None)
        with self.assertRaises(TransitionLogError):
            await runner.run_forever()
        await asyncio.sleep(0)
        self.assertTrue(other_cancelled.is_set())
        active_workers = [
            task for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and task.get_name().startswith("market-discovery-")
        ]
        self.assertEqual(active_workers, [])
