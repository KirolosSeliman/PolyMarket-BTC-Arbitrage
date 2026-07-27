"""Async orchestration for Market Discovery, sources, state, and storage."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import itertools
import time
from collections.abc import AsyncIterator

from polymarket_btc.data_collection.market_discovery import (
    GammaClient,
    MarketDiscoveryRunner,
    MarketResolver,
    TimeframeSnapshot,
    TransitionController,
    TransitionLogger,
)

from .config import MarketDataConfig
from .event_bus import EventBus
from .health import HealthRegistry, write_health_file
from .models import (
    EventSource,
    EventStream,
    GatewayInvariantError,
    MarketDataEvent,
    MarketDataSnapshot,
    MarketWindowPayload,
    MarketWindowStatePayload,
    SnapshotTickPayload,
    ShutdownTimeoutError,
)
from .snapshot import SnapshotPublisher
from .reducer import MarketDataReducer
from .sources.binance_spot import BinanceSpotSource
from .sources.polymarket_clob import PolymarketClobSource
from .sources.rtds_chainlink import ChainlinkRtdsSource
from .state import StateStore
from .storage import ParquetSnapshotWriter, RawEventStorage, recover_partial_files


class MarketDataGateway:
    def __init__(
        self,
        config: MarketDataConfig,
        *,
        start_live_sources: bool = True,
        start_market_discovery: bool = True,
    ) -> None:
        self.config = config
        self.start_live_sources = start_live_sources
        self.start_market_discovery = start_market_discovery
        self.bus = EventBus(
            config.queues.state_capacity,
            config.queues.storage_capacity,
            config.queues.market_state_capacity,
            put_timeout_seconds=config.queues.put_timeout_seconds,
        )
        self.health_registry = HealthRegistry()
        self.state = StateStore(
            chainlink_stale_after_ms=config.rtds.stale_after_ms,
            binance_depth_stale_after_ms=config.binance.stale_depth_after_ms,
            binance_trade_stale_after_ms=config.binance.stale_trade_after_ms,
            health_registry=self.health_registry,
        )
        self.raw_storage = RawEventStorage(
            config.storage.data_dir,
            zstd_level=config.storage.zstd_level,
            rotate_seconds=config.storage.raw_rotate_seconds,
            rotate_bytes=config.storage.raw_rotate_bytes,
        )
        self.parquet_storage = ParquetSnapshotWriter(
            config.storage.data_dir,
            zstd_level=config.storage.zstd_level,
            rotate_seconds=config.storage.snapshot_rotate_seconds,
            rotate_rows=config.storage.snapshot_rotate_rows,
        )
        self._raw_written = 0
        self._snapshots_written = 0
        self.event_counts = {stream: 0 for stream in EventStream}
        self.publisher = SnapshotPublisher(
            subscriber_capacity=config.queues.subscriber_capacity,
        )
        self.reducer = MarketDataReducer(self.state)
        self._tick_sequence = 0
        self._sequence = itertools.count(1)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[object]] = []
        self._entered = False
        self._fatal: BaseException | None = None
        self.health: dict[str, object] = self.health_registry.to_health_file_payload(time.time_ns())
        self.chainlink = ChainlinkRtdsSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.binance = BinanceSpotSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.clob = PolymarketClobSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        resolver = MarketResolver(GammaClient())
        controller = TransitionController(resolver)
        transition_log = config.storage.data_dir / "market_discovery" / "transitions.jsonl"
        self.discovery = MarketDiscoveryRunner(
            resolver,
            controller,
            TransitionLogger(transition_log),
            self.market_discovery_callback,
        )

    def next_sequence(self) -> int:
        return next(self._sequence)

    async def _publish_source(self, event: MarketDataEvent) -> None:
        """Allocate sequence numbers only for valid events submitted to the bus."""
        accepted = await self.bus.publish(event, self.next_sequence)
        if accepted:
            self.event_counts[event.stream] += 1

    def market_discovery_callback(self, snapshot: TimeframeSnapshot) -> None:
        self.bus.publish_market_state_nowait(snapshot)

    def _write_snapshot(self, snapshot: MarketDataSnapshot) -> None:
        self.parquet_storage.write(snapshot)
        self._snapshots_written += 1

    async def _reducer(self) -> None:
        while not self._stop.is_set() or not self.bus.state_queue.empty():
            try:
                event = await asyncio.wait_for(self.bus.state_queue.get(), timeout=0.1)
            except TimeoutError:
                continue
            try:
                snapshot = self.reducer.apply(event)
                if snapshot is not None:
                    self.publisher.publish(snapshot)
                    self._write_snapshot(snapshot)
            finally:
                self.bus.state_queue.task_done()

    async def _raw_writer(self) -> None:
        last_flush = time.monotonic()
        while not self._stop.is_set() or not self.bus.storage_queue.empty():
            try:
                first = await asyncio.wait_for(
                    self.bus.storage_queue.get(), timeout=0.05
                )
            except TimeoutError:
                continue
            batch = [first]
            if self.bus.storage_queue.qsize() < self.config.storage.raw_flush_events:
                await asyncio.sleep(
                    self.config.storage.raw_flush_interval_ms / 1_000
                )
            batch_limit = (
                self.bus.storage_queue.qsize() + 1
                if self._stop.is_set()
                else self.config.storage.raw_flush_events
            )
            while (
                len(batch) < batch_limit
                and not self.bus.storage_queue.empty()
            ):
                batch.append(self.bus.storage_queue.get_nowait())
            now = time.monotonic()
            fsync = (
                now - last_flush
                >= self.config.storage.raw_fsync_interval_ms / 1_000
            )
            try:
                await asyncio.to_thread(self._persist_raw_batch, batch, fsync)
                self._raw_written += len(batch)
                if fsync:
                    last_flush = now
            finally:
                for _event in batch:
                    self.bus.storage_queue.task_done()

    def _persist_raw_batch(
        self,
        events: list[MarketDataEvent],
        fsync: bool,
    ) -> None:
        for event in events:
            self.raw_storage.write(event)
        self.raw_storage.flush(fsync=fsync)

    async def _market_manager(self) -> None:
        while not self._stop.is_set() or not self.bus.market_state_queue.empty():
            try:
                value = await asyncio.wait_for(
                    self.bus.market_state_queue.get(), timeout=0.1
                )
            except TimeoutError:
                continue
            try:
                if not isinstance(value, TimeframeSnapshot):
                    raise GatewayInvariantError("market state queue contains an invalid value")
                self.clob.on_market_snapshot(value)
                now_ns = time.time_ns()
                event_time_ns = int(
                    value.updated_at_utc.timestamp() * 1_000_000_000
                )
                def market_payload(market: object) -> MarketWindowPayload | None:
                    if market is None:
                        return None
                    return MarketWindowPayload(
                        value.timeframe,
                        market.market_id,  # type: ignore[union-attr]
                        market.condition_id,  # type: ignore[union-attr]
                        market.slug,  # type: ignore[union-attr]
                        int(market.start_time_utc.timestamp() * 1_000_000_000),  # type: ignore[union-attr]
                        int(market.end_time_utc.timestamp() * 1_000_000_000),  # type: ignore[union-attr]
                        market.up_token_id,  # type: ignore[union-attr]
                        market.down_token_id,  # type: ignore[union-attr]
                        market.resolution_source,  # type: ignore[union-attr]
                    )
                payload = MarketWindowStatePayload(
                    value.state.value,
                    value.timeframe,
                    market_payload(value.current_market),
                    market_payload(value.next_market),
                    (
                        None
                        if value.expected_transition_utc is None
                        else int(value.expected_transition_utc.timestamp() * 1_000_000_000)
                    ),
                    event_time_ns,
                    value.attempt_count,
                    value.last_error,
                )
                event = MarketDataEvent(
                    2,
                    0,
                    f"market:{value.timeframe.value}:{value.state.value}:{event_time_ns}",
                    EventSource.MARKET_DISCOVERY,
                    EventStream.MARKET_WINDOW_STATE,
                    f"BTC-{value.timeframe.value}",
                    event_time_ns,
                    None,
                    now_ns,
                    time.monotonic_ns(),
                    None,
                    value.timeframe,
                    None,
                    None,
                    None,
                    None,
                    payload,
                )
                accepted = await self.bus.publish(event, self.next_sequence)
                if accepted:
                    self.event_counts[event.stream] += 1
            finally:
                self.bus.market_state_queue.task_done()

    async def _snapshot_scheduler(self) -> None:
        interval = self.config.service.snapshot_interval_ms / 1_000
        next_slot = time.monotonic()
        while not self._stop.is_set():
            next_slot += interval
            delay = next_slot - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    return
                except TimeoutError:
                    pass
            else:
                next_slot = time.monotonic() + interval
            self._tick_sequence += 1
            now_ns = time.time_ns()
            event = MarketDataEvent(
                2,
                0,
                f"snapshot-tick:{self._tick_sequence}",
                EventSource.MARKET_DISCOVERY,
                EventStream.SNAPSHOT_TICK,
                "gateway",
                now_ns,
                None,
                now_ns,
                time.monotonic_ns(),
                str(self._tick_sequence),
                None,
                None,
                None,
                None,
                None,
                SnapshotTickPayload(
                    self._tick_sequence,
                    now_ns,
                    self.health_registry.all_source_snapshots(now_ns),
                ),
            )
            accepted = await self.bus.publish(event, self.next_sequence)
            if accepted:
                self.event_counts[event.stream] += 1

    async def _health_writer(self) -> None:
        while not self._stop.is_set():
            self._update_health("running")
            write_health_file(self.config.health.health_file, self.health)
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.config.health.update_interval_seconds,
                )
            except TimeoutError:
                pass

    def _update_health(self, state: str) -> None:
        snapshot = self.state.snapshot(time.time_ns(), 0)
        self.health.update({
            "gateway_state": state,
            "ready": snapshot.ready_for_strategy,
            "not_ready_reasons": list(snapshot.not_ready_reasons),
            "connections": {
                "rtds": self.chainlink.connected,
                "binance": self.binance.connected,
                "clob": self.clob.connected,
            },
            "reconnect_count": (
                self.chainlink.reconnect_count
                + self.binance.reconnect_count
                + self.clob.reconnect_count
            ),
            "invalid_message_count": (
                self.chainlink.invalid_count
                + self.binance.invalid_count
                + self.clob.invalid_count
            ),
            "duplicate_count": sum(
                row.duplicate_count
                for _source, row in self.health_registry.all_source_snapshots(time.time_ns())
            ),
            "divergence_count": self.clob.divergence_count,
            "queue_size": {
                "state": self.bus.state_queue.qsize(),
                "storage": self.bus.storage_queue.qsize(),
                "market_state": self.bus.market_state_queue.qsize(),
            },
            "queue_capacity": {
                "state": self.bus.state_queue.maxsize,
                "storage": self.bus.storage_queue.maxsize,
                "market_state": self.bus.market_state_queue.maxsize,
            },
            "queue_high_water": dict(self.bus.high_water),
            "raw_events_written": self._raw_written,
            "snapshots_written": self._snapshots_written,
            "active_token_ids": sorted(self.clob.assets),
        })
        self.health_registry.update_runtime(**self.health)
        self.health = self.health_registry.to_health_file_payload(time.time_ns())

    def _spawn(self, coroutine: object, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)  # type: ignore[arg-type]
        task.add_done_callback(self._task_done)
        self._tasks.append(task)

    def _task_done(self, task: asyncio.Task[object]) -> None:
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None and self._fatal is None:
            self._fatal = exception
            self._stop.set()

    async def __aenter__(self) -> "MarketDataGateway":
        if self._entered:
            raise RuntimeError("gateway cannot be entered twice")
        self._entered = True
        recover_partial_files(
            self.config.storage.data_dir,
            zstd_level=self.config.storage.zstd_level,
        )
        self._spawn(self._reducer(), "market-data-reducer")
        self._spawn(self._raw_writer(), "market-data-raw-writer")
        self._spawn(self._market_manager(), "market-data-market-manager")
        self._spawn(self._snapshot_scheduler(), "market-data-snapshot-scheduler")
        self._spawn(self._health_writer(), "market-data-health")
        if self.start_live_sources:
            self._spawn(self.chainlink.run(), "market-data-rtds")
            self._spawn(self.binance.run(), "market-data-binance")
            self._spawn(self.clob.run(), "market-data-clob")
        if self.start_market_discovery:
            self._spawn(self.discovery.run_forever(), "market-discovery")
        return self

    async def snapshots(self) -> AsyncIterator[MarketDataSnapshot]:
        queue = self.publisher.subscribe()
        try:
            while True:
                if self._fatal is not None:
                    raise self._fatal
                try:
                    snapshot = await asyncio.wait_for(queue.get(), timeout=0.5)
                except TimeoutError:
                    if self._stop.is_set():
                        break
                    continue
                if snapshot is None:
                    break
                yield snapshot
        finally:
            self.publisher.unsubscribe(queue)

    async def shutdown(self) -> None:
        if not self._entered:
            return
        self._update_health("stopping")
        await self.publisher.stop()
        await asyncio.gather(
            self.chainlink.stop(),
            self.binance.stop(),
            self.clob.stop(),
        )
        for task in self._tasks:
            if task.get_name() in {
                "market-discovery",
                "market-data-rtds",
                "market-data-binance",
                "market-data-clob",
                "market-data-snapshot-publisher",
                "market-data-snapshot-scheduler",
                "market-data-health",
            }:
                task.cancel()
        self._stop.set()
        try:
            async with asyncio.timeout(self.config.service.shutdown_timeout_seconds):
                await self.bus.state_queue.join()
                await self.bus.storage_queue.join()
                await self.bus.market_state_queue.join()
                await asyncio.gather(*self._tasks, return_exceptions=True)
        except TimeoutError as exc:
            self.health["last_fatal_error"] = "shutdown_timeout"
            raise ShutdownTimeoutError("gateway shutdown timed out") from exc
        finally:
            self.raw_storage.flush(fsync=True)
            self.raw_storage.close()
            self.parquet_storage.close()
            self.bus.close()
            self._update_health("stopped")
            write_health_file(self.config.health.health_file, self.health)
            self._entered = False

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.shutdown()
