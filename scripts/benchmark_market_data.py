"""Five-minute offline throughput and latency benchmark for the gateway core."""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from decimal import Decimal
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time

from polymarket_btc.data_collection.market_discovery import Timeframe
from polymarket_btc.data_collection.market_data.event_bus import EventBus
from polymarket_btc.data_collection.market_data.health import HealthRegistry
from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    BinanceBookTickerPayload,
    BinanceDepth20Payload,
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    Outcome,
    PolymarketBookPayload,
    PolymarketPriceChangePayload,
    PriceLevel,
    SnapshotTickPayload,
    TakerSide,
)
from polymarket_btc.data_collection.market_data.state import StateStore
from polymarket_btc.data_collection.market_data.reducer import MarketDataReducer
from polymarket_btc.data_collection.market_data.storage import (
    ParquetSnapshotWriter,
    RawEventStorage,
)


RATES = {
    EventStream.CHAINLINK_PRICE: 10,
    EventStream.BINANCE_AGG_TRADE: 500,
    EventStream.BINANCE_BOOK_TICKER: 500,
    EventStream.BINANCE_DEPTH20: 10,
    EventStream.POLYMARKET_PRICE_CHANGE: 500,
}


def percentile(values: list[float], percent: int) -> float:
    if not values:
        return 0.0
    return statistics.quantiles(values, n=100, method="inclusive")[percent - 1]


def process_rss_bytes() -> int:
    """Return resident memory without enabling allocation tracing."""
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        handle = kernel32.GetCurrentProcess()
        if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
            raise OSError("GetProcessMemoryInfo failed")
        return int(counters.WorkingSetSize)
    import resource

    maximum_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(maximum_rss * (1 if sys.platform == "darwin" else 1024))


def make_event(stream: EventStream, sequence: int) -> MarketDataEvent:
    wall_ns = time.time_ns()
    monotonic_ns = time.monotonic_ns()
    source = EventSource.BINANCE_SPOT
    timeframe = None
    outcome = None
    asset_id = None
    market_id = None
    condition_id = None
    if stream is EventStream.CHAINLINK_PRICE:
        source = EventSource.CHAINLINK_RTDS
        payload = ChainlinkPricePayload("btc/usd", Decimal("67000.25"))
    elif stream is EventStream.BINANCE_AGG_TRADE:
        payload = BinanceAggTradePayload(
            "BTCUSDT",
            sequence,
            Decimal("67000.25"),
            Decimal("0.01"),
            sequence,
            sequence,
            wall_ns,
            TakerSide.BUY,
        )
    elif stream is EventStream.BINANCE_DEPTH20:
        levels = tuple(
            PriceLevel(Decimal("67000") - index, Decimal("1"))
            for index in range(1, 21)
        )
        asks = tuple(
            PriceLevel(Decimal("67000") + index, Decimal("1"))
            for index in range(1, 21)
        )
        payload = BinanceDepth20Payload("BTCUSDT", sequence, levels, asks)
    elif stream is EventStream.BINANCE_BOOK_TICKER:
        payload = BinanceBookTickerPayload(
            "BTCUSDT", sequence, Decimal("66999"), Decimal("2"),
            Decimal("67001"), Decimal("2"),
        )
    elif stream is EventStream.POLYMARKET_PRICE_CHANGE:
        source = EventSource.POLYMARKET_CLOB
        timeframe = Timeframe.FIVE_MINUTES if sequence % 2 else Timeframe.FIFTEEN_MINUTES
        outcome = Outcome.UP if sequence % 4 < 2 else Outcome.DOWN
        asset_id = f"{timeframe.value}-{outcome.value}"
        market_id = f"market-{timeframe.value}"
        condition_id = f"condition-{timeframe.value}"
        payload = PolymarketPriceChangePayload(
            "BUY", Decimal("0.50"), Decimal("10"), Decimal("0.49"), Decimal("0.51"),
            f"change-{sequence}",
        )
    else:
        source = EventSource.POLYMARKET_CLOB
        timeframe = (
            Timeframe.FIVE_MINUTES
            if sequence % 2
            else Timeframe.FIFTEEN_MINUTES
        )
        outcome = Outcome.UP if sequence % 4 < 2 else Outcome.DOWN
        asset_id = f"{timeframe.value}-{outcome.value}"
        market_id = f"market-{timeframe.value}"
        condition_id = f"condition-{timeframe.value}"
        payload = PolymarketBookPayload(
            (PriceLevel(Decimal("0.49"), Decimal("10")),),
            (PriceLevel(Decimal("0.51"), Decimal("10")),),
            f"hash-{sequence}",
        )
    return MarketDataEvent(
        1,
        sequence,
        f"benchmark:{stream.value}:{sequence}",
        source,
        stream,
        "BTC",
        wall_ns,
        wall_ns,
        wall_ns,
        monotonic_ns,
        str(sequence),
        timeframe,
        market_id,
        condition_id,
        asset_id,
        outcome,
        payload,
    )


def make_snapshot_tick(sequence: int, health: tuple[object, ...]) -> MarketDataEvent:
    now_ns = time.time_ns()
    return MarketDataEvent(
        2, sequence, f"benchmark:snapshot:{sequence}", EventSource.MARKET_DISCOVERY,
        EventStream.SNAPSHOT_TICK, "gateway", now_ns, None, now_ns,
        time.monotonic_ns(), str(sequence), None, None, None, None, None,
        SnapshotTickPayload(sequence, now_ns, health),
    )


async def run_benchmark(duration_seconds: float) -> dict[str, object]:
    if duration_seconds <= 0:
        raise ValueError("duration must be positive")
    bus = EventBus(100_000, 200_000, 100, put_timeout_seconds=1.0)
    snapshot_queue: asyncio.Queue[object] = asyncio.Queue(20_000)
    state = StateStore(health_registry=HealthRegistry())
    reducer = MarketDataReducer(state)
    health_registry = state.health_registry
    source_to_reducer: deque[float] = deque(maxlen=200_000)
    reducer_to_snapshot: deque[float] = deque(maxlen=20_000)
    snapshot_jitter: deque[float] = deque(maxlen=20_000)
    raw_lag: deque[float] = deque(maxlen=200_000)
    parquet_lag: deque[float] = deque(maxlen=20_000)
    produced = reduced = stored = snapshots = 0
    stop = asyncio.Event()
    warmup_memory = 0
    peak_memory = process_rss_bytes()
    post_warmup_peak = peak_memory
    process_cpu_start = time.process_time()
    next_sequence = 0
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        raw_storage = RawEventStorage(root, zstd_level=3, rotate_seconds=300, rotate_bytes=1 << 30)
        parquet_storage = ParquetSnapshotWriter(root, zstd_level=3, rotate_seconds=300, rotate_rows=20_000)

        def allocate_sequence() -> int:
            nonlocal next_sequence
            next_sequence += 1
            return next_sequence

        async def reduce_events() -> None:
            nonlocal reduced, snapshots
            while not stop.is_set() or not bus.state_queue.empty():
                try:
                    event = await asyncio.wait_for(bus.state_queue.get(), 0.1)
                except TimeoutError:
                    continue
                try:
                    source_to_reducer.append((time.monotonic_ns() - event.received_monotonic_ns) / 1_000_000)
                    started = time.monotonic_ns()
                    snapshot = reducer.apply(event)
                    reduced += 1
                    if snapshot is not None:
                        reducer_to_snapshot.append((time.monotonic_ns() - started) / 1_000_000)
                        await snapshot_queue.put(snapshot)
                        snapshots += 1
                finally:
                    bus.state_queue.task_done()

        async def store_events() -> None:
            nonlocal stored
            while not stop.is_set() or not bus.storage_queue.empty():
                try:
                    first = await asyncio.wait_for(bus.storage_queue.get(), 0.1)
                except TimeoutError:
                    continue
                batch = [first]
                while len(batch) < 1_000 and not bus.storage_queue.empty():
                    batch.append(bus.storage_queue.get_nowait())
                try:
                    def write_batch() -> None:
                        for event in batch:
                            raw_storage.write(event)
                        raw_storage.flush(fsync=False)
                    await asyncio.to_thread(write_batch)
                    now_ns = time.monotonic_ns()
                    raw_lag.extend(
                        (now_ns - event.received_monotonic_ns) / 1_000_000
                        for event in batch
                    )
                    stored += len(batch)
                finally:
                    for _event in batch:
                        bus.storage_queue.task_done()

        async def store_snapshots() -> None:
            while not stop.is_set() or not snapshot_queue.empty():
                try:
                    snapshot = await asyncio.wait_for(snapshot_queue.get(), 0.1)
                except TimeoutError:
                    continue
                try:
                    await asyncio.to_thread(parquet_storage.write, snapshot)
                    parquet_lag.append((time.time_ns() - snapshot.snapshot_timestamp_ns) / 1_000_000)
                finally:
                    snapshot_queue.task_done()

        start = time.monotonic()
        tasks = [asyncio.create_task(reduce_events()), asyncio.create_task(store_events()), asyncio.create_task(store_snapshots())]
        stream_counts = {stream: 0 for stream in RATES}
        tick_count = 0
        next_memory_sample = start + 1
        try:
            while time.monotonic() - start < duration_seconds:
                elapsed = min(time.monotonic() - start, duration_seconds)
                for stream, rate in RATES.items():
                    target = int(elapsed * rate)
                    while stream_counts[stream] < target:
                        stream_counts[stream] += 1
                        event = make_event(stream, allocate_sequence())
                        await bus.publish(event, sequence_allocator=allocate_sequence)
                        produced += 1
                target_ticks = int(elapsed * 4)
                while tick_count < target_ticks:
                    tick_count += 1
                    tick = make_snapshot_tick(allocate_sequence(), health_registry.all_source_snapshots(time.time_ns()))
                    await bus.publish(tick, sequence_allocator=allocate_sequence)
                    produced += 1
                    now = time.monotonic()
                    snapshot_jitter.append(abs(now - (start + tick_count / 4)) * 1_000)
                now = time.monotonic()
                if now >= next_memory_sample:
                    current_memory = process_rss_bytes()
                    peak_memory = max(peak_memory, current_memory)
                    if not warmup_memory and elapsed >= duration_seconds * 0.8:
                        warmup_memory = current_memory
                    if warmup_memory:
                        post_warmup_peak = max(post_warmup_peak, current_memory)
                    next_memory_sample = now + 1
                await asyncio.sleep(0.005)
        finally:
            stop.set()
            await bus.state_queue.join()
            await bus.storage_queue.join()
            await snapshot_queue.join()
            await asyncio.gather(*tasks, return_exceptions=True)
            raw_storage.flush(fsync=True)
            raw_storage.close()
            parquet_storage.close()
            if not warmup_memory:
                warmup_memory = process_rss_bytes()
                post_warmup_peak = warmup_memory
            files = [path for path in root.rglob("*") if path.is_file()]
            storage_file_count = len(files)
            storage_bytes = sum(path.stat().st_size for path in files)
        final_memory = process_rss_bytes()

    elapsed_total = time.monotonic() - start
    process_cpu_seconds = time.process_time() - process_cpu_start
    average_cpu_percent = process_cpu_seconds / elapsed_total * 100
    expected_rate = sum(RATES.values()) + 4
    throughput = produced / duration_seconds
    memory_growth = max(0, post_warmup_peak - warmup_memory)
    reducer_values = list(source_to_reducer)
    snapshot_values = list(reducer_to_snapshot)
    jitter_values = list(snapshot_jitter)
    raw_values = list(raw_lag)
    parquet_values = list(parquet_lag)
    passed = (
        reduced == produced and stored == produced and snapshots == tick_count
        and throughput >= expected_rate * 0.95
        and percentile(reducer_values, 99) < 50
        and percentile(jitter_values, 99) < 100
        and memory_growth < 10 * 1024 * 1024
        and not bus.state_queue.qsize() and not bus.storage_queue.qsize() and not snapshot_queue.qsize()
    )
    return {
        "duration_seconds": round(elapsed_total, 3),
        "expected_rate_per_second": expected_rate,
        "throughput_per_second": round(throughput, 2),
        "produced": produced, "reduced": reduced, "stored": stored, "snapshots": snapshots,
        "snapshot_ticks": tick_count, "queue_high_water": dict(bus.high_water),
        "source_to_reducer_p50_ms": round(percentile(reducer_values, 50), 3),
        "source_to_reducer_p95_ms": round(percentile(reducer_values, 95), 3),
        "source_to_reducer_p99_ms": round(percentile(reducer_values, 99), 3),
        "reducer_to_snapshot_p50_ms": round(percentile(snapshot_values, 50), 3),
        "reducer_to_snapshot_p95_ms": round(percentile(snapshot_values, 95), 3),
        "reducer_to_snapshot_p99_ms": round(percentile(snapshot_values, 99), 3),
        "snapshot_jitter_p50_ms": round(percentile(jitter_values, 50), 3),
        "snapshot_jitter_p95_ms": round(percentile(jitter_values, 95), 3),
        "snapshot_jitter_p99_ms": round(percentile(jitter_values, 99), 3),
        "raw_writer_lag_p99_ms": round(percentile(raw_values, 99), 3),
        "parquet_writer_lag_p99_ms": round(percentile(parquet_values, 99), 3),
        "memory_growth_after_warmup_bytes": memory_growth,
        "peak_rss_bytes": peak_memory,
        "process_cpu_seconds": round(process_cpu_seconds, 3),
        "average_process_cpu_percent": round(average_cpu_percent, 2),
        "logical_cpu_count": os.cpu_count(), "storage_file_count": storage_file_count,
        "storage_bytes": storage_bytes, "passed": passed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=300)
    args = parser.parse_args()
    report = asyncio.run(run_benchmark(args.duration_seconds))
    print(json.dumps(report, separators=(",", ":")))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
