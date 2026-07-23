"""Five-minute offline throughput and latency benchmark for the gateway core."""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
from decimal import Decimal
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time

from polymarket_btc.data_collection.market_discovery import Timeframe
from polymarket_btc.data_collection.market_data.event_bus import EventBus
from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    BinanceDepth20Payload,
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    Outcome,
    PolymarketBookPayload,
    PriceLevel,
    TakerSide,
)
from polymarket_btc.data_collection.market_data.state import StateStore
from polymarket_btc.data_collection.market_data.storage import RawEventStorage


RATES = {
    EventStream.CHAINLINK_PRICE: 10,
    EventStream.BINANCE_AGG_TRADE: 500,
    EventStream.BINANCE_DEPTH20: 10,
    EventStream.POLYMARKET_BOOK: 40,
}


def percentile_99(values: list[float]) -> float:
    if not values:
        return 0.0
    return statistics.quantiles(values, n=100, method="inclusive")[98]


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


async def run_benchmark(duration_seconds: float) -> dict[str, object]:
    if duration_seconds <= 0:
        raise ValueError("duration must be positive")
    bus = EventBus(50_000, 100_000, 100, put_timeout_seconds=1.0)
    state = StateStore()
    latencies_ms: deque[float] = deque(maxlen=100_000)
    jitter_ms: deque[float] = deque(maxlen=10_000)
    produced = 0
    reduced = 0
    stored = 0
    stop = asyncio.Event()
    warmup_memory = 0
    with tempfile.TemporaryDirectory() as directory:
        storage = RawEventStorage(
            Path(directory), zstd_level=3, rotate_seconds=300, rotate_bytes=1 << 30
        )

        async def reduce_events() -> None:
            nonlocal reduced
            while not stop.is_set() or not bus.state_queue.empty():
                try:
                    event = await asyncio.wait_for(bus.state_queue.get(), 0.1)
                except TimeoutError:
                    continue
                state.apply(event)
                latencies_ms.append(
                    (time.monotonic_ns() - event.received_monotonic_ns) / 1_000_000
                )
                reduced += 1
                bus.state_queue.task_done()

        async def store_events() -> None:
            nonlocal stored
            while not stop.is_set() or not bus.storage_queue.empty():
                try:
                    first = await asyncio.wait_for(bus.storage_queue.get(), 0.1)
                except TimeoutError:
                    continue
                batch = [first]
                if bus.storage_queue.qsize() < 1_000:
                    await asyncio.sleep(0.05)
                while len(batch) < 1_000 and not bus.storage_queue.empty():
                    batch.append(bus.storage_queue.get_nowait())
                try:
                    await asyncio.to_thread(write_batch, batch)
                    stored += len(batch)
                finally:
                    for _event in batch:
                        bus.storage_queue.task_done()

        def write_batch(events: list[MarketDataEvent]) -> None:
            for event in events:
                storage.write(event)
            storage.flush(fsync=False)

        async def publish_snapshots(start: float) -> None:
            deadline = start + 0.25
            sequence = 0
            while not stop.is_set():
                await asyncio.sleep(max(0, deadline - time.monotonic()))
                actual = time.monotonic()
                jitter_ms.append(abs(actual - deadline) * 1_000)
                sequence += 1
                state.snapshot(time.time_ns(), sequence)
                deadline += 0.25

        start = time.monotonic()
        peak_memory = process_rss_bytes()
        next_memory_sample = start + 1
        tasks = [
            asyncio.create_task(reduce_events()),
            asyncio.create_task(store_events()),
            asyncio.create_task(publish_snapshots(start)),
        ]
        stream_counts = {stream: 0 for stream in RATES}
        try:
            while True:
                elapsed = min(time.monotonic() - start, duration_seconds)
                for stream, rate in RATES.items():
                    target = int(elapsed * rate)
                    while stream_counts[stream] < target:
                        produced += 1
                        stream_counts[stream] += 1
                        await bus.publish(make_event(stream, produced))
                        if produced % 32 == 0:
                            await asyncio.sleep(0)
                now = time.monotonic()
                if now >= next_memory_sample:
                    current_memory = process_rss_bytes()
                    peak_memory = max(peak_memory, current_memory)
                    if (
                        not warmup_memory
                        and elapsed >= duration_seconds * 0.8
                    ):
                        warmup_memory = current_memory
                    next_memory_sample = now + 1
                if elapsed >= duration_seconds:
                    break
                await asyncio.sleep(0.005)
        finally:
            stop.set()
            await bus.state_queue.join()
            await bus.storage_queue.join()
            await asyncio.gather(*tasks, return_exceptions=True)
            storage.flush(fsync=True)
            storage.close()
        final_memory = process_rss_bytes()

    elapsed_total = time.monotonic() - start
    latency_p99 = percentile_99(list(latencies_ms))
    jitter_p99 = percentile_99(list(jitter_ms))
    memory_growth = max(0, final_memory - warmup_memory)
    expected_rate = sum(RATES.values())
    throughput = produced / duration_seconds
    passed = (
        reduced == produced
        and stored == produced
        and throughput >= expected_rate * 0.95
        and latency_p99 < 50
        and jitter_p99 < 100
        and memory_growth < 10 * 1024 * 1024
    )
    return {
        "duration_seconds": round(elapsed_total, 3),
        "drain_seconds": round(max(0, elapsed_total - duration_seconds), 3),
        "expected_rate_per_second": expected_rate,
        "throughput_per_second": round(throughput, 2),
        "produced": produced,
        "reduced": reduced,
        "stored": stored,
        "event_to_state_p99_ms": round(latency_p99, 3),
        "snapshot_jitter_p99_ms": round(jitter_p99, 3),
        "memory_growth_after_warmup_bytes": memory_growth,
        "peak_rss_bytes": peak_memory,
        "passed": passed,
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
