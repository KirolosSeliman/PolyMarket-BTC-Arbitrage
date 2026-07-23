"""Run and assess the explicitly opt-in live 24-hour gateway soak."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import time
import tracemalloc

from polymarket_btc.data_collection.market_data.config import load_config
from polymarket_btc.data_collection.market_data.gateway import MarketDataGateway


async def soak(config_path: Path, duration_seconds: float) -> dict[str, object]:
    if duration_seconds <= 0:
        raise ValueError("duration must be positive")
    config = load_config(config_path)
    expected_snapshots = duration_seconds * 1_000 / config.service.snapshot_interval_ms
    snapshots = 0
    ready_snapshots = 0
    queue_high_water = {"state": 0, "storage": 0, "market_state": 0}
    tracemalloc.start()
    started = time.monotonic()
    warmup_memory = 0
    async with MarketDataGateway(config) as gateway:
        async for snapshot in gateway.snapshots():
            snapshots += 1
            ready_snapshots += int(snapshot.ready_for_strategy)
            queue_high_water["state"] = max(
                queue_high_water["state"], gateway.bus.state_queue.qsize()
            )
            queue_high_water["storage"] = max(
                queue_high_water["storage"], gateway.bus.storage_queue.qsize()
            )
            queue_high_water["market_state"] = max(
                queue_high_water["market_state"],
                gateway.bus.market_state_queue.qsize(),
            )
            elapsed = time.monotonic() - started
            if not warmup_memory and elapsed >= min(3600, duration_seconds * 0.2):
                warmup_memory = tracemalloc.get_traced_memory()[0]
            if elapsed >= duration_seconds:
                break
        reconnects = {
            "rtds": gateway.chainlink.reconnect_count,
            "binance": gateway.binance.reconnect_count,
            "clob": gateway.clob.reconnect_count,
        }
        fatal = None if gateway._fatal is None else repr(gateway._fatal)
        raw_events = gateway._raw_written
        duplicate_count = gateway.bus.duplicate_count
    final_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    ratio = snapshots / expected_snapshots if expected_snapshots else 0
    report = {
        "duration_seconds": round(time.monotonic() - started, 3),
        "snapshots": snapshots,
        "ready_snapshots": ready_snapshots,
        "snapshot_delivery_ratio": round(ratio, 6),
        "raw_events": raw_events,
        "duplicate_count": duplicate_count,
        "reconnects": reconnects,
        "queue_high_water": queue_high_water,
        "memory_growth_after_warmup_bytes": max(0, final_memory - warmup_memory),
        "peak_traced_memory_bytes": peak_memory,
        "fatal_error": fatal,
        "passed": fatal is None and ratio >= 0.95 and raw_events > 0,
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=86_400)
    parser.add_argument("--report", type=Path, default=Path("data/runtime/soak-report.json"))
    args = parser.parse_args()
    report = asyncio.run(soak(args.config, args.duration_seconds))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, separators=(",", ":")))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
