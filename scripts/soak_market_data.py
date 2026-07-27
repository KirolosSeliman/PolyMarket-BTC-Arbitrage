"""Run and assess the explicitly opt-in live 24-hour gateway soak."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import shutil
import time
import tracemalloc
from itertools import islice

from polymarket_btc.data_collection.market_data.config import load_config
from polymarket_btc.data_collection.market_data.gateway import MarketDataGateway
from polymarket_btc.data_collection.market_data.replay import read_raw_events


async def soak(config_path: Path, duration_seconds: float) -> dict[str, object]:
    if duration_seconds <= 0:
        raise ValueError("duration must be positive")
    config = load_config(config_path)
    expected_snapshots = duration_seconds * 1_000 / config.service.snapshot_interval_ms
    snapshots = 0
    ready_snapshots = 0
    market_5m_ids: set[str] = set()
    market_15m_ids: set[str] = set()
    clob_sessions: set[str] = set()
    queue_high_water = {"state": 0, "storage": 0, "market_state": 0}
    tracemalloc.start()
    started = time.monotonic()
    warmup_memory = 0
    async with MarketDataGateway(config) as gateway:
        async for snapshot in gateway.snapshots():
            snapshots += 1
            ready_snapshots += int(snapshot.ready_for_strategy)
            if snapshot.market_5m is not None and snapshot.market_5m.market_id:
                market_5m_ids.add(snapshot.market_5m.market_id)
                for book in (snapshot.market_5m.up, snapshot.market_5m.down):
                    if book is not None and book.source_session_id:
                        clob_sessions.add(book.source_session_id)
            if snapshot.market_15m is not None and snapshot.market_15m.market_id:
                market_15m_ids.add(snapshot.market_15m.market_id)
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
        duplicate_count = gateway.bus.duplicate_count
    runtime = gateway.runtime_report()
    fatal = runtime["fatal_error"]
    raw_events = int(runtime["raw_events_written"])
    health_file_valid = False
    try:
        health = json.loads(config.health.health_file.read_text(encoding="utf-8"))
        health_file_valid = (
            isinstance(health, dict)
            and isinstance(health.get("gateway_state"), str)
            and isinstance(health.get("updated_at_ns"), int)
        )
    except (OSError, ValueError):
        pass
    replay_sample_valid = False
    try:
        replay_sample_valid = bool(next(iter(islice(read_raw_events(config.storage.data_dir / "raw"), 1)), None))
    except Exception:
        replay_sample_valid = False
    final_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    ratio = snapshots / expected_snapshots if expected_snapshots else 0
    operational_24h = duration_seconds >= 86_400
    memory_growth = max(0, final_memory - warmup_memory)
    runtime_public = {
        key: value for key, value in runtime.items() if key != "final_snapshot"
    }
    criteria = {
        "fatal_error_absent": fatal is None,
        "readiness_observed": ready_snapshots > 0,
        "snapshots_expected": ratio >= 0.95,
        "market_5m_transition_observed": len(market_5m_ids) >= 2,
        "market_15m_transition_observed": len(market_15m_ids) >= 2,
        "clob_session_observed": len(clob_sessions) >= 2,
        "raw_manifests_valid": bool(runtime["raw_manifest_valid"]),
        "parquet_manifests_valid": bool(runtime["parquet_manifest_valid"]),
        "replay_sample_valid": replay_sample_valid,
        "health_file_valid": health_file_valid,
        "memory_stable": memory_growth < 10 * 1024 * 1024,
        "disk_available": shutil.disk_usage(config.storage.data_dir).free > 0,
        "queues_drained": bool(runtime["queues_drained"]),
    }
    report = {
        "duration_seconds": round(time.monotonic() - started, 3),
        "snapshots": snapshots,
        "ready_snapshots": ready_snapshots,
        "snapshot_delivery_ratio": round(ratio, 6),
        "raw_events": raw_events,
        "duplicate_count": duplicate_count,
        "reconnects": reconnects,
        "queue_high_water": queue_high_water,
        "memory_growth_after_warmup_bytes": memory_growth,
        "peak_traced_memory_bytes": peak_memory,
        "fatal_error": fatal,
        "market_5m_ids": sorted(market_5m_ids),
        "market_15m_ids": sorted(market_15m_ids),
        "clob_sessions": sorted(clob_sessions),
        "runtime_report": runtime_public,
        "criteria": criteria,
        "validation_operational_24h": operational_24h,
        "validation_status": (
            "validation opérationnelle 24 h exécutée"
            if operational_24h
            else "validation opérationnelle 24 h non exécutée"
        ),
        "passed": operational_24h and raw_events > 0 and all(criteria.values()),
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
