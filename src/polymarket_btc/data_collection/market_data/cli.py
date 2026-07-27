"""Command line entry point for the read-only market data gateway."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
import json
import logging
from pathlib import Path
import signal
import sys
import tempfile
import time

from .config import ConfigurationError, load_config
from .gateway import MarketDataGateway
from .health import read_status
from .models import (
    BackpressureFatalError,
    EventStream,
    GatewayInvariantError,
    ReplayIntegrityError,
    SourceConnectionError,
    StorageFatalError,
)
from .replay import read_raw_events
from .reducer import MarketDataReducer
from .state import StateStore
from .storage import ParquetSnapshotWriter


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp_ns": time.time_ns(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            },
            separators=(",", ":"),
        )


def _configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="market-data")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "smoke", "validate-config"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, default=Path("config/market_data.toml"))
        if name == "smoke":
            command.add_argument("--duration-seconds", type=float, default=120)
    status = commands.add_parser("status")
    status.add_argument("--health-file", type=Path, required=True)
    replay = commands.add_parser("replay")
    replay.add_argument("--input", type=Path, required=True)
    replay.add_argument("--output", type=Path, required=True)
    replay.add_argument("--speed", type=float, default=1.0)
    return parser


async def _run_gateway(config_path: Path, duration: float | None) -> dict[str, object]:
    config = load_config(config_path)
    _configure_logging(config.service.log_level)
    if duration is not None:
        temporary = tempfile.TemporaryDirectory()
        data_dir = Path(temporary.name)
        config = replace(
            config,
            storage=replace(config.storage, data_dir=data_dir),
            health=replace(config.health, health_file=data_dir / "runtime" / "health.json"),
        )
    else:
        temporary = None
    counts = {"snapshots": 0}
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, name):
            try:
                loop.add_signal_handler(getattr(signal, name), stop.set)
            except NotImplementedError:
                pass
    gateway = MarketDataGateway(config)
    final_snapshot = None
    desired_tokens: set[str] = set()
    initialized_tokens: set[str] = set()
    try:
        async with gateway:
            async def consume() -> None:
                async for _snapshot in gateway.snapshots():
                    counts["snapshots"] += 1
            consumer = asyncio.create_task(consume())
            if duration is None:
                await stop.wait()
            else:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=duration)
                except TimeoutError:
                    pass
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
            final_snapshot = gateway.state.snapshot(time.time_ns(), 0)
            desired_tokens = set(gateway.clob._desired)
            initialized_tokens = {
                token
                for token in desired_tokens
                if token in gateway.state._books_by_asset
                and gateway.state._books_by_asset[token].initialized
            }
        generated_files = (
            sorted(
                str(path.relative_to(config.storage.data_dir))
                for path in config.storage.data_dir.rglob("*")
                if path.is_file()
            )
            if config.storage.data_dir.exists()
            else []
        )
        disk_bytes = sum(
            path.stat().st_size
            for path in config.storage.data_dir.rglob("*")
            if path.is_file()
        ) if config.storage.data_dir.exists() else 0
        report = {
            **counts,
            "chainlink_events": gateway.event_counts[EventStream.CHAINLINK_PRICE],
            "agg_trade_events": gateway.event_counts[EventStream.BINANCE_AGG_TRADE],
            "book_ticker_events": gateway.event_counts[EventStream.BINANCE_BOOK_TICKER],
            "depth20_events": gateway.event_counts[EventStream.BINANCE_DEPTH20],
            "clob_events": sum(
                count
                for stream, count in gateway.event_counts.items()
                if stream.value.startswith("polymarket_")
            ),
            "active_clob_tokens": len(desired_tokens),
            "initialized_clob_tokens": len(initialized_tokens),
            "raw_events": gateway._raw_written,
            "reconnect_count": (
                gateway.chainlink.reconnect_count
                + gateway.binance.reconnect_count
                + gateway.clob.reconnect_count
            ),
            "invalid_count": (
                gateway.chainlink.invalid_count
                + gateway.binance.invalid_count
                + gateway.clob.invalid_count
            ),
            "duplicate_count": sum(
                row.duplicate_count
                for _source, row in gateway.health_registry.all_source_snapshots(time.time_ns())
            ),
            "maximum_queue_usage": dict(gateway.bus.high_water),
            "generated_files": generated_files,
            "disk_bytes": disk_bytes,
            "ready": final_snapshot.ready_for_strategy if final_snapshot else False,
            "not_ready_reasons": (
                list(final_snapshot.not_ready_reasons)
                if final_snapshot
                else ["snapshot_missing"]
            ),
        }
    finally:
        if temporary is not None:
            temporary.cleanup()
    return report


def _replay(input_path: Path, output_path: Path, speed: float) -> int:
    if speed < 0:
        raise ValueError("speed cannot be negative")
    state = StateStore()
    reducer = MarketDataReducer(state)
    writer = ParquetSnapshotWriter(output_path, zstd_level=6)
    previous_ns: int | None = None
    processed = 0
    ticks = 0
    snapshots = 0
    first_sequence = None
    last_sequence = None
    first_snapshot = None
    last_snapshot = None
    for event in read_raw_events(input_path):
        if speed and previous_ns is not None:
            time.sleep(max(0, event.received_wall_timestamp_ns - previous_ns) / 1_000_000_000 / speed)
        previous_ns = event.received_wall_timestamp_ns
        processed += 1
        first_sequence = event.ingest_sequence if first_sequence is None else first_sequence
        last_sequence = event.ingest_sequence
        snapshot = reducer.apply(event)
        if event.stream is EventStream.SNAPSHOT_TICK:
            ticks += 1
        if snapshot is not None:
            writer.write(snapshot)
            snapshots += 1
            first_snapshot = snapshot.snapshot_sequence if first_snapshot is None else first_snapshot
            last_snapshot = snapshot.snapshot_sequence
    manifests = writer.close()
    print(json.dumps({
        "events_processed": processed,
        "ticks_processed": ticks,
        "snapshots_produced": snapshots,
        "first_ingest_sequence": first_sequence,
        "last_ingest_sequence": last_sequence,
        "first_snapshot_sequence": first_snapshot,
        "last_snapshot_sequence": last_snapshot,
        "integrity_status": "valid",
        "output_manifests": [str(path) for path in manifests],
    }, separators=(",", ":")))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "validate-config":
            config = load_config(args.config)
            print(json.dumps({"valid": True, "data_dir": str(config.storage.data_dir)}))
            return 0
        if args.command == "status":
            code, payload = read_status(args.health_file)
            print(json.dumps(payload or {"error": "health_unavailable"}, separators=(",", ":")))
            return code
        if args.command == "replay":
            return _replay(args.input, args.output, args.speed)
        report = asyncio.run(
            _run_gateway(args.config, args.duration_seconds if args.command == "smoke" else None)
        )
        print(json.dumps(report, separators=(",", ":")))
        if args.command == "smoke":
            smoke_ok = all(
                report[key]
                for key in (
                    "snapshots",
                    "chainlink_events",
                    "agg_trade_events",
                    "book_ticker_events",
                    "depth20_events",
                    "clob_events",
                )
            ) and report["initialized_clob_tokens"] == report["active_clob_tokens"]
            return 0 if smoke_ok else 1
        return 0
    except ConfigurationError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2
    except (StorageFatalError, ReplayIntegrityError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 3
    except SourceConnectionError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 4
    except (GatewayInvariantError, BackpressureFatalError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 5
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
