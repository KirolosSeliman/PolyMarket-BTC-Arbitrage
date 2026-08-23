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
    live = commands.add_parser("live")
    live.add_argument("--config", type=Path, default=Path("config/market_data.toml"))
    live.add_argument("--host", default="127.0.0.1")
    live.add_argument("--port", type=int, default=8765)
    live.add_argument("--duration-seconds", type=float, default=None)
    control = commands.add_parser("control")
    control.add_argument("--config", type=Path, default=Path("config/market_data.toml"))
    control.add_argument("--host", default="127.0.0.1")
    control.add_argument("--port", type=int, default=8780)
    control.add_argument("--collections-dir", type=Path, default=Path("data/collections"))
    control.add_argument("--plugins-dir", type=Path, default=Path("plugins"))
    control.add_argument("--concepts-dir", type=Path, default=Path("concepts"))
    control.add_argument("--microsystems-dir", type=Path, default=Path("microsystems"))
    control.add_argument("--execution-dir", type=Path, default=Path("execution_profiles"))
    control.add_argument("--management-dir", type=Path, default=Path("management_profiles"))
    control.add_argument("--filter-dir", type=Path, default=Path("filter_profiles"))
    control.add_argument("--strategies-dir", type=Path, default=Path("strategies"))
    control.add_argument("--concept-feedback-dir", type=Path, default=Path("concept_feedback"))
    control.add_argument("--microsystem-feedback-dir", type=Path, default=Path("microsystem_feedback"))
    control.add_argument("--filter-feedback-dir", type=Path, default=Path("filter_feedback"))
    control.add_argument(
        "--plugin-prompt", type=Path, default=Path("docs/nouveau_plugin_prompt.md"),
    )
    control.add_argument(
        "--concept-prompt", type=Path, default=Path("docs/nouveau_concept_prompt.md"),
    )
    control.add_argument(
        "--microsystem-prompt", type=Path, default=Path("docs/nouveau_microsystem_prompt.md"),
    )
    control.add_argument(
        "--execution-prompt", type=Path, default=Path("docs/nouveau_execution_prompt.md"),
    )
    control.add_argument(
        "--management-prompt", type=Path, default=Path("docs/nouveau_management_prompt.md"),
    )
    control.add_argument(
        "--filter-prompt", type=Path, default=Path("docs/nouveau_strategy_filter_prompt.md"),
    )
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
            stop_task = asyncio.create_task(stop.wait())
            fatal_task = asyncio.create_task(gateway.wait_for_fatal())
            try:
                if duration is None:
                    await asyncio.wait(
                        {stop_task, fatal_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                else:
                    done, _ = await asyncio.wait(
                        {stop_task, fatal_task},
                        timeout=duration,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if fatal_task in done:
                        await fatal_task
            finally:
                stop_task.cancel()
                fatal_task.cancel()
                await asyncio.gather(stop_task, fatal_task, return_exceptions=True)
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
        runtime = gateway.runtime_report()
        counts_by_stream = runtime["event_counts"]
        criteria = {
            "chainlink_received": counts_by_stream.get(EventStream.CHAINLINK_PRICE.value, 0) > 0,
            "agg_trade_received": counts_by_stream.get(EventStream.BINANCE_AGG_TRADE.value, 0) > 0,
            "book_ticker_received": counts_by_stream.get(EventStream.BINANCE_BOOK_TICKER.value, 0) > 0,
            "depth20_received": counts_by_stream.get(EventStream.BINANCE_DEPTH20.value, 0) > 0,
            "market_5m_active": final_snapshot is not None and final_snapshot.market_5m is not None,
            "market_15m_active": final_snapshot is not None and final_snapshot.market_15m is not None,
            "four_active_tokens": len(runtime["active_token_ids"]) == 4,
            "all_active_books_initialized": runtime["initialized_active_books"] == 4,
            "gateway_ready": bool(final_snapshot and final_snapshot.ready_for_strategy),
            "fatal_error_absent": runtime["fatal_error"] is None,
            "raw_manifest_valid": runtime["raw_manifest_valid"],
            "parquet_manifest_valid": runtime["parquet_manifest_valid"],
            "parquet_readable": runtime["parquet_readable"],
            "health_ready": bool(runtime["health_ready"]),
            "queues_drained": runtime["queues_drained"],
        }
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
            "criteria": criteria,
            "passed": all(criteria.values()),
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


def _live(args: argparse.Namespace) -> int:
    from .live_view import run_live_view

    config = load_config(args.config)
    _configure_logging(config.service.log_level)
    report = asyncio.run(
        run_live_view(
            config,
            host=args.host,
            port=args.port,
            duration_seconds=args.duration_seconds,
        )
    )
    print(json.dumps(report, separators=(",", ":")))
    return 0


async def _run_control(
    config_path: Path, host: str, port: int, collections_dir: Path, plugins_dir: Path,
    concepts_dir: Path, microsystems_dir: Path, execution_dir: Path, management_dir: Path, filter_dir: Path,
    strategies_dir: Path, concept_feedback_dir: Path, microsystem_feedback_dir: Path, filter_feedback_dir: Path,
    plugin_prompt: Path, concept_prompt: Path, microsystem_prompt: Path, execution_prompt: Path,
    management_prompt: Path, filter_prompt: Path,
) -> None:
    from ..control.concept_refinement import ConceptRefinementManager
    from ..control.microsystem_refinement import MicrosystemRefinementManager
    from ..control.runs import CollectionRunManager
    from ..control.server import ControlPanelServer
    from ..control.strategies import StrategyManager
    from ..control.strategy_filter_refinement import StrategyFilterRefinementManager

    manager = CollectionRunManager(
        config_path=config_path, collections_dir=collections_dir, plugins_dir=plugins_dir,
        concepts_dir=concepts_dir, microsystems_dir=microsystems_dir, execution_dir=execution_dir,
        management_dir=management_dir, filter_dir=filter_dir,
    )
    strategy_manager = StrategyManager(strategies_dir=strategies_dir, runs=manager)
    concept_feedback_manager = ConceptRefinementManager(feedback_dir=concept_feedback_dir, runs=manager)
    microsystem_feedback_manager = MicrosystemRefinementManager(feedback_dir=microsystem_feedback_dir, runs=manager)
    filter_feedback_manager = StrategyFilterRefinementManager(
        feedback_dir=filter_feedback_dir, runs=manager, strategies=strategy_manager,
    )
    server = ControlPanelServer(
        runs=manager, strategies=strategy_manager, concept_feedback=concept_feedback_manager,
        microsystem_feedback=microsystem_feedback_manager, filter_feedback=filter_feedback_manager,
        host=host, port=port,
        prompt_doc_path=plugin_prompt, concept_prompt_doc_path=concept_prompt,
        microsystem_prompt_doc_path=microsystem_prompt, execution_prompt_doc_path=execution_prompt,
        management_prompt_doc_path=management_prompt, filter_prompt_doc_path=filter_prompt,
    )
    await server.start()
    print(f"control panel ready: {server.url}", flush=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, name):
            try:
                loop.add_signal_handler(getattr(signal, name), stop.set)
            except NotImplementedError:
                pass
    try:
        await stop.wait()
    finally:
        if manager.current is not None and manager.current.ended_at_ns is None:
            manager.stop()
            if manager.current.task is not None:
                await asyncio.gather(manager.current.task, return_exceptions=True)
        await server.stop()


def _control(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    _configure_logging(config.service.log_level)
    asyncio.run(_run_control(
        args.config, args.host, args.port, args.collections_dir, args.plugins_dir,
        args.concepts_dir, args.microsystems_dir, args.execution_dir, args.management_dir, args.filter_dir,
        args.strategies_dir, args.concept_feedback_dir, args.microsystem_feedback_dir, args.filter_feedback_dir,
        args.plugin_prompt, args.concept_prompt, args.microsystem_prompt, args.execution_prompt,
        args.management_prompt, args.filter_prompt,
    ))
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
        if args.command == "live":
            return _live(args)
        if args.command == "control":
            return _control(args)
        report = asyncio.run(
            _run_gateway(args.config, args.duration_seconds if args.command == "smoke" else None)
        )
        print(json.dumps(report, separators=(",", ":")))
        if args.command == "smoke":
            return 0 if report["passed"] else 1
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
