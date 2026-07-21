"""Command-line interface for current resolution and continuous transitions."""

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from .client import GammaClient
from .models import ResolveOutcome, Timeframe
from .resolver import MarketResolver
from .transition import MarketDiscoveryRunner, TransitionController
from .transition_log import TRANSITION_LOG_PATH, TransitionLogger


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if is_dataclass(value):
        return {key: _to_jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_to_jsonable(item) for item in value]
    return value


async def _current() -> int:
    resolver = MarketResolver(GammaClient())
    results = await asyncio.gather(*(resolver.resolve_current_market(timeframe) for timeframe in Timeframe))
    payload = {
        result.timeframe.value: {
            "outcome": result.outcome.value,
            "expected_start_utc": _to_jsonable(result.expected_start_utc),
            "market": _to_jsonable(result.market),
            "error": result.error,
        }
        for result in results
    }
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    if any(result.outcome is ResolveOutcome.ERROR for result in results):
        return 2
    if any(result.outcome is ResolveOutcome.NOT_FOUND for result in results):
        return 1
    return 0


async def _run(logger: TransitionLogger) -> None:
    resolver = MarketResolver(GammaClient())
    controller = TransitionController(resolver)

    def print_snapshot(snapshot: object) -> None:
        print(json.dumps(_to_jsonable(snapshot), separators=(",", ":"), ensure_ascii=False), flush=True)

    runner = MarketDiscoveryRunner(resolver, controller, logger, print_snapshot)
    await runner.run_forever()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="market-discovery")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("current", help="resolve the current 5m and 15m markets")
    run_parser = subparsers.add_parser("run", help="run independent 5m and 15m transition workers")
    run_parser.add_argument("--transition-log", type=Path, default=TRANSITION_LOG_PATH)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "current":
            return asyncio.run(_current())
        logger = TransitionLogger(args.transition_log)
        asyncio.run(_run(logger))
        return 0
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
