from __future__ import annotations

import argparse
import json
import sys
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, TextIO

from polymarket_btc.data_collection.common.time import parse_utc_datetime, utc_now
from polymarket_btc.data_collection.market_discovery.config import (
    ConfigError,
    MarketDiscoveryConfig,
    load_market_discovery_config,
)
from polymarket_btc.data_collection.market_discovery.gamma_client import (
    GammaClient,
    GammaClientError,
)
from polymarket_btc.data_collection.market_discovery.models import (
    CandidateValidation,
    DiscoveryResult,
    DiscoveryStatus,
)
from polymarket_btc.data_collection.market_discovery.normalizer import normalize_gamma_market
from polymarket_btc.data_collection.market_discovery.selector import select_markets
from polymarket_btc.data_collection.market_discovery.service import MarketDiscoveryService


DEFAULT_CONFIG_PATH = Path("config/data_collection/market_discovery.yaml")


def main(
    argv: list[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_market_discovery_config(Path(args.config))
    except (OSError, ConfigError) as error:
        print(f"config error: {error}", file=stderr)
        return 2

    if args.validate_config:
        _write({"status": "config_valid"}, as_json=args.json, stdout=stdout)
        return 0

    try:
        now = _parse_now(args.now)
    except ValueError as error:
        print(f"time error: {error}", file=stderr)
        return 2

    if args.network_audit:
        return _run_network_audit(config, now, as_json=args.json, stdout=stdout, stderr=stderr)

    if args.watch:
        return _run_watch(config, args, now, stdout=stdout, stderr=stderr)

    if args.once:
        try:
            result = _run_once(config, args, now)
        except (OSError, json.JSONDecodeError, ValueError) as error:
            print(f"fixture error: {error}", file=stderr)
            return 2
        _write(result, as_json=args.json, stdout=stdout)
        return _exit_code_for_result(result)

    print("one mode is required: --validate-config, --once, --watch, or --network-audit", file=stderr)
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover Polymarket BTC Up/Down 5m markets.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to market discovery YAML config.")
    parser.add_argument("--validate-config", action="store_true", help="Validate configuration and exit.")
    parser.add_argument("--once", action="store_true", help="Run one discovery cycle.")
    parser.add_argument("--watch", action="store_true", help="Run repeated discovery cycles.")
    parser.add_argument("--iterations", type=int, default=None, help="Limit watch mode iterations.")
    parser.add_argument("--fixture", help="Path to a JSON fixture object or array of Gamma market payloads.")
    parser.add_argument("--now", help="UTC timestamp override for deterministic fixture runs.")
    parser.add_argument("--json", action="store_true", help="Write JSON output.")
    parser.add_argument("--network-audit", action="store_true", help="Run optional read-only public-search audit.")
    return parser


def _parse_now(value: str | None) -> datetime:
    if value is None:
        return utc_now()
    return parse_utc_datetime(value, "now")


def _run_once(
    config: MarketDiscoveryConfig,
    args: argparse.Namespace,
    now: datetime,
) -> DiscoveryResult:
    if args.fixture:
        return _discover_from_fixture(config, Path(args.fixture), now)
    return MarketDiscoveryService(config).discover_once(now)


def _run_watch(
    config: MarketDiscoveryConfig,
    args: argparse.Namespace,
    now: datetime,
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    if args.iterations is not None and args.iterations <= 0:
        print("--iterations must be positive", file=stderr)
        return 2

    if args.fixture:
        iterations = args.iterations or 1
        try:
            result = _discover_from_fixture(config, Path(args.fixture), now)
        except (OSError, json.JSONDecodeError, ValueError) as error:
            print(f"fixture error: {error}", file=stderr)
            return 2
        for _ in range(iterations):
            _write(result, as_json=args.json, stdout=stdout)
        return _exit_code_for_result(result)

    service = MarketDiscoveryService(config)
    last_exit_code = 1
    for result in service.poll(args.iterations):
        _write(result, as_json=args.json, stdout=stdout)
        last_exit_code = _exit_code_for_result(result)
    return last_exit_code


def _discover_from_fixture(
    config: MarketDiscoveryConfig,
    fixture_path: Path,
    now: datetime,
) -> DiscoveryResult:
    payloads = _load_fixture_payloads(fixture_path)
    candidates = [
        normalize_gamma_market(payload, config, now)
        for payload in payloads
    ]
    return select_markets(
        candidates,
        now,
        config,
        source_endpoint=f"fixture:{fixture_path}",
        request_metadata={"fixture": str(fixture_path)},
    )


def _run_network_audit(
    config: MarketDiscoveryConfig,
    now: datetime,
    *,
    as_json: bool,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    client = GammaClient(config)
    try:
        payloads = client.search_btc_five_minute_markets(limit=20)
    except GammaClientError as error:
        print(f"provider error: {error}", file=stderr)
        return 2

    candidates: list[CandidateValidation] = [
        normalize_gamma_market(payload, config, now)
        for payload in payloads
    ]
    result = select_markets(
        candidates,
        now,
        config,
        source_endpoint="gamma:/public-search",
        request_metadata={"query": config.provider.search_query, "limit": 20},
    )
    _write(result, as_json=as_json, stdout=stdout)
    return _exit_code_for_result(result)


def _load_fixture_payloads(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list) and all(isinstance(item, dict) for item in raw):
        return raw
    raise ValueError("fixture must be a JSON object or array of objects")


def _exit_code_for_result(result: DiscoveryResult) -> int:
    return 0 if result.status == DiscoveryStatus.SELECTED else 1


def _write(value: Any, *, as_json: bool, stdout: TextIO) -> None:
    if as_json:
        print(json.dumps(_to_jsonable(value), sort_keys=True), file=stdout)
        return

    if isinstance(value, DiscoveryResult):
        selected = value.selected_market.market_id if value.selected_market else "none"
        next_market = value.next_market.market_id if value.next_market else "none"
        print(f"status={value.status.value} selected_market={selected} next_market={next_market}", file=stdout)
    else:
        print(value, file=stdout)


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {
            field.name: _to_jsonable(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
