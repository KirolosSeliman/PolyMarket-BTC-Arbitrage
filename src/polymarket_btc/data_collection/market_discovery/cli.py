from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, TextIO

from polymarket_btc.data_collection.market_discovery.config import ConfigError, load_config
from polymarket_btc.data_collection.market_discovery.discovery import (
    DEFAULT_CONFIG_PATH,
    discover_current_market,
)
from polymarket_btc.data_collection.market_discovery.models import DiscoveryResult, DiscoveryStatus


def main(
    argv: list[str] | None = None,
    *,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    discover: Callable[[], DiscoveryResult] | None = None,
) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    args = _build_parser().parse_args(argv)

    try:
        config = load_config(Path(args.config))
    except (OSError, ConfigError) as error:
        print(f"config error: {error}", file=stderr)
        return 2

    if args.validate_config:
        _write({"status": "config_valid"}, as_json=args.json, stdout=stdout)
        return 0

    result = discover() if discover is not None else discover_current_market(config=config)
    _write(result, as_json=args.json, stdout=stdout)
    return _exit_code_for_result(result)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover the current Polymarket BTC Up/Down 5m market.")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to market discovery YAML config.")
    parser.add_argument("--validate-config", action="store_true", help="Validate configuration and exit.")
    parser.add_argument("--once", action="store_true", help="Run one discovery cycle. This is the default.")
    parser.add_argument("--json", action="store_true", help="Write JSON output.")
    return parser


def _exit_code_for_result(result: DiscoveryResult) -> int:
    if result.status == DiscoveryStatus.SELECTED:
        return 0
    if result.status == DiscoveryStatus.PROVIDER_UNAVAILABLE:
        return 2
    return 1


def _write(value: Any, *, as_json: bool, stdout: TextIO) -> None:
    if as_json:
        print(json.dumps(_to_jsonable(value), sort_keys=True), file=stdout)
        return

    if isinstance(value, DiscoveryResult):
        market_id = value.market.market_id if value.market else "none"
        reason = value.reason or "none"
        print(f"status={value.status.value} market={market_id} reason={reason}", file=stdout)
        return

    print(value, file=stdout)


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
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
