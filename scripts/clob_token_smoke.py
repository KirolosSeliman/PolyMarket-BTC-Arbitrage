from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, TextIO
from urllib.parse import urlencode

from polymarket_btc.data_collection.common.http import (
    HttpStatusError,
    HttpTimeoutError,
    HttpTransportError,
    JsonHttpClient,
)
from polymarket_btc.data_collection.market_discovery.config import ConfigError, load_config
from polymarket_btc.data_collection.market_discovery.discovery import (
    DEFAULT_CONFIG_PATH,
    discover_current_market,
)
from polymarket_btc.data_collection.market_discovery.models import DiscoveryStatus


CLOB_BASE_URL = "https://clob.polymarket.com"


@dataclass(frozen=True)
class BookSummary:
    asset_id: str
    market: str
    bid_count: int
    ask_count: int
    has_hash: bool


def main(argv: list[str] | None = None, *, stdout: TextIO | None = None, stderr: TextIO | None = None) -> int:
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    args = _build_parser().parse_args(argv)

    try:
        config = load_config(Path(args.config))
    except (OSError, ConfigError) as error:
        print(f"config error: {error}", file=stderr)
        return 2

    result = discover_current_market(config=config)
    if result.status != DiscoveryStatus.SELECTED or result.market is None:
        print(json.dumps({"status": result.status.value, "reason": result.reason}), file=stdout)
        return 1 if result.status != DiscoveryStatus.PROVIDER_UNAVAILABLE else 2

    http_client = JsonHttpClient()
    try:
        up_book = _fetch_book(http_client, result.market.up_token_id, args.timeout_seconds)
        down_book = _fetch_book(http_client, result.market.down_token_id, args.timeout_seconds)
    except (HttpStatusError, HttpTimeoutError, HttpTransportError, ValueError) as error:
        print(json.dumps({"status": "clob_smoke_failed", "reason": str(error)}), file=stdout)
        return 2

    print(
        json.dumps(
            {
                "status": "clob_smoke_passed",
                "market": {
                    "market_id": result.market.market_id,
                    "condition_id": result.market.condition_id,
                    "slug": result.market.slug,
                },
                "books": {
                    "up": up_book.__dict__,
                    "down": down_book.__dict__,
                },
            },
            sort_keys=True,
        ),
        file=stdout,
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Development-only read-only CLOB smoke check for discovered BTC 5m outcome tokens."
    )
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to market discovery YAML config.")
    parser.add_argument("--timeout-seconds", type=float, default=3.0, help="Per-CLOB-request timeout.")
    return parser


def _fetch_book(http_client: JsonHttpClient, token_id: str, timeout_seconds: float) -> BookSummary:
    url = f"{CLOB_BASE_URL}/book?{urlencode({'token_id': token_id})}"
    payload = http_client.get_json(url, timeout_seconds=timeout_seconds)
    if not isinstance(payload, Mapping):
        raise ValueError("CLOB book response must be an object")

    asset_id = _required_text(payload.get("asset_id"), "asset_id")
    if asset_id != token_id:
        raise ValueError("CLOB book asset_id did not match requested token_id")

    return BookSummary(
        asset_id=asset_id,
        market=_required_text(payload.get("market"), "market"),
        bid_count=_list_count(payload.get("bids"), "bids"),
        ask_count=_list_count(payload.get("asks"), "asks"),
        has_hash=bool(_required_text(payload.get("hash"), "hash")),
    )


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"CLOB book {field_name} must be a non-empty string")
    return value.strip()


def _list_count(value: object, field_name: str) -> int:
    if not isinstance(value, list):
        raise ValueError(f"CLOB book {field_name} must be a list")
    return len(value)


if __name__ == "__main__":
    raise SystemExit(main())
