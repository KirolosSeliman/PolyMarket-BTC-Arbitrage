"""Fetches and caches Binance's real tradable USDT-quoted symbol lists (spot
+ USDT-M perpetual futures), so the control panel's "Sources intégrées"
catalog can offer every crypto Binance actually lists instead of a hand
written few (see gateway.py's build_extra_catalog). Symbol listings change
rarely -- new listings/delistings, not intraday -- so this is cached to disk
with a generous TTL rather than re-fetched on every /api/sources call, which
matters because available_sources() is called from a synchronous HTTP
handler and a cache hit must stay effectively instant.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import urllib.error
import urllib.request

_SPOT_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
_FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
_USER_AGENT = "polymarket-btc-symbol-catalog"
DEFAULT_MAX_AGE_SECONDS = 24 * 3600.0


def _get_json_sync(url: str, timeout: float) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def fetch_spot_usdt_symbols(*, timeout: float = 15.0) -> frozenset[str]:
    """Every base asset spot-tradable against USDT and currently TRADING."""
    data = _get_json_sync(_SPOT_EXCHANGE_INFO_URL, timeout)
    rows = data.get("symbols", []) if isinstance(data, dict) else []
    result: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("status") != "TRADING" or row.get("quoteAsset") != "USDT":
            continue
        if row.get("isSpotTradingAllowed") is False:
            continue
        base = row.get("baseAsset")
        if isinstance(base, str) and base:
            result.add(base.upper())
    return frozenset(result)


def fetch_futures_usdt_perp_symbols(*, timeout: float = 15.0) -> frozenset[str]:
    """Every base asset with an active USDT-M PERPETUAL contract."""
    data = _get_json_sync(_FUTURES_EXCHANGE_INFO_URL, timeout)
    rows = data.get("symbols", []) if isinstance(data, dict) else []
    result: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("status") != "TRADING" or row.get("quoteAsset") != "USDT":
            continue
        if row.get("contractType") != "PERPETUAL":
            continue
        base = row.get("baseAsset")
        if isinstance(base, str) and base:
            result.add(base.upper())
    return frozenset(result)


@dataclass(frozen=True, slots=True)
class SymbolCatalog:
    spot: frozenset[str]
    futures: frozenset[str]
    fetched_at_utc: str

    def all_symbols(self) -> frozenset[str]:
        return self.spot | self.futures


def _write_cache(path: Path, catalog: SymbolCatalog) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    value = {
        "spot": sorted(catalog.spot),
        "futures": sorted(catalog.futures),
        "fetched_at_utc": catalog.fetched_at_utc,
    }
    partial = path.with_suffix(path.suffix + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    partial.replace(path)


def _read_cache(path: Path) -> SymbolCatalog | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    try:
        return SymbolCatalog(
            spot=frozenset(value["spot"]),
            futures=frozenset(value["futures"]),
            fetched_at_utc=value["fetched_at_utc"],
        )
    except (KeyError, TypeError):
        return None


def _cache_age_seconds(catalog: SymbolCatalog, *, now: datetime | None = None) -> float:
    fetched = datetime.fromisoformat(catalog.fetched_at_utc)
    current = now if now is not None else datetime.now(UTC)
    return (current - fetched).total_seconds()


def load_cached_or_fetch(
    cache_path: Path, *, max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS, force: bool = False,
) -> SymbolCatalog:
    """A fresh-enough cache is returned as-is (no network at all). Otherwise
    fetches live from Binance and writes the cache for next time. A fetch
    failure with a stale-but-present cache falls back to serving the stale
    data rather than breaking the whole catalog -- Binance's symbol list
    rarely changes fast enough for staleness to matter; a network hiccup
    shouldn't take the control panel's source list down with it."""
    cache_path = Path(cache_path)
    if not force:
        cached = _read_cache(cache_path)
        if cached is not None and _cache_age_seconds(cached) < max_age_seconds:
            return cached
    try:
        catalog = SymbolCatalog(
            spot=fetch_spot_usdt_symbols(),
            futures=fetch_futures_usdt_perp_symbols(),
            fetched_at_utc=datetime.now(UTC).isoformat(),
        )
    except (OSError, urllib.error.URLError, ValueError):
        cached = _read_cache(cache_path)
        if cached is not None:
            return cached
        raise
    _write_cache(cache_path, catalog)
    return catalog


def validate_symbol(symbol: str, catalog: SymbolCatalog, market: str) -> str:
    """Uppercase-normalizes `symbol` and confirms it's tradable in `market`
    ("spot" or "futures") per `catalog`. Raises ValueError if unknown -- the
    control panel's own runtime validation, separate from config.py's static
    TOML schema check (which stays BTC-only, see gateway.py's SourceInfo)."""
    normalized = symbol.upper()
    available = catalog.spot if market == "spot" else catalog.futures
    if normalized not in available:
        raise ValueError(f"{normalized} is not a tradable {market} USDT symbol on Binance")
    return normalized


__all__ = [
    "DEFAULT_MAX_AGE_SECONDS",
    "SymbolCatalog",
    "fetch_futures_usdt_perp_symbols",
    "fetch_spot_usdt_symbols",
    "load_cached_or_fetch",
    "validate_symbol",
]
