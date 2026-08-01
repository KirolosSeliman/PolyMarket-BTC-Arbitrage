"""One-shot bulk historical fetchers for USDT-M Futures, backing the control
panel's "Mode déjà collecté" for the built-in sources that genuinely have a
historical archive at Binance. Each fetcher pages through the relevant REST
endpoint over a [start_ts_ns, end_ts_ns) range and writes every record
straight into a RawEventStorage as a real MarketDataEvent -- same event/
payload types and same on-disk partitioning the live sources already use, so
storage format stays uniform regardless of whether data arrived live or via
bulk fetch.

What's real and what isn't, verified directly against the live endpoints
(not assumed) before writing this:

- klines, aggTrades, markPriceKlines, fundingRate: unlimited history, free.
- openInterestHist, topLongShortAccountRatio: Binance only retains the last
  30 days server-side, even on paid plans -- both fetchers here clamp the
  requested start to that window and log when they had to.
- Order book/DOM and liquidations have NO historical archive anywhere at
  Binance -- there is deliberately no fetcher for either here; those sources
  stay mode="collect" only in gateway.py's SOURCE_CATALOG.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
import json
import time
import urllib.request

from ..models import (
    BinanceFuturesLongShortRatioPayload,
    BinanceFuturesOpenInterestPayload,
    BinanceFuturesMarkPricePayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    parse_decimal,
    parse_signed_decimal,
)
from ..storage import RawEventStorage
from .binance_futures_rest_streams import parse_rest_agg_trades
from .binance_kline_ticker import parse_kline_data

_USER_AGENT = "polymarket-btc-historical-collector"
OPEN_INTEREST_HISTORY_LIMIT_DAYS = 30
_DEFAULT_RANGE_MS = 24 * 3600 * 1000


def _get_json_sync(url: str, timeout: float) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


async def _get_json(url: str, *, timeout: float = 15.0) -> object:
    return await asyncio.to_thread(_get_json_sync, url, timeout)


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field} must be a positive integer")
    return value


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def _resolve_range_ms(start_ts_ns: int | None, end_ts_ns: int | None) -> tuple[int, int]:
    """Both bounds are optional (a plugin/source is free to be given neither)
    -- default to the same 24h window the UI's own date pickers default to,
    rather than attempting an unbounded "since inception" fetch."""
    end_ms = end_ts_ns // 1_000_000 if end_ts_ns is not None else time.time_ns() // 1_000_000
    start_ms = start_ts_ns // 1_000_000 if start_ts_ns is not None else end_ms - _DEFAULT_RANGE_MS
    return start_ms, end_ms


async def _paginate_by_time(
    fetch_page: Callable[[int, int, int], Awaitable[list]],
    *,
    start_ms: int,
    end_ms: int,
    page_limit: int,
    advance: Callable[[list], int],
    on_progress: Callable[[float], None] | None = None,
) -> AsyncIterator[list]:
    """Calls fetch_page(current_start, end_ms, page_limit) repeatedly,
    yielding each non-empty page, advancing current_start to advance(page)
    (the caller knows how to read "one past the last row" for its own row
    shape -- a list row vs. a dict row differ). Stops on an empty page, on
    reaching end_ms, or defensively if advance() ever fails to move forward
    (would otherwise loop forever on a malformed/repeating response).
    on_progress, when given, is called with a 0..1 fraction of the
    [start_ms, end_ms) span covered so far after every page -- this is the
    one place that already tracks the cursor, so it's the natural place to
    report it rather than duplicating the bookkeeping in every caller."""
    current = start_ms
    span = max(1, end_ms - start_ms)
    while current < end_ms:
        page = await fetch_page(current, end_ms, page_limit)
        if not page:
            return
        yield page
        next_start = advance(page)
        if next_start <= current:
            return
        current = next_start
        if on_progress is not None:
            on_progress(min(1.0, (current - start_ms) / span))
        await asyncio.sleep(0.15)  # rate-limit courtesy, not a real constraint at this volume


# --- Klines / mark price klines (identical 12-column row shape, confirmed
# live) share one row->event converter, tagged with a different EventSource
# so they land in separate raw-storage directories despite sharing the
# BINANCE_KLINE stream name. ---

def _kline_row_to_event(
    row: list, *, symbol: str, interval: str, event_source: EventSource, ingest_sequence: int,
) -> MarketDataEvent:
    open_time_ms, close_time_ms = int(row[0]), int(row[6])
    now_ns = time.time_ns()
    message = {
        "e": "kline",
        "s": symbol,
        "k": {
            "t": open_time_ms, "T": close_time_ms, "s": symbol, "i": interval,
            "o": row[1], "h": row[2], "l": row[3], "c": row[4],
            "v": row[5], "q": row[7], "n": int(row[8]),
            "x": now_ns >= close_time_ms * 1_000_000,
        },
    }
    payload = parse_kline_data(message, market="futures", symbol=symbol, interval=interval)
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_historical:{event_source.value}:{symbol}:{interval}:{payload.open_time_ns}",
        source=event_source,
        stream=EventStream.BINANCE_KLINE,
        instrument=symbol,
        source_timestamp_ns=payload.close_time_ns,
        server_timestamp_ns=payload.close_time_ns,
        received_wall_timestamp_ns=now_ns,
        received_monotonic_ns=time.monotonic_ns(),
        source_sequence=None,
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=payload,
    )


async def fetch_and_store_historical_klines(
    *,
    symbol: str,
    start_ts_ns: int | None,
    end_ts_ns: int | None,
    raw_storage: RawEventStorage,
    next_sequence: Callable[[], int],
    log: Callable[[str], None],
    interval: str = "1m",
    on_progress: Callable[[float], None] | None = None,
) -> int:
    start_ms, end_ms = _resolve_range_ms(start_ts_ns, end_ts_ns)

    async def fetch_page(page_start: int, page_end: int, limit: int) -> list:
        url = (
            f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}"
            f"&startTime={page_start}&endTime={page_end}&limit={limit}"
        )
        data = await _get_json(url)
        return data if isinstance(data, list) else []

    count = 0
    async for page in _paginate_by_time(
        fetch_page, start_ms=start_ms, end_ms=end_ms, page_limit=1500,
        advance=lambda rows: int(rows[-1][6]) + 1, on_progress=on_progress,
    ):
        for row in page:
            raw_storage.write(_kline_row_to_event(
                row, symbol=symbol, interval=interval,
                event_source=EventSource.BINANCE_FUTURES_KLINE, ingest_sequence=next_sequence(),
            ))
            count += 1
        log(f"klines : {count} bougies récupérées (jusqu'à {_ms_to_iso(int(page[-1][6]))})")
    if on_progress is not None:
        on_progress(1.0)
    log(f"terminé : {count} bougies au total")
    return count


async def fetch_and_store_historical_agg_trades(
    *,
    symbol: str,
    start_ts_ns: int | None,
    end_ts_ns: int | None,
    raw_storage: RawEventStorage,
    next_sequence: Callable[[], int],
    log: Callable[[str], None],
    on_progress: Callable[[float], None] | None = None,
) -> int:
    start_ms, end_ms = _resolve_range_ms(start_ts_ns, end_ts_ns)

    async def fetch_page(page_start: int, page_end: int, limit: int) -> list:
        url = (
            f"https://fapi.binance.com/fapi/v1/aggTrades?symbol={symbol}"
            f"&startTime={page_start}&endTime={page_end}&limit={limit}"
        )
        data = await _get_json(url)
        return data if isinstance(data, list) else []

    count = 0
    async for page in _paginate_by_time(
        fetch_page, start_ms=start_ms, end_ms=end_ms, page_limit=1000,
        advance=lambda rows: int(rows[-1]["T"]) + 1, on_progress=on_progress,
    ):
        now_ns = time.time_ns()
        events = parse_rest_agg_trades(page, next_sequence=next_sequence, now_ns=now_ns, symbol=symbol)
        for event in events:
            raw_storage.write(event)
        count += len(events)
        log(f"trades : {count} récupérés (jusqu'à {_ms_to_iso(int(page[-1]['T']))})")
    if on_progress is not None:
        on_progress(1.0)
    log(f"terminé : {count} trades au total")
    return count


def _funding_rate_row_to_event(row: dict, *, symbol: str, ingest_sequence: int) -> MarketDataEvent:
    now_ns = time.time_ns()
    funding_time_ms = _positive_int(row.get("fundingTime"), "fundingTime")
    try:
        # markPrice doubles as index_price here -- the fundingRate endpoint
        # doesn't expose a separate index price, unlike the live premiumIndex
        # poll. A documented approximation, not a real second measurement.
        mark_price = parse_decimal(str(row.get("markPrice")), "markPrice", strictly_positive=True)
        funding_rate = parse_signed_decimal(str(row.get("fundingRate")), "fundingRate")
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_historical:fundingRate:{symbol}:{funding_time_ms}",
        source=EventSource.BINANCE_FUTURES_MARK_PRICE,
        stream=EventStream.BINANCE_FUTURES_MARK_PRICE,
        instrument=symbol,
        source_timestamp_ns=funding_time_ms * 1_000_000,
        server_timestamp_ns=funding_time_ms * 1_000_000,
        received_wall_timestamp_ns=now_ns,
        received_monotonic_ns=time.monotonic_ns(),
        source_sequence=None,
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=BinanceFuturesMarkPricePayload(
            mark_price=mark_price, index_price=mark_price,
            funding_rate=funding_rate, next_funding_time_ns=funding_time_ms * 1_000_000,
        ),
    )


async def fetch_and_store_historical_mark_price(
    *,
    symbol: str,
    start_ts_ns: int | None,
    end_ts_ns: int | None,
    raw_storage: RawEventStorage,
    next_sequence: Callable[[], int],
    log: Callable[[str], None],
    interval: str = "1m",
    on_progress: Callable[[float], None] | None = None,
) -> int:
    """Mirrors the live poller's BinanceFuturesMarkPricePayload, which
    already bundles mark/index price with the funding rate -- so this fetches
    both mark price candles (markPriceKlines) and funding payment history
    (fundingRate) under the one "mark price / funding" catalog entry. Two
    sub-fetches share on_progress's 0..1 range evenly (klines: 0..0.5,
    funding: 0.5..1.0) -- a rough split, not proportional to actual work,
    but good enough for an ETA and simpler than weighting by row count."""
    start_ms, end_ms = _resolve_range_ms(start_ts_ns, end_ts_ns)
    total = 0
    klines_progress = None if on_progress is None else (lambda f: on_progress(f * 0.5))
    funding_progress = None if on_progress is None else (lambda f: on_progress(0.5 + f * 0.5))

    async def fetch_klines_page(page_start: int, page_end: int, limit: int) -> list:
        url = (
            f"https://fapi.binance.com/fapi/v1/markPriceKlines?symbol={symbol}&interval={interval}"
            f"&startTime={page_start}&endTime={page_end}&limit={limit}"
        )
        data = await _get_json(url)
        return data if isinstance(data, list) else []

    count = 0
    async for page in _paginate_by_time(
        fetch_klines_page, start_ms=start_ms, end_ms=end_ms, page_limit=1500,
        advance=lambda rows: int(rows[-1][6]) + 1, on_progress=klines_progress,
    ):
        for row in page:
            raw_storage.write(_kline_row_to_event(
                row, symbol=symbol, interval=interval,
                event_source=EventSource.BINANCE_FUTURES_MARK_PRICE, ingest_sequence=next_sequence(),
            ))
            count += 1
    if klines_progress is not None:
        klines_progress(1.0)
    log(f"mark price : {count} bougies récupérées")
    total += count

    async def fetch_funding_page(page_start: int, page_end: int, limit: int) -> list:
        url = (
            f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}"
            f"&startTime={page_start}&endTime={page_end}&limit={limit}"
        )
        data = await _get_json(url)
        return data if isinstance(data, list) else []

    count = 0
    async for page in _paginate_by_time(
        fetch_funding_page, start_ms=start_ms, end_ms=end_ms, page_limit=1000,
        advance=lambda rows: int(rows[-1]["fundingTime"]) + 1, on_progress=funding_progress,
    ):
        for row in page:
            raw_storage.write(_funding_rate_row_to_event(row, symbol=symbol, ingest_sequence=next_sequence()))
            count += 1
    if on_progress is not None:
        on_progress(1.0)
    log(f"funding : {count} paiements récupérés")
    total += count

    log(f"terminé : {total} enregistrements au total (mark price + funding)")
    return total


def _open_interest_row_to_event(row: dict, *, symbol: str, ingest_sequence: int) -> MarketDataEvent:
    now_ns = time.time_ns()
    ts_ms = _positive_int(row.get("timestamp"), "timestamp")
    try:
        # openInterestHist uses "sumOpenInterest", not "openInterest" (the
        # live /fapi/v1/openInterest endpoint's field name) -- different
        # endpoint, different shape, same payload type is still the right fit.
        open_interest = parse_decimal(str(row.get("sumOpenInterest")), "sumOpenInterest", allow_zero=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_historical:openInterest:{symbol}:{ts_ms}",
        source=EventSource.BINANCE_FUTURES_REST,
        stream=EventStream.BINANCE_FUTURES_OPEN_INTEREST,
        instrument=symbol,
        source_timestamp_ns=ts_ms * 1_000_000,
        server_timestamp_ns=ts_ms * 1_000_000,
        received_wall_timestamp_ns=now_ns,
        received_monotonic_ns=time.monotonic_ns(),
        source_sequence=None,
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=BinanceFuturesOpenInterestPayload(open_interest=open_interest),
    )


def _long_short_ratio_row_to_event(row: dict, *, symbol: str, ingest_sequence: int) -> MarketDataEvent:
    now_ns = time.time_ns()
    ts_ms = _positive_int(row.get("timestamp"), "timestamp")
    try:
        long_account = parse_decimal(str(row.get("longAccount")), "longAccount", allow_zero=True)
        short_account = parse_decimal(str(row.get("shortAccount")), "shortAccount", allow_zero=True)
        ratio = parse_decimal(str(row.get("longShortRatio")), "longShortRatio", allow_zero=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=ingest_sequence,
        event_id=f"binance_futures_historical:longShortRatio:{symbol}:{ts_ms}",
        source=EventSource.BINANCE_FUTURES_REST,
        stream=EventStream.BINANCE_FUTURES_LONG_SHORT_RATIO,
        instrument=symbol,
        source_timestamp_ns=ts_ms * 1_000_000,
        server_timestamp_ns=ts_ms * 1_000_000,
        received_wall_timestamp_ns=now_ns,
        received_monotonic_ns=time.monotonic_ns(),
        source_sequence=None,
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=BinanceFuturesLongShortRatioPayload(
            long_account_ratio=long_account, short_account_ratio=short_account, long_short_ratio=ratio,
        ),
    )


async def fetch_and_store_historical_open_interest_and_long_short(
    *,
    symbol: str,
    start_ts_ns: int | None,
    end_ts_ns: int | None,
    raw_storage: RawEventStorage,
    next_sequence: Callable[[], int],
    log: Callable[[str], None],
    on_progress: Callable[[float], None] | None = None,
) -> int:
    """Binance only retains 30 days of open interest / long-short history
    server-side (confirmed live, no paid tier changes this) -- clamps the
    requested start accordingly and logs it plainly rather than silently
    returning less than what was asked for."""
    start_ms, end_ms = _resolve_range_ms(start_ts_ns, end_ts_ns)
    now_ms = time.time_ns() // 1_000_000
    floor_ms = now_ms - OPEN_INTEREST_HISTORY_LIMIT_DAYS * 86_400_000
    if start_ms < floor_ms:
        log(
            f"open interest / long-short : Binance ne conserve que les "
            f"{OPEN_INTEREST_HISTORY_LIMIT_DAYS} derniers jours, plage ajustée "
            f"à partir de {_ms_to_iso(floor_ms)}"
        )
        start_ms = floor_ms
    if start_ms >= end_ms:
        log("open interest / long-short : rien à récupérer (plage entièrement hors des 30 derniers jours)")
        if on_progress is not None:
            on_progress(1.0)
        return 0

    total = 0
    oi_progress = None if on_progress is None else (lambda f: on_progress(f * 0.5))
    long_short_progress = None if on_progress is None else (lambda f: on_progress(0.5 + f * 0.5))

    async def fetch_oi_page(page_start: int, page_end: int, limit: int) -> list:
        url = (
            f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m"
            f"&startTime={page_start}&endTime={page_end}&limit={limit}"
        )
        data = await _get_json(url)
        return data if isinstance(data, list) else []

    count = 0
    async for page in _paginate_by_time(
        fetch_oi_page, start_ms=start_ms, end_ms=end_ms, page_limit=500,
        advance=lambda rows: int(rows[-1]["timestamp"]) + 1, on_progress=oi_progress,
    ):
        for row in page:
            raw_storage.write(_open_interest_row_to_event(row, symbol=symbol, ingest_sequence=next_sequence()))
            count += 1
    if oi_progress is not None:
        oi_progress(1.0)
    log(f"open interest : {count} points récupérés")
    total += count

    async def fetch_long_short_page(page_start: int, page_end: int, limit: int) -> list:
        url = (
            f"https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol={symbol}&period=5m"
            f"&startTime={page_start}&endTime={page_end}&limit={limit}"
        )
        data = await _get_json(url)
        return data if isinstance(data, list) else []

    count = 0
    async for page in _paginate_by_time(
        fetch_long_short_page, start_ms=start_ms, end_ms=end_ms, page_limit=500,
        advance=lambda rows: int(rows[-1]["timestamp"]) + 1, on_progress=long_short_progress,
    ):
        for row in page:
            raw_storage.write(_long_short_ratio_row_to_event(row, symbol=symbol, ingest_sequence=next_sequence()))
            count += 1
    if on_progress is not None:
        on_progress(1.0)
    log(f"long/short ratio : {count} points récupérés")
    total += count

    log(f"terminé : {total} enregistrements au total (open interest + long/short)")
    return total


# base catalog key -> fetcher, all sharing the identical call signature so
# runs.py can dispatch generically without knowing which one it's calling.
HISTORICAL_FETCHERS: dict[str, Callable[..., Awaitable[int]]] = {
    "binance_futures_kline": fetch_and_store_historical_klines,
    "binance_futures_trade": fetch_and_store_historical_agg_trades,
    "binance_futures_mark_price": fetch_and_store_historical_mark_price,
    "binance_futures_open_interest_long_short": fetch_and_store_historical_open_interest_and_long_short,
}


__all__ = [
    "HISTORICAL_FETCHERS",
    "OPEN_INTEREST_HISTORY_LIMIT_DAYS",
    "fetch_and_store_historical_agg_trades",
    "fetch_and_store_historical_klines",
    "fetch_and_store_historical_mark_price",
    "fetch_and_store_historical_open_interest_and_long_short",
]
