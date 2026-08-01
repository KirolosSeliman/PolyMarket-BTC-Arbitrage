"""Pure parsers for Binance kline (`<symbol>@kline_1m`) and 24h rolling
ticker (`<symbol>@ticker`) messages, shared between the Spot combined-stream
parser and the standalone Futures single-stream sources -- the wire shapes
are identical across both markets for the fields this project uses.
"""

from __future__ import annotations

from decimal import Decimal

from ..models import (
    BinanceKlinePayload,
    BinanceTicker24hPayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    parse_decimal,
    parse_signed_decimal,
)


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field} must be a positive integer")
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InvalidEventError(f"{field} must be a non-negative integer")
    return value


def parse_kline_data(
    data: object,
    *,
    market: str,
    symbol: str,
    interval: str,
    timestamp_multiplier: int = 1_000_000,
) -> BinanceKlinePayload:
    """timestamp_multiplier converts k.t/k.T to nanoseconds: 1_000_000 when the
    connection reports milliseconds (the default), 1_000 when it was opened
    with Binance's `timeUnit=MICROSECOND` (Spot's combined stream)."""
    if not isinstance(data, dict) or data.get("e") != "kline" or data.get("s") != symbol:
        raise InvalidEventError("unexpected kline message identity")
    candle = data.get("k")
    if not isinstance(candle, dict) or candle.get("s") != symbol or candle.get("i") != interval:
        raise InvalidEventError("unexpected kline candle identity")
    open_time_raw = _positive_int(candle.get("t"), "k.t")
    close_time_raw = _positive_int(candle.get("T"), "k.T")
    if close_time_raw <= open_time_raw:
        raise InvalidEventError("kline close time must be after open time")
    if not isinstance(candle.get("x"), bool):
        raise InvalidEventError("k.x must be boolean")
    try:
        open_price = parse_decimal(str(candle.get("o")), "k.o", strictly_positive=True)
        high_price = parse_decimal(str(candle.get("h")), "k.h", strictly_positive=True)
        low_price = parse_decimal(str(candle.get("l")), "k.l", strictly_positive=True)
        close_price = parse_decimal(str(candle.get("c")), "k.c", strictly_positive=True)
        base_volume = parse_decimal(str(candle.get("v")), "k.v", allow_zero=True)
        quote_volume = parse_decimal(str(candle.get("q")), "k.q", allow_zero=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    if low_price > high_price or open_price > high_price or open_price < low_price:
        raise InvalidEventError("kline open/high/low are inconsistent")
    if close_price > high_price or close_price < low_price:
        raise InvalidEventError("kline close is outside the high/low range")
    trade_count = _nonnegative_int(candle.get("n"), "k.n")
    return BinanceKlinePayload(
        market=market,
        interval=interval,
        open_time_ns=open_time_raw * timestamp_multiplier,
        close_time_ns=close_time_raw * timestamp_multiplier,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        base_volume=base_volume,
        quote_volume=quote_volume,
        trade_count=trade_count,
        is_closed=candle["x"],
    )


def parse_ticker_24h_data(data: object, *, market: str, symbol: str) -> BinanceTicker24hPayload:
    if not isinstance(data, dict) or data.get("e") != "24hrTicker" or data.get("s") != symbol:
        raise InvalidEventError("unexpected 24h ticker message identity")
    try:
        price_change = parse_signed_decimal(str(data.get("p")), "p")
        price_change_percent = parse_signed_decimal(str(data.get("P")), "P")
        weighted_avg_price = parse_decimal(str(data.get("w")), "w", strictly_positive=True)
        last_price = parse_decimal(str(data.get("c")), "c", strictly_positive=True)
        open_price = parse_decimal(str(data.get("o")), "o", strictly_positive=True)
        high_price = parse_decimal(str(data.get("h")), "h", strictly_positive=True)
        low_price = parse_decimal(str(data.get("l")), "l", strictly_positive=True)
        base_volume = parse_decimal(str(data.get("v")), "v", allow_zero=True)
        quote_volume = parse_decimal(str(data.get("q")), "q", allow_zero=True)
    except ValueError as exc:
        raise InvalidEventError(str(exc)) from exc
    if low_price > high_price:
        raise InvalidEventError("24h ticker low exceeds high")
    return BinanceTicker24hPayload(
        market=market,
        price_change=price_change,
        price_change_percent=price_change_percent,
        weighted_avg_price=weighted_avg_price,
        last_price=last_price,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        base_volume=base_volume,
        quote_volume=quote_volume,
    )


def _common_fields(
    *,
    event_source: EventSource,
    received_wall_timestamp_ns: int,
    received_monotonic_ns: int,
    ingest_sequence: int,
) -> dict[str, object]:
    return {
        "schema_version": 2,
        "ingest_sequence": ingest_sequence,
        "source": event_source,
        "instrument": "BTCUSDT",
        "received_wall_timestamp_ns": received_wall_timestamp_ns,
        "received_monotonic_ns": received_monotonic_ns,
        "timeframe": None,
        "market_id": None,
        "condition_id": None,
        "asset_id": None,
        "outcome": None,
        "source_sequence": None,
        "source_session_id": None,
    }


def parse_futures_kline_message(
    message: object,
    *,
    received_wall_timestamp_ns: int,
    received_monotonic_ns: int,
    ingest_sequence: int,
) -> MarketDataEvent:
    payload = parse_kline_data(message, market="futures", symbol="BTCUSDT", interval="1m")
    return MarketDataEvent(
        **_common_fields(
            event_source=EventSource.BINANCE_FUTURES_KLINE,
            received_wall_timestamp_ns=received_wall_timestamp_ns,
            received_monotonic_ns=received_monotonic_ns,
            ingest_sequence=ingest_sequence,
        ),
        event_id=f"binance_futures:kline:BTCUSDT:1m:{payload.open_time_ns}:{payload.is_closed}",
        stream=EventStream.BINANCE_KLINE,
        source_timestamp_ns=payload.close_time_ns,
        server_timestamp_ns=payload.close_time_ns,
        payload=payload,
    )


def parse_futures_ticker_message(
    message: object,
    *,
    received_wall_timestamp_ns: int,
    received_monotonic_ns: int,
    ingest_sequence: int,
) -> MarketDataEvent:
    payload = parse_ticker_24h_data(message, market="futures", symbol="BTCUSDT")
    return MarketDataEvent(
        **_common_fields(
            event_source=EventSource.BINANCE_FUTURES_TICKER,
            received_wall_timestamp_ns=received_wall_timestamp_ns,
            received_monotonic_ns=received_monotonic_ns,
            ingest_sequence=ingest_sequence,
        ),
        event_id=f"binance_futures:ticker24h:BTCUSDT:{received_wall_timestamp_ns}",
        stream=EventStream.BINANCE_TICKER_24H,
        source_timestamp_ns=None,
        server_timestamp_ns=None,
        payload=payload,
    )


__all__ = [
    "parse_futures_kline_message",
    "parse_futures_ticker_message",
    "parse_kline_data",
    "parse_ticker_24h_data",
]
