"""Projects a MarketDataSnapshot into a compact JSON frame for the live view.

Display-only: every Decimal becomes a float, so this projection is lossy and
must never feed storage, replay, or strategy code. Derived spreads live here
rather than in the browser so the arithmetic stays in one place.
"""

from __future__ import annotations

from decimal import Decimal
import json

from polymarket_btc.data_collection.market_discovery import Timeframe

from ..models import (
    BinanceAggTradePayload,
    BinanceDomSnapshotPayload,
    BinanceKlinePayload,
    BinanceTicker24hPayload,
    MarketDataSnapshot,
    OrderBookSnapshot,
    PolymarketTimeframeSnapshot,
    PriceLevel,
)

FRAME_SCHEMA_VERSION = 2

DEPTH_LEVELS = 15
BOOK_LEVELS = 10
RECENT_TRADES = 20


def _f(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _levels(levels: tuple[PriceLevel, ...], limit: int) -> list[list[float]]:
    return [[float(level.price), float(level.quantity)] for level in levels[:limit]]


def _bps(delta: Decimal | None, reference: Decimal | None) -> float | None:
    if delta is None or not reference:
        return None
    return float(delta / reference * 10_000)


def _mid(best_bid: Decimal | None, best_ask: Decimal | None) -> Decimal | None:
    if best_bid is None or best_ask is None:
        return None
    return (best_bid + best_ask) / 2


def _book(book: OrderBookSnapshot | None, now_ns: int) -> dict[str, object] | None:
    if book is None:
        return None
    mid = _mid(book.best_bid, book.best_ask)
    spread = (
        book.best_ask - book.best_bid
        if book.best_bid is not None and book.best_ask is not None
        else None
    )
    return {
        "asset_id": book.asset_id,
        "outcome": book.outcome.value,
        "best_bid": _f(book.best_bid),
        "best_ask": _f(book.best_ask),
        "mid": _f(mid),
        "spread": _f(spread),
        "last_trade": _f(book.last_trade_price),
        "tick_size": _f(book.tick_size),
        "initialized": book.initialized,
        "coherent": book.coherent,
        "resolved": book.resolved,
        "age_ms": (
            None
            if book.last_event_timestamp_ns is None
            else max(0, (now_ns - book.last_event_timestamp_ns) // 1_000_000)
        ),
        "bids": _levels(book.bids, BOOK_LEVELS),
        "asks": _levels(book.asks, BOOK_LEVELS),
    }


def _timeframe(
    market: PolymarketTimeframeSnapshot | None,
    now_ns: int,
) -> dict[str, object] | None:
    """UP + DOWN books plus the two complementary-pair arbitrage legs.

    A binary pair must price to 1. Lifting both asks below 1 locks the
    difference; hitting both bids above 1 does the same on the short side.
    """
    if market is None:
        return None
    up = _book(market.up, now_ns)
    down = _book(market.down, now_ns)

    def leg(side: str, books: tuple[dict[str, object] | None, ...]) -> float | None:
        values = [book[side] for book in books if book is not None]
        if len(values) != 2 or any(value is None for value in values):
            return None
        return sum(values)  # type: ignore[arg-type]

    buy_both = leg("best_ask", (up, down))
    sell_both = leg("best_bid", (up, down))
    mid_sum = leg("mid", (up, down))
    return {
        "timeframe": market.timeframe.value,
        "market_id": market.market_id,
        "condition_id": market.condition_id,
        "start_ns": market.start_timestamp_ns,
        "end_ns": market.end_timestamp_ns,
        "remaining_ms": market.remaining_ms,
        "resolved": market.resolved,
        "ready": market.ready,
        "not_ready": list(market.not_ready_reasons),
        "up": up,
        "down": down,
        "mid_sum": mid_sum,
        "buy_both_cost": buy_both,
        "sell_both_credit": sell_both,
        "buy_both_edge": None if buy_both is None else 1.0 - buy_both,
        "sell_both_edge": None if sell_both is None else sell_both - 1.0,
    }


def _trade(trade: BinanceAggTradePayload | None) -> dict[str, object] | None:
    if trade is None:
        return None
    return {
        "price": float(trade.price),
        "quantity": float(trade.quantity),
        "taker_side": trade.taker_side.value,
        "trade_timestamp_ns": trade.trade_timestamp_ns,
    }


def _trades(trades: tuple[BinanceAggTradePayload, ...]) -> list[dict[str, object]]:
    result = [_trade(trade) for trade in trades[:RECENT_TRADES]]
    return [row for row in result if row is not None]


def _kline(kline: BinanceKlinePayload | None) -> dict[str, object] | None:
    if kline is None:
        return None
    return {
        "interval": kline.interval,
        "open_time_ns": kline.open_time_ns,
        "close_time_ns": kline.close_time_ns,
        "open": float(kline.open),
        "high": float(kline.high),
        "low": float(kline.low),
        "close": float(kline.close),
        "base_volume": float(kline.base_volume),
        "quote_volume": float(kline.quote_volume),
        "trade_count": kline.trade_count,
        "is_closed": kline.is_closed,
    }


def _ticker_24h(ticker: BinanceTicker24hPayload | None) -> dict[str, object] | None:
    if ticker is None:
        return None
    return {
        "price_change": float(ticker.price_change),
        "price_change_percent": float(ticker.price_change_percent),
        "weighted_avg_price": float(ticker.weighted_avg_price),
        "last_price": float(ticker.last_price),
        "open_price": float(ticker.open_price),
        "high_price": float(ticker.high_price),
        "low_price": float(ticker.low_price),
        "base_volume": float(ticker.base_volume),
        "quote_volume": float(ticker.quote_volume),
    }


def _dom(dom: BinanceDomSnapshotPayload | None) -> dict[str, object] | None:
    if dom is None:
        return None
    return {
        "market": dom.market,
        "mid": float(dom.mid_price),
        "bucket_size": float(dom.bucket_size),
        "buckets": [
            [float(row.price), float(row.bid_quantity), float(row.ask_quantity)]
            for row in dom.buckets
        ],
    }


def build_frame(snapshot: MarketDataSnapshot) -> dict[str, object]:
    now_ns = snapshot.snapshot_timestamp_ns
    binance = snapshot.binance
    spot_mid = binance.mid_price
    mark = (
        snapshot.futures_mark_price.mark_price
        if snapshot.futures_mark_price is not None
        else None
    )
    chainlink_price = snapshot.chainlink.price

    futures_basis = None if mark is None or spot_mid is None else mark - spot_mid
    chainlink_basis = (
        None if chainlink_price is None or spot_mid is None else chainlink_price - spot_mid
    )

    liquidation = snapshot.futures_last_liquidation
    ratio = snapshot.futures_long_short_ratio
    futures_trade = snapshot.futures_last_trade

    return {
        "frame_schema_version": FRAME_SCHEMA_VERSION,
        "seq": snapshot.snapshot_sequence,
        "ts_ns": now_ns,
        "ready": snapshot.ready_for_strategy,
        "not_ready": list(snapshot.not_ready_reasons),
        "health": [
            {
                "source": source.value,
                "connected": row.connected,
                "stale": row.stale,
                "age_ms": row.age_ms,
                "session_id": row.current_session_id,
                "reconnects": row.reconnect_count,
                "invalid": row.invalid_count,
                "duplicates": row.duplicate_count,
                "stale_sessions": row.stale_session_count,
                "divergences": row.divergence_count,
                "protocol_errors": row.protocol_error_count,
            }
            for source, row in snapshot.health
        ],
        "spot": {
            "last": _f(binance.last_price),
            "taker_side": None if binance.taker_side is None else binance.taker_side.value,
            "bid": _f(binance.best_bid),
            "ask": _f(binance.best_ask),
            "bid_qty": _f(binance.best_bid_quantity),
            "ask_qty": _f(binance.best_ask_quantity),
            "mid": _f(spot_mid),
            "spread": _f(binance.spread),
            "spread_bps": _f(binance.spread_bps),
            "microprice": _f(binance.microprice),
            "top1_imbalance": _f(binance.top1_imbalance),
            "bid_notional": _f(binance.top20_bid_notional),
            "ask_notional": _f(binance.top20_ask_notional),
            "depth_imbalance": _f(binance.top20_depth_imbalance),
            "bids": _levels(binance.depth_bids, DEPTH_LEVELS),
            "asks": _levels(binance.depth_asks, DEPTH_LEVELS),
            "rolling": [
                {
                    "seconds": window.window_seconds,
                    "buy": float(window.buy_volume),
                    "sell": float(window.sell_volume),
                    "vwap": _f(window.vwap),
                }
                for window in binance.rolling_windows
            ],
        },
        "futures": {
            "last": _f(futures_trade.price) if futures_trade is not None else None,
            "taker_side": None if futures_trade is None else futures_trade.taker_side.value,
            "mark": _f(mark),
            "index": (
                _f(snapshot.futures_mark_price.index_price)
                if snapshot.futures_mark_price is not None
                else None
            ),
            "funding_rate": (
                _f(snapshot.futures_mark_price.funding_rate)
                if snapshot.futures_mark_price is not None
                else None
            ),
            "next_funding_ns": (
                snapshot.futures_mark_price.next_funding_time_ns
                if snapshot.futures_mark_price is not None
                else None
            ),
            "open_interest": (
                _f(snapshot.futures_open_interest.open_interest)
                if snapshot.futures_open_interest is not None
                else None
            ),
            "long_account_ratio": None if ratio is None else _f(ratio.long_account_ratio),
            "short_account_ratio": None if ratio is None else _f(ratio.short_account_ratio),
            "long_short_ratio": None if ratio is None else _f(ratio.long_short_ratio),
            "last_liquidation": (
                None
                if liquidation is None
                else {
                    "side": liquidation.side,
                    "price": float(liquidation.price),
                    "quantity": float(liquidation.quantity),
                    "average_price": float(liquidation.average_price),
                    "order_status": liquidation.order_status,
                }
            ),
        },
        "chainlink": {
            "price": _f(chainlink_price),
            "age_ms": snapshot.chainlink.age_ms,
            "source_ts_ns": snapshot.chainlink.source_timestamp_ns,
        },
        "dom": {
            "spot": _dom(snapshot.spot_dom),
            "futures": _dom(snapshot.futures_dom),
        },
        "trades": {
            "spot": _trades(snapshot.spot_recent_trades),
            "futures": _trades(snapshot.futures_recent_trades),
        },
        "klines": {
            "spot": _kline(snapshot.spot_kline),
            "futures": _kline(snapshot.futures_kline),
        },
        "ticker_24h": {
            "spot": _ticker_24h(snapshot.spot_ticker_24h),
            "futures": _ticker_24h(snapshot.futures_ticker_24h),
        },
        "polymarket": {
            Timeframe.FIVE_MINUTES.value: _timeframe(snapshot.market_5m, now_ns),
            Timeframe.FIFTEEN_MINUTES.value: _timeframe(snapshot.market_15m, now_ns),
        },
        "basis": {
            "futures_minus_spot": _f(futures_basis),
            "futures_minus_spot_bps": _bps(futures_basis, spot_mid),
            "chainlink_minus_spot": _f(chainlink_basis),
            "chainlink_minus_spot_bps": _bps(chainlink_basis, spot_mid),
        },
    }


def frame_json(snapshot: MarketDataSnapshot) -> str:
    return json.dumps(build_frame(snapshot), separators=(",", ":"), allow_nan=False)


__all__ = ["FRAME_SCHEMA_VERSION", "build_frame", "frame_json"]
