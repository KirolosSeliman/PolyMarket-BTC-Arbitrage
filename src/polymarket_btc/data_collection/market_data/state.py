"""Single-writer live state reducer."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from typing import cast

from polymarket_btc.data_collection.market_discovery import (
    DiscoveryState,
    Timeframe,
    TimeframeSnapshot,
)

from .models import (
    BinanceAggTradePayload,
    BinanceBookTickerPayload,
    BinanceDepth20Payload,
    BinanceSnapshot,
    ChainlinkPricePayload,
    ChainlinkSnapshot,
    EventSource,
    EventStream,
    MarketDataEvent,
    MarketDataSnapshot,
    OrderBookSnapshot,
    Outcome,
    PolymarketBookPayload,
    PolymarketLastTradePayload,
    PolymarketPriceChangePayload,
    PolymarketSnapshot,
    PolymarketTickSizePayload,
    PolymarketTimeframeSnapshot,
    PriceLevel,
    RollingWindowSnapshot,
    SourceHealthSnapshot,
    TakerSide,
)


@dataclass(slots=True)
class _Health:
    connected: bool = False
    last_message_ns: int | None = None
    reconnect_count: int = 0
    invalid_count: int = 0
    duplicate_count: int = 0


@dataclass(slots=True)
class _Book:
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_trade_price: Decimal | None = None
    tick_size: Decimal | None = None
    initialized: bool = False
    coherent: bool = True
    resolved: bool = False


@dataclass(slots=True)
class _RollingAccumulator:
    seconds: int
    rows: deque[tuple[int, BinanceAggTradePayload]] = field(default_factory=deque)
    buy_quantity: Decimal = Decimal(0)
    sell_quantity: Decimal = Decimal(0)
    total_quantity: Decimal = Decimal(0)
    notional: Decimal = Decimal(0)

    def add(self, received_ns: int, trade: BinanceAggTradePayload) -> None:
        self.rows.append((received_ns, trade))
        if trade.taker_side is TakerSide.BUY:
            self.buy_quantity += trade.quantity
        else:
            self.sell_quantity += trade.quantity
        self.total_quantity += trade.quantity
        self.notional += trade.price * trade.quantity
        self.expire(received_ns)

    def expire(self, now_ns: int) -> None:
        cutoff = now_ns - self.seconds * 1_000_000_000
        while self.rows and self.rows[0][0] < cutoff:
            _received_ns, trade = self.rows.popleft()
            if trade.taker_side is TakerSide.BUY:
                self.buy_quantity -= trade.quantity
            else:
                self.sell_quantity -= trade.quantity
            self.total_quantity -= trade.quantity
            self.notional -= trade.price * trade.quantity

    def snapshot(self, now_ns: int) -> RollingWindowSnapshot:
        self.expire(now_ns)
        return RollingWindowSnapshot(
            self.seconds,
            self.buy_quantity,
            self.sell_quantity,
            self.notional / self.total_quantity if self.total_quantity else None,
        )


class StateStore:
    def __init__(
        self,
        *,
        chainlink_stale_after_ms: int = 10_000,
        binance_depth_stale_after_ms: int = 1_000,
        binance_trade_stale_after_ms: int = 5_000,
    ) -> None:
        self._chainlink_stale_after_ms = chainlink_stale_after_ms
        self._binance_depth_stale_after_ms = binance_depth_stale_after_ms
        self._binance_trade_stale_after_ms = binance_trade_stale_after_ms
        self._health = {source: _Health() for source in EventSource}
        self._chainlink: MarketDataEvent | None = None
        self._agg_trade: MarketDataEvent | None = None
        self._book_ticker: MarketDataEvent | None = None
        self._depth: MarketDataEvent | None = None
        self._rolling_windows = tuple(
            _RollingAccumulator(seconds) for seconds in (1, 5, 30, 60)
        )
        self._markets: dict[Timeframe, TimeframeSnapshot] = {}
        self._books: dict[tuple[Timeframe, Outcome], _Book] = {}

    def set_connected(self, source: EventSource, connected: bool) -> None:
        self._health[source].connected = connected

    def record_reconnect(self, source: EventSource) -> None:
        self._health[source].reconnect_count += 1

    def record_invalid(self, source: EventSource) -> None:
        self._health[source].invalid_count += 1

    def apply_market_snapshot(self, snapshot: TimeframeSnapshot) -> None:
        self._markets[snapshot.timeframe] = snapshot
        self._health[EventSource.MARKET_DISCOVERY].connected = True
        self._health[EventSource.MARKET_DISCOVERY].last_message_ns = int(
            snapshot.updated_at_utc.timestamp() * 1_000_000_000
        )

    def apply(self, event: MarketDataEvent) -> None:
        health = self._health[event.source]
        health.connected = True
        health.last_message_ns = event.received_wall_timestamp_ns
        if event.stream is EventStream.CHAINLINK_PRICE:
            self._chainlink = event
        elif event.stream is EventStream.BINANCE_AGG_TRADE:
            self._agg_trade = event
            payload = cast(BinanceAggTradePayload, event.payload)
            for window in self._rolling_windows:
                window.add(event.received_wall_timestamp_ns, payload)
        elif event.stream is EventStream.BINANCE_BOOK_TICKER:
            self._book_ticker = event
        elif event.stream is EventStream.BINANCE_DEPTH20:
            self._depth = event
        elif event.timeframe is not None and event.outcome is not None:
            book = self._books.setdefault((event.timeframe, event.outcome), _Book())
            if event.stream is EventStream.POLYMARKET_BOOK:
                payload = cast(PolymarketBookPayload, event.payload)
                book.bids = {level.price: level.quantity for level in payload.bids}
                book.asks = {level.price: level.quantity for level in payload.asks}
                book.initialized = True
                book.coherent = self._coherent(book)
            elif event.stream is EventStream.POLYMARKET_PRICE_CHANGE:
                payload = cast(PolymarketPriceChangePayload, event.payload)
                levels = book.bids if payload.side == "BUY" else book.asks
                if payload.quantity == 0:
                    levels.pop(payload.price, None)
                else:
                    levels[payload.price] = payload.quantity
                book.coherent = self._coherent(book)
            elif event.stream is EventStream.POLYMARKET_LAST_TRADE:
                book.last_trade_price = cast(PolymarketLastTradePayload, event.payload).price
            elif event.stream is EventStream.POLYMARKET_TICK_SIZE_CHANGE:
                book.tick_size = cast(PolymarketTickSizePayload, event.payload).new_tick_size
            elif event.stream is EventStream.POLYMARKET_MARKET_RESOLVED:
                book.resolved = True

    @staticmethod
    def _coherent(book: _Book) -> bool:
        return not book.bids or not book.asks or max(book.bids) < min(book.asks)

    @staticmethod
    def _age_ms(event: MarketDataEvent | None, now_ns: int) -> int | None:
        if event is None:
            return None
        return max(0, (now_ns - event.received_wall_timestamp_ns) // 1_000_000)

    def _rolling(self, now_ns: int) -> tuple[RollingWindowSnapshot, ...]:
        return tuple(window.snapshot(now_ns) for window in self._rolling_windows)

    def _binance_snapshot(self, now_ns: int) -> BinanceSnapshot:
        ticker = (
            cast(BinanceBookTickerPayload, self._book_ticker.payload)
            if self._book_ticker else None
        )
        depth = cast(BinanceDepth20Payload, self._depth.payload) if self._depth else None
        trade = cast(BinanceAggTradePayload, self._agg_trade.payload) if self._agg_trade else None
        bid = ticker.best_bid_price if ticker else None
        ask = ticker.best_ask_price if ticker else None
        bid_quantity = ticker.best_bid_quantity if ticker else None
        ask_quantity = ticker.best_ask_quantity if ticker else None
        spread = ask - bid if bid is not None and ask is not None else None
        mid = (ask + bid) / 2 if bid is not None and ask is not None else None
        spread_bps = spread / mid * 10_000 if spread is not None and mid else None
        denominator = (
            bid_quantity + ask_quantity
            if bid_quantity is not None and ask_quantity is not None else None
        )
        microprice = (
            (ask * bid_quantity + bid * ask_quantity) / denominator
            if ask is not None and bid is not None and denominator else None
        )
        top1_imbalance = (
            (bid_quantity - ask_quantity) / denominator
            if denominator else None
        )
        bids = depth.bids if depth else ()
        asks = depth.asks if depth else ()
        bid_notional = sum((level.price * level.quantity for level in bids), Decimal(0))
        ask_notional = sum((level.price * level.quantity for level in asks), Decimal(0))
        depth_total = bid_notional + ask_notional
        return BinanceSnapshot(
            trade.price if trade else None,
            trade.taker_side if trade else None,
            bid,
            ask,
            bid_quantity,
            ask_quantity,
            bids,
            asks,
            mid,
            spread,
            spread_bps,
            microprice,
            top1_imbalance,
            bid_notional if depth else None,
            ask_notional if depth else None,
            (bid_notional - ask_notional) / depth_total if depth_total else None,
            self._rolling(now_ns),
        )

    def _book_snapshot(self, timeframe: Timeframe, outcome: Outcome) -> OrderBookSnapshot | None:
        book = self._books.get((timeframe, outcome))
        if book is None:
            return None
        bids = tuple(
            PriceLevel(price, book.bids[price])
            for price in sorted(book.bids, reverse=True)[:20]
        )
        asks = tuple(
            PriceLevel(price, book.asks[price])
            for price in sorted(book.asks)[:20]
        )
        return OrderBookSnapshot(
            bids,
            asks,
            bids[0].price if bids else None,
            asks[0].price if asks else None,
            book.last_trade_price,
            book.tick_size,
            book.initialized,
            book.coherent,
        )

    def _timeframe_snapshot(
        self,
        timeframe: Timeframe,
        now_ns: int,
    ) -> PolymarketTimeframeSnapshot | None:
        discovery = self._markets.get(timeframe)
        if discovery is None or discovery.current_market is None:
            return None
        market = discovery.current_market
        up = self._book_snapshot(timeframe, Outcome.UP)
        down = self._book_snapshot(timeframe, Outcome.DOWN)
        reasons: list[str] = []
        if discovery.state is not DiscoveryState.ACTIVE:
            reasons.append(f"{timeframe.value}:market_not_active")
        for name, book in (("up", up), ("down", down)):
            if book is None or not book.initialized:
                reasons.append(f"{timeframe.value}:{name}_book_uninitialized")
            elif not book.coherent:
                reasons.append(f"{timeframe.value}:{name}_book_incoherent")
        resolved = any(
            self._books.get((timeframe, outcome), _Book()).resolved
            for outcome in (Outcome.UP, Outcome.DOWN)
        )
        if resolved:
            reasons.append(f"{timeframe.value}:market_resolved")
        end_ns = int(market.end_time_utc.timestamp() * 1_000_000_000)
        return PolymarketTimeframeSnapshot(
            timeframe,
            market.market_id,
            market.condition_id,
            int(market.start_time_utc.timestamp() * 1_000_000_000),
            end_ns,
            max(0, (end_ns - now_ns) // 1_000_000),
            up,
            down,
            resolved,
            not reasons,
            tuple(reasons),
        )

    def snapshot(self, now_ns: int, snapshot_sequence: int) -> MarketDataSnapshot:
        chainlink_payload = (
            cast(ChainlinkPricePayload, self._chainlink.payload)
            if self._chainlink else None
        )
        chainlink_age = self._age_ms(self._chainlink, now_ns)
        chainlink = ChainlinkSnapshot(
            chainlink_payload.price if chainlink_payload else None,
            self._chainlink.source_timestamp_ns if self._chainlink else None,
            self._chainlink.received_wall_timestamp_ns if self._chainlink else None,
            chainlink_age,
        )
        binance = self._binance_snapshot(now_ns)
        market_5m = self._timeframe_snapshot(Timeframe.FIVE_MINUTES, now_ns)
        market_15m = self._timeframe_snapshot(Timeframe.FIFTEEN_MINUTES, now_ns)
        reasons: list[str] = []
        if not self._health[EventSource.CHAINLINK_RTDS].connected:
            reasons.append("chainlink_disconnected")
        if (
            chainlink_age is None
            or chainlink_age > self._chainlink_stale_after_ms
        ):
            reasons.append("chainlink_stale")
        if not self._health[EventSource.BINANCE_SPOT].connected:
            reasons.append("binance_disconnected")
        depth_age = self._age_ms(self._depth, now_ns)
        trade_age = self._age_ms(self._agg_trade, now_ns)
        if (
            depth_age is None
            or depth_age > self._binance_depth_stale_after_ms
        ):
            reasons.append("binance_depth_stale")
        if (
            trade_age is None
            or trade_age > self._binance_trade_stale_after_ms
        ):
            reasons.append("binance_trade_stale")
        if self._book_ticker is None:
            reasons.append("binance_book_ticker_missing")
        if not self._health[EventSource.POLYMARKET_CLOB].connected:
            reasons.append("clob_disconnected")
        markets = [market for market in (market_5m, market_15m) if market is not None]
        if not markets:
            reasons.append("no_active_market")
        for market in markets:
            reasons.extend(market.not_ready_reasons)
        health_rows = []
        for source in EventSource:
            row = self._health[source]
            age = (
                None if row.last_message_ns is None
                else max(0, (now_ns - row.last_message_ns) // 1_000_000)
            )
            health_rows.append((source, SourceHealthSnapshot(
                connected=row.connected,
                stale=not row.connected,
                current_session_id=None,
                last_message_timestamp_ns=row.last_message_ns,
                age_ms=age,
                reconnect_count=row.reconnect_count,
                invalid_count=row.invalid_count,
                duplicate_count=row.duplicate_count,
                stale_session_count=0,
                divergence_count=0,
                protocol_error_count=0,
            )))
        return MarketDataSnapshot(
            1,
            snapshot_sequence,
            now_ns,
            market_5m,
            market_15m,
            chainlink,
            binance,
            PolymarketSnapshot(market_5m, market_15m),
            tuple(health_rows),
            not reasons,
            tuple(dict.fromkeys(reasons)),
        )
