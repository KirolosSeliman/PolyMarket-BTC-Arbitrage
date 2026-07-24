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
    MarketWindowPayload,
    MarketWindowStatePayload,
    OrderBookSnapshot,
    Outcome,
    PolymarketBookPayload,
    PolymarketBestBidAskPayload,
    PolymarketLastTradePayload,
    PolymarketPriceChangePayload,
    PolymarketResolvedPayload,
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
    asset_id: str
    market_id: str
    condition_id: str
    timeframe: Timeframe
    outcome: Outcome
    source_session_id: str | None
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_trade_price: Decimal | None = None
    tick_size: Decimal | None = None
    initialized: bool = False
    coherent: bool = True
    resolved: bool = False
    divergence_count: int = 0
    last_event_timestamp_ns: int | None = None


@dataclass(slots=True)
class _TimeframeContext:
    state: DiscoveryState
    current_market_id: str | None
    current_condition_id: str | None
    current_up_asset_id: str | None
    current_down_asset_id: str | None
    current_start_ns: int | None
    current_end_ns: int | None
    next_market_id: str | None
    next_condition_id: str | None
    next_up_asset_id: str | None
    next_down_asset_id: str | None
    next_start_ns: int | None
    next_end_ns: int | None
    expected_transition_ns: int | None
    last_error: str | None


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
        self._contexts: dict[Timeframe, _TimeframeContext] = {}
        self._books_by_asset: dict[str, _Book] = {}

    def set_connected(self, source: EventSource, connected: bool) -> None:
        self._health[source].connected = connected

    def record_reconnect(self, source: EventSource) -> None:
        self._health[source].reconnect_count += 1

    def record_invalid(self, source: EventSource) -> None:
        self._health[source].invalid_count += 1

    def apply_market_snapshot(self, snapshot: TimeframeSnapshot) -> None:
        def window(market: object) -> MarketWindowPayload | None:
            if market is None:
                return None
            return MarketWindowPayload(
                snapshot.timeframe,
                market.market_id,  # type: ignore[union-attr]
                market.condition_id,  # type: ignore[union-attr]
                market.slug,  # type: ignore[union-attr]
                int(market.start_time_utc.timestamp() * 1_000_000_000),  # type: ignore[union-attr]
                int(market.end_time_utc.timestamp() * 1_000_000_000),  # type: ignore[union-attr]
                market.up_token_id,  # type: ignore[union-attr]
                market.down_token_id,  # type: ignore[union-attr]
                market.resolution_source,  # type: ignore[union-attr]
            )
        updated_ns = int(snapshot.updated_at_utc.timestamp() * 1_000_000_000)
        self._apply_market_state(MarketWindowStatePayload(
            snapshot.state.value,
            snapshot.timeframe,
            window(snapshot.current_market),
            window(snapshot.next_market),
            (
                None
                if snapshot.expected_transition_utc is None
                else int(snapshot.expected_transition_utc.timestamp() * 1_000_000_000)
            ),
            updated_ns,
            snapshot.attempt_count,
            snapshot.last_error,
        ))
        self._health[EventSource.MARKET_DISCOVERY].connected = True
        self._health[EventSource.MARKET_DISCOVERY].last_message_ns = updated_ns

    @staticmethod
    def _context_window(
        market: MarketWindowPayload | None,
    ) -> tuple[str | None, str | None, str | None, str | None, int | None, int | None]:
        if market is None:
            return (None, None, None, None, None, None)
        return (
            market.market_id,
            market.condition_id,
            market.up_token_id,
            market.down_token_id,
            market.start_timestamp_ns,
            market.end_timestamp_ns,
        )

    def _apply_market_state(self, payload: MarketWindowStatePayload) -> None:
        current = self._context_window(payload.current_market)
        next_market = self._context_window(payload.next_market)
        self._contexts[payload.timeframe] = _TimeframeContext(
            DiscoveryState(payload.state),
            *current,
            *next_market,
            payload.expected_transition_timestamp_ns,
            payload.last_error,
        )
        referenced = {
            asset_id
            for context in self._contexts.values()
            for asset_id in (
                context.current_up_asset_id,
                context.current_down_asset_id,
                context.next_up_asset_id,
                context.next_down_asset_id,
            )
            if asset_id is not None
        }
        for asset_id in tuple(self._books_by_asset):
            if asset_id not in referenced:
                del self._books_by_asset[asset_id]

    def _asset_is_referenced(self, asset_id: str) -> bool:
        return any(
            asset_id in {
                context.current_up_asset_id,
                context.current_down_asset_id,
                context.next_up_asset_id,
                context.next_down_asset_id,
            }
            for context in self._contexts.values()
        )

    def apply(self, event: MarketDataEvent) -> None:
        health = self._health[event.source]
        health.connected = True
        health.last_message_ns = event.received_wall_timestamp_ns
        if event.stream is EventStream.MARKET_WINDOW_STATE:
            self._apply_market_state(cast(MarketWindowStatePayload, event.payload))
        elif event.stream is EventStream.CHAINLINK_PRICE:
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
        elif (
            event.source is EventSource.POLYMARKET_CLOB
            and event.asset_id is not None
            and event.timeframe is not None
            and event.outcome is not None
            and event.market_id is not None
            and event.condition_id is not None
            and self._asset_is_referenced(event.asset_id)
        ):
            book = self._books_by_asset.get(event.asset_id)
            if book is None:
                book = self._books_by_asset[event.asset_id] = _Book(
                    event.asset_id,
                    event.market_id,
                    event.condition_id,
                    event.timeframe,
                    event.outcome,
                    event.source_session_id,
                )
            if (
                book.market_id != event.market_id
                or book.condition_id != event.condition_id
                or book.timeframe is not event.timeframe
                or book.outcome is not event.outcome
            ):
                return
            book.last_event_timestamp_ns = event.source_timestamp_ns
            if event.stream is EventStream.POLYMARKET_BOOK:
                payload = cast(PolymarketBookPayload, event.payload)
                book.bids = {level.price: level.quantity for level in payload.bids}
                book.asks = {level.price: level.quantity for level in payload.asks}
                book.initialized = True
                book.coherent = self._coherent(book)
                book.divergence_count = 0
                book.source_session_id = event.source_session_id
            elif event.stream is EventStream.POLYMARKET_PRICE_CHANGE:
                payload = cast(PolymarketPriceChangePayload, event.payload)
                levels = book.bids if payload.side == "BUY" else book.asks
                if payload.quantity == 0:
                    levels.pop(payload.price, None)
                else:
                    levels[payload.price] = payload.quantity
                book.coherent = self._coherent(book)
            elif event.stream is EventStream.POLYMARKET_BEST_BID_ASK:
                payload = cast(PolymarketBestBidAskPayload, event.payload)
                best_bid = max(book.bids, default=None)
                best_ask = min(book.asks, default=None)
                if best_bid != payload.best_bid or best_ask != payload.best_ask:
                    book.divergence_count += 1
            elif event.stream is EventStream.POLYMARKET_LAST_TRADE:
                book.last_trade_price = cast(PolymarketLastTradePayload, event.payload).price
            elif event.stream is EventStream.POLYMARKET_TICK_SIZE_CHANGE:
                book.tick_size = cast(PolymarketTickSizePayload, event.payload).new_tick_size
            elif event.stream is EventStream.POLYMARKET_MARKET_RESOLVED:
                payload = cast(PolymarketResolvedPayload, event.payload)
                if event.asset_id in payload.affected_asset_ids or not payload.affected_asset_ids:
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

    def _book_snapshot(self, asset_id: str | None) -> OrderBookSnapshot | None:
        if asset_id is None:
            return None
        book = self._books_by_asset.get(asset_id)
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
            asset_id=book.asset_id,
            market_id=book.market_id,
            condition_id=book.condition_id,
            outcome=book.outcome,
            source_session_id=book.source_session_id,
            bids=bids,
            asks=asks,
            best_bid=bids[0].price if bids else None,
            best_ask=asks[0].price if asks else None,
            last_trade_price=book.last_trade_price,
            tick_size=book.tick_size,
            initialized=book.initialized,
            coherent=book.coherent,
            resolved=book.resolved,
            last_event_timestamp_ns=book.last_event_timestamp_ns,
        )

    def _timeframe_snapshot(
        self,
        timeframe: Timeframe,
        now_ns: int,
    ) -> PolymarketTimeframeSnapshot | None:
        context = self._contexts.get(timeframe)
        if context is None or context.current_market_id is None:
            return None
        up = self._book_snapshot(context.current_up_asset_id)
        down = self._book_snapshot(context.current_down_asset_id)
        reasons: list[str] = []
        if context.state is not DiscoveryState.ACTIVE:
            reasons.append(f"{timeframe.value}:market_not_active")
        for name, book in (("up", up), ("down", down)):
            if book is None or not book.initialized:
                reasons.append(f"{timeframe.value}:{name}_book_uninitialized")
            elif not book.coherent:
                reasons.append(f"{timeframe.value}:{name}_book_incoherent")
        resolved = any(book is not None and book.resolved for book in (up, down))
        if resolved:
            reasons.append(f"{timeframe.value}:market_resolved")
        end_ns = context.current_end_ns
        if end_ns is None:
            return None
        return PolymarketTimeframeSnapshot(
            timeframe,
            context.current_market_id,
            context.current_condition_id,
            context.current_start_ns,
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
