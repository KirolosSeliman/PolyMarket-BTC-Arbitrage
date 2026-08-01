from datetime import UTC, datetime, timedelta
from decimal import Decimal
import unittest

from polymarket_btc.data_collection.market_discovery import (
    DiscoveryState,
    MarketWindow,
    Timeframe,
    TimeframeSnapshot,
)
from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    BinanceBookTickerPayload,
    BinanceDepth20Payload,
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    MarketWindowPayload,
    MarketWindowStatePayload,
    Outcome,
    PolymarketBookPayload,
    PriceLevel,
    PolymarketResolvedPayload,
    PolymarketBestBidAskPayload,
    PolymarketPriceChangePayload,
    TakerSide,
    SourceStatusPayload,
)
from polymarket_btc.data_collection.market_data.state import StateStore


NOW_NS = 1_750_000_000_000_000_000


def event(stream, payload, sequence, *, timeframe=None, asset=None, outcome=None):
    return MarketDataEvent(
        1, sequence, f"{stream.value}-{sequence}",
        EventSource.CHAINLINK_RTDS if stream is EventStream.CHAINLINK_PRICE
        else EventSource.BINANCE_SPOT if stream.name.startswith("BINANCE")
        else EventSource.POLYMARKET_CLOB,
        stream, asset or "BTCUSDT", NOW_NS, None, NOW_NS, sequence, str(sequence),
        timeframe, "market-1" if timeframe else None,
        "condition-1" if timeframe else None, asset, outcome, payload,
    )


class StateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = StateStore()
        for source in (
            EventSource.CHAINLINK_RTDS,
            EventSource.BINANCE_SPOT,
            EventSource.POLYMARKET_CLOB,
        ):
            self.store.set_connected(source, True)

    def test_binance_metrics_and_full_depth_replacement(self) -> None:
        self.store.apply(event(
            EventStream.BINANCE_BOOK_TICKER,
            BinanceBookTickerPayload(
                "BTCUSDT", 1, Decimal("99"), Decimal("2"),
                Decimal("101"), Decimal("3"),
            ),
            1,
        ))
        self.store.apply(event(
            EventStream.BINANCE_DEPTH20,
            BinanceDepth20Payload(
                "BTCUSDT", 2,
                (PriceLevel(Decimal("99"), Decimal("2")),),
                (PriceLevel(Decimal("101"), Decimal("3")),),
            ),
            2,
        ))
        self.store.apply(event(
            EventStream.BINANCE_AGG_TRADE,
            BinanceAggTradePayload(
                "BTCUSDT", 3, Decimal("100"), Decimal("4"), 3, 3,
                NOW_NS, TakerSide.BUY,
            ),
            3,
        ))
        snapshot = self.store.snapshot(NOW_NS + 1_000_000, 1)
        self.assertEqual(snapshot.binance.mid_price, Decimal("100"))
        self.assertEqual(snapshot.binance.spread, Decimal("2"))
        self.assertEqual(snapshot.binance.microprice, Decimal("99.8"))
        self.assertEqual(snapshot.binance.depth_bids[0].price, Decimal("99"))

    def test_readiness_requires_chainlink_binance_and_both_clob_books(self) -> None:
        now = datetime.fromtimestamp(NOW_NS / 1_000_000_000, UTC)
        market = MarketWindow(
            Timeframe.FIVE_MINUTES, "market-1", "condition-1", "slug",
            now - timedelta(seconds=10), now + timedelta(seconds=290),
            "up-token", "down-token", "chainlink",
        )
        self.store.apply_market_snapshot(TimeframeSnapshot(
            Timeframe.FIVE_MINUTES, DiscoveryState.ACTIVE, market, None,
            market.end_time_utc, now, 0, None,
        ))
        self.store.apply(event(
            EventStream.CHAINLINK_PRICE,
            ChainlinkPricePayload("btc/usd", Decimal("100")),
            1,
        ))
        self.store.apply(event(
            EventStream.BINANCE_BOOK_TICKER,
            BinanceBookTickerPayload(
                "BTCUSDT", 1, Decimal("99"), Decimal("1"),
                Decimal("101"), Decimal("1"),
            ),
            2,
        ))
        self.store.apply(event(
            EventStream.BINANCE_DEPTH20,
            BinanceDepth20Payload(
                "BTCUSDT", 2,
                (PriceLevel(Decimal("99"), Decimal("1")),),
                (PriceLevel(Decimal("101"), Decimal("1")),),
            ),
            3,
        ))
        self.store.apply(event(
            EventStream.BINANCE_AGG_TRADE,
            BinanceAggTradePayload(
                "BTCUSDT", 3, Decimal("100"), Decimal("1"), 3, 3,
                NOW_NS, TakerSide.BUY,
            ),
            4,
        ))
        for sequence, outcome, asset in (
            (5, Outcome.UP, "up-token"),
            (6, Outcome.DOWN, "down-token"),
        ):
            self.store.apply(event(
                EventStream.POLYMARKET_BOOK,
                PolymarketBookPayload(
                    (PriceLevel(Decimal(".49"), Decimal("10")),),
                    (PriceLevel(Decimal(".51"), Decimal("10")),),
                    f"hash-{sequence}",
                ),
                sequence,
                timeframe=Timeframe.FIVE_MINUTES,
                asset=asset,
                outcome=outcome,
            ))
        snapshot = self.store.snapshot(NOW_NS + 1_000_000, 1)
        self.assertTrue(snapshot.ready_for_strategy)
        self.assertEqual(snapshot.not_ready_reasons, ())
        self.store.set_connected(EventSource.BINANCE_SPOT, False)
        disconnected = self.store.snapshot(NOW_NS + 2_000_000, 2)
        self.assertFalse(disconnected.ready_for_strategy)
        self.assertIn("binance_disconnected", disconnected.not_ready_reasons)

    def _market_state_event(
        self,
        timeframe: Timeframe,
        *,
        current_suffix: str,
        next_suffix: str | None = None,
        state: str = "active",
        sequence: int = 100,
    ) -> MarketDataEvent:
        duration_ns = timeframe.duration_seconds * 1_000_000_000

        def window(suffix: str, start_ns: int) -> MarketWindowPayload:
            return MarketWindowPayload(
                timeframe,
                f"market-{suffix}",
                f"condition-{suffix}",
                f"slug-{suffix}",
                start_ns,
                start_ns + duration_ns,
                f"{suffix}-up",
                f"{suffix}-down",
                "chainlink",
            )

        return MarketDataEvent(
            2,
            sequence,
            f"market:{timeframe.value}:{sequence}",
            EventSource.MARKET_DISCOVERY,
            EventStream.MARKET_WINDOW_STATE,
            f"BTC-{timeframe.value}",
            NOW_NS,
            None,
            NOW_NS,
            sequence,
            str(sequence),
            timeframe,
            None,
            None,
            None,
            None,
            MarketWindowStatePayload(
                state,
                timeframe,
                window(current_suffix, NOW_NS),
                None if next_suffix is None else window(next_suffix, NOW_NS + duration_ns),
                NOW_NS + duration_ns,
                NOW_NS,
                0,
                None,
            ),
        )

    def _clob_book_event(
        self,
        timeframe: Timeframe,
        asset: str,
        outcome: Outcome,
        price: str,
        sequence: int,
        session_id: str | None = None,
    ) -> MarketDataEvent:
        suffix = asset.removesuffix("-up").removesuffix("-down")
        return MarketDataEvent(
            2,
            sequence,
            f"book:{asset}:{sequence}",
            EventSource.POLYMARKET_CLOB,
            EventStream.POLYMARKET_BOOK,
            asset,
            NOW_NS,
            None,
            NOW_NS,
            sequence,
            str(sequence),
            timeframe,
            f"market-{suffix}",
            f"condition-{suffix}",
            asset,
            outcome,
            PolymarketBookPayload(
                (PriceLevel(Decimal(price), Decimal("10")),),
                (PriceLevel(Decimal(price) + Decimal(".02"), Decimal("10")),),
                f"hash-{sequence}",
            ),
            session_id,
        )

    def _clob_status(
        self,
        connected: bool,
        session_id: str,
        sequence: int,
    ) -> MarketDataEvent:
        return MarketDataEvent(
            2,
            sequence,
            f"clob-status:{session_id}:{connected}",
            EventSource.POLYMARKET_CLOB,
            EventStream.SOURCE_STATUS,
            "POLYMARKET_CLOB",
            NOW_NS,
            None,
            NOW_NS,
            sequence,
            str(sequence),
            None,
            None,
            None,
            None,
            None,
            SourceStatusPayload(
                EventSource.POLYMARKET_CLOB,
                connected,
                session_id,
                1,
                None if connected else "socket closed",
            ),
            session_id,
        )

    def test_next_ready_books_do_not_replace_current_books(self) -> None:
        for timeframe in (Timeframe.FIVE_MINUTES, Timeframe.FIFTEEN_MINUTES):
            with self.subTest(timeframe=timeframe):
                store = StateStore()
                store.apply(self._market_state_event(
                    timeframe,
                    current_suffix=f"{timeframe.value}-a",
                    next_suffix=f"{timeframe.value}-b",
                    state="next_ready",
                ))
                for sequence, suffix, price in (
                    (1, f"{timeframe.value}-a", ".40"),
                    (2, f"{timeframe.value}-b", ".70"),
                ):
                    store.apply(self._clob_book_event(
                        timeframe, f"{suffix}-up", Outcome.UP, price, sequence
                    ))
                    store.apply(self._clob_book_event(
                        timeframe, f"{suffix}-down", Outcome.DOWN, price, sequence + 10
                    ))

                before = store.snapshot(NOW_NS, 1)
                market = before.market_5m if timeframe is Timeframe.FIVE_MINUTES else before.market_15m
                self.assertEqual(market.market_id, f"market-{timeframe.value}-a")
                self.assertEqual(market.up.asset_id, f"{timeframe.value}-a-up")
                self.assertEqual(market.up.best_bid, Decimal(".40"))

                store.apply(self._market_state_event(
                    timeframe,
                    current_suffix=f"{timeframe.value}-b",
                    state="active",
                    sequence=101,
                ))
                after = store.snapshot(NOW_NS, 2)
                market = after.market_5m if timeframe is Timeframe.FIVE_MINUTES else after.market_15m
                self.assertEqual(market.market_id, f"market-{timeframe.value}-b")
                self.assertEqual(market.up.asset_id, f"{timeframe.value}-b-up")
                self.assertEqual(market.up.best_bid, Decimal(".70"))

    def test_resolved_and_retired_assets_are_isolated_by_asset(self) -> None:
        timeframe = Timeframe.FIVE_MINUTES
        store = StateStore()
        store.apply(self._market_state_event(
            timeframe,
            current_suffix="a",
            next_suffix="b",
            state="next_ready",
        ))
        for sequence, suffix in ((1, "a"), (2, "b")):
            for outcome in (Outcome.UP, Outcome.DOWN):
                asset = f"{suffix}-{outcome.value}"
                store.apply(self._clob_book_event(timeframe, asset, outcome, ".40", sequence))
        store.apply(MarketDataEvent(
            2, 20, "resolved:a-up", EventSource.POLYMARKET_CLOB,
            EventStream.POLYMARKET_MARKET_RESOLVED, "a-up", NOW_NS, None,
            NOW_NS, 20, "20", timeframe, "market-a", "condition-a",
            "a-up", Outcome.UP,
            PolymarketResolvedPayload(
                "a-up", Outcome.UP, ("a-up", "a-down"), "market-a", "condition-a"
            ),
        ))
        self.assertTrue(store.snapshot(NOW_NS, 1).market_5m.resolved)

        store.apply(self._market_state_event(
            timeframe,
            current_suffix="b",
            state="active",
            sequence=101,
        ))
        current = store.snapshot(NOW_NS, 2).market_5m
        self.assertFalse(current.resolved)
        self.assertNotIn("a-up", store._books_by_asset)
        self.assertNotIn("a-down", store._books_by_asset)

    def test_reconnect_rejects_old_session_and_requires_new_full_books(self) -> None:
        timeframe = Timeframe.FIVE_MINUTES
        store = StateStore()
        store.apply(self._market_state_event(timeframe, current_suffix="a"))
        store.apply(self._clob_status(True, "session-1", 1))
        for outcome in (Outcome.UP, Outcome.DOWN):
            store.apply(self._clob_book_event(
                timeframe,
                f"a-{outcome.value}",
                outcome,
                ".40",
                2,
                "session-1",
            ))
        self.assertTrue(store.snapshot(NOW_NS, 1).market_5m.ready)

        store.apply(self._clob_status(False, "session-1", 4))
        disconnected = store.snapshot(NOW_NS, 2)
        self.assertFalse(disconnected.market_5m.ready)
        self.assertFalse(disconnected.market_5m.up.initialized)
        self.assertEqual(disconnected.market_5m.up.bids, ())

        store.apply(self._clob_status(True, "session-2", 5))
        store.apply(self._clob_book_event(
            timeframe, "a-up", Outcome.UP, ".80", 6, "session-1"
        ))
        stale = store.snapshot(NOW_NS, 3)
        self.assertFalse(stale.market_5m.up.initialized)

        delta = MarketDataEvent(
            2, 7, "delta:new", EventSource.POLYMARKET_CLOB,
            EventStream.POLYMARKET_PRICE_CHANGE, "a-up", NOW_NS, None,
            NOW_NS, 7, "7", timeframe, "market-a", "condition-a",
            "a-up", Outcome.UP,
            PolymarketPriceChangePayload(
                "BUY", Decimal(".80"), Decimal("1"), Decimal(".80"), Decimal(".82"), None
            ),
            "session-2",
        )
        store.apply(delta)
        self.assertFalse(store.snapshot(NOW_NS, 4).market_5m.up.initialized)

        store.apply(self._clob_book_event(
            timeframe, "a-up", Outcome.UP, ".50", 8, "session-2"
        ))
        self.assertFalse(store.snapshot(NOW_NS, 5).market_5m.ready)
        store.apply(self._clob_book_event(
            timeframe, "a-down", Outcome.DOWN, ".50", 9, "session-2"
        ))
        restored = store.snapshot(NOW_NS, 6)
        self.assertTrue(restored.market_5m.ready)
        self.assertEqual(restored.market_5m.up.source_session_id, "session-2")

    def test_third_divergence_invalidates_until_new_full_book(self) -> None:
        timeframe = Timeframe.FIVE_MINUTES
        store = StateStore()
        store.apply(self._market_state_event(timeframe, current_suffix="a"))
        store.apply(self._clob_status(True, "session-1", 1))
        store.apply(self._clob_book_event(
            timeframe, "a-up", Outcome.UP, ".40", 2, "session-1"
        ))

        def best(sequence: int, bid: str, ask: str) -> MarketDataEvent:
            return MarketDataEvent(
                2, sequence, f"best:{sequence}", EventSource.POLYMARKET_CLOB,
                EventStream.POLYMARKET_BEST_BID_ASK, "a-up", NOW_NS, None,
                NOW_NS, sequence, str(sequence), timeframe, "market-a",
                "condition-a", "a-up", Outcome.UP,
                PolymarketBestBidAskPayload(
                    Decimal(bid), Decimal(ask), Decimal(ask) - Decimal(bid)
                ),
                "session-1",
            )

        for sequence in (3, 4):
            store.apply(best(sequence, ".10", ".90"))
            self.assertTrue(store.snapshot(NOW_NS, sequence).market_5m.up.coherent)
        store.apply(best(5, ".10", ".90"))
        self.assertFalse(store.snapshot(NOW_NS, 5).market_5m.up.coherent)
        store.apply(best(6, ".40", ".42"))
        self.assertFalse(store.snapshot(NOW_NS, 6).market_5m.up.coherent)
        store.apply(self._clob_book_event(
            timeframe, "a-up", Outcome.UP, ".45", 7, "session-1"
        ))
        self.assertTrue(store.snapshot(NOW_NS, 7).market_5m.up.coherent)

    def test_market_window_state_marks_market_discovery_connected(self) -> None:
        # Market Discovery has no socket to emit SOURCE_STATUS on -- a
        # fresh MARKET_WINDOW_STATE event is its only "I'm alive" signal.
        store = StateStore()
        before = store.health_registry.source_snapshot(EventSource.MARKET_DISCOVERY, NOW_NS)
        self.assertFalse(before.connected)
        store.apply(self._market_state_event(Timeframe.FIVE_MINUTES, current_suffix="a"))
        after = store.health_registry.source_snapshot(EventSource.MARKET_DISCOVERY, NOW_NS)
        self.assertTrue(after.connected)


if __name__ == "__main__":
    unittest.main()
