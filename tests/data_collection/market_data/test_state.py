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
    TakerSide,
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


if __name__ == "__main__":
    unittest.main()
