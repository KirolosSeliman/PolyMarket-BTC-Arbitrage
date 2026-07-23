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
    Outcome,
    PolymarketBookPayload,
    PriceLevel,
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


if __name__ == "__main__":
    unittest.main()
