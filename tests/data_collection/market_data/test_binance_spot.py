from decimal import Decimal
import unittest

from polymarket_btc.data_collection.market_data.models import (
    InvalidEventError,
    TakerSide,
)
from polymarket_btc.data_collection.market_data.sources.binance_spot import (
    BinanceMessageParser,
    _streams_for,
    build_combined_url,
)

from .fixtures import NOW_NS, agg_trade, book_ticker, depth20


class BinanceParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = BinanceMessageParser("session-1", timestamp_unit="microsecond")

    def parse(self, message: dict[str, object]):
        return self.parser.parse(
            message,
            received_wall_timestamp_ns=NOW_NS,
            received_monotonic_ns=1,
            ingest_sequence=1,
            now_ns=NOW_NS,
        )

    def test_combined_url_contains_exact_streams_and_microseconds(self) -> None:
        self.assertEqual(
            build_combined_url("wss://stream.binance.com:9443/stream", True),
            "wss://stream.binance.com:9443/stream?streams="
            "btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth20@100ms"
            "/btcusdt@kline_1m/btcusdt@ticker"
            "&timeUnit=MICROSECOND",
        )

    def test_agg_trade_taker_side_mapping(self) -> None:
        self.assertIs(self.parse(agg_trade(maker=True)).payload.taker_side, TakerSide.SELL)
        other = BinanceMessageParser("session-2", timestamp_unit="microsecond")
        event = other.parse(
            agg_trade(maker=False),
            received_wall_timestamp_ns=NOW_NS,
            received_monotonic_ns=1,
            ingest_sequence=1,
            now_ns=NOW_NS,
        )
        self.assertIs(event.payload.taker_side, TakerSide.BUY)

    def test_agg_trade_rejects_non_increasing_id_and_invalid_values(self) -> None:
        self.parse(agg_trade())
        with self.assertRaises(InvalidEventError):
            self.parse(agg_trade())
        for field, value in (("p", "0"), ("q", "0"), ("s", "ETHUSDT")):
            parser = BinanceMessageParser("new", timestamp_unit="microsecond")
            message = agg_trade()
            message["data"][field] = value  # type: ignore[index]
            with self.subTest(field=field), self.assertRaises(InvalidEventError):
                parser.parse(
                    message,
                    received_wall_timestamp_ns=NOW_NS,
                    received_monotonic_ns=1,
                    ingest_sequence=1,
                    now_ns=NOW_NS,
                )

    def test_book_ticker_validates_spread_and_sequence(self) -> None:
        event = self.parse(book_ticker())
        self.assertEqual(event.payload.best_bid_price, Decimal("67234.40"))
        crossed = book_ticker()
        crossed["data"]["b"] = "67234.50"  # type: ignore[index]
        with self.assertRaises(InvalidEventError):
            BinanceMessageParser("new", timestamp_unit="microsecond").parse(
                crossed,
                received_wall_timestamp_ns=NOW_NS,
                received_monotonic_ns=1,
                ingest_sequence=1,
                now_ns=NOW_NS,
            )
        with self.assertRaises(InvalidEventError):
            self.parse(book_ticker())

    def test_depth20_is_full_sorted_snapshot_with_at_most_twenty_levels(self) -> None:
        event = self.parse(depth20())
        self.assertEqual(len(event.payload.bids), 20)
        self.assertEqual(event.source_timestamp_ns, None)
        too_many = depth20(21)
        with self.assertRaises(InvalidEventError):
            BinanceMessageParser("new", timestamp_unit="microsecond").parse(
                too_many,
                received_wall_timestamp_ns=NOW_NS,
                received_monotonic_ns=1,
                ingest_sequence=1,
                now_ns=NOW_NS,
            )
        unsorted = depth20(2)
        unsorted["data"]["bids"].reverse()  # type: ignore[index,union-attr]
        with self.assertRaises(InvalidEventError):
            BinanceMessageParser("new2", timestamp_unit="microsecond").parse(
                unsorted,
                received_wall_timestamp_ns=NOW_NS,
                received_monotonic_ns=1,
                ingest_sequence=1,
                now_ns=NOW_NS,
            )


class SymbolOverrideTests(unittest.TestCase):
    """Omitting `symbol` must reproduce today's exact BTC behavior; passing
    one must fully replace every "BTCUSDT" literal, not just some of them."""

    def test_default_symbol_is_still_btcusdt(self) -> None:
        parser = BinanceMessageParser("session-1", timestamp_unit="microsecond")
        self.assertEqual(parser.symbol, "BTCUSDT")

    def test_eth_override_parses_eth_flavored_message_end_to_end(self) -> None:
        parser = BinanceMessageParser("session-1", timestamp_unit="microsecond", symbol="ethusdt")
        self.assertEqual(parser.symbol, "ETHUSDT")
        message = agg_trade()
        message["stream"] = "ethusdt@aggTrade"
        message["data"]["s"] = "ETHUSDT"  # type: ignore[index]
        event = parser.parse(
            message,
            received_wall_timestamp_ns=NOW_NS,
            received_monotonic_ns=1,
            ingest_sequence=1,
            now_ns=NOW_NS,
        )
        self.assertEqual(event.instrument, "ETHUSDT")
        self.assertEqual(event.payload.symbol, "ETHUSDT")
        self.assertIn("ETHUSDT", event.event_id)

    def test_eth_override_rejects_a_btc_flavored_message(self) -> None:
        # A parser built for ETH must not silently accept BTC data that
        # happens to arrive on the wrong stream.
        parser = BinanceMessageParser("session-1", timestamp_unit="microsecond", symbol="ETHUSDT")
        with self.assertRaises(InvalidEventError):
            parser.parse(
                agg_trade(),
                received_wall_timestamp_ns=NOW_NS,
                received_monotonic_ns=1,
                ingest_sequence=1,
                now_ns=NOW_NS,
            )

    def test_streams_for_builds_lowercase_symbol_streams(self) -> None:
        self.assertEqual(
            _streams_for("ETHUSDT"),
            (
                "ethusdt@aggTrade", "ethusdt@bookTicker", "ethusdt@depth20@100ms",
                "ethusdt@kline_1m", "ethusdt@ticker",
            ),
        )

    def test_build_combined_url_accepts_custom_streams(self) -> None:
        url = build_combined_url(
            "wss://stream.binance.com:9443/stream", True, streams=_streams_for("ETHUSDT"),
        )
        self.assertIn("ethusdt@aggTrade", url)
        self.assertNotIn("btcusdt", url)


if __name__ == "__main__":
    unittest.main()
