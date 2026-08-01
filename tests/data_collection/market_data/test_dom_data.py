from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.market_data.config import ConfigurationError, load_config
from polymarket_btc.data_collection.market_data.models import (
    BinanceDomSnapshotPayload,
    DomBucketPayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    event_from_dict,
    json_dumps,
)
from polymarket_btc.data_collection.market_data.state import StateStore
from polymarket_btc.data_collection.market_data.storage import (
    snapshot_from_parquet_row,
    snapshot_to_parquet_row,
)
from polymarket_btc.data_collection.market_data.sources.binance_futures_liquidations import (
    BinanceFuturesLiquidationSource,
    _matching_symbol,
    parse_liquidation_message,
)
from polymarket_btc.data_collection.market_data.sources.binance_futures_mark_price import (
    parse_mark_price_message,
)
from polymarket_btc.data_collection.market_data.sources.binance_futures_rest_poller import (
    parse_long_short_ratio,
    parse_open_interest,
)
from polymarket_btc.data_collection.market_data.sources.dom_bucketing import build_dom_snapshot
from polymarket_btc.data_collection.market_data.sources.full_depth_reconstruction import (
    FullDepthOrderBook,
    ResyncRequired,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


class FullDepthOrderBookTests(unittest.TestCase):
    def test_snapshot_then_buffered_replay_applies_in_order(self) -> None:
        book = FullDepthOrderBook(symbol="BTCUSDT")
        book.buffer_event({"U": 5, "u": 6, "b": [["100.00", "1"]], "a": [["101.00", "1"]]})
        book.buffer_event({"U": 7, "u": 7, "b": [["100.00", "2"]], "a": []})
        book.apply_snapshot({
            "lastUpdateId": 5,
            "bids": [["99.00", "3"], ["98.00", "1"]],
            "asks": [["102.00", "3"], ["103.00", "1"]],
        })
        for buffered in book.drain_buffer():
            book.apply_event(buffered)
        self.assertTrue(book.ready)
        self.assertEqual(book.bids[Decimal("100.00")], Decimal("2"))
        self.assertEqual(book.asks[Decimal("101.00")], Decimal("1"))
        self.assertEqual(book.last_update_id, 7)

    def test_zero_quantity_removes_level(self) -> None:
        book = FullDepthOrderBook(symbol="BTCUSDT")
        book.apply_snapshot({"lastUpdateId": 1, "bids": [], "asks": [["100.00", "1"]]})
        book.apply_event({"U": 2, "u": 2, "b": [], "a": [["100.00", "0"]]})
        self.assertNotIn(Decimal("100.00"), book.asks)

    def test_spot_sequence_gap_raises_resync_required(self) -> None:
        book = FullDepthOrderBook(symbol="BTCUSDT")
        book.apply_snapshot({"lastUpdateId": 1, "bids": [["1", "1"]], "asks": [["2", "1"]]})
        book.apply_event({"U": 2, "u": 2, "b": [], "a": []})
        with self.assertRaises(ResyncRequired):
            book.apply_event({"U": 10, "u": 10, "b": [], "a": []})

    def test_futures_pu_continuity_and_mismatch(self) -> None:
        book = FullDepthOrderBook(symbol="BTCUSDT")
        book.apply_snapshot({"lastUpdateId": 100, "bids": [["50000", "1"]], "asks": [["50010", "1"]]})
        book.apply_event({"U": 90, "u": 101, "pu": 99, "b": [], "a": []})
        self.assertTrue(book.ready)
        book.apply_event({"U": 102, "u": 102, "pu": 101, "b": [], "a": [["50010", "3"]]})
        self.assertEqual(book.asks[Decimal("50010")], Decimal("3"))
        with self.assertRaises(ResyncRequired):
            book.apply_event({"U": 105, "u": 105, "pu": 999, "b": [], "a": []})

    def test_apply_event_before_snapshot_requires_resync(self) -> None:
        book = FullDepthOrderBook(symbol="BTCUSDT")
        with self.assertRaises(ResyncRequired):
            book.apply_event({"U": 1, "u": 1, "b": [], "a": []})


class DomBucketingTests(unittest.TestCase):
    def test_buckets_sum_quantities_within_range(self) -> None:
        book = FullDepthOrderBook(symbol="BTCUSDT")
        book.bids = {Decimal("63680.00"): Decimal("1.45"), Decimal("63683.00"): Decimal("0.10")}
        book.asks = {Decimal("63690.00"): Decimal("3.55")}
        snapshot = build_dom_snapshot(book, "spot", bucket_size=Decimal("10"), price_range=Decimal("50"))
        self.assertIsNotNone(snapshot)
        bucket = next(b for b in snapshot.buckets if b.price == Decimal("63680"))
        self.assertEqual(bucket.bid_quantity, Decimal("1.55"))

    def test_returns_none_without_a_mid_price(self) -> None:
        book = FullDepthOrderBook(symbol="BTCUSDT")
        self.assertIsNone(
            build_dom_snapshot(book, "spot", bucket_size=Decimal("10"), price_range=Decimal("50"))
        )


class MarkPriceParserTests(unittest.TestCase):
    def test_valid_message_parses_signed_funding_rate(self) -> None:
        event = parse_mark_price_message(
            {
                "e": "markPriceUpdate", "s": "BTCUSDT", "p": "67000.5", "i": "67001.2",
                "r": "-0.00010000", "T": 1_700_000_000_000, "E": 1_699_999_999_000,
            },
            received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        self.assertEqual(event.payload.funding_rate, Decimal("-0.00010000"))
        self.assertEqual(event.payload.mark_price, Decimal("67000.5"))

    def test_wrong_event_type_is_rejected(self) -> None:
        with self.assertRaises(InvalidEventError):
            parse_mark_price_message(
                {"e": "wrong", "s": "BTCUSDT", "p": "1", "i": "1", "r": "0", "T": 1, "E": 1},
                received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
            )


class LiquidationParserTests(unittest.TestCase):
    def test_other_symbol_returns_none_not_an_error(self) -> None:
        result = parse_liquidation_message(
            {
                "e": "forceOrder",
                "o": {"s": "ETHUSDT", "S": "SELL", "p": "1", "q": "1", "ap": "1", "X": "FILLED", "T": 1},
            },
            received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        self.assertIsNone(result)

    def test_matching_symbol_produces_event(self) -> None:
        event = parse_liquidation_message(
            {
                "e": "forceOrder",
                "o": {
                    "s": "BTCUSDT", "S": "SELL", "p": "9910", "q": "0.014",
                    "ap": "9910", "X": "FILLED", "T": 1_568_014_460_893,
                },
            },
            received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        self.assertIsNotNone(event)
        self.assertEqual(event.payload.side, "SELL")

    def test_invalid_side_is_rejected(self) -> None:
        with self.assertRaises(InvalidEventError):
            parse_liquidation_message(
                {
                    "e": "forceOrder",
                    "o": {"s": "BTCUSDT", "S": "SIDEWAYS", "p": "1", "q": "1", "ap": "1", "X": "FILLED", "T": 1},
                },
                received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
            )


class MatchingSymbolPeekTests(unittest.TestCase):
    """_matching_symbol lets the multi-symbol run() loop decide whether a
    message is worth spending a sequence number on before it does -- see
    binance_futures_liquidations.py's run() comment for why that matters."""

    def test_returns_the_symbol_when_it_is_in_the_filter_set(self) -> None:
        message = {"e": "forceOrder", "o": {"s": "ETHUSDT"}}
        self.assertEqual(_matching_symbol(message, frozenset({"BTCUSDT", "ETHUSDT"})), "ETHUSDT")

    def test_returns_none_for_an_untracked_symbol(self) -> None:
        message = {"e": "forceOrder", "o": {"s": "SOLUSDT"}}
        self.assertIsNone(_matching_symbol(message, frozenset({"BTCUSDT", "ETHUSDT"})))

    def test_returns_none_for_malformed_messages_without_raising(self) -> None:
        self.assertIsNone(_matching_symbol("not a dict", frozenset({"BTCUSDT"})))
        self.assertIsNone(_matching_symbol({"o": "not a dict either"}, frozenset({"BTCUSDT"})))
        self.assertIsNone(_matching_symbol({}, frozenset({"BTCUSDT"})))


class LiquidationSourceMultiSymbolTests(unittest.TestCase):
    def test_default_construction_matches_only_btcusdt(self) -> None:
        source = BinanceFuturesLiquidationSource(
            load_config(REPOSITORY_ROOT / "config" / "market_data.toml"),
            lambda event: None, lambda: 0, None, None,
        )
        self.assertEqual(source._symbol_filters, frozenset({"BTCUSDT"}))
        self.assertTrue(source._is_default)

    def test_symbol_filters_override_matches_multiple_symbols(self) -> None:
        source = BinanceFuturesLiquidationSource(
            load_config(REPOSITORY_ROOT / "config" / "market_data.toml"),
            lambda event: None, lambda: 0, None, None,
            symbol_filters=frozenset({"BTCUSDT", "ETHUSDT"}),
        )
        self.assertEqual(source._symbol_filters, frozenset({"BTCUSDT", "ETHUSDT"}))
        self.assertFalse(source._is_default)


class RestPollerParserTests(unittest.TestCase):
    def test_open_interest_parses(self) -> None:
        event = parse_open_interest(
            {"symbol": "BTCUSDT", "openInterest": "10659.509", "time": 1}, ingest_sequence=1, now_ns=1
        )
        self.assertEqual(event.payload.open_interest, Decimal("10659.509"))

    def test_long_short_ratio_uses_latest_entry(self) -> None:
        event = parse_long_short_ratio(
            [{
                "symbol": "BTCUSDT", "longAccount": "0.58", "shortAccount": "0.42",
                "longShortRatio": "1.38", "timestamp": 1,
            }],
            ingest_sequence=1, now_ns=1,
        )
        self.assertIsNotNone(event)
        self.assertEqual(event.payload.long_short_ratio, Decimal("1.38"))

    def test_empty_long_short_response_returns_none(self) -> None:
        self.assertIsNone(parse_long_short_ratio([], ingest_sequence=1, now_ns=1))


class ConfigTests(unittest.TestCase):
    def test_repository_config_includes_futures_and_dom_sections(self) -> None:
        config = load_config(REPOSITORY_ROOT / "config" / "market_data.toml")
        self.assertEqual(config.binance_futures.symbol, "btcusdt")
        self.assertEqual(config.binance_futures.depth_stream, "btcusdt@depth@100ms")
        self.assertEqual(config.dom.bucket_size, 10)

    def test_bad_futures_depth_url_host_is_rejected(self) -> None:
        text = (REPOSITORY_ROOT / "config" / "market_data.toml").read_text()
        text = text.replace(
            "wss://fstream.binance.com/ws/btcusdt@depth@100ms",
            "wss://evil.example.com/ws/btcusdt@depth@100ms",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(text)
            with self.assertRaises(ConfigurationError):
                load_config(path)


class DomSnapshotStorageRoundTripTests(unittest.TestCase):
    def test_populated_dom_fields_round_trip_through_parquet(self) -> None:
        state = StateStore()
        event = MarketDataEvent(
            schema_version=2,
            ingest_sequence=1,
            event_id="test:dom:1",
            source=EventSource.BINANCE_SPOT_FULL_DEPTH,
            stream=EventStream.BINANCE_SPOT_DOM_SNAPSHOT,
            instrument="BTCUSDT",
            source_timestamp_ns=None,
            server_timestamp_ns=None,
            received_wall_timestamp_ns=1,
            received_monotonic_ns=1,
            source_sequence="1",
            timeframe=None,
            market_id=None,
            condition_id=None,
            asset_id=None,
            outcome=None,
            payload=BinanceDomSnapshotPayload(
                market="spot",
                mid_price=Decimal("63680"),
                bucket_size=Decimal("10"),
                buckets=(DomBucketPayload(Decimal("63680"), Decimal("1.5"), Decimal("2.0")),),
            ),
        )
        state.apply(event)
        snapshot = state.snapshot(1_750_000_000_000_000_000, 1)
        self.assertIsNotNone(snapshot.spot_dom)
        restored = snapshot_from_parquet_row(snapshot_to_parquet_row(snapshot))
        self.assertEqual(restored, snapshot)


class EventFromDictRoundTripTests(unittest.TestCase):
    """Every stream that RawEventStorage can write must survive
    event_from_dict(json.loads(json_dumps(event))) -- that call runs on
    every segment finalize (rotation or shutdown) as a self-check, and
    again on replay. A stream missing from event_from_dict's dispatch
    silently falls through to the legacy MARKET_WINDOW_STATE branch and
    raises there, turning every raw-storage finalize into a crash."""

    def test_dom_mark_price_liquidation_open_interest_and_long_short_round_trip(self) -> None:
        dom_event = MarketDataEvent(
            schema_version=2, ingest_sequence=1, event_id="dom:1",
            source=EventSource.BINANCE_SPOT_FULL_DEPTH, stream=EventStream.BINANCE_SPOT_DOM_SNAPSHOT,
            instrument="BTCUSDT", source_timestamp_ns=None, server_timestamp_ns=None,
            received_wall_timestamp_ns=1, received_monotonic_ns=1, source_sequence="1",
            timeframe=None, market_id=None, condition_id=None, asset_id=None, outcome=None,
            payload=BinanceDomSnapshotPayload(
                market="spot", mid_price=Decimal("63680"), bucket_size=Decimal("10"),
                buckets=(DomBucketPayload(Decimal("63680"), Decimal("1.5"), Decimal("2.0")),),
            ),
        )
        mark_price_event = parse_mark_price_message(
            {
                "e": "markPriceUpdate", "s": "BTCUSDT", "p": "67000.5", "i": "67001.2",
                "r": "-0.00010000", "T": 1_700_000_000_000, "E": 1_699_999_999_000,
            },
            received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        liquidation_event = parse_liquidation_message(
            {
                "e": "forceOrder",
                "o": {
                    "s": "BTCUSDT", "S": "SELL", "p": "9910", "q": "0.014",
                    "ap": "9910", "X": "FILLED", "T": 1_568_014_460_893,
                },
            },
            received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        open_interest_event = parse_open_interest(
            {"symbol": "BTCUSDT", "openInterest": "10659.509", "time": 1}, ingest_sequence=1, now_ns=1
        )
        long_short_event = parse_long_short_ratio(
            [{
                "symbol": "BTCUSDT", "longAccount": "0.58", "shortAccount": "0.42",
                "longShortRatio": "1.38", "timestamp": 1,
            }],
            ingest_sequence=1, now_ns=1,
        )
        for event in (
            dom_event, mark_price_event, liquidation_event, open_interest_event, long_short_event,
        ):
            restored = event_from_dict(json.loads(json_dumps(event)))
            self.assertEqual(restored, event)


if __name__ == "__main__":
    unittest.main()
