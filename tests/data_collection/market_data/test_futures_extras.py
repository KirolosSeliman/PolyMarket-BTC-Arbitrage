from decimal import Decimal
import json
import unittest

from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    BinanceKlinePayload,
    BinanceTicker24hPayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    TakerSide,
    event_from_dict,
    json_dumps,
)
from polymarket_btc.data_collection.market_data.state import StateStore
from polymarket_btc.data_collection.market_data.storage import (
    snapshot_from_parquet_row,
    snapshot_to_parquet_row,
)
from polymarket_btc.data_collection.market_data.sources.binance_futures_trade import (
    FuturesTradeParser,
)
from polymarket_btc.data_collection.market_data.sources.binance_kline_ticker import (
    parse_futures_kline_message,
    parse_futures_ticker_message,
    parse_kline_data,
    parse_ticker_24h_data,
)
from polymarket_btc.data_collection.market_data.sources.binance_spot import (
    STREAMS,
    BinanceMessageParser,
)

NOW_NS = 1_750_000_000_000_000_000


def kline_message(*, closed: bool = False, symbol: str = "BTCUSDT", interval: str = "1m") -> dict:
    return {
        "e": "kline",
        "E": 1_700_000_000_123,
        "s": symbol,
        "k": {
            "t": 1_700_000_000_000, "T": 1_700_000_059_999, "s": symbol, "i": interval,
            "o": "67000.00", "h": "67050.00", "l": "66950.00", "c": "67020.00",
            "v": "12.345", "n": 88, "x": closed, "q": "827000.5",
        },
    }


def ticker_message(symbol: str = "BTCUSDT") -> dict:
    return {
        "e": "24hrTicker", "E": 1_700_000_000_123, "s": symbol,
        "p": "-942.20", "P": "-1.474", "w": "64277.98", "c": "64878.81",
        "o": "65821.01", "h": "65990.00", "l": "63936.62",
        "v": "18234.5", "q": "1173456789.1",
    }


def futures_trade_message(*, aggregate_id: int = 100, maker: bool = True) -> dict:
    return {
        "e": "aggTrade", "E": 1_700_000_000_123, "s": "BTCUSDT",
        "a": aggregate_id, "p": "64900.10", "q": "0.250",
        "f": aggregate_id, "l": aggregate_id, "T": 1_700_000_000_100, "m": maker,
    }


class KlineTickerParserTests(unittest.TestCase):
    def test_valid_kline_parses_ohlc_and_close_flag(self) -> None:
        payload = parse_kline_data(
            kline_message(closed=True), market="futures", symbol="BTCUSDT", interval="1m"
        )
        self.assertEqual(payload.open, Decimal("67000.00"))
        self.assertEqual(payload.close, Decimal("67020.00"))
        self.assertTrue(payload.is_closed)
        self.assertEqual(payload.open_time_ns, 1_700_000_000_000 * 1_000_000)

    def test_kline_microsecond_multiplier_used_for_spot_combined_stream(self) -> None:
        payload = parse_kline_data(
            kline_message(), market="spot", symbol="BTCUSDT", interval="1m",
            timestamp_multiplier=1_000,
        )
        self.assertEqual(payload.open_time_ns, 1_700_000_000_000 * 1_000)

    def test_kline_wrong_interval_is_rejected(self) -> None:
        with self.assertRaises(InvalidEventError):
            parse_kline_data(kline_message(interval="5m"), market="futures", symbol="BTCUSDT", interval="1m")

    def test_kline_close_outside_high_low_is_rejected(self) -> None:
        message = kline_message()
        message["k"]["c"] = "70000.00"
        with self.assertRaises(InvalidEventError):
            parse_kline_data(message, market="futures", symbol="BTCUSDT", interval="1m")

    def test_valid_ticker_parses_signed_change(self) -> None:
        payload = parse_ticker_24h_data(ticker_message(), market="spot", symbol="BTCUSDT")
        self.assertEqual(payload.price_change, Decimal("-942.20"))
        self.assertEqual(payload.price_change_percent, Decimal("-1.474"))
        self.assertEqual(payload.last_price, Decimal("64878.81"))

    def test_ticker_wrong_symbol_is_rejected(self) -> None:
        with self.assertRaises(InvalidEventError):
            parse_ticker_24h_data(ticker_message(symbol="ETHUSDT"), market="spot", symbol="BTCUSDT")

    def test_ticker_low_above_high_is_rejected(self) -> None:
        message = ticker_message()
        message["l"] = "70000.00"
        with self.assertRaises(InvalidEventError):
            parse_ticker_24h_data(message, market="spot", symbol="BTCUSDT")

    def test_futures_wrapper_functions_tag_market_and_source(self) -> None:
        kline_event = parse_futures_kline_message(
            kline_message(), received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        self.assertEqual(kline_event.source, EventSource.BINANCE_FUTURES_KLINE)
        self.assertEqual(kline_event.payload.market, "futures")
        ticker_event = parse_futures_ticker_message(
            ticker_message(), received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        self.assertEqual(ticker_event.source, EventSource.BINANCE_FUTURES_TICKER)
        self.assertEqual(ticker_event.payload.market, "futures")


class SpotCombinedStreamKlineTickerTests(unittest.TestCase):
    def test_combined_parser_dispatches_kline_and_ticker(self) -> None:
        parser = BinanceMessageParser("session-1", timestamp_unit="microsecond")
        kline_event = parser.parse(
            {"stream": STREAMS[3], "data": kline_message()},
            received_wall_timestamp_ns=NOW_NS, received_monotonic_ns=1, ingest_sequence=1, now_ns=NOW_NS,
        )
        self.assertEqual(kline_event.stream, EventStream.BINANCE_KLINE)
        self.assertEqual(kline_event.payload.market, "spot")
        ticker_event = parser.parse(
            {"stream": STREAMS[4], "data": ticker_message()},
            received_wall_timestamp_ns=NOW_NS, received_monotonic_ns=1, ingest_sequence=2, now_ns=NOW_NS,
        )
        self.assertEqual(ticker_event.stream, EventStream.BINANCE_TICKER_24H)
        self.assertEqual(ticker_event.payload.market, "spot")


class FuturesTradeParserTests(unittest.TestCase):
    def test_valid_trade_maps_maker_flag_to_taker_side(self) -> None:
        parser = FuturesTradeParser()
        event = parser.parse(
            futures_trade_message(maker=True),
            received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        self.assertEqual(event.source, EventSource.BINANCE_FUTURES_TRADE)
        self.assertEqual(event.stream, EventStream.BINANCE_FUTURES_AGG_TRADE)
        self.assertIs(event.payload.taker_side, TakerSide.SELL)

    def test_non_increasing_aggregate_id_is_rejected(self) -> None:
        parser = FuturesTradeParser()
        parser.parse(
            futures_trade_message(aggregate_id=100),
            received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
        )
        with self.assertRaises(InvalidEventError):
            parser.parse(
                futures_trade_message(aggregate_id=100),
                received_wall_timestamp_ns=2, received_monotonic_ns=2, ingest_sequence=2,
            )

    def test_wrong_event_type_is_rejected(self) -> None:
        parser = FuturesTradeParser()
        with self.assertRaises(InvalidEventError):
            parser.parse(
                {"e": "wrong", "s": "BTCUSDT"},
                received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
            )


def _futures_trade_event(aggregate_id: int, sequence: int) -> MarketDataEvent:
    return FuturesTradeParser().parse(
        futures_trade_message(aggregate_id=aggregate_id),
        received_wall_timestamp_ns=sequence, received_monotonic_ns=sequence, ingest_sequence=sequence,
    )


class StateStoreExtrasTests(unittest.TestCase):
    def test_futures_trade_updates_last_trade_and_recent_tape(self) -> None:
        store = StateStore()
        for i in range(3):
            store.apply(_futures_trade_event(100 + i, i + 1))
        snapshot = store.snapshot(NOW_NS, 1)
        self.assertEqual(snapshot.futures_last_trade.aggregate_trade_id, 102)
        # newest first
        self.assertEqual([t.aggregate_trade_id for t in snapshot.futures_recent_trades], [102, 101, 100])

    def test_recent_trades_tape_is_bounded(self) -> None:
        store = StateStore()
        for i in range(50):
            store.apply(_futures_trade_event(i + 1, i + 1))
        snapshot = store.snapshot(NOW_NS, 1)
        self.assertEqual(len(snapshot.futures_recent_trades), 40)
        self.assertEqual(snapshot.futures_recent_trades[0].aggregate_trade_id, 50)

    def test_kline_and_ticker_route_by_market_field(self) -> None:
        store = StateStore()
        spot_parser = BinanceMessageParser("session-1", timestamp_unit="microsecond")
        spot_kline = spot_parser.parse(
            {"stream": STREAMS[3], "data": kline_message()},
            received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1, now_ns=NOW_NS,
        )
        futures_kline = parse_futures_kline_message(
            kline_message(closed=True), received_wall_timestamp_ns=2, received_monotonic_ns=2, ingest_sequence=2,
        )
        store.apply(spot_kline)
        store.apply(futures_kline)
        snapshot = store.snapshot(NOW_NS, 1)
        self.assertFalse(snapshot.spot_kline.is_closed)
        self.assertTrue(snapshot.futures_kline.is_closed)

        spot_ticker = spot_parser.parse(
            {"stream": STREAMS[4], "data": ticker_message()},
            received_wall_timestamp_ns=3, received_monotonic_ns=3, ingest_sequence=3, now_ns=NOW_NS,
        )
        store.apply(spot_ticker)
        snapshot = store.snapshot(NOW_NS, 1)
        self.assertEqual(snapshot.spot_ticker_24h.last_price, Decimal("64878.81"))
        self.assertIsNone(snapshot.futures_ticker_24h)


class ReplayRoundTripTests(unittest.TestCase):
    def test_event_from_dict_round_trips_new_streams(self) -> None:
        events = [
            _futures_trade_event(1, 1),
            parse_futures_kline_message(
                kline_message(closed=True), received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
            ),
            parse_futures_ticker_message(
                ticker_message(), received_wall_timestamp_ns=1, received_monotonic_ns=1, ingest_sequence=1,
            ),
        ]
        for event in events:
            restored = event_from_dict(json.loads(json_dumps(event)))
            self.assertEqual(restored, event)


class StorageRoundTripTests(unittest.TestCase):
    def test_new_snapshot_fields_round_trip_through_parquet(self) -> None:
        store = StateStore()
        store.apply(_futures_trade_event(1, 1))
        store.apply(parse_futures_kline_message(
            kline_message(closed=True), received_wall_timestamp_ns=2, received_monotonic_ns=2, ingest_sequence=2,
        ))
        store.apply(parse_futures_ticker_message(
            ticker_message(), received_wall_timestamp_ns=3, received_monotonic_ns=3, ingest_sequence=3,
        ))
        spot_parser = BinanceMessageParser("session-1", timestamp_unit="microsecond")
        store.apply(spot_parser.parse(
            {"stream": STREAMS[3], "data": kline_message()},
            received_wall_timestamp_ns=4, received_monotonic_ns=4, ingest_sequence=4, now_ns=NOW_NS,
        ))

        snapshot = store.snapshot(NOW_NS, 1)
        self.assertIsNotNone(snapshot.futures_last_trade)
        self.assertIsNotNone(snapshot.futures_kline)
        self.assertIsNotNone(snapshot.spot_kline)
        self.assertIsNotNone(snapshot.futures_ticker_24h)
        self.assertEqual(len(snapshot.futures_recent_trades), 1)

        restored = snapshot_from_parquet_row(snapshot_to_parquet_row(snapshot))
        self.assertEqual(restored, snapshot)


if __name__ == "__main__":
    unittest.main()
