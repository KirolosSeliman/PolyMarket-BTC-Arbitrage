import asyncio
from decimal import Decimal
import json
import unittest
from unittest.mock import patch

from polymarket_btc.data_collection.market_data.models import (
    EventSource,
    EventStream,
    InvalidEventError,
)
from polymarket_btc.data_collection.market_data.sources.binance_futures_rest_streams import (
    BinanceFuturesKlineRestSource,
    BinanceFuturesTradeRestSource,
    kline_url,
    parse_premium_index,
    parse_rest_agg_trades,
    parse_rest_klines,
    parse_rest_ticker_24h,
)


def premium_index() -> dict:
    return {
        "symbol": "BTCUSDT", "markPrice": "64831.20000000", "indexPrice": "64829.40000000",
        "estimatedSettlePrice": "64828.00", "lastFundingRate": "-0.00006300",
        "interestRate": "0.00010000", "nextFundingTime": 1_700_000_400_000, "time": 1_700_000_000_123,
    }


def klines() -> list:
    return [
        [1_699_999_940_000, "64800.00", "64820.00", "64790.00", "64810.00",
         "5.5", 1_699_999_999_999, "356755.0", 200, "2.5", "162025.0", "0"],
        [1_700_000_000_000, "64810.00", "64850.00", "64795.00", "64831.90",
         "12.5", 1_700_000_059_999, "810125.0", 88, "6.0", "389480.0", "0"],
    ]


def ticker_24h() -> dict:
    return {
        "symbol": "BTCUSDT", "priceChange": "-85.30", "priceChangePercent": "-0.131",
        "weightedAvgPrice": "64550.20", "lastPrice": "64831.90", "openPrice": "64917.20",
        "highPrice": "65310.00", "lowPrice": "63801.40", "volume": "182034.4",
        "quoteVolume": "11748302983.2", "openTime": 1, "closeTime": 2, "firstId": 1, "lastId": 2, "count": 3,
    }


def agg_trade_row(aggregate_id: int) -> dict:
    return {
        "a": aggregate_id, "p": "64831.90", "q": "0.250",
        "f": aggregate_id, "l": aggregate_id, "T": 1_700_000_000_100, "m": True,
    }


class RestParserTests(unittest.TestCase):
    def test_premium_index_parses_signed_funding_rate(self) -> None:
        event = parse_premium_index(premium_index(), ingest_sequence=1, now_ns=1)
        self.assertEqual(event.source, EventSource.BINANCE_FUTURES_MARK_PRICE)
        self.assertEqual(event.payload.funding_rate, Decimal("-0.00006300"))
        self.assertEqual(event.payload.next_funding_time_ns, 1_700_000_400_000 * 1_000_000)

    def test_premium_index_wrong_symbol_is_rejected(self) -> None:
        message = premium_index()
        message["symbol"] = "ETHUSDT"
        with self.assertRaises(InvalidEventError):
            parse_premium_index(message, ingest_sequence=1, now_ns=1)

    def test_rest_klines_uses_last_row_and_derives_closed_flag(self) -> None:
        event = parse_rest_klines(klines(), ingest_sequence=1, now_ns=1_700_000_030_000_000_000)
        self.assertEqual(event.source, EventSource.BINANCE_FUTURES_KLINE)
        self.assertEqual(event.payload.close, Decimal("64831.90"))
        self.assertFalse(event.payload.is_closed)  # now_ns is before close_time

        closed_event = parse_rest_klines(klines(), ingest_sequence=1, now_ns=1_700_000_100_000_000_000)
        self.assertTrue(closed_event.payload.is_closed)  # now_ns is after close_time

    def test_rest_klines_empty_response_is_rejected(self) -> None:
        with self.assertRaises(InvalidEventError):
            parse_rest_klines([], ingest_sequence=1, now_ns=1)

    def test_rest_ticker_maps_camel_case_fields(self) -> None:
        event = parse_rest_ticker_24h(ticker_24h(), ingest_sequence=1, now_ns=1)
        self.assertEqual(event.source, EventSource.BINANCE_FUTURES_TICKER)
        self.assertEqual(event.payload.price_change_percent, Decimal("-0.131"))
        self.assertEqual(event.payload.last_price, Decimal("64831.90"))

    def test_rest_agg_trades_parses_each_row(self) -> None:
        events = parse_rest_agg_trades(
            [agg_trade_row(101), agg_trade_row(102)], next_sequence=lambda: 1, now_ns=1
        )
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].stream, EventStream.BINANCE_FUTURES_AGG_TRADE)
        self.assertEqual(events[1].payload.aggregate_trade_id, 102)

    def test_rest_agg_trades_empty_response_returns_no_events(self) -> None:
        self.assertEqual(parse_rest_agg_trades([], next_sequence=lambda: 1, now_ns=1), [])

    def test_rest_agg_trades_malformed_row_is_rejected(self) -> None:
        with self.assertRaises(InvalidEventError):
            parse_rest_agg_trades([{"a": 1}], next_sequence=lambda: 1, now_ns=1)

    def test_default_symbol_is_still_btcusdt(self) -> None:
        # Golden-value regression guard: omitting `symbol` must reproduce
        # today's exact BTC output.
        event = parse_rest_klines(klines(), ingest_sequence=1, now_ns=1)
        self.assertEqual(event.instrument, "BTCUSDT")
        self.assertIn("BTCUSDT", event.event_id)

    def test_eth_symbol_override_flows_through_klines_and_ticker(self) -> None:
        eth_klines = klines()
        eth_ticker = ticker_24h()
        eth_ticker["symbol"] = "ETHUSDT"

        kline_event = parse_rest_klines(eth_klines, ingest_sequence=1, now_ns=1, symbol="ETHUSDT")
        self.assertEqual(kline_event.instrument, "ETHUSDT")
        self.assertIn("ETHUSDT", kline_event.event_id)

        ticker_event = parse_rest_ticker_24h(eth_ticker, ingest_sequence=1, now_ns=1, symbol="ETHUSDT")
        self.assertEqual(ticker_event.instrument, "ETHUSDT")

    def test_eth_symbol_override_rejects_a_btc_flavored_response(self) -> None:
        with self.assertRaises(InvalidEventError):
            parse_rest_ticker_24h(ticker_24h(), ingest_sequence=1, now_ns=1, symbol="ETHUSDT")

    def test_url_builders_embed_the_requested_symbol(self) -> None:
        self.assertIn("symbol=ETHUSDT", kline_url("ETHUSDT"))
        self.assertNotIn("BTCUSDT", kline_url("ETHUSDT"))


class _Response:
    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


class TradeRestSourcePollingTests(unittest.IsolatedAsyncioTestCase):
    async def test_cursor_advances_and_only_refetches_new_trades(self) -> None:
        requested_urls: list[str] = []
        responses = [
            [agg_trade_row(100)],           # seed poll (no fromId)
            [agg_trade_row(101), agg_trade_row(102)],  # incremental poll from 101
            [],                              # nothing new
        ]

        def fake_urlopen(request, timeout=None):
            requested_urls.append(request.full_url)
            return _Response(responses.pop(0))

        published: list = []

        async def publish(event) -> None:
            published.append(event)

        source = BinanceFuturesTradeRestSource(
            _config_with_fast_polls(), publish, lambda: 1, None, None,
        )
        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            for _ in range(3):
                for event in await source._inner._poll_once():
                    await publish(event)

        self.assertEqual([event.payload.aggregate_trade_id for event in published], [100, 101, 102])
        self.assertNotIn("fromId", requested_urls[0])
        self.assertIn("fromId=101", requested_urls[1])
        self.assertIn("fromId=103", requested_urls[2])


class RestSourceSymbolOverrideTests(unittest.IsolatedAsyncioTestCase):
    async def test_kline_source_polls_the_eth_url_when_overridden(self) -> None:
        requested_urls: list[str] = []

        def fake_urlopen(request, timeout=None):
            requested_urls.append(request.full_url)
            return _Response(klines())

        source = BinanceFuturesKlineRestSource(
            _config_with_fast_polls(), lambda event: None, lambda: 1, None, None, symbol="ethusdt",
        )
        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            await source._inner._poll_once()

        self.assertIn("symbol=ETHUSDT", requested_urls[0])
        self.assertEqual(source._inner._instrument, "ETHUSDT")


def _config_with_fast_polls():
    from pathlib import Path

    from polymarket_btc.data_collection.market_data.config import load_config

    root = Path(__file__).resolve().parents[3]
    return load_config(root / "config" / "market_data.toml")


if __name__ == "__main__":
    unittest.main()
