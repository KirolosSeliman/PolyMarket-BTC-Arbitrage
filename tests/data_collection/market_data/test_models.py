from dataclasses import FrozenInstanceError
from decimal import Decimal
import json
import unittest

from polymarket_btc.data_collection.market_discovery import Timeframe
from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    PriceLevel,
    TakerSide,
    json_dumps,
    parse_decimal,
)


class ModelTests(unittest.TestCase):
    def test_price_level_is_immutable_and_uses_decimal(self) -> None:
        level = PriceLevel(price=Decimal("0.51"), quantity=Decimal("12.25"))
        with self.assertRaises(FrozenInstanceError):
            level.quantity = Decimal("1")  # type: ignore[misc]

    def test_parse_decimal_rejects_non_finite_and_negative_values(self) -> None:
        for value in ("NaN", "Infinity", "-Infinity", "-0.01"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_decimal(value, "price")

    def test_zero_quantity_requires_explicit_permission(self) -> None:
        with self.assertRaises(ValueError):
            parse_decimal("0", "quantity", strictly_positive=True)
        self.assertEqual(
            parse_decimal("0", "quantity", allow_zero=True),
            Decimal("0"),
        )

    def test_event_serializes_decimal_and_enums_as_strings(self) -> None:
        event = MarketDataEvent(
            schema_version=1,
            ingest_sequence=7,
            event_id="chainlink:1:2",
            source=EventSource.CHAINLINK_RTDS,
            stream=EventStream.CHAINLINK_PRICE,
            instrument="BTC/USD",
            source_timestamp_ns=1_000_000,
            server_timestamp_ns=2_000_000,
            received_wall_timestamp_ns=3_000_000,
            received_monotonic_ns=4_000_000,
            source_sequence="1",
            timeframe=Timeframe.FIVE_MINUTES,
            market_id=None,
            condition_id=None,
            asset_id=None,
            outcome=None,
            payload=ChainlinkPricePayload("btc/usd", Decimal("67234.5000")),
        )
        encoded = json.loads(json_dumps(event))
        self.assertEqual(encoded["payload"]["price"], "67234.5000")
        self.assertEqual(encoded["source"], "chainlink_rtds")
        self.assertEqual(encoded["timeframe"], "5m")

    def test_agg_trade_payload_preserves_taker_side(self) -> None:
        payload = BinanceAggTradePayload(
            symbol="BTCUSDT",
            aggregate_trade_id=10,
            price=Decimal("100"),
            quantity=Decimal("0.1"),
            first_trade_id=20,
            last_trade_id=21,
            trade_timestamp_ns=1_000,
            taker_side=TakerSide.SELL,
        )
        self.assertIs(payload.taker_side, TakerSide.SELL)


if __name__ == "__main__":
    unittest.main()
