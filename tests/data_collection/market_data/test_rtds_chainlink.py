from decimal import Decimal
import unittest

from polymarket_btc.data_collection.market_data.models import InvalidEventError
from polymarket_btc.data_collection.market_data.sources.rtds_chainlink import (
    SUBSCRIPTION,
    parse_chainlink_message,
)

from .fixtures import NOW_NS, chainlink_update


class ChainlinkParserTests(unittest.TestCase):
    def test_subscription_is_exact(self) -> None:
        self.assertEqual(
            SUBSCRIPTION,
            {
                "action": "subscribe",
                "subscriptions": [{
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": '{"symbol":"btc/usd"}',
                }],
            },
        )

    def test_valid_update_uses_payload_and_server_timestamps(self) -> None:
        event = parse_chainlink_message(
            chainlink_update(),
            received_wall_timestamp_ns=NOW_NS + 3_000_000,
            received_monotonic_ns=10,
            ingest_sequence=1,
            now_ns=NOW_NS + 3_000_000,
        )
        self.assertEqual(event.payload.price, Decimal("67234.5000"))
        self.assertEqual(event.source_timestamp_ns, NOW_NS)
        self.assertEqual(event.server_timestamp_ns, NOW_NS + 2_000_000)

    def test_wrong_topic_type_symbol_and_missing_price_are_rejected(self) -> None:
        mutations = [
            ("topic", "other"),
            ("type", "subscribe"),
            ("symbol", "eth/usd"),
            ("value", None),
        ]
        for field, value in mutations:
            message = chainlink_update()
            if field in {"symbol", "value"}:
                message["payload"][field] = value  # type: ignore[index]
            else:
                message[field] = value
            with self.subTest(field=field), self.assertRaises(InvalidEventError):
                parse_chainlink_message(
                    message,
                    received_wall_timestamp_ns=NOW_NS,
                    received_monotonic_ns=1,
                    ingest_sequence=1,
                    now_ns=NOW_NS,
                )

    def test_future_and_out_of_order_timestamps_are_rejected(self) -> None:
        future = chainlink_update()
        future["payload"]["timestamp"] += 6_000  # type: ignore[index,operator]
        with self.assertRaises(InvalidEventError):
            parse_chainlink_message(
                future,
                received_wall_timestamp_ns=NOW_NS,
                received_monotonic_ns=1,
                ingest_sequence=1,
                now_ns=NOW_NS,
            )
        with self.assertRaises(InvalidEventError):
            parse_chainlink_message(
                chainlink_update(),
                received_wall_timestamp_ns=NOW_NS,
                received_monotonic_ns=1,
                ingest_sequence=1,
                now_ns=NOW_NS,
                last_source_timestamp_ns=NOW_NS + 1,
            )


if __name__ == "__main__":
    unittest.main()
