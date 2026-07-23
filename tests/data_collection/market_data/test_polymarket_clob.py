from decimal import Decimal
import unittest

from polymarket_btc.data_collection.market_data.models import InvalidEventError
from polymarket_btc.data_collection.market_data.sources.polymarket_clob import (
    ClobBookState,
    apply_clob_message,
    initial_subscription,
    subscription_update,
)

from .fixtures import NOW_NS, clob_book


class ClobParserTests(unittest.TestCase):
    def test_subscription_messages_are_exact(self) -> None:
        self.assertEqual(
            initial_subscription(["a", "b"]),
            {"assets_ids": ["a", "b"], "type": "market", "custom_feature_enabled": True},
        )
        self.assertEqual(
            subscription_update(["c"], "subscribe"),
            {"assets_ids": ["c"], "operation": "subscribe", "custom_feature_enabled": True},
        )
        self.assertEqual(
            subscription_update(["a"], "unsubscribe"),
            {"assets_ids": ["a"], "operation": "unsubscribe"},
        )

    def test_book_replaces_complete_state(self) -> None:
        books = {"up-token": ClobBookState(asset_id="up-token")}
        events = apply_clob_message(
            clob_book(),
            books,
            received_wall_timestamp_ns=NOW_NS,
            received_monotonic_ns=1,
            ingest_sequence_start=1,
        )
        self.assertEqual(len(events), 1)
        self.assertTrue(books["up-token"].initialized)
        self.assertEqual(books["up-token"].best_bid, Decimal(".49"))
        self.assertEqual(books["up-token"].best_ask, Decimal(".52"))

    def test_price_change_replaces_and_zero_removes_level(self) -> None:
        books = {"up-token": ClobBookState(asset_id="up-token")}
        apply_clob_message(clob_book(), books, NOW_NS, 1, 1)
        message = {
            "event_type": "price_change",
            "market": "condition-1",
            "timestamp": "1750000000001",
            "price_changes": [
                {"asset_id": "up-token", "price": ".49", "size": "7", "side": "BUY"},
                {"asset_id": "up-token", "price": ".48", "size": "0", "side": "BUY"},
            ],
        }
        events = apply_clob_message(message, books, NOW_NS, 1, 2)
        self.assertEqual(len(events), 2)
        self.assertEqual(books["up-token"].bids[Decimal(".49")], Decimal("7"))
        self.assertNotIn(Decimal(".48"), books["up-token"].bids)

    def test_unknown_token_is_rejected(self) -> None:
        with self.assertRaises(InvalidEventError):
            apply_clob_message(
                clob_book("unknown"),
                {"known": ClobBookState(asset_id="known")},
                NOW_NS,
                1,
                1,
            )


if __name__ == "__main__":
    unittest.main()
