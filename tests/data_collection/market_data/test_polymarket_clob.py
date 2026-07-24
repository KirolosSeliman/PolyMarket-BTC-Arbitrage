from decimal import Decimal
import unittest

from polymarket_btc.data_collection.market_discovery import Timeframe
from polymarket_btc.data_collection.market_data.models import Outcome
from polymarket_btc.data_collection.market_data.models import InvalidEventError
from polymarket_btc.data_collection.market_data.sources.polymarket_clob import (
    ClobAssetMetadata,
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

    def _assets(self) -> dict[str, ClobAssetMetadata]:
        return {
            "up-token": ClobAssetMetadata(
                "up-token",
                Timeframe.FIVE_MINUTES,
                "market-1",
                "condition-1",
                Outcome.UP,
            ),
            "down-token": ClobAssetMetadata(
                "down-token",
                Timeframe.FIVE_MINUTES,
                "market-1",
                "condition-1",
                Outcome.DOWN,
            ),
        }

    def test_book_parser_is_pure_and_emits_complete_payload(self) -> None:
        assets = self._assets()
        events = apply_clob_message(
            clob_book(),
            assets,
            received_wall_timestamp_ns=NOW_NS,
            received_monotonic_ns=1,
            ingest_sequence_start=1,
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].payload.bids[0].price, Decimal(".49"))
        self.assertEqual(events[0].payload.asks[0].price, Decimal(".52"))
        self.assertEqual(assets, self._assets())

    def test_price_change_parser_emits_each_delta_without_local_book(self) -> None:
        message = {
            "event_type": "price_change",
            "market": "condition-1",
            "timestamp": "1750000000001",
            "price_changes": [
                {"asset_id": "up-token", "price": ".49", "size": "7", "side": "BUY"},
                {"asset_id": "up-token", "price": ".48", "size": "0", "side": "BUY"},
            ],
        }
        events = apply_clob_message(message, self._assets(), NOW_NS, 1, 2)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].payload.quantity, Decimal("7"))
        self.assertEqual(events[1].payload.quantity, Decimal("0"))

    def test_unknown_token_is_rejected(self) -> None:
        with self.assertRaises(InvalidEventError):
            apply_clob_message(
                clob_book("unknown"),
                self._assets(),
                NOW_NS,
                1,
                1,
            )

    def test_market_resolved_without_single_asset_id_emits_one_event_per_asset(self) -> None:
        message = {
            "event_type": "market_resolved",
            "id": "1031769",
            "market": "condition-1",
            "condition_id": "condition-1",
            "assets_ids": ["up-token", "down-token"],
            "winning_asset_id": "up-token",
            "winning_outcome": "Yes",
            "timestamp": "1766790415550",
        }

        events = apply_clob_message(message, self._assets(), NOW_NS, 1, 10)

        self.assertEqual([event.asset_id for event in events], ["up-token", "down-token"])
        self.assertTrue(all(event.payload.affected_asset_ids == ("up-token", "down-token") for event in events))
        self.assertTrue(all(event.payload.winning_asset_id == "up-token" for event in events))
        self.assertTrue(all(event.payload.winning_outcome is Outcome.UP for event in events))

    def test_market_resolved_can_resolve_assets_from_market_identifier(self) -> None:
        events = apply_clob_message(
            {
                "event_type": "market_resolved",
                "market": "condition-1",
                "winning_asset_id": "down-token",
                "winning_outcome": "No",
                "timestamp": "1766790415550",
            },
            self._assets(),
            NOW_NS,
            1,
            10,
        )

        self.assertEqual([event.asset_id for event in events], ["up-token", "down-token"])
        self.assertTrue(all(event.payload.winning_outcome is Outcome.DOWN for event in events))

    def test_market_resolved_for_unknown_market_is_rejected_without_crash(self) -> None:
        with self.assertRaises(InvalidEventError):
            apply_clob_message(
                {
                    "event_type": "market_resolved",
                    "market": "unknown",
                    "assets_ids": ["unknown-up", "unknown-down"],
                    "timestamp": "1766790415550",
                },
                self._assets(),
                NOW_NS,
                1,
                10,
            )


if __name__ == "__main__":
    unittest.main()
