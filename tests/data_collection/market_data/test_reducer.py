from decimal import Decimal
import unittest

from polymarket_btc.data_collection.market_data.models import (
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    SnapshotTickPayload,
)
from polymarket_btc.data_collection.market_data.health import HealthRegistry
from polymarket_btc.data_collection.market_data.reducer import MarketDataReducer
from polymarket_btc.data_collection.market_data.state import StateStore


class ReducerTests(unittest.TestCase):
    def test_normal_event_does_not_create_snapshot(self) -> None:
        reducer = MarketDataReducer(StateStore())
        event = MarketDataEvent(
            2, 1, "chainlink-1", EventSource.CHAINLINK_RTDS,
            EventStream.CHAINLINK_PRICE, "BTC/USD", 1, 1, 1, 1, "1",
            None, None, None, None, None,
            ChainlinkPricePayload("btc/usd", Decimal("1")),
        )
        self.assertIsNone(reducer.apply(event))

    def test_snapshot_tick_creates_snapshot_using_tick_sequence_and_health(self) -> None:
        registry = HealthRegistry()
        state = StateStore(health_registry=registry)
        reducer = MarketDataReducer(state)
        health = registry.all_source_snapshots(100)
        event = MarketDataEvent(
            2, 2, "tick-8", EventSource.MARKET_DISCOVERY,
            EventStream.SNAPSHOT_TICK, "gateway", 100, None, 100, 2, "8",
            None, None, None, None, None,
            SnapshotTickPayload(8, 100, health),
        )
        snapshot = reducer.apply(event)
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.snapshot_sequence, 8)
        self.assertEqual(snapshot.snapshot_timestamp_ns, 100)
        self.assertEqual(snapshot.health, health)


if __name__ == "__main__":
    unittest.main()
