import unittest

from polymarket_btc.data_collection.market_data.snapshot import SnapshotPublisher
from polymarket_btc.data_collection.market_data.state import StateStore


class SnapshotPublisherTests(unittest.IsolatedAsyncioTestCase):
    async def test_fanout_preserves_snapshot_identity(self) -> None:
        publisher = SnapshotPublisher(
            StateStore(), interval_ms=250, subscriber_capacity=2
        )
        first = publisher.subscribe()
        second = publisher.subscribe()
        snapshot = publisher.publish_once(1)
        self.assertIs(await first.get(), snapshot)
        self.assertIs(await second.get(), snapshot)

    async def test_slow_subscriber_is_removed_without_blocking(self) -> None:
        publisher = SnapshotPublisher(
            StateStore(), interval_ms=250, subscriber_capacity=1
        )
        publisher.subscribe()
        publisher.publish_once(1)
        publisher.publish_once(2)
        self.assertEqual(publisher.slow_subscribers_removed, 1)


if __name__ == "__main__":
    unittest.main()
