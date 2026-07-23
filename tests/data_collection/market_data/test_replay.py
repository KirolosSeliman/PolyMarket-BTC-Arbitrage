from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.market_data.models import (
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
)
from polymarket_btc.data_collection.market_data.replay import read_raw_events
from polymarket_btc.data_collection.market_data.storage import RawEventStorage


class ReplayTests(unittest.TestCase):
    def test_replay_verifies_and_restores_ingest_order(self) -> None:
        event = MarketDataEvent(
            1, 1, "event-1", EventSource.CHAINLINK_RTDS,
            EventStream.CHAINLINK_PRICE, "BTC/USD", 1, 1, 1, 1, "1",
            None, None, None, None, None,
            ChainlinkPricePayload("btc/usd", Decimal("1.25")),
        )
        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            storage.write(event)
            storage.close()
            replayed = list(read_raw_events(Path(directory) / "raw"))
            self.assertEqual(replayed, [event])


if __name__ == "__main__":
    unittest.main()
