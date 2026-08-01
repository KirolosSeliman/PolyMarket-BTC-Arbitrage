"""The single most important regression guard for multi-symbol collection:
an extra symbol's events must never reach state_queue (and therefore never
reach StateStore / the BTC composite snapshot pipeline), while still landing
in raw storage, correctly separated from BTC's own segments.
"""

from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.market_data.event_bus import EventBus
from polymarket_btc.data_collection.market_data.models import (
    BinanceBookTickerPayload,
    EventSource,
    EventStream,
    MarketDataEvent,
)
from polymarket_btc.data_collection.market_data.storage import RawEventStorage

NOW_NS = 1_750_000_000_000_000_000


def _book_ticker_event(*, instrument: str, sequence: int) -> MarketDataEvent:
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=0,
        event_id=f"binance:bookTicker:{instrument}:{sequence}",
        source=EventSource.BINANCE_SPOT,
        stream=EventStream.BINANCE_BOOK_TICKER,
        instrument=instrument,
        source_timestamp_ns=None,
        server_timestamp_ns=None,
        received_wall_timestamp_ns=NOW_NS,
        received_monotonic_ns=NOW_NS,
        source_sequence=str(sequence),
        timeframe=None,
        market_id=None,
        condition_id=None,
        asset_id=None,
        outcome=None,
        payload=BinanceBookTickerPayload(
            instrument, sequence, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"),
        ),
    )


class EventBusRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_publish_storage_only_never_reaches_state_queue(self) -> None:
        bus = EventBus(10, 10, 10, put_timeout_seconds=1.0)
        btc_event = _book_ticker_event(instrument="BTCUSDT", sequence=1)
        eth_event = _book_ticker_event(instrument="ETHUSDT", sequence=2)

        await bus.publish(btc_event, lambda: 1)
        await bus.publish_storage_only(eth_event, lambda: 2)

        # state_queue only ever saw the BTC event -- an ETH event landing
        # here would silently overwrite StateStore's single-symbol fields.
        self.assertEqual(bus.state_queue.qsize(), 1)
        queued = bus.state_queue.get_nowait()
        self.assertEqual(queued.instrument, "BTCUSDT")

        # storage_queue saw both -- extra symbols are archived, just not reduced.
        self.assertEqual(bus.storage_queue.qsize(), 2)
        stored_instruments = {
            bus.storage_queue.get_nowait().instrument for _ in range(2)
        }
        self.assertEqual(stored_instruments, {"BTCUSDT", "ETHUSDT"})


class RawStorageInstrumentIsolationTests(unittest.TestCase):
    def test_btc_and_eth_events_land_in_separate_instrument_segments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            btc_event = _book_ticker_event(instrument="BTCUSDT", sequence=1)
            eth_event = _book_ticker_event(instrument="ETHUSDT", sequence=2)
            storage.write(btc_event)
            storage.write(eth_event)
            manifests = storage.close()

            self.assertEqual(len(manifests), 2)
            paths = {str(path.parent) for path in manifests}
            btc_dir = next(p for p in paths if "instrument=BTCUSDT" in p)
            eth_dir = next(p for p in paths if "instrument=ETHUSDT" in p)
            self.assertNotEqual(btc_dir, eth_dir)
            # same source/stream partition otherwise -- only instrument differs
            self.assertEqual(
                btc_dir.rsplit("instrument=", 1)[0], eth_dir.rsplit("instrument=", 1)[0],
            )


if __name__ == "__main__":
    unittest.main()
