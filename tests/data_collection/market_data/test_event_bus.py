import asyncio
from dataclasses import replace
from decimal import Decimal
import unittest

from polymarket_btc.data_collection.market_data.event_bus import EventBus
from polymarket_btc.data_collection.market_data.models import (
    BackpressureFatalError,
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
)


def event(sequence: int, event_id: str | None = None) -> MarketDataEvent:
    return MarketDataEvent(
        1, sequence, event_id or f"event-{sequence}",
        EventSource.CHAINLINK_RTDS, EventStream.CHAINLINK_PRICE, "BTC/USD",
        sequence, sequence, sequence, sequence, str(sequence),
        None, None, None, None, None,
        ChainlinkPricePayload("btc/usd", Decimal("1")),
    )


class EventBusTests(unittest.IsolatedAsyncioTestCase):
    async def test_publish_fans_out_in_order(self) -> None:
        bus = EventBus(10, 10, 2, put_timeout_seconds=0.1)
        await bus.publish(event(1))
        await bus.publish(event(2))
        self.assertEqual((await bus.state_queue.get()).ingest_sequence, 1)
        self.assertEqual((await bus.storage_queue.get()).ingest_sequence, 1)
        self.assertEqual((await bus.state_queue.get()).ingest_sequence, 2)

    async def test_event_bus_has_no_generic_seen_cache(self) -> None:
        bus = EventBus(10, 10, 2, put_timeout_seconds=0.1)
        self.assertTrue(await bus.publish(event(1, "same")))
        self.assertTrue(await bus.publish(replace(event(2), event_id="same")))
        self.assertFalse(hasattr(bus, "_seen"))
        self.assertEqual(bus.state_queue.qsize(), 2)
        self.assertEqual(bus.storage_queue.qsize(), 2)

    async def test_bus_allocates_every_accepted_event_sequence(self) -> None:
        bus = EventBus(10, 10, 2, put_timeout_seconds=0.1)
        next_value = iter((10, 11)).__next__
        self.assertTrue(await bus.publish(event(0, "first"), next_value))
        self.assertTrue(await bus.publish(event(0, "second"), next_value))
        first = await bus.state_queue.get()
        second = await bus.state_queue.get()
        self.assertEqual((first.ingest_sequence, second.ingest_sequence), (10, 11))

    async def test_backpressure_is_fatal_without_dropping_oldest(self) -> None:
        bus = EventBus(1, 1, 1, put_timeout_seconds=0.01)
        await bus.publish(event(1))
        with self.assertRaises(BackpressureFatalError):
            await bus.publish(event(2))
        self.assertEqual((await bus.state_queue.get()).event_id, "event-1")

    async def test_market_state_callback_raises_when_queue_is_full(self) -> None:
        bus = EventBus(1, 1, 1, put_timeout_seconds=0.01)
        bus.publish_market_state_nowait(object())
        with self.assertRaises(BackpressureFatalError):
            bus.publish_market_state_nowait(object())


if __name__ == "__main__":
    unittest.main()
