import asyncio
from decimal import Decimal
import json
import unittest

from polymarket_btc.data_collection.market_discovery import Timeframe
from polymarket_btc.data_collection.market_data.live_view.frame import build_frame
from polymarket_btc.data_collection.market_data.live_view.server import LiveViewServer
from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    BinanceBookTickerPayload,
    BinanceDepth20Payload,
    BinanceDomSnapshotPayload,
    BinanceFuturesMarkPricePayload,
    BinanceKlinePayload,
    BinanceTicker24hPayload,
    DomBucketPayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    MarketDataSnapshot,
    MarketWindowPayload,
    MarketWindowStatePayload,
    Outcome,
    PolymarketBookPayload,
    PriceLevel,
    TakerSide,
)
from polymarket_btc.data_collection.market_data.state import StateStore


NOW_NS = 1_750_000_000_000_000_000
UP_TOKEN = "up-token"
DOWN_TOKEN = "down-token"


def event(stream, payload, sequence, *, source, asset=None, outcome=None, timeframe=None):
    return MarketDataEvent(
        2, sequence, f"{stream.value}-{sequence}", source, stream,
        asset or "BTCUSDT", NOW_NS, None, NOW_NS, sequence, str(sequence),
        timeframe, "market-1" if timeframe else None,
        "condition-1" if timeframe else None, asset, outcome, payload,
    )


def populated_snapshot() -> MarketDataSnapshot:
    store = StateStore()
    for source in (
        EventSource.CHAINLINK_RTDS,
        EventSource.BINANCE_SPOT,
        EventSource.POLYMARKET_CLOB,
    ):
        store.set_connected(source, True)
    store.apply(event(
        EventStream.MARKET_WINDOW_STATE,
        MarketWindowStatePayload(
            "active",
            Timeframe.FIVE_MINUTES,
            MarketWindowPayload(
                Timeframe.FIVE_MINUTES, "market-1", "condition-1", "slug-1",
                NOW_NS, NOW_NS + 300_000_000_000, UP_TOKEN, DOWN_TOKEN, "chainlink",
            ),
            None, None, NOW_NS, 0, None,
        ),
        1,
        source=EventSource.MARKET_DISCOVERY,
        timeframe=Timeframe.FIVE_MINUTES,
    ))
    store.apply(event(
        EventStream.BINANCE_BOOK_TICKER,
        BinanceBookTickerPayload(
            "BTCUSDT", 1, Decimal("67234.40"), Decimal("1.2"),
            Decimal("67234.50"), Decimal("0.8"),
        ),
        2,
        source=EventSource.BINANCE_SPOT,
    ))
    store.apply(event(
        EventStream.BINANCE_DEPTH20,
        BinanceDepth20Payload(
            "BTCUSDT", 2,
            (PriceLevel(Decimal("67234.40"), Decimal("1.2")),),
            (PriceLevel(Decimal("67234.50"), Decimal("0.8")),),
        ),
        3,
        source=EventSource.BINANCE_SPOT,
    ))
    store.apply(event(
        EventStream.BINANCE_AGG_TRADE,
        BinanceAggTradePayload(
            "BTCUSDT", 1, Decimal("67234.45"), Decimal("0.5"),
            1, 1, NOW_NS, TakerSide.BUY,
        ),
        4,
        source=EventSource.BINANCE_SPOT,
    ))
    store.apply(event(
        EventStream.BINANCE_FUTURES_MARK_PRICE,
        BinanceFuturesMarkPricePayload(
            Decimal("67240.00"), Decimal("67238.00"), Decimal("0.0001"), NOW_NS,
        ),
        5,
        source=EventSource.BINANCE_FUTURES_MARK_PRICE,
    ))
    store.apply(event(
        EventStream.BINANCE_SPOT_DOM_SNAPSHOT,
        BinanceDomSnapshotPayload(
            "spot", Decimal("67234.45"), Decimal("10"),
            (DomBucketPayload(Decimal("67230"), Decimal("2"), Decimal("3")),),
        ),
        6,
        source=EventSource.BINANCE_SPOT_FULL_DEPTH,
    ))
    store.apply(event(
        EventStream.BINANCE_FUTURES_AGG_TRADE,
        BinanceAggTradePayload(
            "BTCUSDT", 1, Decimal("67240.10"), Decimal("0.3"),
            1, 1, NOW_NS, TakerSide.SELL,
        ),
        8,
        source=EventSource.BINANCE_FUTURES_TRADE,
    ))
    store.apply(event(
        EventStream.BINANCE_KLINE,
        BinanceKlinePayload(
            "futures", "1m", NOW_NS - 60_000_000_000, NOW_NS,
            Decimal("67200"), Decimal("67260"), Decimal("67190"), Decimal("67240.10"),
            Decimal("12.5"), Decimal("840125.0"), 88, False,
        ),
        9,
        source=EventSource.BINANCE_FUTURES_KLINE,
    ))
    store.apply(event(
        EventStream.BINANCE_TICKER_24H,
        BinanceTicker24hPayload(
            "futures", Decimal("-942.20"), Decimal("-1.474"), Decimal("64277.98"),
            Decimal("64878.81"), Decimal("65821.01"), Decimal("65990.00"),
            Decimal("63936.62"), Decimal("18234.5"), Decimal("1173456789.1"),
        ),
        10,
        source=EventSource.BINANCE_FUTURES_TICKER,
    ))
    for asset, outcome, bid, ask in (
        (UP_TOKEN, Outcome.UP, "0.48", "0.52"),
        (DOWN_TOKEN, Outcome.DOWN, "0.47", "0.51"),
    ):
        store.apply(event(
            EventStream.POLYMARKET_BOOK,
            PolymarketBookPayload(
                (PriceLevel(Decimal(bid), Decimal("30")),),
                (PriceLevel(Decimal(ask), Decimal("25")),),
                "book-hash",
            ),
            7,
            source=EventSource.POLYMARKET_CLOB,
            asset=asset,
            outcome=outcome,
            timeframe=Timeframe.FIVE_MINUTES,
        ))
    return store.snapshot(NOW_NS + 1_000_000, 42)


class FrameTests(unittest.TestCase):
    def test_frame_is_json_serialisable_and_carries_every_section(self) -> None:
        frame = build_frame(populated_snapshot())
        payload = json.loads(json.dumps(frame, allow_nan=False))
        self.assertEqual(payload["seq"], 42)
        self.assertEqual(payload["spot"]["bid"], 67234.40)
        self.assertEqual(payload["spot"]["ask"], 67234.50)
        self.assertEqual(payload["futures"]["mark"], 67240.0)
        self.assertEqual(payload["dom"]["spot"]["buckets"], [[67230.0, 2.0, 3.0]])
        self.assertEqual(len(payload["health"]), len(EventSource))
        self.assertIsNone(payload["polymarket"]["15m"])

    def test_futures_trade_kline_and_ticker_sections(self) -> None:
        frame = build_frame(populated_snapshot())
        self.assertEqual(frame["futures"]["last"], 67240.10)
        self.assertEqual(frame["futures"]["taker_side"], "sell")
        self.assertEqual(len(frame["trades"]["futures"]), 1)
        self.assertEqual(frame["trades"]["futures"][0]["price"], 67240.10)
        self.assertEqual(len(frame["trades"]["spot"]), 1)
        self.assertEqual(frame["trades"]["spot"][0]["taker_side"], "buy")
        self.assertEqual(frame["klines"]["futures"]["close"], 67240.10)
        self.assertFalse(frame["klines"]["futures"]["is_closed"])
        self.assertIsNone(frame["klines"]["spot"])
        self.assertEqual(frame["ticker_24h"]["futures"]["price_change_percent"], -1.474)
        self.assertIsNone(frame["ticker_24h"]["spot"])
        json.dumps(frame, allow_nan=False)

    def test_complementary_pair_legs_are_derived_from_both_books(self) -> None:
        market = build_frame(populated_snapshot())["polymarket"]["5m"]
        self.assertEqual(market["up"]["best_bid"], 0.48)
        self.assertEqual(market["down"]["best_ask"], 0.51)
        # asks 0.52 + 0.51 = 1.03 -> lifting both legs costs more than it pays
        self.assertAlmostEqual(market["buy_both_cost"], 1.03)
        self.assertAlmostEqual(market["buy_both_edge"], -0.03)
        # bids 0.48 + 0.47 = 0.95 -> hitting both bids also loses
        self.assertAlmostEqual(market["sell_both_credit"], 0.95)
        self.assertAlmostEqual(market["sell_both_edge"], -0.05)

    def test_empty_state_produces_a_frame_without_raising(self) -> None:
        frame = build_frame(StateStore().snapshot(NOW_NS, 0))
        self.assertFalse(frame["ready"])
        self.assertIsNone(frame["spot"]["mid"])
        self.assertIsNone(frame["dom"]["spot"])
        json.dumps(frame, allow_nan=False)


class LiveViewServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.snapshot = populated_snapshot()
        self.subscribers: list[asyncio.Queue] = []

        def subscribe() -> asyncio.Queue:
            queue: asyncio.Queue = asyncio.Queue(16)
            self.subscribers.append(queue)
            return queue

        self.server = LiveViewServer(
            subscribe=subscribe,
            unsubscribe=lambda queue: self.subscribers.remove(queue),
            latest_snapshot=lambda: self.snapshot,
            health=lambda: {"gateway_state": "running"},
            host="127.0.0.1",
            port=0,
        )
        await self.server.start()

    async def asyncTearDown(self) -> None:
        await self.server.stop()

    async def _request(self, target: str) -> tuple[str, bytes]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        writer.write(f"GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(-1), timeout=5)
        writer.close()
        await writer.wait_closed()
        head, _, body = raw.partition(b"\r\n\r\n")
        return head.decode("latin-1"), body

    async def test_dashboard_page_is_served(self) -> None:
        head, body = await self._request("/")
        self.assertIn("200 OK", head)
        self.assertIn("text/html", head)
        self.assertIn(b"/stream", body)

    async def test_frame_endpoint_returns_the_current_snapshot(self) -> None:
        head, body = await self._request("/frame.json")
        self.assertIn("200 OK", head)
        self.assertEqual(json.loads(body)["seq"], 42)

    async def test_unknown_path_is_404_and_post_is_405(self) -> None:
        head, _body = await self._request("/nope")
        self.assertIn("404", head)
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        writer.write(b"POST / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(-1), timeout=5)
        writer.close()
        await writer.wait_closed()
        self.assertIn("405", raw.decode("latin-1"))

    async def test_stream_pushes_published_snapshots_and_releases_the_subscription(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        writer.write(b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()

        header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        self.assertIn(b"text/event-stream", header)

        first = await self._read_event(reader)
        self.assertEqual(first["seq"], 42)   # frame courante servie à la connexion

        for _ in range(50):
            if self.subscribers:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(len(self.subscribers), 1)

        published = build_frame(self.snapshot)
        self.subscribers[0].put_nowait(self.snapshot)
        pushed = await self._read_event(reader)
        self.assertEqual(pushed["seq"], published["seq"])

        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionResetError:
            pass
        for _ in range(200):
            if not self.subscribers:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(self.subscribers, [])

    async def test_stream_coalesces_to_the_newest_queued_snapshot(self) -> None:
        from dataclasses import replace

        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        writer.write(b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        await self._read_event(reader)

        for _ in range(50):
            if self.subscribers:
                break
            await asyncio.sleep(0.01)
        queue = self.subscribers[0]
        for sequence in (43, 44, 45):
            queue.put_nowait(replace(self.snapshot, snapshot_sequence=sequence))

        pushed = await self._read_event(reader)
        self.assertEqual(pushed["seq"], 45)

        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionResetError:
            pass

    @staticmethod
    async def _read_event(reader: asyncio.StreamReader) -> dict:
        """Decode one chunked SSE `data:` event, skipping keepalive comments."""
        while True:
            size_line = await asyncio.wait_for(reader.readline(), timeout=5)
            size = int(size_line.strip(), 16)
            body = await asyncio.wait_for(reader.readexactly(size + 2), timeout=5)
            text = body[:size].decode("utf-8")
            if text.startswith("data: "):
                return json.loads(text[len("data: "):])


if __name__ == "__main__":
    unittest.main()
