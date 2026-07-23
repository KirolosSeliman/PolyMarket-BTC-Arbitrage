import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import tempfile
import time
import unittest

from websockets.asyncio.server import serve

from polymarket_btc.data_collection.market_discovery import (
    DiscoveryState,
    MarketWindow,
    Timeframe,
    TimeframeSnapshot,
)
from polymarket_btc.data_collection.market_data.config import load_config
from polymarket_btc.data_collection.market_data.gateway import MarketDataGateway
from polymarket_btc.data_collection.market_data.replay import read_raw_events


class OfflineIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_three_fake_sources_transition_storage_and_replay(self) -> None:
        subscriptions: list[dict[str, object]] = []

        async def rtds(connection) -> None:
            await connection.recv()
            while True:
                now_ms = time.time_ns() // 1_000_000
                try:
                    await connection.send(json.dumps({
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "timestamp": now_ms,
                        "payload": {
                            "symbol": "btc/usd",
                            "timestamp": now_ms,
                            "value": "67000.25",
                        },
                    }))
                    await asyncio.sleep(0.1)
                except Exception:
                    return

        async def binance(connection) -> None:
            trade_id = 0
            while True:
                now_us = time.time_ns() // 1_000
                trade_id += 1
                messages = [
                    {"stream": "btcusdt@aggTrade", "data": {
                        "e": "aggTrade", "E": now_us, "s": "BTCUSDT",
                        "a": trade_id, "p": "67000", "q": "1",
                        "f": trade_id, "l": trade_id,
                        "T": now_us, "m": False, "M": True,
                    }},
                    {"stream": "btcusdt@bookTicker", "data": {
                        "u": trade_id, "s": "BTCUSDT", "b": "66999", "B": "2",
                        "a": "67001", "A": "2",
                    }},
                    {"stream": "btcusdt@depth20@100ms", "data": {
                        "lastUpdateId": trade_id,
                        "bids": [["66999", "2"]],
                        "asks": [["67001", "2"]],
                    }},
                ]
                try:
                    for message in messages:
                        await connection.send(json.dumps(message))
                    await asyncio.sleep(0.1)
                except Exception:
                    return

        async def clob(connection) -> None:
            while True:
                try:
                    raw = await connection.recv()
                except Exception:
                    return
                if raw == "PING":
                    await connection.send("PONG")
                    continue
                message = json.loads(raw)
                subscriptions.append(message)
                if message.get("operation") == "unsubscribe":
                    continue
                for asset in message.get("assets_ids", []):
                    await connection.send(json.dumps({
                        "event_type": "book",
                        "asset_id": asset,
                        "market": f"condition-{asset}",
                        "bids": [{"price": ".49", "size": "10"}],
                        "asks": [{"price": ".51", "size": "10"}],
                        "timestamp": str(time.time_ns() // 1_000_000),
                        "hash": f"book-{asset}",
                    }))

        async with (
            serve(rtds, "127.0.0.1", 0) as rtds_server,
            serve(binance, "127.0.0.1", 0) as binance_server,
            serve(clob, "127.0.0.1", 0) as clob_server,
        ):
            root = Path(__file__).resolve().parents[3]
            with tempfile.TemporaryDirectory() as directory:
                config = load_config(root / "config" / "market_data.toml")
                config = replace(
                    config,
                    service=replace(config.service, snapshot_interval_ms=50),
                    rtds=replace(
                        config.rtds,
                        url=f"ws://127.0.0.1:{rtds_server.sockets[0].getsockname()[1]}",
                        heartbeat_seconds=60,
                    ),
                    binance=replace(
                        config.binance,
                        url=f"ws://127.0.0.1:{binance_server.sockets[0].getsockname()[1]}",
                        stale_depth_after_ms=30_000,
                        stale_trade_after_ms=30_000,
                    ),
                    clob=replace(
                        config.clob,
                        url=f"ws://127.0.0.1:{clob_server.sockets[0].getsockname()[1]}",
                        heartbeat_seconds=60,
                        unsubscribe_grace_seconds=0.05,
                    ),
                    storage=replace(config.storage, data_dir=Path(directory)),
                    health=replace(
                        config.health,
                        health_file=Path(directory) / "runtime" / "health.json",
                    ),
                )
                now = datetime.now(UTC)

                def market(timeframe: Timeframe, suffix: str) -> MarketWindow:
                    duration = timeframe.duration_seconds
                    return MarketWindow(
                        timeframe, f"market-{suffix}", f"condition-{suffix}",
                        f"slug-{suffix}", now - timedelta(seconds=1),
                        now + timedelta(seconds=duration - 1),
                        f"{suffix}-up", f"{suffix}-down", "chainlink",
                    )

                five = market(Timeframe.FIVE_MINUTES, "5m")
                fifteen = market(Timeframe.FIFTEEN_MINUTES, "15m")
                gateway = MarketDataGateway(
                    config,
                    start_live_sources=True,
                    start_market_discovery=False,
                )
                gateway.market_discovery_callback(TimeframeSnapshot(
                    Timeframe.FIVE_MINUTES, DiscoveryState.ACTIVE, five, None,
                    five.end_time_utc, now, 0, None,
                ))
                gateway.market_discovery_callback(TimeframeSnapshot(
                    Timeframe.FIFTEEN_MINUTES, DiscoveryState.ACTIVE, fifteen, None,
                    fifteen.end_time_utc, now, 0, None,
                ))
                async with gateway:
                    deadline = asyncio.get_running_loop().time() + 20
                    ready = False
                    async for snapshot in gateway.snapshots():
                        ready = snapshot.ready_for_strategy
                        if ready or asyncio.get_running_loop().time() >= deadline:
                            break
                    self.assertTrue(
                        ready,
                        (
                            snapshot.not_ready_reasons,
                            gateway.chainlink.invalid_count,
                            gateway.binance.invalid_count,
                            gateway.clob.invalid_count,
                            gateway.chainlink.reconnect_count,
                            gateway.binance.reconnect_count,
                            gateway.clob.reconnect_count,
                            subscriptions,
                            gateway._fatal,
                        ),
                    )
                replayed = list(read_raw_events(Path(directory) / "raw"))
                self.assertGreaterEqual(len(replayed), 9)
                self.assertTrue(list((Path(directory) / "snapshots").rglob("*.parquet")))
                self.assertTrue(subscriptions)


if __name__ == "__main__":
    unittest.main()
