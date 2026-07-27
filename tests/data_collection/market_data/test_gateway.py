from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.market_data.config import load_config
from polymarket_btc.data_collection.market_data.gateway import MarketDataGateway
from polymarket_btc.data_collection.market_data.models import (
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
)


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_offline_gateway_reduces_stores_and_publishes(self) -> None:
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            config = load_config(root / "config" / "market_data.toml")
            config = replace(
                config,
                service=replace(config.service, snapshot_interval_ms=50),
                storage=replace(config.storage, data_dir=Path(directory)),
                health=replace(
                    config.health,
                    health_file=Path(directory) / "runtime" / "health.json",
                    update_interval_seconds=0.05,
                ),
            )
            async with MarketDataGateway(
                config,
                start_live_sources=False,
                start_market_discovery=False,
            ) as gateway:
                event = MarketDataEvent(
                    1, gateway.next_sequence(), "chainlink:1:1",
                    EventSource.CHAINLINK_RTDS, EventStream.CHAINLINK_PRICE,
                    "BTC/USD", 1, 1, 1, 1, "1",
                    None, None, None, None, None,
                    ChainlinkPricePayload("btc/usd", Decimal("100")),
                )
                await gateway.bus.publish(event)
                async for snapshot in gateway.snapshots():
                    self.assertEqual(snapshot.chainlink.price, Decimal("100"))
                    break
            runtime = gateway.runtime_report()
            self.assertTrue(runtime["health_file_valid"])
            self.assertTrue(list((Path(directory) / "raw").rglob("*.jsonl.zst")))
            self.assertTrue(list((Path(directory) / "snapshots").rglob("*.parquet")))
            self.assertTrue(runtime["raw_manifest_valid"])
            self.assertTrue(runtime["parquet_manifest_valid"])
            self.assertTrue(runtime["parquet_readable"])


if __name__ == "__main__":
    unittest.main()
