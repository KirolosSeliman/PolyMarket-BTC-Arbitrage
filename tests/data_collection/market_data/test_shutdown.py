from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.market_data.config import load_config
from polymarket_btc.data_collection.market_data.gateway import MarketDataGateway


class ShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_finalizes_health_and_tasks(self) -> None:
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            config = load_config(root / "config" / "market_data.toml")
            health = Path(directory) / "runtime" / "health.json"
            config = replace(
                config,
                storage=replace(config.storage, data_dir=Path(directory)),
                health=replace(config.health, health_file=health),
            )
            gateway = MarketDataGateway(
                config,
                start_live_sources=False,
                start_market_discovery=False,
            )
            await gateway.__aenter__()
            await gateway.shutdown()
            self.assertTrue(health.exists())
            self.assertTrue(all(task.done() for task in gateway._tasks))


if __name__ == "__main__":
    unittest.main()
