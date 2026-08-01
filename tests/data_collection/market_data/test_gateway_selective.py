from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.market_data.config import load_config
from polymarket_btc.data_collection.market_data.gateway import SOURCE_KEYS, MarketDataGateway

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

_LIVE_SOURCE_TASK_NAMES = {
    "market-data-rtds", "market-data-binance", "market-data-clob",
    "market-data-spot-full-depth", "market-data-futures-depth",
    "market-data-futures-mark-price", "market-data-futures-liquidations",
    "market-data-futures-rest", "market-data-futures-trade",
    "market-data-futures-kline", "market-data-futures-ticker", "market-discovery",
}
_ALWAYS_ON_TASK_NAMES = {
    "market-data-reducer", "market-data-raw-writer", "market-data-parquet-writer",
    "market-data-market-manager", "market-data-snapshot-scheduler", "market-data-health",
}


def _config(data_dir: Path):
    config = load_config(REPOSITORY_ROOT / "config" / "market_data.toml")
    return replace(
        config,
        storage=replace(config.storage, data_dir=data_dir),
        health=replace(config.health, health_file=data_dir / "runtime" / "health.json"),
    )


class SourceEnabledFilterTests(unittest.TestCase):
    def test_none_enables_every_known_key(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gateway = MarketDataGateway(_config(Path(directory)))
            for key in SOURCE_KEYS:
                self.assertTrue(gateway._source_enabled(key))
            self.assertTrue(gateway._source_enabled("some_unknown_key"))

    def test_explicit_set_restricts_to_its_members(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gateway = MarketDataGateway(
                _config(Path(directory)), enabled_sources=frozenset({"chainlink", "polymarket"}),
            )
            self.assertTrue(gateway._source_enabled("chainlink"))
            self.assertTrue(gateway._source_enabled("polymarket"))
            self.assertFalse(gateway._source_enabled("binance_spot"))
            self.assertFalse(gateway._source_enabled("binance_futures_trade"))


class SelectiveSpawnIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_enabled_sources_spawns_no_live_source_task(self) -> None:
        # frozenset() (nothing enabled), unlike None (everything enabled),
        # must gate every live-source spawn out -- this exercises the real
        # __aenter__ wiring with zero network I/O, since nothing connects.
        with tempfile.TemporaryDirectory() as directory:
            config = _config(Path(directory))
            config = replace(config, service=replace(config.service, snapshot_interval_ms=50))
            gateway = MarketDataGateway(
                config,
                start_live_sources=True,
                start_market_discovery=True,
                enabled_sources=frozenset(),
            )
            async with gateway:
                task_names = {task.get_name() for task in gateway._tasks}
            self.assertEqual(task_names & _LIVE_SOURCE_TASK_NAMES, set())
            self.assertEqual(task_names, _ALWAYS_ON_TASK_NAMES)


if __name__ == "__main__":
    unittest.main()
