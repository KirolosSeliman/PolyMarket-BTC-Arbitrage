from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.market_data.config import (
    ConfigurationError,
    load_config,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


class ConfigTests(unittest.TestCase):
    def test_repository_config_is_valid_and_resolves_data_dir(self) -> None:
        config = load_config(REPOSITORY_ROOT / "config" / "market_data.toml")
        self.assertEqual(config.binance.symbol, "btcusdt")
        self.assertEqual(config.binance.depth_stream, "btcusdt@depth20@100ms")
        self.assertTrue(config.storage.data_dir.is_absolute())
        self.assertEqual(config.queues.state_capacity, 50_000)

    def test_unknown_key_is_rejected(self) -> None:
        text = (REPOSITORY_ROOT / "config" / "market_data.toml").read_text()
        text = text.replace('log_level = "INFO"', 'log_level = "INFO"\nunknown = 1')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(text)
            with self.assertRaises(ConfigurationError):
                load_config(path)

    def test_non_wss_url_is_rejected(self) -> None:
        text = (REPOSITORY_ROOT / "config" / "market_data.toml").read_text()
        text = text.replace(
            "wss://ws-live-data.polymarket.com",
            "https://ws-live-data.polymarket.com",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(text)
            with self.assertRaises(ConfigurationError):
                load_config(path)

    def test_custom_endpoint_requires_explicit_opt_in(self) -> None:
        text = (REPOSITORY_ROOT / "config" / "market_data.toml").read_text()
        text = text.replace(
            "wss://ws-live-data.polymarket.com",
            "wss://localhost:8765",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(text)
            with self.assertRaises(ConfigurationError):
                load_config(path)
            self.assertEqual(
                load_config(path, allow_custom_endpoints=True).rtds.url,
                "wss://localhost:8765",
            )


if __name__ == "__main__":
    unittest.main()
