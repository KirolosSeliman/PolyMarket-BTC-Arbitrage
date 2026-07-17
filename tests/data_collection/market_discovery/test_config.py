from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from polymarket_btc.data_collection.market_discovery.config import ConfigError, load_config


VALID_CONFIG = {
    "version": 1,
    "market_discovery": {
        "gamma_base_url": "https://gamma-api.polymarket.com",
        "request_timeout_seconds": 3,
        "max_retries": 1,
        "retry_delay_seconds": 0.5,
    },
}


class ConfigTests(unittest.TestCase):
    def write_config(self, value: dict) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        path = Path(temp_dir.name) / "market_discovery.yaml"
        path.write_text(yaml.safe_dump(value), encoding="utf-8")
        return path

    def test_loads_minimal_runtime_config(self) -> None:
        config = load_config(self.write_config(VALID_CONFIG))

        self.assertEqual(config.gamma_base_url, "https://gamma-api.polymarket.com")
        self.assertEqual(config.request_timeout_seconds, 3)
        self.assertEqual(config.max_retries, 1)
        self.assertEqual(config.retry_delay_seconds, 0.5)

    def test_rejects_unknown_key(self) -> None:
        invalid = {
            "version": 1,
            "market_discovery": {
                **VALID_CONFIG["market_discovery"],
                "selection": {"reject_closed": False},
            },
        }

        with self.assertRaisesRegex(ConfigError, "unknown market_discovery key"):
            load_config(self.write_config(invalid))

    def test_rejects_non_https_url(self) -> None:
        invalid = {
            "version": 1,
            "market_discovery": {
                **VALID_CONFIG["market_discovery"],
                "gamma_base_url": "http://gamma-api.polymarket.com",
            },
        }

        with self.assertRaisesRegex(ConfigError, "HTTPS"):
            load_config(self.write_config(invalid))

    def test_rejects_invalid_timeout(self) -> None:
        invalid = {
            "version": 1,
            "market_discovery": {
                **VALID_CONFIG["market_discovery"],
                "request_timeout_seconds": 0,
            },
        }

        with self.assertRaisesRegex(ConfigError, "request_timeout_seconds"):
            load_config(self.write_config(invalid))

    def test_rejects_invalid_retry_count(self) -> None:
        invalid = {
            "version": 1,
            "market_discovery": {
                **VALID_CONFIG["market_discovery"],
                "max_retries": -1,
            },
        }

        with self.assertRaisesRegex(ConfigError, "max_retries"):
            load_config(self.write_config(invalid))


if __name__ == "__main__":
    unittest.main()
