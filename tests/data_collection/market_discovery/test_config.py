from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from polymarket_btc.data_collection.market_discovery.config import (  # noqa: E402
    ConfigError,
    load_market_discovery_config,
)


VALID_CONFIG = {
    "version": 1,
    "market_discovery": {
        "provider": {
            "name": "polymarket",
            "gamma_base_url": "https://gamma-api.polymarket.com",
            "slug_template": "btc-updown-5m-{start_epoch}",
            "search_query": "bitcoin up down 5m",
        },
        "target": {
            "asset": "BTC",
            "market_family": "up_down",
            "duration_seconds": 300,
            "expected_outcomes": ["Up", "Down"],
            "resolution_source": "https://data.chain.link/streams/btc-usd",
        },
        "polling": {
            "interval_seconds": 15,
            "request_timeout_seconds": 10,
            "max_retries": 3,
            "retry_base_delay_seconds": 1,
            "retry_max_delay_seconds": 15,
            "preload_next_market": True,
        },
        "selection": {
            "require_unique_active_match": True,
            "require_condition_id": True,
            "require_token_ids": True,
            "require_order_book": True,
            "reject_missing_timestamps": True,
            "reject_unknown_outcomes": True,
            "reject_ambiguous_token_mapping": True,
            "reject_closed": True,
            "reject_archived": True,
        },
        "failure_policy": {
            "no_match": "retry",
            "multiple_matches": "reject",
            "invalid_candidate": "reject",
        },
    },
}


class MarketDiscoveryConfigTests(unittest.TestCase):
    def write_config(self, config: dict) -> Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        path = Path(temp_dir.name) / "market_discovery.yaml"
        path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
        return path

    def test_loads_valid_market_discovery_config(self) -> None:
        config = load_market_discovery_config(self.write_config(VALID_CONFIG))

        self.assertEqual(config.version, 1)
        self.assertEqual(config.provider.name, "polymarket")
        self.assertEqual(config.provider.gamma_base_url, "https://gamma-api.polymarket.com")
        self.assertEqual(config.target.asset, "BTC")
        self.assertEqual(config.target.expected_outcomes, ("Up", "Down"))
        self.assertEqual(config.polling.interval_seconds, 15)
        self.assertTrue(config.selection.require_order_book)
        self.assertEqual(config.failure_policy.multiple_matches, "reject")

    def test_rejects_missing_market_discovery_section(self) -> None:
        invalid = {"version": 1}

        with self.assertRaisesRegex(ConfigError, "market_discovery"):
            load_market_discovery_config(self.write_config(invalid))

    def test_rejects_unknown_top_level_keys(self) -> None:
        invalid = {**VALID_CONFIG, "unexpected": True}

        with self.assertRaisesRegex(ConfigError, "unknown top-level key"):
            load_market_discovery_config(self.write_config(invalid))

    def test_rejects_non_five_minute_duration(self) -> None:
        invalid = yaml.safe_load(yaml.safe_dump(VALID_CONFIG))
        invalid["market_discovery"]["target"]["duration_seconds"] = 900

        with self.assertRaisesRegex(ConfigError, "duration_seconds"):
            load_market_discovery_config(self.write_config(invalid))

    def test_rejects_non_https_gamma_base_url(self) -> None:
        invalid = yaml.safe_load(yaml.safe_dump(VALID_CONFIG))
        invalid["market_discovery"]["provider"]["gamma_base_url"] = "http://gamma-api.polymarket.com"

        with self.assertRaisesRegex(ConfigError, "HTTPS"):
            load_market_discovery_config(self.write_config(invalid))

    def test_rejects_invalid_polling_values(self) -> None:
        invalid = yaml.safe_load(yaml.safe_dump(VALID_CONFIG))
        invalid["market_discovery"]["polling"]["interval_seconds"] = 0

        with self.assertRaisesRegex(ConfigError, "interval_seconds"):
            load_market_discovery_config(self.write_config(invalid))


if __name__ == "__main__":
    unittest.main()
