from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from polymarket_btc.data_collection.market_discovery.cli import main
from polymarket_btc.data_collection.market_discovery.config import MarketDiscoveryConfig
from polymarket_btc.data_collection.market_discovery.models import DiscoveredMarket, DiscoveryResult, DiscoveryStatus


class CliTests(unittest.TestCase):
    def make_market(self) -> DiscoveredMarket:
        start = datetime(2026, 7, 17, 19, 50, tzinfo=UTC)
        return DiscoveredMarket(
            market_id="m1",
            condition_id="0xcondition",
            slug="btc-updown-5m-1784317800",
            start_time_utc=start,
            end_time_utc=start + timedelta(minutes=5),
            up_token_id="up",
            down_token_id="down",
            resolution_source="https://data.chain.link/streams/btc-usd",
        )

    def test_selected_json_output_is_concise(self) -> None:
        market = self.make_market()
        stdout = io.StringIO()

        exit_code = main(
            ["--json"],
            stdout=stdout,
            discover=lambda config: DiscoveryResult(DiscoveryStatus.SELECTED, market=market),
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "status": "selected",
                "market": {
                    "market_id": "m1",
                    "condition_id": "0xcondition",
                    "slug": "btc-updown-5m-1784317800",
                    "start_time_utc": "2026-07-17T19:50:00Z",
                    "end_time_utc": "2026-07-17T19:55:00Z",
                    "up_token_id": "up",
                    "down_token_id": "down",
                    "resolution_source": "https://data.chain.link/streams/btc-usd",
                },
                "reason": None,
            },
        )

    def test_no_match_exit_code(self) -> None:
        stdout = io.StringIO()

        exit_code = main(
            ["--json"],
            stdout=stdout,
            discover=lambda config: DiscoveryResult(DiscoveryStatus.NO_MATCH, reason="no_market_at_expected_slug"),
        )

        self.assertEqual(exit_code, 1)
        self.assertEqual(json.loads(stdout.getvalue())["status"], "no_match")

    def test_provider_unavailable_exit_code(self) -> None:
        stdout = io.StringIO()

        exit_code = main(
            ["--json"],
            stdout=stdout,
            discover=lambda config: DiscoveryResult(DiscoveryStatus.PROVIDER_UNAVAILABLE, reason="timeout"),
        )

        self.assertEqual(exit_code, 2)
        self.assertEqual(json.loads(stdout.getvalue())["status"], "provider_unavailable")

    def test_invalid_config_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "invalid.yaml"
            path.write_text(yaml.safe_dump({"version": 2, "market_discovery": {}}), encoding="utf-8")
            stderr = io.StringIO()

            exit_code = main(["--config", str(path), "--validate-config"], stderr=stderr)

        self.assertEqual(exit_code, 2)
        self.assertIn("config error", stderr.getvalue())

    def test_validate_config_success(self) -> None:
        stdout = io.StringIO()

        exit_code = main(["--validate-config", "--json"], stdout=stdout)

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {"status": "config_valid"})

    def test_cli_without_config_uses_builtin_defaults_from_any_cwd(self) -> None:
        stdout = io.StringIO()
        seen_configs: list[MarketDiscoveryConfig] = []
        original_cwd = Path.cwd()

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                os.chdir(temp_dir)
                exit_code = main(
                    ["--json"],
                    stdout=stdout,
                    discover=lambda config: seen_configs.append(config)
                    or DiscoveryResult(DiscoveryStatus.SELECTED, market=self.make_market()),
                )
            finally:
                os.chdir(original_cwd)

        self.assertEqual(exit_code, 0)
        self.assertEqual(seen_configs[0].gamma_base_url, "https://gamma-api.polymarket.com")

    def test_cli_with_explicit_config_loads_yaml_override(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "override.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "version": 1,
                        "market_discovery": {
                            "gamma_base_url": "https://gamma.example.com",
                            "request_timeout_seconds": 7,
                            "max_retries": 2,
                            "retry_delay_seconds": 1.25,
                        },
                    }
                ),
                encoding="utf-8",
            )
            seen_configs: list[MarketDiscoveryConfig] = []

            exit_code = main(
                ["--config", str(path), "--json"],
                stdout=io.StringIO(),
                discover=lambda config: seen_configs.append(config)
                or DiscoveryResult(DiscoveryStatus.SELECTED, market=self.make_market()),
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(seen_configs[0].gamma_base_url, "https://gamma.example.com")
        self.assertEqual(seen_configs[0].request_timeout_seconds, 7.0)


if __name__ == "__main__":
    unittest.main()
