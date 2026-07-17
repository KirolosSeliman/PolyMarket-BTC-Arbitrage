from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from tests.data_collection.market_discovery.fixtures import (
    btc_5m_payload,
    iso_z,
    payload_for_window,
)

from polymarket_btc.data_collection.market_discovery.cli import main


class CliTests(unittest.TestCase):
    def test_validate_config_outputs_json_success(self) -> None:
        stdout = io.StringIO()

        exit_code = main(
            [
                "--config",
                "config/data_collection/market_discovery.yaml",
                "--validate-config",
                "--json",
            ],
            stdout=stdout,
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {"status": "config_valid"})

    def test_validate_config_reports_invalid_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "invalid.yaml"
            path.write_text(yaml.safe_dump({"version": 2, "market_discovery": {}}), encoding="utf-8")
            stderr = io.StringIO()

            exit_code = main(["--config", str(path), "--validate-config"], stderr=stderr)

        self.assertEqual(exit_code, 2)
        self.assertIn("version", stderr.getvalue())

    def test_once_with_fixture_outputs_selected_market_json(self) -> None:
        start = datetime(2026, 7, 17, 19, 50, tzinfo=UTC)
        next_start = start + timedelta(minutes=5)
        fixture = [
            payload_for_window(start, "current"),
            payload_for_window(next_start, "next"),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "markets.json"
            path.write_text(json.dumps(fixture), encoding="utf-8")
            stdout = io.StringIO()

            exit_code = main(
                [
                    "--config",
                    "config/data_collection/market_discovery.yaml",
                    "--once",
                    "--fixture",
                    str(path),
                    "--now",
                    "2026-07-17T19:51:00Z",
                    "--json",
                ],
                stdout=stdout,
            )

        body = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(body["status"], "selected")
        self.assertEqual(body["selected_market"]["market_id"], "current")
        self.assertEqual(body["next_market"]["market_id"], "next")

    def test_once_with_fixture_no_match_returns_nonzero(self) -> None:
        start = datetime(2026, 7, 17, 19, 50, tzinfo=UTC)
        fixture = [btc_5m_payload(eventStartTime=iso_z(start), endDate=iso_z(start + timedelta(minutes=5)))]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "markets.json"
            path.write_text(json.dumps(fixture), encoding="utf-8")
            stdout = io.StringIO()

            exit_code = main(
                [
                    "--config",
                    "config/data_collection/market_discovery.yaml",
                    "--once",
                    "--fixture",
                    str(path),
                    "--now",
                    "2026-07-17T19:49:59.999000Z",
                    "--json",
                ],
                stdout=stdout,
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(json.loads(stdout.getvalue())["status"], "no_match")

    def test_invalid_fixture_returns_clean_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "markets.json"
            path.write_text("not-json", encoding="utf-8")
            stderr = io.StringIO()

            exit_code = main(
                [
                    "--config",
                    "config/data_collection/market_discovery.yaml",
                    "--once",
                    "--fixture",
                    str(path),
                    "--json",
                ],
                stderr=stderr,
            )

        self.assertEqual(exit_code, 2)
        self.assertIn("fixture error", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
