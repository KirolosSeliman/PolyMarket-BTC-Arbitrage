from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

import scripts.clob_token_smoke as clob_token_smoke


class ClobSmokeTests(unittest.TestCase):
    def test_imports_and_uses_builtin_default_config_without_path(self) -> None:
        config = clob_token_smoke.resolve_config(None)

        self.assertEqual(config.gamma_base_url, "https://gamma-api.polymarket.com")
        self.assertEqual(config.request_timeout_seconds, 3.0)

    def test_explicit_config_override_is_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "override.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "version": 1,
                        "market_discovery": {
                            "gamma_base_url": "https://gamma.example.com",
                            "request_timeout_seconds": 5,
                            "max_retries": 0,
                            "retry_delay_seconds": 0,
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = clob_token_smoke.resolve_config(str(path))

        self.assertEqual(config.gamma_base_url, "https://gamma.example.com")
        self.assertEqual(config.max_retries, 0)


if __name__ == "__main__":
    unittest.main()
