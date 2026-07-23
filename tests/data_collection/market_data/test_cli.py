import json
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.market_data.cli import main
from polymarket_btc.data_collection.market_data.health import write_health_file


class CliTests(unittest.TestCase):
    def test_validate_config(self) -> None:
        root = Path(__file__).resolve().parents[3]
        self.assertEqual(
            main(["validate-config", "--config", str(root / "config" / "market_data.toml")]),
            0,
        )

    def test_status_exit_codes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.json"
            self.assertEqual(main(["status", "--health-file", str(path)]), 2)
            write_health_file(path, {"ready": False})
            self.assertEqual(main(["status", "--health-file", str(path)]), 1)
            write_health_file(path, {"ready": True})
            self.assertEqual(main(["status", "--health-file", str(path)]), 0)


if __name__ == "__main__":
    unittest.main()
