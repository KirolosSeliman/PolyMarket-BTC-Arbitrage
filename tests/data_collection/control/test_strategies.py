from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.control.runs import CollectionRunManager
from polymarket_btc.data_collection.control.strategies import StrategyManager

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

_CONCEPT = '''
CONCEPT_INFO = {
    "label": "Z-score", "description": "desc",
    "data_sources": ["binance_futures_kline"],
    "config_schema": [{"name": "window", "type": "number", "label": "Window", "default": 10}],
}

def compute(context):
    return None
'''

_MICROSYSTEM = '''
MICROSYSTEM_INFO = {
    "label": "Trend", "description": "desc",
    "concept_inputs": ["zscore"],
    "data_inputs": ["binance_futures_kline"],
}

def compute(context):
    return None
'''

_EXECUTION = '''
EXECUTION_INFO = {
    "label": "Conservative", "description": "desc",
    "config_schema": [{"name": "max_position_usd", "type": "number", "label": "Max", "default": 500}],
}

def execute(context):
    return None
'''

_MANAGEMENT = '''
MANAGEMENT_INFO = {
    "label": "Fixed SL/TP", "description": "desc",
    "config_schema": [{"name": "stop_loss_pct", "type": "number", "label": "SL %", "default": 1.0}],
}

def manage(context):
    return None
'''


def _seeded_symbol_cache(root: Path) -> Path:
    cache_path = root / "cache" / "binance_symbols.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({
        "spot": ["BTC", "ETH"], "futures": ["BTC", "ETH"], "fetched_at_utc": datetime.now(UTC).isoformat(),
    }))
    return cache_path


class StrategyManagerTests(unittest.TestCase):
    def _setup(self, root: Path) -> StrategyManager:
        (root / "concepts").mkdir(parents=True, exist_ok=True)
        (root / "concepts" / "zscore.py").write_text(_CONCEPT, encoding="utf-8")
        (root / "microsystems").mkdir(parents=True, exist_ok=True)
        (root / "microsystems" / "trend.py").write_text(_MICROSYSTEM, encoding="utf-8")
        (root / "execution_profiles").mkdir(parents=True, exist_ok=True)
        (root / "execution_profiles" / "conservative.py").write_text(_EXECUTION, encoding="utf-8")
        (root / "management_profiles").mkdir(parents=True, exist_ok=True)
        (root / "management_profiles" / "fixed_sltp.py").write_text(_MANAGEMENT, encoding="utf-8")
        manager = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
        )
        return StrategyManager(strategies_dir=root / "strategies", runs=manager)

    def test_round_trip_save_and_list(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategies = self._setup(root)
            saved = strategies.save_strategy(
                name="my_strategy",
                concepts=[{"instance_id": "concept_1", "concept_id": "zscore", "config": {"window": 20}}],
                microsystems=[{
                    "instance_id": "micro_1", "microsystem_id": "trend",
                    "concept_instance_ids": ["concept_1"], "config": {},
                }],
                execution={"execution_id": "conservative", "config": {"max_position_usd": 1000}},
                management={"management_id": "fixed_sltp", "config": {"stop_loss_pct": 2.5}},
            )
            self.assertEqual(saved["concepts"][0]["config"], {"window": 20})
            self.assertEqual(saved["concepts"][0]["data_bindings"], {"Bougies": "binance_futures_kline"})
            self.assertEqual(saved["microsystems"][0]["data_bindings"], {"Bougies": "binance_futures_kline"})
            self.assertEqual(saved["execution"]["config"], {"max_position_usd": 1000})
            self.assertEqual(saved["management"]["management_id"], "fixed_sltp")
            self.assertEqual(saved["management"]["config"], {"stop_loss_pct": 2.5})

            on_disk = json.loads((root / "strategies" / "my_strategy.json").read_text())
            self.assertEqual(on_disk["name"], "my_strategy")

            listed = strategies.list_strategies()
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0]["name"], "my_strategy")
            self.assertEqual(listed[0]["concept_count"], 1)
            self.assertEqual(listed[0]["microsystem_count"], 1)
            self.assertTrue(listed[0]["has_execution"])
            self.assertTrue(listed[0]["has_management"])

    def test_unknown_concept_id_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x", concepts=[{"instance_id": "c1", "concept_id": "nope", "config": {}}],
                    microsystems=[], execution=None, management=None,
                )

    def test_unknown_microsystem_id_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x", concepts=[],
                    microsystems=[{
                        "instance_id": "m1", "microsystem_id": "nope",
                        "concept_instance_ids": [], "data_sources": [], "config": {},
                    }],
                    execution=None,
                    management=None,
                )

    def test_unknown_execution_id_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x", concepts=[], microsystems=[],
                    execution={"execution_id": "nope", "config": {}}, management=None,
                )

    def test_unknown_management_id_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x", concepts=[], microsystems=[],
                    execution=None, management={"management_id": "nope", "config": {}},
                )

    def test_duplicate_instance_id_across_concepts_and_microsystems_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x",
                    concepts=[{"instance_id": "dup", "concept_id": "zscore", "config": {}}],
                    microsystems=[{
                        "instance_id": "dup", "microsystem_id": "trend",
                        "concept_instance_ids": [], "data_sources": [], "config": {},
                    }],
                    execution=None,
                    management=None,
                )

    def test_microsystem_referencing_unknown_concept_instance_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x", concepts=[],
                    microsystems=[{
                        "instance_id": "m1", "microsystem_id": "trend",
                        "concept_instance_ids": ["ghost"], "data_sources": [], "config": {},
                    }],
                    execution=None,
                    management=None,
                )

    def test_microsystem_data_binding_to_unknown_key_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x", concepts=[],
                    microsystems=[{
                        "instance_id": "m1", "microsystem_id": "trend",
                        "concept_instance_ids": [], "config": {},
                        "data_bindings": {"Bougies": "not_a_real_source"},
                    }],
                    execution=None,
                    management=None,
                )

    def test_microsystem_data_binding_wrong_type_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x", concepts=[],
                    microsystems=[{
                        "instance_id": "m1", "microsystem_id": "trend",
                        "concept_instance_ids": [], "config": {},
                        "data_bindings": {"Bougies": "chainlink"},
                    }],
                    execution=None,
                    management=None,
                )

    def test_concept_data_binding_defaults_and_override(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            saved = strategies.save_strategy(
                name="x", concepts=[{"instance_id": "c1", "concept_id": "zscore", "config": {}}],
                microsystems=[], execution=None, management=None,
            )
            self.assertEqual(saved["concepts"][0]["data_bindings"], {"Bougies": "binance_futures_kline"})

            saved2 = strategies.save_strategy(
                name="y", concepts=[{
                    "instance_id": "c1", "concept_id": "zscore", "config": {},
                    "data_bindings": {"Bougies": "binance_futures_kline:ETH"},
                }],
                microsystems=[], execution=None, management=None,
            )
            self.assertEqual(saved2["concepts"][0]["data_bindings"], {"Bougies": "binance_futures_kline:ETH"})

    def test_duplicate_name_without_overwrite_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            strategies.save_strategy(name="dup", concepts=[], microsystems=[], execution=None, management=None)
            with self.assertRaises(FileExistsError):
                strategies.save_strategy(name="dup", concepts=[], microsystems=[], execution=None, management=None)
            # overwrite=True succeeds.
            strategies.save_strategy(name="dup", concepts=[], microsystems=[], execution=None, management=None, overwrite=True)

    def test_invalid_name_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(name="not valid!", concepts=[], microsystems=[], execution=None, management=None)

    def test_a_corrupt_strategy_file_is_skipped_not_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategies = self._setup(root)
            strategies.save_strategy(name="good", concepts=[], microsystems=[], execution=None, management=None)
            (root / "strategies" / "corrupt.json").write_text("not json", encoding="utf-8")
            listed = strategies.list_strategies()
            self.assertEqual([s["name"] for s in listed], ["good"])


class RepoSeedStrategyTests(unittest.TestCase):
    """The real strategies/model_base_polymarket.json committed at the repo
    root -- must load cleanly and stay an honest "nothing configured yet"
    placeholder."""

    def test_model_base_polymarket_is_discoverable_and_empty(self) -> None:
        manager = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=REPOSITORY_ROOT / "data" / "collections",
            plugins_dir=REPOSITORY_ROOT / "plugins",
            concepts_dir=REPOSITORY_ROOT / "concepts",
            microsystems_dir=REPOSITORY_ROOT / "microsystems",
            execution_dir=REPOSITORY_ROOT / "execution_profiles",
            management_dir=REPOSITORY_ROOT / "management_profiles",
            symbol_cache_path=REPOSITORY_ROOT / "data" / "cache" / "binance_symbols.json",
        )
        strategies = StrategyManager(strategies_dir=REPOSITORY_ROOT / "strategies", runs=manager)
        listed = {s["name"]: s for s in strategies.list_strategies()}
        self.assertIn("model_base_polymarket", listed)
        entry = listed["model_base_polymarket"]
        self.assertEqual(entry["concept_count"], 0)
        self.assertEqual(entry["microsystem_count"], 0)
        self.assertFalse(entry["has_execution"])
        self.assertFalse(entry["has_management"])


if __name__ == "__main__":
    unittest.main()
