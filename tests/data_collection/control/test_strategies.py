from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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

_FILTER = '''
FILTER_INFO = {
    "label": "Reward risk", "description": "desc",
    "config_schema": [{"name": "min_ratio", "type": "number", "label": "Min", "default": 1.5}],
}

def filter(context):
    return None
'''

_ALWAYS_VETO_FILTER = '''
FILTER_INFO = {"label": "Always veto", "description": "desc"}

def filter(context):
    return {"veto": True}
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
        (root / "filter_profiles").mkdir(parents=True, exist_ok=True)
        (root / "filter_profiles" / "reward_risk.py").write_text(_FILTER, encoding="utf-8")
        (root / "filter_profiles" / "always_veto.py").write_text(_ALWAYS_VETO_FILTER, encoding="utf-8")
        manager = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles", filter_dir=root / "filter_profiles",
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
                filter={"filter_id": "reward_risk", "config": {"min_ratio": 2.0}},
            )
            self.assertEqual(saved["concepts"][0]["config"], {"window": 20})
            self.assertEqual(saved["concepts"][0]["data_bindings"], {"Bougies": "binance_futures_kline"})
            self.assertEqual(saved["microsystems"][0]["data_bindings"], {"Bougies": "binance_futures_kline"})
            self.assertEqual(saved["execution"]["config"], {"max_position_usd": 1000})
            self.assertEqual(saved["management"]["management_id"], "fixed_sltp")
            self.assertEqual(saved["management"]["config"], {"stop_loss_pct": 2.5})
            self.assertEqual(saved["filter"]["filter_id"], "reward_risk")
            self.assertEqual(saved["filter"]["config"], {"min_ratio": 2.0})

            on_disk = json.loads((root / "strategies" / "my_strategy.json").read_text())
            self.assertEqual(on_disk["name"], "my_strategy")

            listed = strategies.list_strategies()
            self.assertEqual(len(listed), 1)
            self.assertEqual(listed[0]["name"], "my_strategy")
            self.assertEqual(listed[0]["concept_count"], 1)
            self.assertEqual(listed[0]["microsystem_count"], 1)
            self.assertTrue(listed[0]["has_execution"])
            self.assertTrue(listed[0]["has_management"])
            self.assertTrue(listed[0]["has_filter"])

    def test_filter_defaults_to_none_and_reports_has_filter_false(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategies = self._setup(root)
            saved = strategies.save_strategy(
                name="no_filter_strategy", concepts=[], microsystems=[],
                execution={"execution_id": "conservative", "config": {}},
                management={"management_id": "fixed_sltp", "config": {}},
            )
            self.assertIsNone(saved["filter"])
            listed = strategies.list_strategies()
            self.assertFalse(listed[0]["has_filter"])

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

    def test_unknown_filter_id_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.save_strategy(
                    name="x", concepts=[], microsystems=[],
                    execution=None, management=None, filter={"filter_id": "nope", "config": {}},
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

    def test_delete_strategy_removes_the_file_and_it_disappears_from_list(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            strategies.save_strategy(name="throwaway", concepts=[], microsystems=[], execution=None, management=None)
            self.assertIn("throwaway", [s["name"] for s in strategies.list_strategies()])

            strategies.delete_strategy("throwaway")

            self.assertNotIn("throwaway", [s["name"] for s in strategies.list_strategies()])
            self.assertIsNone(strategies.load_strategy("throwaway"))

    def test_delete_unknown_strategy_raises_file_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(FileNotFoundError):
                strategies.delete_strategy("nope")

    def test_delete_invalid_name_raises_value_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.delete_strategy("../outside")

    def test_preview_example_runs_a_saved_shaped_definition_against_synthetic_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            result = strategies.preview_example(
                concepts=[{"instance_id": "concept_1", "concept_id": "zscore", "config": {"window": 20}}],
                microsystems=[{
                    "instance_id": "micro_1", "microsystem_id": "trend",
                    "concept_instance_ids": ["concept_1"], "config": {},
                }],
                execution={"execution_id": "conservative", "config": {}},
                management={"management_id": "fixed_sltp", "config": {}},
            )
            self.assertEqual(set(result), {"price_path", "candles", "timeline", "trades", "evaluation_steps"})
            self.assertGreater(result["evaluation_steps"], 0)

    def test_preview_example_passes_filter_dir_and_filter_entry_through(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            captured: dict = {}

            def fake_run_example_scenario(**kwargs):
                captured.update(kwargs)
                return {"price_path": [], "candles": None, "timeline": [], "trades": [], "evaluation_steps": 0}

            with patch(
                "polymarket_btc.data_collection.control.strategies.run_example_scenario",
                side_effect=fake_run_example_scenario,
            ):
                strategies.preview_example(
                    concepts=[], microsystems=[], execution=None, management=None,
                    filter={"filter_id": "always_veto", "config": {}},
                )
            self.assertEqual(captured["filter_dir"], strategies.runs.filter_dir)
            self.assertEqual(captured["strategy"]["filter"]["filter_id"], "always_veto")

    def test_preview_example_seed_controls_the_synthetic_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            kwargs = dict(
                concepts=[{"instance_id": "concept_1", "concept_id": "zscore", "config": {}}],
                microsystems=[], execution=None, management=None,
            )
            default_seed = strategies.preview_example(**kwargs)
            same_seed = strategies.preview_example(seed=42, **kwargs)
            other_seed = strategies.preview_example(seed=7, **kwargs)
            self.assertEqual(default_seed["candles"], same_seed["candles"])
            self.assertNotEqual(default_seed["candles"], other_seed["candles"])

    def test_preview_example_validates_like_save_strategy_does(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            with self.assertRaises(ValueError):
                strategies.preview_example(
                    concepts=[{"instance_id": "c1", "concept_id": "nope", "config": {}}],
                    microsystems=[], execution=None, management=None,
                )

    def test_preview_example_does_not_write_a_strategy_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategies = self._setup(root)
            strategies.preview_example(
                concepts=[{"instance_id": "concept_1", "concept_id": "zscore", "config": {}}],
                microsystems=[], execution=None, management=None,
            )
            self.assertEqual(list((root / "strategies").glob("*.json")), [])

    def test_preview_example_searches_seeds_until_a_winning_trade_is_found(self) -> None:
        """An "example" is meant to show the strategy working, not a coin
        flip -- see StrategyManager.preview_example's own reasoning."""
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            losing = {"price_path": [], "candles": None, "timeline": [], "trades": [{"outcome": "loss"}], "evaluation_steps": 0}
            winning = {"price_path": [], "candles": None, "timeline": [], "trades": [{"outcome": "win"}], "evaluation_steps": 0}
            calls: list[int] = []

            def fake_run_example_scenario(*, seed, **_kwargs):
                calls.append(seed)
                return winning if seed == 44 else losing

            with patch(
                "polymarket_btc.data_collection.control.strategies.run_example_scenario",
                side_effect=fake_run_example_scenario,
            ):
                result = strategies.preview_example(
                    concepts=[], microsystems=[],
                    execution={"execution_id": "conservative", "config": {}},
                    management={"management_id": "fixed_sltp", "config": {}},
                    seed=42,
                )
            self.assertEqual(result, winning)
            # Stops the instant a win is found -- 42, 43, then 44 (the win),
            # never keeps searching past it.
            self.assertEqual(calls, [42, 43, 44])

    def test_preview_example_falls_back_to_the_last_attempt_if_no_win_is_ever_found(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            losing = {"price_path": [], "candles": None, "timeline": [], "trades": [{"outcome": "loss"}], "evaluation_steps": 0}
            with patch(
                "polymarket_btc.data_collection.control.strategies.run_example_scenario", return_value=losing,
            ) as mocked:
                result = strategies.preview_example(
                    concepts=[], microsystems=[],
                    execution={"execution_id": "conservative", "config": {}},
                    management={"management_id": "fixed_sltp", "config": {}},
                    seed=1,
                )
            self.assertEqual(result, losing)
            self.assertEqual(mocked.call_count, 30)

    def test_preview_example_skips_the_win_search_when_no_execution_is_configured(self) -> None:
        """A concept/microsystem-only preview never simulates trades at all
        (no execution profile to run) -- searching 30 seeds for a "win" that
        can structurally never happen would just be 30x the work for
        nothing, so this path takes a single, direct call instead."""
        with tempfile.TemporaryDirectory() as directory:
            strategies = self._setup(Path(directory))
            empty = {"price_path": [], "candles": None, "timeline": [], "trades": [], "evaluation_steps": 0}
            with patch(
                "polymarket_btc.data_collection.control.strategies.run_example_scenario", return_value=empty,
            ) as mocked:
                strategies.preview_example(concepts=[], microsystems=[], execution=None, management=None, seed=1)
            self.assertEqual(mocked.call_count, 1)

    def test_a_corrupt_strategy_file_is_skipped_not_fatal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategies = self._setup(root)
            strategies.save_strategy(name="good", concepts=[], microsystems=[], execution=None, management=None)
            (root / "strategies" / "corrupt.json").write_text("not json", encoding="utf-8")
            listed = strategies.list_strategies()
            self.assertEqual([s["name"] for s in listed], ["good"])


if __name__ == "__main__":
    unittest.main()
