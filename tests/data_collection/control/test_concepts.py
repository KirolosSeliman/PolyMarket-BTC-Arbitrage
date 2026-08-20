from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.control.concepts import discover_concepts

_VALID = '''
CONCEPT_INFO = {
    "label": "Z-score", "description": "desc",
    "data_sources": ["binance_futures_kline"],
    "config_schema": [{"name": "window", "type": "number", "label": "Window", "default": 10}],
}

def compute(context):
    return None
'''

_WITH_DETAIL_AND_CATEGORY = '''
CONCEPT_INFO = {
    "label": "Detailed", "description": "desc", "category": "Momentum",
    "detail": "a longer explanation", "data_sources": ["binance_futures_kline"],
}

def compute(context):
    return None
'''

_MISSING_INFO = '''
def compute(context):
    return None
'''

_MISSING_COMPUTE = '''
CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}
'''

_ASYNC_COMPUTE = '''
CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}

async def compute(context):
    return None
'''

_EMPTY_DATA_SOURCES = '''
CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": []}

def compute(context):
    return None
'''

_MISSING_LABEL = '''
CONCEPT_INFO = {"description": "y", "data_sources": ["binance_futures_kline"]}

def compute(context):
    return None
'''

_BAD_CONFIG_SCHEMA = '''
CONCEPT_INFO = {
    "label": "x", "description": "y", "data_sources": ["binance_futures_kline"],
    "config_schema": [{"name": "x"}],
}

def compute(context):
    return None
'''

_CRASHES_ON_IMPORT = '''
raise RuntimeError("boom")
'''

_WITH_LOOKBACK = '''
CONCEPT_INFO = {
    "label": "Windowed", "description": "desc", "data_sources": ["binance_futures_trade"],
}

def required_lookback_seconds(config):
    return config.get("window_seconds", 60)

def compute(context):
    return None
'''

_WITH_ASYNC_LOOKBACK = '''
CONCEPT_INFO = {
    "label": "Bad hook", "description": "desc", "data_sources": ["binance_futures_trade"],
}

async def required_lookback_seconds(config):
    return 60

def compute(context):
    return None
'''


class DiscoverConceptsTests(unittest.TestCase):
    def test_missing_directory_returns_empty(self) -> None:
        self.assertEqual(discover_concepts(Path("/nonexistent/concepts/dir")), [])

    def test_valid_concept_is_discovered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "zscore.py").write_text(_VALID, encoding="utf-8")
            found = discover_concepts(root)
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0].id, "zscore")
            self.assertEqual(found[0].label, "Z-score")
            self.assertEqual(found[0].category, "Général")
            self.assertIsNone(found[0].detail)
            self.assertEqual(found[0].data_sources, ("binance_futures_kline",))
            self.assertEqual(len(found[0].config_schema), 1)

    def test_category_and_detail_are_read_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "detailed.py").write_text(_WITH_DETAIL_AND_CATEGORY, encoding="utf-8")
            found = discover_concepts(root)
            self.assertEqual(found[0].category, "Momentum")
            self.assertEqual(found[0].detail, "a longer explanation")

    def test_missing_concept_info_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_MISSING_INFO, encoding="utf-8")
            self.assertEqual(discover_concepts(root), [])

    def test_missing_compute_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_MISSING_COMPUTE, encoding="utf-8")
            self.assertEqual(discover_concepts(root), [])

    def test_async_compute_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_ASYNC_COMPUTE, encoding="utf-8")
            self.assertEqual(discover_concepts(root), [])

    def test_empty_data_sources_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_EMPTY_DATA_SOURCES, encoding="utf-8")
            self.assertEqual(discover_concepts(root), [])

    def test_missing_label_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_MISSING_LABEL, encoding="utf-8")
            self.assertEqual(discover_concepts(root), [])

    def test_malformed_config_schema_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_BAD_CONFIG_SCHEMA, encoding="utf-8")
            self.assertEqual(discover_concepts(root), [])

    def test_a_crashing_file_does_not_hide_the_others(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "crashes.py").write_text(_CRASHES_ON_IMPORT, encoding="utf-8")
            (root / "good.py").write_text(_VALID, encoding="utf-8")
            found = discover_concepts(root)
            self.assertEqual([c.id for c in found], ["good"])

    def test_concept_without_required_lookback_seconds_has_none(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "zscore.py").write_text(_VALID, encoding="utf-8")
            found = discover_concepts(root)
            self.assertIsNone(found[0].required_lookback_seconds)

    def test_required_lookback_seconds_is_picked_up_and_callable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "windowed.py").write_text(_WITH_LOOKBACK, encoding="utf-8")
            found = discover_concepts(root)
            self.assertIsNotNone(found[0].required_lookback_seconds)
            self.assertEqual(found[0].required_lookback_seconds({"window_seconds": 120}), 120)

    def test_async_required_lookback_seconds_is_ignored_not_fatal(self) -> None:
        """An async hook can't be awaited by the (sync) backtest walk -- the
        concept itself still loads normally, just without the optimization,
        same graceful-degradation spirit as a missing/failing hook."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad_hook.py").write_text(_WITH_ASYNC_LOOKBACK, encoding="utf-8")
            found = discover_concepts(root)
            self.assertEqual(len(found), 1)
            self.assertIsNone(found[0].required_lookback_seconds)

    def test_hidden_files_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "_hidden.py").write_text(_VALID, encoding="utf-8")
            self.assertEqual(discover_concepts(root), [])


if __name__ == "__main__":
    unittest.main()
