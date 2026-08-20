from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.control.microsystems import discover_microsystems

_VALID_WITH_CONCEPTS = '''
MICROSYSTEM_INFO = {
    "label": "Trend", "description": "desc",
    "concept_inputs": ["funding_zscore"],
}

def compute(context):
    return None
'''

_VALID_WITH_DATA = '''
MICROSYSTEM_INFO = {
    "label": "Direct", "description": "desc",
    "data_inputs": ["binance_futures_kline"],
}

def compute(context):
    return None
'''

_BOTH_EMPTY = '''
MICROSYSTEM_INFO = {"label": "x", "description": "y"}

def compute(context):
    return None
'''

_ASYNC_COMPUTE = '''
MICROSYSTEM_INFO = {"label": "x", "description": "y", "concept_inputs": ["a"]}

async def compute(context):
    return None
'''

_MISSING_COMPUTE = '''
MICROSYSTEM_INFO = {"label": "x", "description": "y", "concept_inputs": ["a"]}
'''

_CRASHES_ON_IMPORT = '''
raise RuntimeError("boom")
'''


class DiscoverMicrosystemsTests(unittest.TestCase):
    def test_missing_directory_returns_empty(self) -> None:
        self.assertEqual(discover_microsystems(Path("/nonexistent/microsystems/dir")), [])

    def test_valid_microsystem_with_concept_inputs_is_discovered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "trend.py").write_text(_VALID_WITH_CONCEPTS, encoding="utf-8")
            found = discover_microsystems(root)
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0].concept_inputs, ("funding_zscore",))
            self.assertEqual(found[0].data_inputs, ())
            self.assertEqual(found[0].category, "Général")

    def test_valid_microsystem_with_only_data_inputs_is_discovered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "direct.py").write_text(_VALID_WITH_DATA, encoding="utf-8")
            found = discover_microsystems(root)
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0].data_inputs, ("binance_futures_kline",))

    def test_both_inputs_empty_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_BOTH_EMPTY, encoding="utf-8")
            self.assertEqual(discover_microsystems(root), [])

    def test_async_compute_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_ASYNC_COMPUTE, encoding="utf-8")
            self.assertEqual(discover_microsystems(root), [])

    def test_missing_compute_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_MISSING_COMPUTE, encoding="utf-8")
            self.assertEqual(discover_microsystems(root), [])

    def test_a_crashing_file_does_not_hide_the_others(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "crashes.py").write_text(_CRASHES_ON_IMPORT, encoding="utf-8")
            (root / "good.py").write_text(_VALID_WITH_CONCEPTS, encoding="utf-8")
            found = discover_microsystems(root)
            self.assertEqual([m.id for m in found], ["good"])

    def test_hidden_files_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "_hidden.py").write_text(_VALID_WITH_CONCEPTS, encoding="utf-8")
            self.assertEqual(discover_microsystems(root), [])

    def test_microsystem_without_required_lookback_seconds_has_none(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "direct.py").write_text(_VALID_WITH_DATA, encoding="utf-8")
            found = discover_microsystems(root)
            self.assertIsNone(found[0].required_lookback_seconds)

    def test_required_lookback_seconds_is_picked_up_and_callable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "windowed.py").write_text(
                _VALID_WITH_DATA + '\n\ndef required_lookback_seconds(config):\n    return 300\n',
                encoding="utf-8",
            )
            found = discover_microsystems(root)
            self.assertIsNotNone(found[0].required_lookback_seconds)
            self.assertEqual(found[0].required_lookback_seconds({}), 300)


if __name__ == "__main__":
    unittest.main()
