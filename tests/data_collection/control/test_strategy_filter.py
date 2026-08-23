from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.control.strategy_filter import discover_filter_profiles

_VALID = '''
FILTER_INFO = {
    "label": "Reward risk", "description": "desc",
    "config_schema": [{"name": "min_ratio", "type": "number", "label": "Min", "default": 1.5}],
}

def filter(context):
    return None
'''

_MISSING_FILTER = '''
FILTER_INFO = {"label": "x", "description": "y"}
'''

_ASYNC_FILTER = '''
FILTER_INFO = {"label": "x", "description": "y"}

async def filter(context):
    return None
'''

_WRONG_FUNCTION_NAME = '''
FILTER_INFO = {"label": "x", "description": "y"}

def compute(context):
    return None
'''

_CRASHES_ON_IMPORT = '''
raise RuntimeError("boom")
'''


class DiscoverFilterProfilesTests(unittest.TestCase):
    def test_missing_directory_returns_empty(self) -> None:
        self.assertEqual(discover_filter_profiles(Path("/nonexistent/filter/dir")), [])

    def test_valid_filter_profile_is_discovered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "reward_risk.py").write_text(_VALID, encoding="utf-8")
            found = discover_filter_profiles(root)
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0].id, "reward_risk")
            self.assertEqual(len(found[0].config_schema), 1)

    def test_missing_filter_function_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_MISSING_FILTER, encoding="utf-8")
            self.assertEqual(discover_filter_profiles(root), [])

    def test_async_filter_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_ASYNC_FILTER, encoding="utf-8")
            self.assertEqual(discover_filter_profiles(root), [])

    def test_wrong_function_name_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_WRONG_FUNCTION_NAME, encoding="utf-8")
            self.assertEqual(discover_filter_profiles(root), [])

    def test_a_crashing_file_does_not_hide_the_others(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "crashes.py").write_text(_CRASHES_ON_IMPORT, encoding="utf-8")
            (root / "good.py").write_text(_VALID, encoding="utf-8")
            found = discover_filter_profiles(root)
            self.assertEqual([f.id for f in found], ["good"])

    def test_hidden_files_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "_hidden.py").write_text(_VALID, encoding="utf-8")
            self.assertEqual(discover_filter_profiles(root), [])


if __name__ == "__main__":
    unittest.main()
