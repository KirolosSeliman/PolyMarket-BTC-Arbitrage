from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.control.execution import discover_execution_profiles

_VALID = '''
EXECUTION_INFO = {
    "label": "Conservative", "description": "desc",
    "config_schema": [{"name": "max_position_usd", "type": "number", "label": "Max", "default": 500}],
}

def execute(context):
    return None
'''

_MISSING_EXECUTE = '''
EXECUTION_INFO = {"label": "x", "description": "y"}
'''

_ASYNC_EXECUTE = '''
EXECUTION_INFO = {"label": "x", "description": "y"}

async def execute(context):
    return None
'''

_WRONG_FUNCTION_NAME = '''
EXECUTION_INFO = {"label": "x", "description": "y"}

def compute(context):
    return None
'''

_CRASHES_ON_IMPORT = '''
raise RuntimeError("boom")
'''


class DiscoverExecutionProfilesTests(unittest.TestCase):
    def test_missing_directory_returns_empty(self) -> None:
        self.assertEqual(discover_execution_profiles(Path("/nonexistent/execution/dir")), [])

    def test_valid_execution_profile_is_discovered(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "conservative.py").write_text(_VALID, encoding="utf-8")
            found = discover_execution_profiles(root)
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0].id, "conservative")
            self.assertEqual(len(found[0].config_schema), 1)

    def test_missing_execute_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_MISSING_EXECUTE, encoding="utf-8")
            self.assertEqual(discover_execution_profiles(root), [])

    def test_async_execute_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_ASYNC_EXECUTE, encoding="utf-8")
            self.assertEqual(discover_execution_profiles(root), [])

    def test_wrong_function_name_is_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bad.py").write_text(_WRONG_FUNCTION_NAME, encoding="utf-8")
            self.assertEqual(discover_execution_profiles(root), [])

    def test_a_crashing_file_does_not_hide_the_others(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "crashes.py").write_text(_CRASHES_ON_IMPORT, encoding="utf-8")
            (root / "good.py").write_text(_VALID, encoding="utf-8")
            found = discover_execution_profiles(root)
            self.assertEqual([e.id for e in found], ["good"])

    def test_hidden_files_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "_hidden.py").write_text(_VALID, encoding="utf-8")
            self.assertEqual(discover_execution_profiles(root), [])


if __name__ == "__main__":
    unittest.main()
