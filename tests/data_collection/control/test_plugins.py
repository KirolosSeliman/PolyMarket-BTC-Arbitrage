import asyncio
from pathlib import Path
import tempfile
import unittest

from polymarket_btc.data_collection.control.plugins import (
    PluginContext,
    discover_plugins,
    run_plugin,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

_VALID_PLUGIN = '''
PLUGIN_INFO = {"label": "Valide", "description": "un plugin de test"}

async def run(context):
    context.log("ran")
'''

_MISSING_INFO_PLUGIN = '''
async def run(context):
    pass
'''

_NOT_ASYNC_PLUGIN = '''
PLUGIN_INFO = {"label": "Pas async", "description": "..."}

def run(context):
    pass
'''

_BROKEN_PLUGIN = "this is not valid python syntax !!!"

_CRASHING_PLUGIN = '''
PLUGIN_INFO = {"label": "Plante", "description": "..."}

async def run(context):
    raise RuntimeError("boom")
'''


class DiscoverPluginsTests(unittest.TestCase):
    def test_missing_directory_returns_empty(self) -> None:
        self.assertEqual(discover_plugins(Path("/no/such/dir")), [])

    def test_discovers_only_well_formed_plugins(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "good.py").write_text(_VALID_PLUGIN, encoding="utf-8")
            (root / "missing_info.py").write_text(_MISSING_INFO_PLUGIN, encoding="utf-8")
            (root / "not_async.py").write_text(_NOT_ASYNC_PLUGIN, encoding="utf-8")
            (root / "broken.py").write_text(_BROKEN_PLUGIN, encoding="utf-8")
            (root / "_hidden.py").write_text(_VALID_PLUGIN, encoding="utf-8")
            (root / "readme.txt").write_text("not python", encoding="utf-8")

            found = discover_plugins(root)
            self.assertEqual([info.id for info in found], ["good"])
            self.assertEqual(found[0].label, "Valide")
            self.assertEqual(found[0].description, "un plugin de test")
            self.assertEqual(found[0].category, "Général")  # not declared -> default
            self.assertEqual(found[0].mode, "collect")  # not declared -> default

    def test_declared_mode_access_is_used(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "existing.py").write_text(
                'PLUGIN_INFO = {"label": "Existant", "description": "...", "mode": "access"}\n'
                "async def run(context):\n    pass\n",
                encoding="utf-8",
            )
            found = discover_plugins(root)
            self.assertEqual(found[0].mode, "access")

    def test_unrecognized_mode_falls_back_to_collect(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "typo.py").write_text(
                'PLUGIN_INFO = {"label": "Typo", "description": "...", "mode": "acess"}\n'
                "async def run(context):\n    pass\n",
                encoding="utf-8",
            )
            found = discover_plugins(root)
            self.assertEqual(found[0].mode, "collect")

    def test_declared_category_is_used_and_new_categories_need_no_registration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "news.py").write_text(
                'PLUGIN_INFO = {"label": "News", "description": "...", "category": "News"}\n'
                "async def run(context):\n    pass\n",
                encoding="utf-8",
            )
            found = discover_plugins(root)
            self.assertEqual(found[0].category, "News")  # a brand-new category, just from the string

    def test_example_plugin_in_repo_is_discoverable(self) -> None:
        found = discover_plugins(REPOSITORY_ROOT / "plugins")
        ids = [info.id for info in found]
        self.assertIn("example_funding_history", ids)
        example = next(info for info in found if info.id == "example_funding_history")
        self.assertTrue(example.label)
        self.assertTrue(example.description)
        self.assertEqual(example.category, "Crypto")
        self.assertEqual(example.mode, "collect")


class RunPluginTests(unittest.IsolatedAsyncioTestCase):
    async def test_crashing_plugin_is_isolated_and_logged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "crash.py").write_text(_CRASHING_PLUGIN, encoding="utf-8")
            info = discover_plugins(root)[0]
            logs: list[str] = []
            context = PluginContext(root, asyncio.Event(), logs.append)
            await run_plugin(info, context)  # must not raise
            self.assertTrue(any("boom" in line for line in logs))

    async def test_well_behaved_plugin_runs_to_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "good.py").write_text(_VALID_PLUGIN, encoding="utf-8")
            info = discover_plugins(root)[0]
            logs: list[str] = []
            context = PluginContext(root, asyncio.Event(), logs.append)
            await run_plugin(info, context)
            self.assertEqual(logs, ["ran"])


if __name__ == "__main__":
    unittest.main()
