import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_btc.data_collection.control.runs import (
    CollectionRunManager,
    RunState,
    export_run,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


class _KlineResponse:
    """Same fake urlopen response shape used throughout the REST-source
    tests (see test_binance_futures_historical.py / test_futures_rest_streams.py)."""

    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_KlineResponse":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


def _seeded_symbol_cache(root: Path) -> Path:
    """Pre-seeds a fresh, minimal Binance symbol-catalog cache so
    CollectionRunManager.available_sources()/start() never touch the real
    network in tests that aren't specifically about that integration --
    see test_symbol_catalog.py for the catalog module's own tests."""
    cache_path = root / "cache" / "binance_symbols.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({
        "spot": ["BTC", "ETH"], "futures": ["BTC", "ETH"], "fetched_at_utc": datetime.now(UTC).isoformat(),
    }))
    return cache_path


class ExportRunTests(unittest.TestCase):
    def test_consolidates_parquet_parts_and_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            snapshots = data_dir / "snapshots" / "date=2026-07-30" / "hour=12"
            snapshots.mkdir(parents=True)
            for index, sequence in enumerate((1, 2)):
                table = pa.table({"snapshot_sequence": [sequence], "value": [f"row-{index}"]})
                pq.write_table(table, snapshots / f"part-{index}.parquet")
            (data_dir / "funding_history.jsonl").write_text('{"a": 1}\n', encoding="utf-8")

            state = RunState(
                run_id="test-run", sources=["chainlink"], plugins=["example"],
                duration_seconds=30.0, data_dir=data_dir, started_at_ns=time.time_ns(),
            )
            state.ended_at_ns = time.time_ns()
            manifest = export_run(state)

            self.assertEqual(manifest["dataset_file"], "dataset.parquet")
            self.assertEqual(manifest["snapshot_row_count"], 2)
            self.assertEqual(manifest["plugin_files"], ["funding_history.jsonl"])
            self.assertEqual(manifest["mode"], "collect")
            self.assertIsNone(manifest["error"])
            self.assertTrue((data_dir / "dataset.parquet").is_file())
            self.assertTrue((data_dir / "manifest.json").is_file())

    def test_no_parquet_parts_still_writes_a_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            state = RunState(
                run_id="empty-run", sources=["chainlink"], plugins=[],
                duration_seconds=None, data_dir=data_dir, started_at_ns=time.time_ns(),
            )
            state.ended_at_ns = time.time_ns()
            manifest = export_run(state)
            self.assertIsNone(manifest["dataset_file"])
            self.assertEqual(manifest["snapshot_row_count"], 0)


class CollectionRunManagerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_validates_and_rejects_overlapping_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = CollectionRunManager(
                config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
                collections_dir=root / "collections",
                symbol_cache_path=_seeded_symbol_cache(root),
                plugins_dir=root / "plugins",
            )
            with self.assertRaises(ValueError):
                manager.start(sources=[], plugins=[], duration_seconds=None)
            with self.assertRaises(ValueError):
                manager.start(sources=["not_a_real_source"], plugins=[], duration_seconds=None)
            self.assertIsNone(manager.current)

    async def test_full_lifecycle_start_status_stop_export(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = CollectionRunManager(
                config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
                collections_dir=root / "collections",
                symbol_cache_path=_seeded_symbol_cache(root),
                plugins_dir=root / "plugins",
            )
            state = manager.start(sources=["chainlink"], plugins=[], duration_seconds=None)
            self.assertEqual(manager.status()["run_id"], state.run_id)
            self.assertTrue(manager.status()["running"])

            with self.assertRaises(RuntimeError):
                manager.start(sources=["chainlink"], plugins=[], duration_seconds=None)

            manager.stop()
            for _ in range(200):
                if not manager.status()["running"]:
                    break
                await asyncio.sleep(0.05)
            self.assertFalse(manager.status()["running"])
            self.assertIsNotNone(manager.status()["export"])
            self.assertTrue((state.data_dir / "manifest.json").is_file())

            runs = manager.list_runs()
            self.assertEqual(runs[0]["run_id"], state.run_id)

            # a new run can start once the previous one has fully wound down
            second = manager.start(sources=["chainlink"], plugins=[], duration_seconds=None)
            self.assertNotEqual(second.run_id, state.run_id)
            manager.stop()
            for _ in range(200):
                if not manager.status()["running"]:
                    break
                await asyncio.sleep(0.05)


class MergedCatalogTests(unittest.IsolatedAsyncioTestCase):
    """available_sources()/start() both go through _merged_catalog() --
    BTC's static entries plus every extra symbol the (cached) Binance
    catalog knows about."""

    def _manager(self, root: Path) -> CollectionRunManager:
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
        )

    def test_available_sources_includes_generated_entries_for_cached_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            keys = {row["key"] for row in manager.available_sources()}
            self.assertIn("binance_spot", keys)  # BTC, static
            self.assertIn("binance_spot:ETH", keys)  # generated, from the seeded cache

    async def test_start_accepts_a_generated_extra_symbol_key(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            state = manager.start(sources=["binance_spot:ETH"], plugins=[], duration_seconds=None)
            self.assertEqual(state.sources, ["binance_spot:ETH"])
            manager.stop()
            for _ in range(200):
                if not manager.status()["running"]:
                    break
                await asyncio.sleep(0.05)

    def test_a_broken_symbol_cache_falls_back_to_btc_only_instead_of_raising(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = CollectionRunManager(
                config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
                collections_dir=root / "collections",
                # Deliberately not seeded/writable as valid JSON, and no
                # network available in tests -- load_cached_or_fetch will
                # fail, _merged_catalog must degrade gracefully rather than
                # taking /api/sources down with it.
                symbol_cache_path=root / "cache" / "not_json_at_all.json",
                plugins_dir=root / "plugins",
            )
            (root / "cache").mkdir(parents=True, exist_ok=True)
            (root / "cache" / "not_json_at_all.json").write_text("not json", encoding="utf-8")
            with patch(
                "polymarket_btc.data_collection.market_data.binance_symbol_catalog._get_json_sync",
                side_effect=OSError("no network in tests"),
            ):
                sources = manager.available_sources()
            keys = {row["key"] for row in sources}
            self.assertIn("binance_spot", keys)
            self.assertFalse(any(key.endswith(":ETH") for key in keys))


_ACCESS_PLUGIN = '''
PLUGIN_INFO = {"label": "Accès", "description": "...", "mode": "access"}

async def run(context):
    context.log(f"bounds={context.start_ts_ns}:{context.end_ts_ns}")
    (context.data_dir / "converted.jsonl").write_text('{"ok": true}\\n', encoding="utf-8")
'''

_COLLECT_PLUGIN = '''
PLUGIN_INFO = {"label": "Collecte", "description": "..."}

async def run(context):
    context.log("collect")
'''


class AccessModeTests(unittest.IsolatedAsyncioTestCase):
    def _manager(self, root: Path) -> CollectionRunManager:
        plugins_dir = root / "plugins"
        plugins_dir.mkdir(parents=True, exist_ok=True)
        (plugins_dir / "access_plugin.py").write_text(_ACCESS_PLUGIN, encoding="utf-8")
        (plugins_dir / "collect_plugin.py").write_text(_COLLECT_PLUGIN, encoding="utf-8")
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=plugins_dir,
        )

    async def test_access_run_skips_the_gateway_and_runs_only_the_plugin_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            state = manager.start(
                sources=[], plugins=["access_plugin"], duration_seconds=None,
                mode="access", start_ts_ns=1_000, end_ts_ns=2_000,
            )
            self.assertEqual(state.mode, "access")
            self.assertIsNone(state.gateway)
            for _ in range(200):
                if not manager.status()["running"]:
                    break
                await asyncio.sleep(0.05)
            status = manager.status()
            self.assertFalse(status["running"])
            self.assertEqual(status["mode"], "access")
            self.assertIn("bounds=1000:2000", status["plugin_logs"]["access_plugin"][0])
            self.assertEqual(status["export"]["plugin_files"], ["converted.jsonl"])
            self.assertEqual(status["export"]["mode"], "access")
            self.assertTrue((state.data_dir / "converted.jsonl").is_file())

    async def test_access_run_rejects_a_builtin_source_without_access_support(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.start(
                    sources=["chainlink"], plugins=[], duration_seconds=None, mode="access",
                )
            self.assertIsNone(manager.current)

    async def test_access_run_rejects_a_collect_only_plugin(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.start(
                    sources=[], plugins=["collect_plugin"], duration_seconds=None, mode="access",
                )
            self.assertIsNone(manager.current)

    async def test_access_run_rejects_empty_selection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.start(sources=[], plugins=[], duration_seconds=None, mode="access")

    async def test_unknown_mode_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.start(sources=[], plugins=[], duration_seconds=None, mode="bogus")

    async def test_access_run_fetches_a_real_builtin_historical_source(self) -> None:
        rows = [[
            1_000, "64800.00", "64820.00", "64790.00", "64810.00",
            "5.5", 59_999, "356755.0", 200, "2.5", "162025.0", "0",
        ]]

        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with patch("urllib.request.urlopen", side_effect=[_KlineResponse(rows), _KlineResponse([])]):
                state = manager.start(
                    sources=["binance_futures_kline"], plugins=[], duration_seconds=None,
                    mode="access", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                )
                for _ in range(200):
                    if not manager.status()["running"]:
                        break
                    await asyncio.sleep(0.05)
            status = manager.status()
            self.assertFalse(status["running"])
            self.assertIn("binance_futures_kline", status["plugin_logs"])
            log_lines = status["plugin_logs"]["binance_futures_kline"]
            self.assertTrue(any("terminé" in line for line in log_lines))
            raw_files = status["export"]["raw_files"]
            self.assertEqual(len(raw_files), 1)
            self.assertIn("instrument=BTCUSDT", raw_files[0])
            self.assertTrue((state.data_dir / raw_files[0]).is_file())
            self.assertEqual(status["source_progress"]["binance_futures_kline"], 1.0)

    async def test_access_run_source_failure_does_not_take_down_a_concurrent_plugin(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with patch("urllib.request.urlopen", side_effect=OSError("network unavailable")):
                manager.start(
                    sources=["binance_futures_kline"], plugins=["access_plugin"], duration_seconds=None,
                    mode="access", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                )
                for _ in range(200):
                    if not manager.status()["running"]:
                        break
                    await asyncio.sleep(0.05)
            status = manager.status()
            self.assertFalse(status["running"])
            self.assertTrue(any("erreur" in line for line in status["plugin_logs"]["binance_futures_kline"]))
            # the concurrently-selected plugin still ran to completion.
            self.assertIn("bounds=0:200000000000", status["plugin_logs"]["access_plugin"][0])
            self.assertEqual(status["export"]["plugin_files"], ["converted.jsonl"])

    async def test_stop_cancels_a_slow_running_historical_fetch(self) -> None:
        # A wide range means many pagination round trips (one 1-minute
        # candle per page, ~0.15s apart -- see _paginate_by_time), so left
        # uninterrupted this would take tens of seconds. stop() must cut it
        # short almost immediately, not wait for it to finish naturally --
        # this is the exact bug fixed in _run_access (it used to just
        # asyncio.gather the fetch tasks with no stop_event race at all).
        call_count = 0
        base_ms = 1_700_000_000_000

        def fake_urlopen(request, timeout=None):
            nonlocal call_count
            open_ms = base_ms + call_count * 60_000
            call_count += 1
            row = [open_ms, "1", "1", "1", "1", "1", open_ms + 59_999, "1", 1, "1", "1", "0"]
            return _KlineResponse([row])

        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                manager.start(
                    sources=["binance_futures_kline"], plugins=[], duration_seconds=None,
                    mode="access",
                    start_ts_ns=base_ms * 1_000_000, end_ts_ns=(base_ms + 300 * 60_000) * 1_000_000,
                )
                await asyncio.sleep(0.3)
                mid_status = manager.status()
                self.assertTrue(mid_status["running"])  # still going when we cancel it
                mid_progress = mid_status["source_progress"]["binance_futures_kline"]
                self.assertGreater(mid_progress, 0.0)
                self.assertLess(mid_progress, 1.0)
                manager.stop()
                for _ in range(100):
                    if not manager.status()["running"]:
                        break
                    await asyncio.sleep(0.05)
            status = manager.status()
            self.assertFalse(status["running"])
            self.assertLess(call_count, 300)  # cut short, not left to run every page
            self.assertTrue(any("annulé" in line for line in status["plugin_logs"]["binance_futures_kline"]))


class ImportPluginFileTests(unittest.TestCase):
    def _manager(self, root: Path) -> CollectionRunManager:
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
        )

    def test_valid_plugin_is_written_and_recognized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = self._manager(root)
            content = (
                'PLUGIN_INFO = {"label": "Test", "description": "...", "category": "Test"}\n'
                "async def run(context):\n    pass\n"
            )
            result = manager.import_plugin_file("mon_script.py", content)
            self.assertEqual(result, {"filename": "mon_script.py", "recognized": True})
            self.assertEqual((root / "plugins" / "mon_script.py").read_text(encoding="utf-8"), content)

    def test_malformed_content_is_written_but_flagged_unrecognized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            result = manager.import_plugin_file("mon_script.py", "not a valid plugin at all")
            self.assertFalse(result["recognized"])

    def test_duplicate_filename_requires_explicit_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_plugin_file("mon_script.py", "# v1")
            with self.assertRaises(FileExistsError):
                manager.import_plugin_file("mon_script.py", "# v2")
            manager.import_plugin_file("mon_script.py", "# v2", overwrite=True)
            self.assertEqual(
                (Path(directory) / "plugins" / "mon_script.py").read_text(encoding="utf-8"), "# v2",
            )

    def test_unsafe_filenames_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            for bad_name in (
                "../escape.py", "sub/dir.py", "_hidden.py", "no_extension",
                "script.py.txt", "", "..py",
            ):
                with self.assertRaises(ValueError):
                    manager.import_plugin_file(bad_name, "# content")
            # nothing unsafe should have touched the filesystem
            plugins_dir = Path(directory) / "plugins"
            self.assertFalse((plugins_dir / "escape.py").exists())
            self.assertFalse((Path(directory) / "escape.py").exists())


if __name__ == "__main__":
    unittest.main()
