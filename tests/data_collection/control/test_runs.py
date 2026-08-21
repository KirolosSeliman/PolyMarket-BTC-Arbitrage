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
    _access_mode_source_coverage,
    export_run,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _iso_for_test(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=UTC).isoformat()


class _KlineHeaders:
    def get(self, _name: str) -> str | None:
        return None


class _KlineResponse:
    """Same fake urlopen response shape used throughout the REST-source
    tests (see test_binance_futures_historical.py / test_futures_rest_streams.py)."""

    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode()
        self.headers = _KlineHeaders()

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
            for index, (sequence, ts_ns) in enumerate(((1, 1_000_000_000), (2, 2_000_000_000))):
                table = pa.table({
                    "snapshot_sequence": [sequence], "snapshot_timestamp_ns": [ts_ns], "value": [f"row-{index}"],
                })
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
            self.assertEqual(
                manifest["source_coverage"]["chainlink"],
                {"start_ts_utc": "1970-01-01T00:00:01+00:00", "end_ts_utc": "1970-01-01T00:00:02+00:00"},
            )
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

    def test_access_mode_run_reports_raw_event_count_not_zero(self) -> None:
        """An access-mode run never populates dataset.parquet (no gateway/
        reducer runs), so snapshot_row_count is always 0 for it -- the real
        yield has to come from summing each raw segment's own sidecar
        manifest instead. Regression test for the "0 lignes" collect.html
        display bug: access-mode collections were showing 0 despite really
        having collected data."""
        from decimal import Decimal

        from polymarket_btc.data_collection.market_data.models import (
            BinanceAggTradePayload, EventSource, EventStream, MarketDataEvent, TakerSide,
        )
        from polymarket_btc.data_collection.market_data.storage import RawEventStorage

        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            storage = RawEventStorage(data_dir, zstd_level=3)
            for i in range(3):
                payload = BinanceAggTradePayload(
                    symbol="BTCUSDT", aggregate_trade_id=i, price=Decimal("100"), quantity=Decimal("0.1"),
                    first_trade_id=i, last_trade_id=i, trade_timestamp_ns=i, taker_side=TakerSide.BUY,
                )
                event = MarketDataEvent(
                    schema_version=2, ingest_sequence=i, event_id=f"evt-{i}",
                    source=EventSource.BINANCE_FUTURES_TRADE, stream=EventStream.BINANCE_FUTURES_AGG_TRADE,
                    instrument="BTCUSDT", source_timestamp_ns=i, server_timestamp_ns=i,
                    received_wall_timestamp_ns=i, received_monotonic_ns=time.monotonic_ns(),
                    source_sequence=None, timeframe=None, market_id=None, condition_id=None,
                    asset_id=None, outcome=None, payload=payload,
                )
                storage.write(event)
            storage.close()

            state = RunState(
                run_id="access-run", sources=["binance_futures_trade"], plugins=[],
                duration_seconds=None, data_dir=data_dir, started_at_ns=time.time_ns(), mode="access",
            )
            state.ended_at_ns = time.time_ns()
            manifest = export_run(state)

            self.assertEqual(manifest["snapshot_row_count"], 0)
            self.assertEqual(manifest["raw_event_count"], 3)

    def test_source_coverage_is_per_key_not_the_runs_requested_range(self) -> None:
        """The exact reported bug: a run requested for one wide range where
        one key (BTC kline) genuinely got the whole thing but another
        (ETH kline) only actually got a narrow slice at the end -- the
        manifest's own start_ts_utc/end_ts_utc (the *requested* range) must
        never be read as if it applied to every key; source_coverage must
        report each key's own true, independently-observed range."""
        from decimal import Decimal

        from polymarket_btc.data_collection.market_data.models import (
            BinanceKlinePayload, EventSource, EventStream, MarketDataEvent,
        )
        from polymarket_btc.data_collection.market_data.storage import RawEventStorage

        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            storage = RawEventStorage(data_dir, zstd_level=3)

            def write_kline(instrument: str, ts_ns: int, sequence: int) -> None:
                payload = BinanceKlinePayload(
                    market="futures", interval="1m", open_time_ns=ts_ns - 60_000_000_000, close_time_ns=ts_ns,
                    open=Decimal("100"), high=Decimal("100"), low=Decimal("100"), close=Decimal("100"),
                    base_volume=Decimal("1"), quote_volume=Decimal("1"), trade_count=1, is_closed=True,
                )
                event = MarketDataEvent(
                    schema_version=2, ingest_sequence=sequence, event_id=f"kline-{instrument}-{sequence}",
                    source=EventSource.BINANCE_FUTURES_KLINE, stream=EventStream.BINANCE_KLINE,
                    instrument=instrument, source_timestamp_ns=ts_ns, server_timestamp_ns=ts_ns,
                    received_wall_timestamp_ns=ts_ns, received_monotonic_ns=time.monotonic_ns(),
                    source_sequence=None, timeframe=None, market_id=None, condition_id=None,
                    asset_id=None, outcome=None, payload=payload,
                )
                storage.write(event)

            day1_ns, day18_ns, day19_ns = 1 * 86_400 * 1_000_000_000, 18 * 86_400 * 1_000_000_000, 19 * 86_400 * 1_000_000_000
            write_kline("BTCUSDT", day1_ns, 0)
            write_kline("BTCUSDT", day19_ns, 1)
            write_kline("ETHUSDT", day18_ns, 2)
            write_kline("ETHUSDT", day19_ns, 3)
            storage.close()

            state = RunState(
                run_id="mixed-run", sources=["binance_futures_kline", "binance_futures_kline:ETH"], plugins=[],
                duration_seconds=None, data_dir=data_dir, started_at_ns=time.time_ns(), mode="access",
                start_ts_ns=day1_ns, end_ts_ns=day19_ns,
            )
            state.ended_at_ns = time.time_ns()
            manifest = export_run(state)

            # The requested range (misleading if read per-key) still covers day 1-19.
            self.assertEqual(manifest["start_ts_utc"], _iso_for_test(day1_ns))
            # But each key's OWN actual coverage must differ:
            self.assertEqual(manifest["source_coverage"]["binance_futures_kline"]["start_ts_utc"], _iso_for_test(day1_ns))
            self.assertEqual(manifest["source_coverage"]["binance_futures_kline"]["end_ts_utc"], _iso_for_test(day19_ns))
            self.assertEqual(manifest["source_coverage"]["binance_futures_kline:ETH"]["start_ts_utc"], _iso_for_test(day18_ns))
            self.assertEqual(manifest["source_coverage"]["binance_futures_kline:ETH"]["end_ts_utc"], _iso_for_test(day19_ns))

    def test_source_coverage_uses_the_datas_own_timestamps_not_when_it_was_fetched(self) -> None:
        """The exact bug found live: an access-mode bulk historical fetch
        for an old date range writes its raw segments *now* (received_
        wall_timestamp_ns is essentially "today"), but the klines
        themselves are dated a year ago (source_timestamp_ns). Before this
        fix, source_coverage read only first/last_received_timestamp_ns --
        a real fetch for 2025-08-24 data showed as covering a few hundred
        milliseconds around the moment it was fetched in 2026, not the
        actual historical day requested."""
        from decimal import Decimal

        from polymarket_btc.data_collection.market_data.models import (
            BinanceKlinePayload, EventSource, EventStream, MarketDataEvent,
        )
        from polymarket_btc.data_collection.market_data.storage import RawEventStorage

        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            storage = RawEventStorage(data_dir, zstd_level=3)

            a_year_ago_ns = 1755993600 * 1_000_000_000  # 2025-08-24, the actual candle date
            fetched_now_ns = 1787309600 * 1_000_000_000  # 2026-08-21, when the fetch actually ran

            def write_kline(ts_ns: int, sequence: int) -> None:
                payload = BinanceKlinePayload(
                    market="futures", interval="1m", open_time_ns=ts_ns - 60_000_000_000, close_time_ns=ts_ns,
                    open=Decimal("100"), high=Decimal("100"), low=Decimal("100"), close=Decimal("100"),
                    base_volume=Decimal("1"), quote_volume=Decimal("1"), trade_count=1, is_closed=True,
                )
                event = MarketDataEvent(
                    schema_version=2, ingest_sequence=sequence, event_id=f"kline-{sequence}",
                    source=EventSource.BINANCE_FUTURES_KLINE, stream=EventStream.BINANCE_KLINE,
                    instrument="BTCUSDT", source_timestamp_ns=ts_ns, server_timestamp_ns=ts_ns,
                    received_wall_timestamp_ns=fetched_now_ns, received_monotonic_ns=time.monotonic_ns(),
                    source_sequence=None, timeframe=None, market_id=None, condition_id=None,
                    asset_id=None, outcome=None, payload=payload,
                )
                storage.write(event)

            write_kline(a_year_ago_ns, 0)
            write_kline(a_year_ago_ns + 86_400 * 1_000_000_000, 1)  # a day later, same historical range
            storage.close()

            state = RunState(
                run_id="old-range-run", sources=["binance_futures_kline"], plugins=[],
                duration_seconds=None, data_dir=data_dir, started_at_ns=time.time_ns(), mode="access",
                start_ts_ns=a_year_ago_ns, end_ts_ns=a_year_ago_ns + 86_400 * 1_000_000_000,
            )
            state.ended_at_ns = time.time_ns()
            manifest = export_run(state)

            coverage = manifest["source_coverage"]["binance_futures_kline"]
            self.assertEqual(coverage["start_ts_utc"], _iso_for_test(a_year_ago_ns))
            self.assertEqual(coverage["end_ts_utc"], _iso_for_test(a_year_ago_ns + 86_400 * 1_000_000_000))

    def test_source_coverage_falls_back_to_received_time_for_a_sidecar_without_source_timestamps(self) -> None:
        """A sidecar written before this fix has no first/last_source_
        timestamp_ns fields at all -- must still round-trip via the old
        received-time fields rather than being skipped."""
        raw_dir = Path(tempfile.mkdtemp())
        try:
            segment_dir = raw_dir / "raw" / "source=binance_futures_kline" / "instrument=BTCUSDT"
            segment_dir.mkdir(parents=True)
            data_path = segment_dir / "part-1.jsonl.zst"
            data_path.write_bytes(b"")
            sidecar = {
                "first_received_timestamp_ns": 1_700_000_000_000_000_000,
                "last_received_timestamp_ns": 1_700_000_100_000_000_000,
            }
            (segment_dir / "part-1.manifest.json").write_text(json.dumps(sidecar), encoding="utf-8")

            coverage = _access_mode_source_coverage([data_path])
            self.assertEqual(coverage["binance_futures_kline"]["start_ts_utc"], _iso_for_test(1_700_000_000_000_000_000))
            self.assertEqual(coverage["binance_futures_kline"]["end_ts_utc"], _iso_for_test(1_700_000_100_000_000_000))
        finally:
            import shutil
            shutil.rmtree(raw_dir)


class CollectionRunManagerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_validates_and_rejects_overlapping_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
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
                concepts_dir=root / "concepts",
                microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles",
                management_dir=root / "management_profiles",
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


class DeleteRunTests(unittest.IsolatedAsyncioTestCase):
    def _manager(self, root: Path) -> CollectionRunManager:
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
        )

    async def _finished_run(self, manager: CollectionRunManager) -> str:
        state = manager.start(sources=["chainlink"], plugins=[], duration_seconds=None)
        manager.stop()
        for _ in range(200):
            if not manager.status()["running"]:
                break
            await asyncio.sleep(0.05)
        return state.run_id

    async def test_deletes_the_runs_directory_and_it_disappears_from_list_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            run_id = await self._finished_run(manager)
            self.assertEqual([r["run_id"] for r in manager.list_runs()], [run_id])

            manager.delete_run(run_id)

            self.assertEqual(manager.list_runs(), [])
            self.assertFalse((manager.collections_dir / run_id).exists())

    async def test_unknown_run_id_raises_file_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(FileNotFoundError):
                manager.delete_run("no-such-run")

    async def test_path_traversal_run_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.delete_run("../outside")

    async def test_cannot_delete_the_currently_running_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            state = manager.start(sources=["chainlink"], plugins=[], duration_seconds=None)
            try:
                with self.assertRaises(RuntimeError):
                    manager.delete_run(state.run_id)
            finally:
                manager.stop()
                for _ in range(200):
                    if not manager.status()["running"]:
                        break
                    await asyncio.sleep(0.05)

    async def test_a_different_past_run_can_be_deleted_while_another_is_running(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            old_run_id = await self._finished_run(manager)

            state = manager.start(sources=["chainlink"], plugins=[], duration_seconds=None)
            try:
                manager.delete_run(old_run_id)
                self.assertFalse((manager.collections_dir / old_run_id).exists())
            finally:
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
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
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
                concepts_dir=root / "concepts",
                microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles",
                management_dir=root / "management_profiles",
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
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
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

    async def test_unknown_kline_interval_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.start(
                    sources=["binance_futures_kline"], plugins=[], duration_seconds=None,
                    mode="access", kline_interval="10m",
                )
            self.assertIsNone(manager.current)

    async def test_kline_interval_is_threaded_through_to_the_fetcher_request(self) -> None:
        requested_urls: list[str] = []

        def fake_urlopen(request, timeout=None):
            requested_urls.append(request.full_url)
            return _KlineResponse([])

        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                state = manager.start(
                    sources=["binance_futures_kline"], plugins=[], duration_seconds=None,
                    mode="access", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000, kline_interval="1h",
                )
                for _ in range(200):
                    if not manager.status()["running"]:
                        break
                    await asyncio.sleep(0.05)
            self.assertEqual(state.kline_interval, "1h")
            self.assertTrue(requested_urls)
            self.assertIn("interval=1h", requested_urls[0])
            status = manager.status()
            self.assertEqual(status["export"]["kline_interval"], "1h")

    async def test_kline_interval_is_ignored_by_a_source_with_no_such_notion(self) -> None:
        # aggTrades has no interval concept at all -- passing kline_interval
        # to a run that only selects it must not crash (fetch_and_store_
        # historical_agg_trades doesn't accept an interval kwarg).
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with patch("urllib.request.urlopen", return_value=_KlineResponse([])):
                manager.start(
                    sources=["binance_futures_trade"], plugins=[], duration_seconds=None,
                    mode="access", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000, kline_interval="15m",
                )
                for _ in range(200):
                    if not manager.status()["running"]:
                        break
                    await asyncio.sleep(0.05)
            status = manager.status()
            self.assertIsNone(status["error"])

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

    async def test_access_run_surfaces_a_binance_rejection_instead_of_a_silent_zero(self) -> None:
        """Binance's own rejection shape ({"code":..., "msg":...}, confirmed
        live against an invalid symbol) used to be silently treated as an
        empty page -- indistinguishable from "genuinely no data in this
        range" in both the log and the resulting event count. Both now
        still export 0 events, but only a real rejection also logs why."""
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with patch(
                "urllib.request.urlopen",
                return_value=_KlineResponse({"code": -1121, "msg": "Invalid symbol."}),
            ):
                manager.start(
                    sources=["binance_futures_kline"], plugins=[], duration_seconds=None,
                    mode="access", start_ts_ns=0, end_ts_ns=200_000 * 1_000_000,
                )
                for _ in range(200):
                    if not manager.status()["running"]:
                        break
                    await asyncio.sleep(0.05)
            status = manager.status()
            self.assertFalse(status["running"])
            log_lines = status["plugin_logs"]["binance_futures_kline"]
            self.assertTrue(any("erreur" in line and "Invalid symbol." in line for line in log_lines))
            self.assertEqual(status["export"]["raw_event_count"], 0)

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
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
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


_CONCEPT_SOURCE = (
    'CONCEPT_INFO = {"label": "Test", "description": "...", "data_sources": ["chainlink"]}\n'
    "def compute(context):\n    pass\n"
)
_MICROSYSTEM_SOURCE = (
    'MICROSYSTEM_INFO = {"label": "Test", "description": "...", "concept_inputs": ["x"]}\n'
    "def compute(context):\n    pass\n"
)
_EXECUTION_SOURCE = (
    'EXECUTION_INFO = {"label": "Test", "description": "..."}\n'
    "def execute(context):\n    pass\n"
)
_MANAGEMENT_SOURCE = (
    'MANAGEMENT_INFO = {"label": "Test", "description": "..."}\n'
    "def manage(context):\n    pass\n"
)


class ImportConceptFileTests(unittest.TestCase):
    def _manager(self, root: Path) -> CollectionRunManager:
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
        )

    def test_valid_concept_is_written_and_recognized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = self._manager(root)
            result = manager.import_concept_file("mon_concept.py", _CONCEPT_SOURCE)
            self.assertEqual(result, {"filename": "mon_concept.py", "recognized": True})
            self.assertEqual((root / "concepts" / "mon_concept.py").read_text(encoding="utf-8"), _CONCEPT_SOURCE)

    def test_malformed_content_is_written_but_flagged_unrecognized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            result = manager.import_concept_file("mon_concept.py", "not a valid concept at all")
            self.assertFalse(result["recognized"])

    def test_duplicate_filename_requires_explicit_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_concept_file("mon_concept.py", "# v1")
            with self.assertRaises(FileExistsError):
                manager.import_concept_file("mon_concept.py", "# v2")
            manager.import_concept_file("mon_concept.py", "# v2", overwrite=True)

    def test_unsafe_filename_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.import_concept_file("../escape.py", "# content")


class ImportMicrosystemExecutionAndManagementFileTests(unittest.TestCase):
    def _manager(self, root: Path) -> CollectionRunManager:
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
        )

    def test_valid_microsystem_is_written_and_recognized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = self._manager(root)
            result = manager.import_microsystem_file("mon_micro.py", _MICROSYSTEM_SOURCE)
            self.assertEqual(result, {"filename": "mon_micro.py", "recognized": True})
            self.assertEqual((root / "microsystems" / "mon_micro.py").read_text(encoding="utf-8"), _MICROSYSTEM_SOURCE)

    def test_duplicate_microsystem_filename_requires_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_microsystem_file("mon_micro.py", "# v1")
            with self.assertRaises(FileExistsError):
                manager.import_microsystem_file("mon_micro.py", "# v2")

    def test_valid_execution_profile_is_written_and_recognized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = self._manager(root)
            result = manager.import_execution_profile_file("mon_profil.py", _EXECUTION_SOURCE)
            self.assertEqual(result, {"filename": "mon_profil.py", "recognized": True})
            self.assertEqual(
                (root / "execution_profiles" / "mon_profil.py").read_text(encoding="utf-8"), _EXECUTION_SOURCE,
            )

    def test_duplicate_execution_filename_requires_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_execution_profile_file("mon_profil.py", "# v1")
            with self.assertRaises(FileExistsError):
                manager.import_execution_profile_file("mon_profil.py", "# v2")

    def test_valid_management_profile_is_written_and_recognized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = self._manager(root)
            result = manager.import_management_profile_file("mon_profil_gestion.py", _MANAGEMENT_SOURCE)
            self.assertEqual(result, {"filename": "mon_profil_gestion.py", "recognized": True})
            self.assertEqual(
                (root / "management_profiles" / "mon_profil_gestion.py").read_text(encoding="utf-8"),
                _MANAGEMENT_SOURCE,
            )

    def test_duplicate_management_filename_requires_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_management_profile_file("mon_profil_gestion.py", "# v1")
            with self.assertRaises(FileExistsError):
                manager.import_management_profile_file("mon_profil_gestion.py", "# v2")


class ReadSourceTests(unittest.TestCase):
    def _manager(self, root: Path) -> CollectionRunManager:
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
        )

    def test_read_plugin_source_found(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_plugin_file(
                "mon_plugin.py",
                'PLUGIN_INFO = {"label": "x", "description": "y"}\nasync def run(context): pass\n',
            )
            result = manager.read_plugin_source("mon_plugin")
            self.assertEqual(result["id"], "mon_plugin")
            self.assertEqual(result["filename"], "mon_plugin.py")
            self.assertIn("PLUGIN_INFO", result["content"])

    def test_read_plugin_source_not_found_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(FileNotFoundError):
                manager.read_plugin_source("nope")

    def test_read_concept_source_found(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_concept_file("mon_concept.py", _CONCEPT_SOURCE)
            result = manager.read_concept_source("mon_concept")
            self.assertEqual(result["filename"], "mon_concept.py")
            self.assertIn("CONCEPT_INFO", result["content"])

    def test_read_concept_source_not_found_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(FileNotFoundError):
                manager.read_concept_source("nope")


class BuildPromptTests(unittest.TestCase):
    def _manager(self, root: Path) -> CollectionRunManager:
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
        )

    def test_concept_prompt_embeds_source_and_plugin_context(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_plugin_file(
                "mon_plugin.py",
                'PLUGIN_INFO = {"label": "Mon Plugin", "description": "collecte X"}\n'
                "async def run(context): pass\n",
            )
            content = manager.build_concept_prompt(
                sources=["chainlink"], plugins=["mon_plugin"], template="TEMPLATE_TEXT",
            )
            self.assertIn("TEMPLATE_TEXT", content)
            self.assertIn("Contexte : données sélectionnées", content)
            self.assertIn("chainlink", content)
            self.assertIn("Mon Plugin", content)
            self.assertIn("PLUGIN_INFO", content)  # the plugin's own source code embedded

    def test_concept_prompt_empty_selection_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.build_concept_prompt(sources=[], plugins=[], template="TEMPLATE_TEXT")

    def test_concept_prompt_silently_drops_unknown_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            content = manager.build_concept_prompt(
                sources=["not_a_real_source"], plugins=["not_a_real_plugin"], template="TEMPLATE_TEXT",
            )
            self.assertIn("TEMPLATE_TEXT", content)
            self.assertNotIn("not_a_real_source", content)

    def test_microsystem_prompt_embeds_concept_source_too(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_concept_file("mon_concept.py", _CONCEPT_SOURCE)
            content = manager.build_microsystem_prompt(
                concepts=["mon_concept"], sources=[], plugins=[], template="TEMPLATE_TEXT",
            )
            self.assertIn("mon_concept", content)
            self.assertIn("CONCEPT_INFO", content)  # the concept's own source code embedded

    def test_microsystem_prompt_all_empty_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            with self.assertRaises(ValueError):
                manager.build_microsystem_prompt(concepts=[], sources=[], plugins=[], template="TEMPLATE_TEXT")


class DataRequirementsForTests(unittest.TestCase):
    def _manager(self, root: Path) -> CollectionRunManager:
        return CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins",
            concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems",
            execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
        )

    def test_single_asset_key_is_swappable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            requirements = manager._data_requirements_for(["binance_futures_kline"])
            self.assertEqual(requirements, [{
                "type": "Bougies", "swappable": True,
                "keys": ["binance_futures_kline"], "default_asset": "BTC",
            }])

    def test_two_assets_same_tag_is_locked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            requirements = manager._data_requirements_for(
                ["binance_futures_kline", "binance_futures_kline:ETH"],
            )
            self.assertEqual(len(requirements), 1)
            self.assertEqual(requirements[0]["type"], "Bougies")
            self.assertFalse(requirements[0]["swappable"])
            self.assertEqual(
                requirements[0]["keys"], ["binance_futures_kline", "binance_futures_kline:ETH"],
            )
            self.assertIsNone(requirements[0]["default_asset"])

    def test_plugin_key_is_locked_and_type_is_its_own_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            manager.import_plugin_file(
                "mon_plugin.py",
                'PLUGIN_INFO = {"label": "x", "description": "y"}\nasync def run(context): pass\n',
            )
            requirements = manager._data_requirements_for(["mon_plugin"])
            self.assertEqual(requirements, [{
                "type": "mon_plugin", "swappable": False, "keys": ["mon_plugin"], "default_asset": None,
            }])

    def test_non_asset_scoped_source_is_locked_and_type_is_its_own_key(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            requirements = manager._data_requirements_for(["chainlink"])
            self.assertEqual(requirements, [{
                "type": "chainlink", "swappable": False, "keys": ["chainlink"], "default_asset": None,
            }])

    def test_mixed_swappable_and_locked_types_are_both_present(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manager = self._manager(Path(directory))
            requirements = manager._data_requirements_for(["binance_futures_kline", "chainlink"])
            by_type = {r["type"]: r for r in requirements}
            self.assertTrue(by_type["Bougies"]["swappable"])
            self.assertFalse(by_type["chainlink"]["swappable"])

    def test_available_concepts_and_microsystems_expose_data_requirements(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = self._manager(root)
            manager.import_concept_file(
                "zscore.py",
                'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
                "def compute(context):\n    pass\n",
            )
            manager.import_microsystem_file(
                "trend.py",
                'MICROSYSTEM_INFO = {"label": "x", "description": "y", "data_inputs": ["chainlink"]}\n'
                "def compute(context):\n    pass\n",
            )
            concept_row = manager.available_concepts()[0]
            self.assertEqual(concept_row["data_requirements"][0]["type"], "Bougies")
            microsystem_row = manager.available_microsystems()[0]
            self.assertEqual(microsystem_row["data_requirements"][0]["type"], "chainlink")


if __name__ == "__main__":
    unittest.main()
