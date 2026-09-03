import asyncio
from datetime import UTC, datetime
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from polymarket_btc.data_collection.control import refinement
from polymarket_btc.data_collection.control.concept_refinement import ConceptRefinementManager
from polymarket_btc.data_collection.control.refinement import MIN_NO_FOR_PROMPT, MIN_TOTAL_FOR_PROMPT
from polymarket_btc.data_collection.control.runs import CollectionRunManager
from polymarket_btc.data_collection.market_data.models import (
    BinanceKlinePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
)
from polymarket_btc.data_collection.market_data.storage import RawEventStorage

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

# A concept that emits one "zone" candidate per up-candle -- direction is
# always bullish so the test doesn't need to think about the down case,
# high/low/formed_at come straight from the candle so expected values are
# hand-computable from the fixture data itself.
_ZONE_CONCEPT = '''
CONCEPT_INFO = {
    "label": "Zone test", "description": "d",
    "data_sources": ["binance_futures_kline"],
}

def compute(context):
    candles = context.data.get("binance_futures_kline") or []
    zones = []
    for c in candles:
        if c["close"] > c["open"]:
            zones.append({
                "direction": "bullish", "high": c["high"], "low": c["low"],
                "formed_at": c["timestamp"],
            })
    return {"zones": zones}
'''

# A concept whose "level" only ever gets its swept_at set once a later
# candle's close actually crosses the pool level established by the very
# first candle -- formed_at (the pool's own origin) and swept_at (when it
# resolves) are then genuinely far apart, exactly the shape that would
# never be found at all if the scanner only ever triggered on formed_at
# (see _trigger_timestamp's docstring / the plan's own timing/correctness
# findings).
_LEVEL_CONCEPT = '''
CONCEPT_INFO = {
    "label": "Level test", "description": "d",
    "data_sources": ["binance_futures_kline"],
}

def compute(context):
    candles = context.data.get("binance_futures_kline") or []
    if not candles:
        return {"levels": []}
    pool_level = candles[0]["high"]
    pool_formed_at = candles[0]["timestamp"]
    swept_at = None
    for c in candles[1:]:
        if c["close"] > pool_level:
            swept_at = c["timestamp"]
            break
    if swept_at is None:
        return {"levels": []}
    return {"levels": [{
        "direction": "bullish", "level": pool_level,
        "formed_at": pool_formed_at, "swept_at": swept_at,
    }]}
'''

# A concept whose data_sources names a key nothing in the fixture ever
# collects -- for the "no real coverage at all" error path.
_UNCOVERED_CONCEPT = '''
CONCEPT_INFO = {
    "label": "Uncovered", "description": "d",
    "data_sources": ["binance_futures_mark_price"],
}

def compute(context):
    return {}
'''


def _write_kline_events(run_dir: Path, candles: list[dict[str, float]]) -> None:
    """Full OHLC control (unlike test_backtest_engine.py's own
    _write_kline_events, which only ever sets one price for all four
    fields) -- this module's concepts need real up/down candles and
    distinct high/low values to be exercised meaningfully."""
    storage = RawEventStorage(run_dir, zstd_level=3)
    for i, c in enumerate(candles):
        close_ns = int(c["timestamp"] * 1e9)
        open_ns = close_ns - 60_000_000_000
        payload = BinanceKlinePayload(
            market="futures", interval="1m", open_time_ns=open_ns, close_time_ns=close_ns,
            open=Decimal(str(c["open"])), high=Decimal(str(c["high"])), low=Decimal(str(c["low"])),
            close=Decimal(str(c["close"])), base_volume=Decimal("1"), quote_volume=Decimal("1"),
            trade_count=1, is_closed=True,
        )
        event = MarketDataEvent(
            schema_version=2, ingest_sequence=i, event_id=f"kline-{i}",
            source=EventSource.BINANCE_FUTURES_KLINE, stream=EventStream.BINANCE_KLINE,
            instrument="BTCUSDT", source_timestamp_ns=close_ns, server_timestamp_ns=close_ns,
            received_wall_timestamp_ns=close_ns, received_monotonic_ns=time.monotonic_ns(),
            source_sequence=None, timeframe=None, market_id=None, condition_id=None,
            asset_id=None, outcome=None, payload=payload,
        )
        storage.write(event)
    storage.close()


def _write_manifest(run_dir: Path, *, sources: list[str], start_ts: float, end_ts: float) -> None:
    manifest = {
        "mode": "access", "sources": sources, "plugins": [],
        "start_ts_utc": datetime.fromtimestamp(start_ts, tz=UTC).isoformat(),
        "end_ts_utc": datetime.fromtimestamp(end_ts, tz=UTC).isoformat(),
        "data_dir": str(run_dir),
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


class _RefinementTestBase(unittest.TestCase):
    def _setup(
        self, concept_source: str, *, concept_filename: str = "test_concept.py",
        claude_code_command: list[str] | None = None,
    ) -> ConceptRefinementManager:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        concepts_dir = root / "concepts"
        concepts_dir.mkdir(parents=True)
        (concepts_dir / concept_filename).write_text(concept_source, encoding="utf-8")
        self.concepts_dir = concepts_dir
        self.collections_dir = root / "collections"
        runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=self.collections_dir,
            plugins_dir=root / "plugins", concepts_dir=concepts_dir,
            microsystems_dir=root / "microsystems", execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles", filter_dir=root / "filter_profiles",
        )
        return ConceptRefinementManager(
            feedback_dir=root / "concept_feedback", runs=runs, claude_code_command=claude_code_command,
        )


class ScanZoneDetectionTests(_RefinementTestBase):
    def test_finds_one_candidate_per_up_candle_with_correct_values(self) -> None:
        manager = self._setup(_ZONE_CONCEPT)
        run_dir = self.collections_dir / "run1"
        candles = [
            {"timestamp": 0.0, "open": 100, "high": 101, "low": 99, "close": 99},    # down
            {"timestamp": 60.0, "open": 99, "high": 103, "low": 99, "close": 102},   # up #1
            {"timestamp": 120.0, "open": 102, "high": 104, "low": 101, "close": 101},  # down
            {"timestamp": 180.0, "open": 101, "high": 106, "low": 100, "close": 105},  # up #2
        ]
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=240)

        result = manager.scan("test_concept")
        self.assertEqual(result["added"], 2)
        cache = refinement.load_pool_cache(manager.feedback_dir, "test_concept")
        formed_ats = sorted(c["node"]["formed_at"] for c in cache["candidates"])
        self.assertEqual(formed_ats, [60.0, 180.0])
        by_formed_at = {c["node"]["formed_at"]: c["node"] for c in cache["candidates"]}
        self.assertEqual(by_formed_at[60.0]["high"], 103)
        self.assertEqual(by_formed_at[180.0]["high"], 106)

    def test_incremental_rescan_only_processes_the_new_tail(self) -> None:
        manager = self._setup(_ZONE_CONCEPT)
        run_dir = self.collections_dir / "run1"
        first_batch = [
            {"timestamp": 0.0, "open": 100, "high": 101, "low": 99, "close": 99},
            {"timestamp": 60.0, "open": 99, "high": 103, "low": 99, "close": 102},  # up
        ]
        _write_kline_events(run_dir, first_batch)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=600)
        first_result = manager.scan("test_concept")
        self.assertEqual(first_result["added"], 1)

        # More data collected later -- a second run directory, same manifest
        # shape, appended candles continuing the timeline.
        run_dir_2 = self.collections_dir / "run2"
        second_batch = [
            {"timestamp": 120.0, "open": 102, "high": 104, "low": 101, "close": 101},  # down
            {"timestamp": 180.0, "open": 101, "high": 106, "low": 100, "close": 105},  # up
        ]
        _write_kline_events(run_dir_2, second_batch)
        _write_manifest(run_dir_2, sources=["binance_futures_kline"], start_ts=-60, end_ts=600)

        second_result = manager.scan("test_concept")
        self.assertEqual(second_result["added"], 1)  # only the new up-candle, not a full rescan
        self.assertEqual(second_result["candidate_count"], 2)

    def test_no_real_coverage_raises_clearly(self) -> None:
        manager = self._setup(_UNCOVERED_CONCEPT, concept_filename="uncovered.py")
        with self.assertRaises(ValueError):
            manager.scan("uncovered")

    def test_zero_candidate_concept_scans_cleanly(self) -> None:
        # Every candle a down-candle -- the concept never emits a zone at all.
        manager = self._setup(_ZONE_CONCEPT)
        run_dir = self.collections_dir / "run1"
        candles = [
            {"timestamp": 0.0, "open": 100, "high": 101, "low": 99, "close": 99},
            {"timestamp": 60.0, "open": 99, "high": 100, "low": 98, "close": 98},
        ]
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=120)
        result = manager.scan("test_concept")
        self.assertEqual(result["added"], 0)
        next_result = manager.next_instance(concept_id="test_concept")
        self.assertTrue(next_result["no_candidates"])
        self.assertFalse(next_result["exhausted"])
        self.assertIsNone(next_result["instance"])


class ScanSweepTriggerFixTests(_RefinementTestBase):
    def test_sweep_is_captured_at_resolution_not_at_pool_formation(self) -> None:
        manager = self._setup(_LEVEL_CONCEPT, concept_filename="level_test.py")
        run_dir = self.collections_dir / "run1"
        candles = [
            {"timestamp": 0.0, "open": 100, "high": 100, "low": 95, "close": 98},    # pool forms here, level=100
            {"timestamp": 60.0, "open": 98, "high": 99, "low": 96, "close": 99},     # doesn't cross 100
            {"timestamp": 120.0, "open": 99, "high": 99.5, "low": 97, "close": 97},  # doesn't cross
            {"timestamp": 180.0, "open": 97, "high": 98, "low": 95, "close": 96},    # doesn't cross
            {"timestamp": 240.0, "open": 96, "high": 102, "low": 95, "close": 101},  # crosses 100 -> swept
        ]
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=300)

        result = manager.scan("level_test")
        self.assertEqual(result["added"], 1)
        cache = refinement.load_pool_cache(manager.feedback_dir, "level_test")
        self.assertEqual(len(cache["candidates"]), 1)
        candidate = cache["candidates"][0]
        # The candidate is captured exactly once, keyed to when it actually
        # resolved (240.0), not to the far-earlier pool formation (0.0) --
        # without the swept_at-preferred trigger fix this candidate is
        # never captured at all (see this test class's own docstring
        # reasoning replicated in the fixture concept's own comment).
        self.assertEqual(candidate["trigger_ts"], 240.0)
        self.assertEqual(candidate["node"]["formed_at"], 0.0)
        self.assertEqual(candidate["node"]["swept_at"], 240.0)
        self.assertEqual(candidate["node"]["level"], 100)


class NextInstanceAndLabelTests(_RefinementTestBase):
    def _scanned_manager(self, up_candle_count: int) -> ConceptRefinementManager:
        manager = self._setup(_ZONE_CONCEPT)
        run_dir = self.collections_dir / "run1"
        candles = []
        t = 0.0
        for i in range(up_candle_count):
            candles.append({"timestamp": t, "open": 100, "high": 101, "low": 99, "close": 99})  # down (filler)
            t += 60.0
            candles.append({"timestamp": t, "open": 99, "high": 103 + i, "low": 99, "close": 102})  # up
            t += 60.0
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=t + 60)
        manager.scan("test_concept")
        return manager

    def test_next_instance_never_repeats_an_already_labeled_candidate(self) -> None:
        manager = self._scanned_manager(3)
        seen_keys = set()
        for _ in range(3):
            result = manager.next_instance(concept_id="test_concept")
            self.assertFalse(result["exhausted"])
            instance = result["instance"]
            self.assertIsNotNone(instance)
            key_before = {"shape": instance["shape"], "node": instance["node"]}
            manager.label(
                concept_id="test_concept", shape=instance["shape"], node=instance["node"],
                label="oui", trigger_ts=instance["trigger_ts"],
            )
            frozen = json.dumps(key_before, sort_keys=True)
            self.assertNotIn(frozen, seen_keys)
            seen_keys.add(frozen)
        exhausted = manager.next_instance(concept_id="test_concept")
        self.assertTrue(exhausted["exhausted"])
        self.assertIsNone(exhausted["instance"])

    def test_non_label_requires_a_non_empty_note(self) -> None:
        manager = self._scanned_manager(1)
        instance = manager.next_instance(concept_id="test_concept")["instance"]
        with self.assertRaises(ValueError):
            manager.label(
                concept_id="test_concept", shape=instance["shape"], node=instance["node"],
                label="non", note="   ",
            )

    def test_oui_label_accepted_with_no_note(self) -> None:
        manager = self._scanned_manager(1)
        instance = manager.next_instance(concept_id="test_concept")["instance"]
        progress = manager.label(
            concept_id="test_concept", shape=instance["shape"], node=instance["node"], label="oui",
        )
        self.assertEqual(progress["total"], 1)
        self.assertEqual(progress["no_count"], 0)

    def test_invalid_label_value_rejected(self) -> None:
        manager = self._scanned_manager(1)
        instance = manager.next_instance(concept_id="test_concept")["instance"]
        with self.assertRaises(ValueError):
            manager.label(
                concept_id="test_concept", shape=instance["shape"], node=instance["node"], label="maybe",
            )


class GatingAndPromptTests(_RefinementTestBase):
    def setUp(self) -> None:
        self.manager = self._setup(_ZONE_CONCEPT)
        run_dir = self.collections_dir / "run1"
        candles = []
        t = 0.0
        for i in range(25):
            candles.append({"timestamp": t, "open": 100, "high": 101, "low": 99, "close": 99})
            t += 60.0
            candles.append({"timestamp": t, "open": 99, "high": 103 + i, "low": 99, "close": 102})
            t += 60.0
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=t + 60)
        self.manager.scan("test_concept")

    def _label(self, count: int, no_count: int) -> None:
        for i in range(count):
            instance = self.manager.next_instance(concept_id="test_concept")["instance"]
            if i < no_count:
                self.manager.label(
                    concept_id="test_concept", shape=instance["shape"], node=instance["node"],
                    label="non", note=f"nuance manquée numéro {i}",
                )
            else:
                self.manager.label(
                    concept_id="test_concept", shape=instance["shape"], node=instance["node"], label="oui",
                )

    def test_below_total_threshold_is_not_eligible(self) -> None:
        self._label(MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        progress = self.manager.progress(concept_id="test_concept")
        self.assertFalse(progress["eligible_for_prompt"])
        with self.assertRaises(ValueError):
            self.manager.build_prompt(concept_id="test_concept", template="TEMPLATE")

    def test_at_threshold_with_zero_no_is_not_eligible(self) -> None:
        self._label(MIN_TOTAL_FOR_PROMPT, no_count=0)
        progress = self.manager.progress(concept_id="test_concept")
        self.assertFalse(progress["eligible_for_prompt"])

    def test_at_threshold_with_one_no_is_eligible(self) -> None:
        self._label(MIN_TOTAL_FOR_PROMPT, no_count=MIN_NO_FOR_PROMPT)
        progress = self.manager.progress(concept_id="test_concept")
        self.assertTrue(progress["eligible_for_prompt"])
        prompt = self.manager.build_prompt(concept_id="test_concept", template="TEMPLATE")
        self.assertIn("TEMPLATE", prompt)
        self.assertIn("nuance manquée numéro 0", prompt)

    def test_prompt_includes_every_no_note_verbatim_scaling_up(self) -> None:
        self._label(25, no_count=20)
        prompt = self.manager.build_prompt(concept_id="test_concept", template="TEMPLATE")
        for i in range(20):
            self.assertIn(f"nuance manquée numéro {i}", prompt)


class AutoRefineJobTests(_RefinementTestBase, unittest.IsolatedAsyncioTestCase):
    # start_auto_refine_job schedules its background work via asyncio.
    # create_task (see refinement.run_scan_job), which requires a running
    # event loop -- IsolatedAsyncioTestCase, same pattern used across this
    # app's other job-creation tests.

    def _setup_eligible(self) -> None:
        self.manager = self._setup(_ZONE_CONCEPT, claude_code_command=["claude"])
        run_dir = self.collections_dir / "run1"
        candles = []
        t = 0.0
        for i in range(25):
            candles.append({"timestamp": t, "open": 100, "high": 101, "low": 99, "close": 99})
            t += 60.0
            candles.append({"timestamp": t, "open": 99, "high": 103 + i, "low": 99, "close": 102})
            t += 60.0
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=t + 60)
        self.manager.scan("test_concept")

    def _label(self, count: int, no_count: int) -> None:
        for i in range(count):
            instance = self.manager.next_instance(concept_id="test_concept")["instance"]
            label, note = ("non", f"nuance manquée numéro {i}") if i < no_count else ("oui", "")
            self.manager.label(
                concept_id="test_concept", shape=instance["shape"], node=instance["node"], label=label, note=note,
            )

    async def _wait_for_done(self, job) -> dict[str, object]:
        for _ in range(100):
            status = refinement.scan_job_status(self.manager.jobs, job.job_id)
            if status["done"]:
                return status
            await asyncio.sleep(0.02)
        self.fail("job never completed")

    def test_returns_none_below_eligibility(self) -> None:
        self._setup_eligible()
        self._label(MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        self.assertIsNone(self.manager.start_auto_refine_job(concept_id="test_concept", template="TEMPLATE"))

    def test_returns_none_when_claude_code_command_not_configured(self) -> None:
        self.manager = self._setup(_ZONE_CONCEPT)  # claude_code_command defaults to None
        run_dir = self.collections_dir / "run1"
        _write_kline_events(run_dir, [{"timestamp": 0.0, "open": 100, "high": 101, "low": 99, "close": 102}])
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=60)
        self.manager.scan("test_concept")
        for i in range(MIN_TOTAL_FOR_PROMPT):
            refinement.append_label(
                self.manager.feedback_dir, "test_concept", shape="zone",
                node={"direction": "bullish", "high": 1, "low": 1, "formed_at": float(i)},
                label="non" if i == 0 else "oui", note="x" if i == 0 else "",
            )
        self.assertIsNone(self.manager.start_auto_refine_job(concept_id="test_concept", template="TEMPLATE"))

    async def test_fires_once_at_eligibility_and_imports_auto_suffixed_file(self) -> None:
        self._setup_eligible()
        self._label(MIN_TOTAL_FOR_PROMPT - 1, no_count=1)  # not yet eligible
        well_formed = (
            "FILENAME: test_concept.py\n\n```python\n"
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n\n'
            "def compute(context):\n    return {}\n```\n"
        )
        with patch(
            "polymarket_btc.data_collection.control.concept_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {
                "filename": "test_concept.py",
                "content": well_formed.split("```python\n")[1].split("```")[0],
            }
            # This label crosses the threshold -- must fire.
            instance = self.manager.next_instance(concept_id="test_concept")["instance"]
            self.manager.label(
                concept_id="test_concept", shape=instance["shape"], node=instance["node"],
                label="oui",
            )
            job = self.manager.start_auto_refine_job(concept_id="test_concept", template="TEMPLATE")
            self.assertIsNotNone(job)
            status = await self._wait_for_done(job)
            self.assertIsNone(status["error"])
            self.assertEqual(status["result"]["filename"], "test_concept_auto.py")
            self.assertTrue((self.concepts_dir / "test_concept_auto.py").is_file())
            self.assertTrue((self.concepts_dir / "test_concept.py").is_file())  # original untouched

            # A further label, still eligible, must NOT fire again.
            mock_generate.reset_mock()
            instance = self.manager.next_instance(concept_id="test_concept")["instance"]
            self.manager.label(
                concept_id="test_concept", shape=instance["shape"], node=instance["node"], label="oui",
            )
            second_job = self.manager.start_auto_refine_job(concept_id="test_concept", template="TEMPLATE")
            self.assertIsNone(second_job)
            mock_generate.assert_not_called()

    async def test_failure_releases_claim_so_the_next_label_retries(self) -> None:
        self._setup_eligible()
        self._label(MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        with patch(
            "polymarket_btc.data_collection.control.concept_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.side_effect = ValueError("Claude Code a échoué")
            instance = self.manager.next_instance(concept_id="test_concept")["instance"]
            self.manager.label(
                concept_id="test_concept", shape=instance["shape"], node=instance["node"], label="oui",
            )
            job = self.manager.start_auto_refine_job(concept_id="test_concept", template="TEMPLATE")
            status = await self._wait_for_done(job)
            self.assertIn("échoué", status["error"])

            # Still eligible, and the failure released the claim -- the
            # very next attempt must be allowed to retry.
            mock_generate.side_effect = None
            mock_generate.return_value = {"filename": "test_concept.py", "content": "CONCEPT_INFO = {}\n"}
            retry_job = self.manager.start_auto_refine_job(concept_id="test_concept", template="TEMPLATE")
            self.assertIsNotNone(retry_job)
            retry_status = await self._wait_for_done(retry_job)
            self.assertIsNone(retry_status["error"])


class SyntheticExampleTests(_RefinementTestBase, unittest.IsolatedAsyncioTestCase):
    # generate_synthetic_example_via_claude_code is mocked throughout --
    # no real Claude Code call, no real collected data needed at all
    # (unlike every other test class here) since _generate_synthetic reads
    # the concept's own source directly and runs compute() itself.

    async def _wait_for_done(self, manager: ConceptRefinementManager, job) -> dict[str, object]:
        for _ in range(100):
            status = refinement.scan_job_status(manager.jobs, job.job_id)
            if status["done"]:
                return status
            await asyncio.sleep(0.02)
        self.fail("job never completed")

    async def test_detected_synthetic_instance_matches_next_instance_shape(self) -> None:
        manager = self._setup(_ZONE_CONCEPT, claude_code_command=["claude"])
        synthetic = {
            "binance_futures_kline": [
                {"open": 100, "high": 103, "low": 99, "close": 102, "timestamp": 60.0},
            ],
        }
        with patch(
            "polymarket_btc.data_collection.control.concept_refinement.generate_synthetic_example_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = synthetic
            job = manager.start_synthetic_job(concept_id="test_concept")
            status = await self._wait_for_done(manager, job)
        self.assertIsNone(status["error"])
        result = status["result"]
        self.assertEqual(result["shape"], "zone")
        self.assertEqual(result["node"]["high"], 103)
        self.assertEqual(result["trigger_ts"], 60.0)
        self.assertEqual(result["window"], {"key": "binance_futures_kline", "candles": synthetic["binance_futures_kline"]})
        self.assertTrue(result["synthetic"])

    async def test_nothing_detected_raises_clear_error(self) -> None:
        manager = self._setup(_ZONE_CONCEPT, claude_code_command=["claude"])
        synthetic = {
            "binance_futures_kline": [
                {"open": 100, "high": 101, "low": 99, "close": 99, "timestamp": 60.0},  # down candle, no zone
            ],
        }
        with patch(
            "polymarket_btc.data_collection.control.concept_refinement.generate_synthetic_example_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = synthetic
            job = manager.start_synthetic_job(concept_id="test_concept")
            status = await self._wait_for_done(manager, job)
        self.assertIn("rien détecté", status["error"])

    async def test_compute_exception_surfaces_clearly(self) -> None:
        crashing_concept = (
            'CONCEPT_INFO = {"label": "x", "description": "d", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n    raise RuntimeError(\"boom\")\n"
        )
        manager = self._setup(crashing_concept, claude_code_command=["claude"])
        with patch(
            "polymarket_btc.data_collection.control.concept_refinement.generate_synthetic_example_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {"binance_futures_kline": []}
            job = manager.start_synthetic_job(concept_id="test_concept")
            status = await self._wait_for_done(manager, job)
        self.assertIn("boom", status["error"])

    async def test_not_configured_raises_clearly(self) -> None:
        manager = self._setup(_ZONE_CONCEPT)  # claude_code_command defaults to None
        job = manager.start_synthetic_job(concept_id="test_concept")
        status = await self._wait_for_done(manager, job)
        self.assertIn("non configurée", status["error"])


if __name__ == "__main__":
    unittest.main()
