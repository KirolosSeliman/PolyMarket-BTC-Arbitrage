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
from polymarket_btc.data_collection.control.microsystem_refinement import MicrosystemRefinementManager
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

# A concept emitting one "zone" candidate per up-candle -- same shape as
# test_concept_refinement.py's own _ZONE_CONCEPT (direction always
# bullish, high/low/formed_at straight from the candle).
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

# A microsystem wired to _ZONE_CONCEPT that turns each zone into a compound
# "setup" -- entry_zone (the zone itself, re-shaped) plus a
# confirmation_level (a level derived from it) -- exactly the >=2
# zone/level sub-field shape refinement.looks_like_setup_entry requires,
# mirroring microsystems/fvg_sweep_reversal.py's own real shape
# (initial_fvg + sweep + reversal_fvg) at smaller scale.
_SETUP_MICROSYSTEM = '''
MICROSYSTEM_INFO = {
    "label": "Setup test", "description": "d",
    "concept_inputs": ["zone_concept"],
}

def compute(context):
    zone_result = context.concepts.get("zone_concept") or {}
    zones = zone_result.get("zones") or []
    setups = []
    for z in zones:
        setups.append({
            "signal": "haussier",
            "entry_zone": z,
            "confirmation_level": {
                "direction": "bullish", "level": z["low"],
                "formed_at": z["formed_at"], "swept_at": z["formed_at"] + 30.0,
            },
        })
    return {"setups": setups}
'''

# A microsystem with no concept_inputs at all -- reads data directly and
# builds the exact same compound setup shape, to exercise the
# zero-wired-concepts branch of scan()/_instance_window()'s own key
# resolution (falls back to the microsystem's own data_inputs[0]).
_DATA_ONLY_MICROSYSTEM = '''
MICROSYSTEM_INFO = {
    "label": "Data only setup test", "description": "d",
    "data_inputs": ["binance_futures_kline"],
}

def compute(context):
    candles = context.data.get("binance_futures_kline") or []
    setups = []
    for c in candles:
        if c["close"] > c["open"]:
            setups.append({
                "signal": "haussier",
                "entry_zone": {
                    "direction": "bullish", "high": c["high"], "low": c["low"], "formed_at": c["timestamp"],
                },
                "confirmation_level": {
                    "direction": "bullish", "level": c["low"], "formed_at": c["timestamp"],
                    "swept_at": c["timestamp"] + 30.0,
                },
            })
    return {"setups": setups}
'''

# concept_inputs names a concept id that never resolves, and no data_inputs
# of its own -- for the "no data sources resolvable at all" error path.
_ORPHAN_MICROSYSTEM = '''
MICROSYSTEM_INFO = {
    "label": "Orphan", "description": "d",
    "concept_inputs": ["missing_concept"],
}

def compute(context):
    return {}
'''


def _write_kline_events(run_dir: Path, candles: list[dict[str, float]]) -> None:
    """Full OHLC control, matching test_concept_refinement.py's own helper
    of the same name -- distinct high/low/open/close values are needed to
    exercise the up-candle-only zone concept meaningfully."""
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
        self, microsystem_source: str, *, concept_source: str | None = _ZONE_CONCEPT,
        microsystem_filename: str = "test_micro.py", claude_code_command: list[str] | None = None,
    ) -> MicrosystemRefinementManager:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        concepts_dir = root / "concepts"
        concepts_dir.mkdir(parents=True)
        if concept_source is not None:
            (concepts_dir / "zone_concept.py").write_text(concept_source, encoding="utf-8")
        microsystems_dir = root / "microsystems"
        microsystems_dir.mkdir(parents=True)
        (microsystems_dir / microsystem_filename).write_text(microsystem_source, encoding="utf-8")
        self.collections_dir = root / "collections"
        runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=self.collections_dir,
            plugins_dir=root / "plugins", concepts_dir=concepts_dir,
            microsystems_dir=microsystems_dir, execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles", filter_dir=root / "filter_profiles",
        )
        return MicrosystemRefinementManager(
            feedback_dir=root / "microsystem_feedback", runs=runs, claude_code_command=claude_code_command,
        )


class ScanSetupDetectionTests(_RefinementTestBase):
    def test_finds_one_whole_setup_per_up_candle_not_fragmented(self) -> None:
        manager = self._setup(_SETUP_MICROSYSTEM)
        run_dir = self.collections_dir / "run1"
        candles = [
            {"timestamp": 0.0, "open": 100, "high": 101, "low": 99, "close": 99},    # down
            {"timestamp": 60.0, "open": 99, "high": 103, "low": 99, "close": 102},   # up #1
            {"timestamp": 120.0, "open": 102, "high": 104, "low": 101, "close": 101},  # down
            {"timestamp": 180.0, "open": 101, "high": 106, "low": 100, "close": 105},  # up #2
        ]
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=240)

        result = manager.scan("test_micro")
        self.assertEqual(result["added"], 2)  # 2 whole setups, not 4 fragmented sub-nodes
        cache = refinement.load_pool_cache(manager.feedback_dir, "test_micro")
        self.assertEqual(len(cache["candidates"]), 2)
        for candidate in cache["candidates"]:
            self.assertEqual(candidate["shape"], "setup")
            self.assertIn("entry_zone", candidate["node"])
            self.assertIn("confirmation_level", candidate["node"])
        formed_ats = sorted(c["node"]["entry_zone"]["formed_at"] for c in cache["candidates"])
        self.assertEqual(formed_ats, [60.0, 180.0])

    def test_trigger_ts_is_the_max_of_the_setups_own_subnodes(self) -> None:
        manager = self._setup(_SETUP_MICROSYSTEM)
        run_dir = self.collections_dir / "run1"
        candles = [{"timestamp": 60.0, "open": 99, "high": 103, "low": 99, "close": 102}]
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=0, end_ts=120)
        manager.scan("test_micro")
        cache = refinement.load_pool_cache(manager.feedback_dir, "test_micro")
        candidate = cache["candidates"][0]
        # entry_zone.formed_at=60.0, confirmation_level.swept_at=90.0
        # (preferred over its own formed_at=60.0) -- the setup's own
        # trigger is the later of the two.
        self.assertEqual(candidate["trigger_ts"], 90.0)

    def test_no_real_coverage_raises_clearly(self) -> None:
        uncovered_concept = (
            'CONCEPT_INFO = {"label": "u", "description": "d", '
            '"data_sources": ["binance_futures_mark_price"]}\n'
            "def compute(context):\n    return {}\n"
        )
        uncovered_microsystem = (
            'MICROSYSTEM_INFO = {"label": "u", "description": "d", "concept_inputs": ["zone_concept"]}\n'
            "def compute(context):\n    return {}\n"
        )
        manager = self._setup(
            uncovered_microsystem, concept_source=uncovered_concept, microsystem_filename="uncovered.py",
        )
        with self.assertRaises(ValueError):
            manager.scan("uncovered")

    def test_orphan_concept_input_and_no_data_inputs_raises_clearly(self) -> None:
        manager = self._setup(_ORPHAN_MICROSYSTEM, concept_source=None, microsystem_filename="orphan.py")
        with self.assertRaises(ValueError):
            manager.scan("orphan")

    def test_data_only_microsystem_with_no_concept_inputs_scans_fine(self) -> None:
        manager = self._setup(_DATA_ONLY_MICROSYSTEM, concept_source=None, microsystem_filename="data_only.py")
        run_dir = self.collections_dir / "run1"
        candles = [{"timestamp": 60.0, "open": 99, "high": 103, "low": 99, "close": 102}]
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=0, end_ts=120)
        result = manager.scan("data_only")
        self.assertEqual(result["added"], 1)

    def test_zero_candidate_microsystem_scans_cleanly(self) -> None:
        manager = self._setup(_SETUP_MICROSYSTEM)
        run_dir = self.collections_dir / "run1"
        candles = [{"timestamp": 0.0, "open": 100, "high": 101, "low": 99, "close": 99}]  # down only
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-60, end_ts=60)
        result = manager.scan("test_micro")
        self.assertEqual(result["added"], 0)
        next_result = manager.next_instance(microsystem_id="test_micro")
        self.assertTrue(next_result["no_candidates"])
        self.assertFalse(next_result["exhausted"])


class InstanceWindowDisplayKeyTests(_RefinementTestBase):
    def test_display_key_resolves_through_the_first_wired_concepts_own_data_sources(self) -> None:
        manager = self._setup(_SETUP_MICROSYSTEM)
        run_dir = self.collections_dir / "run1"
        candles = [{"timestamp": 60.0, "open": 99, "high": 103, "low": 99, "close": 102}]
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=0, end_ts=120)
        manager.scan("test_micro")
        instance = manager.next_instance(microsystem_id="test_micro")["instance"]
        self.assertEqual(instance["window"]["key"], "binance_futures_kline")

    def test_window_start_precedes_the_earliest_subnode_not_just_the_trigger(self) -> None:
        # entry_zone.formed_at=60.0 is well before confirmation_level's own
        # swept_at=90.0 trigger -- the window must start before the earlier
        # of the two, not just NEXT_WINDOW_BEFORE_SECONDS before the trigger.
        manager = self._setup(_SETUP_MICROSYSTEM)
        run_dir = self.collections_dir / "run1"
        candles = [{"timestamp": 60.0, "open": 99, "high": 103, "low": 99, "close": 102}]
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, sources=["binance_futures_kline"], start_ts=-3600, end_ts=3600)
        manager.scan("test_micro")
        instance = manager.next_instance(microsystem_id="test_micro")["instance"]
        candles_in_window = instance["window"]["candles"]
        self.assertTrue(any(c["timestamp"] <= 60.0 for c in candles_in_window))


class NextInstanceAndLabelTests(_RefinementTestBase):
    def _scanned_manager(self, up_candle_count: int) -> MicrosystemRefinementManager:
        manager = self._setup(_SETUP_MICROSYSTEM)
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
        manager.scan("test_micro")
        return manager

    def test_next_instance_never_repeats_an_already_labeled_candidate(self) -> None:
        manager = self._scanned_manager(3)
        seen_keys = set()
        for _ in range(3):
            result = manager.next_instance(microsystem_id="test_micro")
            self.assertFalse(result["exhausted"])
            instance = result["instance"]
            self.assertIsNotNone(instance)
            key_before = json.dumps({"shape": instance["shape"], "node": instance["node"]}, sort_keys=True)
            manager.label(
                microsystem_id="test_micro", shape=instance["shape"], node=instance["node"],
                label="oui", trigger_ts=instance["trigger_ts"],
            )
            self.assertNotIn(key_before, seen_keys)
            seen_keys.add(key_before)
        exhausted = manager.next_instance(microsystem_id="test_micro")
        self.assertTrue(exhausted["exhausted"])
        self.assertIsNone(exhausted["instance"])

    def test_non_label_requires_a_non_empty_note(self) -> None:
        manager = self._scanned_manager(1)
        instance = manager.next_instance(microsystem_id="test_micro")["instance"]
        with self.assertRaises(ValueError):
            manager.label(
                microsystem_id="test_micro", shape=instance["shape"], node=instance["node"],
                label="non", note="   ",
            )

    def test_oui_label_accepted_with_no_note(self) -> None:
        manager = self._scanned_manager(1)
        instance = manager.next_instance(microsystem_id="test_micro")["instance"]
        progress = manager.label(
            microsystem_id="test_micro", shape=instance["shape"], node=instance["node"], label="oui",
        )
        self.assertEqual(progress["total"], 1)
        self.assertEqual(progress["no_count"], 0)


class GatingAndPromptTests(_RefinementTestBase):
    def setUp(self) -> None:
        self.manager = self._setup(_SETUP_MICROSYSTEM)
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
        self.manager.scan("test_micro")

    def _label(self, count: int, no_count: int) -> None:
        for i in range(count):
            instance = self.manager.next_instance(microsystem_id="test_micro")["instance"]
            if i < no_count:
                self.manager.label(
                    microsystem_id="test_micro", shape=instance["shape"], node=instance["node"],
                    label="non", note=f"nuance manquée numéro {i}",
                )
            else:
                self.manager.label(
                    microsystem_id="test_micro", shape=instance["shape"], node=instance["node"], label="oui",
                )

    def test_below_total_threshold_is_not_eligible(self) -> None:
        self._label(MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        progress = self.manager.progress(microsystem_id="test_micro")
        self.assertFalse(progress["eligible_for_prompt"])
        with self.assertRaises(ValueError):
            self.manager.build_prompt(microsystem_id="test_micro", template="TEMPLATE")

    def test_at_threshold_with_one_no_is_eligible(self) -> None:
        self._label(MIN_TOTAL_FOR_PROMPT, no_count=MIN_NO_FOR_PROMPT)
        progress = self.manager.progress(microsystem_id="test_micro")
        self.assertTrue(progress["eligible_for_prompt"])
        prompt = self.manager.build_prompt(microsystem_id="test_micro", template="TEMPLATE")
        self.assertIn("TEMPLATE", prompt)
        self.assertIn("nuance manquée numéro 0", prompt)
        self.assertIn("MICROSYSTEM_INFO", prompt)  # embeds the microsystem's own current source


class AutoRefineJobTests(_RefinementTestBase, unittest.IsolatedAsyncioTestCase):
    # See test_concept_refinement.py's AutoRefineJobTests -- identical
    # mechanism, mirrored here for microsystem_id.

    def _setup_eligible(self) -> None:
        self.manager = self._setup(_SETUP_MICROSYSTEM, claude_code_command=["claude"])
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
        self.manager.scan("test_micro")

    def _label(self, count: int, no_count: int) -> None:
        for i in range(count):
            instance = self.manager.next_instance(microsystem_id="test_micro")["instance"]
            label, note = ("non", f"nuance manquée numéro {i}") if i < no_count else ("oui", "")
            self.manager.label(
                microsystem_id="test_micro", shape=instance["shape"], node=instance["node"],
                label=label, note=note,
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
        self.assertIsNone(self.manager.start_auto_refine_job(microsystem_id="test_micro", template="TEMPLATE"))

    async def test_fires_once_at_eligibility_and_imports_auto_suffixed_file(self) -> None:
        self._setup_eligible()
        self._label(MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        well_formed_content = "MICROSYSTEM_INFO = {}\n"
        with patch(
            "polymarket_btc.data_collection.control.microsystem_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {"filename": "test_micro.py", "content": well_formed_content}
            instance = self.manager.next_instance(microsystem_id="test_micro")["instance"]
            self.manager.label(
                microsystem_id="test_micro", shape=instance["shape"], node=instance["node"], label="oui",
            )
            job = self.manager.start_auto_refine_job(microsystem_id="test_micro", template="TEMPLATE")
            self.assertIsNotNone(job)
            status = await self._wait_for_done(job)
            self.assertIsNone(status["error"])
            self.assertEqual(status["result"]["filename"], "test_micro_auto.py")
            self.assertTrue((self.manager.runs.microsystems_dir / "test_micro_auto.py").is_file())

            mock_generate.reset_mock()
            instance = self.manager.next_instance(microsystem_id="test_micro")["instance"]
            self.manager.label(
                microsystem_id="test_micro", shape=instance["shape"], node=instance["node"], label="oui",
            )
            second_job = self.manager.start_auto_refine_job(microsystem_id="test_micro", template="TEMPLATE")
            self.assertIsNone(second_job)
            mock_generate.assert_not_called()

    async def test_failure_releases_claim_so_the_next_label_retries(self) -> None:
        self._setup_eligible()
        self._label(MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        with patch(
            "polymarket_btc.data_collection.control.microsystem_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.side_effect = ValueError("Claude Code a échoué")
            instance = self.manager.next_instance(microsystem_id="test_micro")["instance"]
            self.manager.label(
                microsystem_id="test_micro", shape=instance["shape"], node=instance["node"], label="oui",
            )
            job = self.manager.start_auto_refine_job(microsystem_id="test_micro", template="TEMPLATE")
            status = await self._wait_for_done(job)
            self.assertIn("échoué", status["error"])

            mock_generate.side_effect = None
            mock_generate.return_value = {"filename": "test_micro.py", "content": "MICROSYSTEM_INFO = {}\n"}
            retry_job = self.manager.start_auto_refine_job(microsystem_id="test_micro", template="TEMPLATE")
            self.assertIsNotNone(retry_job)
            retry_status = await self._wait_for_done(retry_job)
            self.assertIsNone(retry_status["error"])


class SyntheticExampleTests(_RefinementTestBase):
    # generate_synthetic_instance is pure/synchronous (no AI call, no job)
    # -- no real collected data needed, it reads the microsystem's own
    # source (and its wired concepts') directly and runs compute() itself.

    def test_setup_microsystem_detects_the_synthetic_breakout(self) -> None:
        # The two-stage path: zone_concept fires on build_synthetic_
        # candle_set's up-candles, then the microsystem combines each into
        # a whole "setup" -- the exact scenario that motivated this feature.
        manager = self._setup(_SETUP_MICROSYSTEM)
        result = manager.generate_synthetic_instance(microsystem_id="test_micro")
        self.assertEqual(result["shape"], "setup")
        self.assertIn("entry_zone", result["node"])
        self.assertIn("confirmation_level", result["node"])
        self.assertEqual(result["window"]["key"], "binance_futures_kline")
        self.assertTrue(result["window"]["candles"])
        self.assertTrue(result["synthetic"])

    def test_data_only_microsystem_with_no_concept_inputs_works_too(self) -> None:
        manager = self._setup(_DATA_ONLY_MICROSYSTEM, concept_source=None, microsystem_filename="data_only.py")
        result = manager.generate_synthetic_instance(microsystem_id="data_only")
        self.assertEqual(result["shape"], "setup")

    def test_no_data_sources_at_all_raises_clearly(self) -> None:
        manager = self._setup(_ORPHAN_MICROSYSTEM, concept_source=None, microsystem_filename="orphan.py")
        with self.assertRaises(ValueError) as ctx:
            manager.generate_synthetic_instance(microsystem_id="orphan")
        self.assertIn("aucune source de données câblée", str(ctx.exception))

    def test_nothing_detected_raises_clear_error(self) -> None:
        never_fires = (
            'MICROSYSTEM_INFO = {"label": "x", "description": "d", "data_inputs": ["binance_futures_kline"]}\n'
            "def compute(context):\n    return {}\n"
        )
        manager = self._setup(never_fires, concept_source=None, microsystem_filename="never_fires.py")
        with self.assertRaises(ValueError) as ctx:
            manager.generate_synthetic_instance(microsystem_id="never_fires")
        self.assertIn("rien déclenché", str(ctx.exception))

    def test_compute_exception_surfaces_clearly(self) -> None:
        crashing = (
            'MICROSYSTEM_INFO = {"label": "x", "description": "d", "data_inputs": ["binance_futures_kline"]}\n'
            "def compute(context):\n    raise RuntimeError(\"boom\")\n"
        )
        manager = self._setup(crashing, concept_source=None, microsystem_filename="crashing.py")
        with self.assertRaises(ValueError) as ctx:
            manager.generate_synthetic_instance(microsystem_id="crashing")
        self.assertIn("boom", str(ctx.exception))

    def test_a_wired_concept_raising_only_loses_its_own_output(self) -> None:
        crashing_concept = (
            'CONCEPT_INFO = {"label": "x", "description": "d", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n    raise RuntimeError(\"concept boom\")\n"
        )
        tolerant_microsystem = (
            'MICROSYSTEM_INFO = {"label": "x", "description": "d", "concept_inputs": ["zone_concept"]}\n'
            "def compute(context):\n"
            "    return {\"setups\": [{\"a\": {\"direction\": \"bullish\", \"high\": 2, \"low\": 1, \"formed_at\": 0.0}, "
            "\"b\": {\"level\": 1, \"formed_at\": 0.0}}]}\n"
        )
        manager = self._setup(tolerant_microsystem, concept_source=crashing_concept)
        result = manager.generate_synthetic_instance(microsystem_id="test_micro")
        self.assertEqual(result["shape"], "setup")


if __name__ == "__main__":
    unittest.main()
