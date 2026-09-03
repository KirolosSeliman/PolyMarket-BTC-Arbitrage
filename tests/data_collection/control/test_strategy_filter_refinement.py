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
from polymarket_btc.data_collection.control.refinement import MIN_TOTAL_FOR_PROMPT
from polymarket_btc.data_collection.control.runs import CollectionRunManager
from polymarket_btc.data_collection.control.strategies import StrategyManager
from polymarket_btc.data_collection.control.strategy_filter_refinement import StrategyFilterRefinementManager
from polymarket_btc.data_collection.market_data.models import (
    BinanceKlinePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
)
from polymarket_btc.data_collection.market_data.storage import RawEventStorage

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

# A trivial concept -- only exists so backtest_eligibility's own
# required-keys coverage isn't vacuously empty (combined_coverage(set(), ...)
# returns [] unconditionally, per its own "not required_keys -> []" rule,
# even when real price data genuinely exists).
_KLINE_READER_CONCEPT = '''
CONCEPT_INFO = {
    "label": "x", "description": "y", "data_sources": ["binance_futures_kline"],
}

def compute(context):
    return {"last_close": None}
'''

_ENTER_ONCE_EXECUTION = '''
EXECUTION_INFO = {"label": "x", "description": "y"}

def execute(context):
    return {"direction": "long"}
'''

_FIXED_SLTP_MANAGEMENT = '''
MANAGEMENT_INFO = {
    "label": "x", "description": "y",
    "config_schema": [
        {"name": "stop_loss_pct", "type": "number", "label": "SL", "default": 5.0},
        {"name": "take_profit_pct", "type": "number", "label": "TP", "default": 5.0},
    ],
}

def manage(context):
    return {"stop_loss_pct": 5.0, "take_profit_pct": 5.0}
'''

_ALWAYS_VETO_FILTER = '''
FILTER_INFO = {"label": "x", "description": "y"}

def filter(context):
    return {"veto": True}
'''


def _write_kline_events(run_dir: Path, candles: list[tuple[float, float]]) -> None:
    """candles: [(close_time_seconds, close_price), ...]."""
    storage = RawEventStorage(run_dir, zstd_level=3)
    for i, (close_s, close_price) in enumerate(candles):
        close_ns = int(close_s * 1e9)
        open_ns = close_ns - 60_000_000_000
        payload = BinanceKlinePayload(
            market="futures", interval="1m", open_time_ns=open_ns, close_time_ns=close_ns,
            open=Decimal(str(close_price)), high=Decimal(str(close_price)), low=Decimal(str(close_price)),
            close=Decimal(str(close_price)), base_volume=Decimal("1"), quote_volume=Decimal("1"),
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


def _write_manifest(run_dir: Path, *, start_ts: float, end_ts: float) -> None:
    manifest = {
        "mode": "access", "sources": ["binance_futures_kline"], "plugins": [],
        "start_ts_utc": datetime.fromtimestamp(start_ts, tz=UTC).isoformat(),
        "end_ts_utc": datetime.fromtimestamp(end_ts, tz=UTC).isoformat(),
        "data_dir": str(run_dir),
    }
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


class _RefinementTestBase(unittest.TestCase):
    def _setup(
        self, *, with_filter: bool = False, claude_code_command: list[str] | None = None,
    ) -> tuple[StrategyFilterRefinementManager, Path]:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        (root / "concepts").mkdir(parents=True)
        (root / "concepts" / "kline_reader.py").write_text(_KLINE_READER_CONCEPT, encoding="utf-8")
        (root / "microsystems").mkdir(parents=True)
        (root / "execution_profiles").mkdir(parents=True)
        (root / "execution_profiles" / "enter_once.py").write_text(_ENTER_ONCE_EXECUTION, encoding="utf-8")
        (root / "management_profiles").mkdir(parents=True)
        (root / "management_profiles" / "fixed_sltp.py").write_text(_FIXED_SLTP_MANAGEMENT, encoding="utf-8")
        (root / "filter_profiles").mkdir(parents=True)
        (root / "filter_profiles" / "always_veto.py").write_text(_ALWAYS_VETO_FILTER, encoding="utf-8")

        self.collections_dir = root / "collections"
        runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=self.collections_dir,
            plugins_dir=root / "plugins", concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems", execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles", filter_dir=root / "filter_profiles",
        )
        strategies = StrategyManager(strategies_dir=root / "strategies", runs=runs)
        strategies.save_strategy(
            name="test_strategy",
            concepts=[{"instance_id": "concept_1", "concept_id": "kline_reader", "config": {}, "data_bindings": {}}],
            microsystems=[],
            execution={"execution_id": "enter_once", "config": {}},
            management={"management_id": "fixed_sltp", "config": {}},
            filter={"filter_id": "always_veto", "config": {}} if with_filter else None,
        )
        manager = StrategyFilterRefinementManager(
            feedback_dir=root / "filter_feedback", runs=runs, strategies=strategies,
            claude_code_command=claude_code_command,
        )
        return manager, root

    def _write_candles(self, root: Path, candle_count: int) -> None:
        """Alternating 100/106 close prices -- with ENTER_ONCE (always
        "long") and a fixed 5% SL/TP, the very next candle after any entry
        crosses the TP (or SL, once a later entry lands at 106), so a trade
        closes and a new one opens almost every step -- plenty of real,
        deterministic trade candidates from a handful of candles."""
        run_dir = self.collections_dir / "run1"
        t = 0.0
        candles = []
        for i in range(candle_count):
            price = 100.0 if i % 2 == 0 else 106.0
            candles.append((t, price))
            t += 60.0
        _write_kline_events(run_dir, candles)
        _write_manifest(run_dir, start_ts=-60.0, end_ts=t + 60.0)


class ScanFindsRealTradesTests(_RefinementTestBase):
    def test_finds_real_trade_candidates_even_with_a_veto_filter_attached(self) -> None:
        # The strategy's own saved filter is always_veto -- if scan()
        # actually replayed with it active, trades would be 0 and there'd
        # be nothing to judge. See the module's own "forced None" docstring.
        manager, root = self._setup(with_filter=True)
        self._write_candles(root, 10)
        result = manager.scan("test_strategy")
        self.assertGreater(result["added"], 0)
        cache = refinement.load_pool_cache(manager.feedback_dir, "test_strategy")
        for candidate in cache["candidates"]:
            self.assertEqual(candidate["shape"], "trade")
            self.assertIn("entry_time", candidate["node"])
            self.assertIn("direction", candidate["node"])

    def test_no_real_coverage_raises_clearly(self) -> None:
        manager, _root = self._setup()
        with self.assertRaises(ValueError):
            manager.scan("test_strategy")

    def test_unknown_strategy_raises_clearly(self) -> None:
        manager, root = self._setup()
        self._write_candles(root, 10)
        with self.assertRaises(ValueError):
            manager.scan("no_such_strategy")

    def test_missing_execution_or_management_raises_clearly(self) -> None:
        manager, root = self._setup()
        self._write_candles(root, 10)
        manager.strategies.save_strategy(
            name="no_execution_strategy", concepts=[], microsystems=[], execution=None, management=None,
        )
        with self.assertRaises(ValueError):
            manager.scan("no_execution_strategy")


class NextInstanceReplayTests(_RefinementTestBase):
    """Coverage for next_instance's window["replay"] -- the bounded replay
    (concept/microsystem outputs leading up to the trade) the review
    screen's "Raisonnement" panel renders."""

    def test_next_instance_includes_a_replay_with_the_reviewing_concept(self) -> None:
        manager, root = self._setup()
        self._write_candles(root, 10)
        manager.scan("test_strategy")
        result = manager.next_instance(strategy_name="test_strategy")
        self.assertFalse(result["exhausted"])
        self.assertFalse(result["no_candidates"])
        window = result["instance"]["window"]
        self.assertIsNotNone(window["replay"])
        self.assertGreater(len(window["replay"]["timeline"]), 0)
        # kline_reader never declares required_lookback_seconds (unbounded)
        # -- estimate_warmup_seconds must fall back to the strategy's full
        # coverage start, so the replay's own first step is no later than
        # the earliest collected candle.
        self.assertEqual(window["replay"]["timeline"][0]["timestamp"], -60.0)
        self.assertIn("concept_1", window["replay"]["timeline"][-1]["concepts"])

    def test_replay_is_none_rather_than_raising_when_it_fails(self) -> None:
        # A strategy whose execution profile is no longer discoverable
        # (deleted since the candidate was found) can't replay -- run_
        # backtest raises for a genuinely unknown execution_id, but next_
        # instance must still return the trade with its candles, just
        # without a reasoning panel, not blow up the whole review flow.
        manager, root = self._setup()
        self._write_candles(root, 10)
        manager.scan("test_strategy")
        (root / "execution_profiles" / "enter_once.py").unlink()
        result = manager.next_instance(strategy_name="test_strategy")
        self.assertIsNone(result["instance"]["window"]["replay"])
        self.assertGreater(len(result["instance"]["window"]["candles"]), 0)


class RescanCachingTests(_RefinementTestBase):
    def _resave(self, manager: StrategyFilterRefinementManager, **overrides: object) -> None:
        base = dict(
            name="test_strategy",
            concepts=[{"instance_id": "concept_1", "concept_id": "kline_reader", "config": {}, "data_bindings": {}}],
            microsystems=[],
            execution={"execution_id": "enter_once", "config": {}},
            management={"management_id": "fixed_sltp", "config": {}},
            overwrite=True,
        )
        base.update(overrides)
        manager.strategies.save_strategy(**base)

    def test_rescan_after_a_no_op_resave_reuses_the_cache(self) -> None:
        manager, root = self._setup()
        self._write_candles(root, 10)
        first = manager.scan("test_strategy")
        self.assertGreater(first["added"], 0)

        # Re-saved with identical content -- only created_at_utc/
        # updated_at_utc actually change. Without excluding those two
        # fields from the cache's own script hash, this alone would wipe
        # the cache and force a full real-data replay all over again.
        self._resave(manager)
        second = manager.scan("test_strategy")
        self.assertEqual(second["added"], 0)
        self.assertEqual(second["candidate_count"], first["candidate_count"])

    def test_rescan_after_a_real_change_does_invalidate_the_cache(self) -> None:
        manager, root = self._setup()
        self._write_candles(root, 10)
        first = manager.scan("test_strategy")
        self.assertGreater(first["added"], 0)

        # A second concept instance is a genuine definition change (not
        # timestamp noise) -- the exclusion above must not become a
        # blanket "never rescan" shortcut.
        self._resave(manager, concepts=[
            {"instance_id": "concept_1", "concept_id": "kline_reader", "config": {}, "data_bindings": {}},
            {"instance_id": "concept_2", "concept_id": "kline_reader", "config": {}, "data_bindings": {}},
        ])
        second = manager.scan("test_strategy")
        # Rescanned from scratch (cache wiped) -- the same real trades are
        # found again, this time counted as newly "added" rather than
        # already-seen.
        self.assertEqual(second["added"], first["candidate_count"])


class BuildPromptTests(_RefinementTestBase):
    def test_no_filter_configured_gets_a_placeholder_not_a_source_read(self) -> None:
        manager, root = self._setup(with_filter=False)
        self._write_candles(root, 10)
        manager.scan("test_strategy")
        instance = manager.next_instance(strategy_name="test_strategy")["instance"]
        manager.label(
            strategy_name="test_strategy", shape=instance["shape"], node=instance["node"],
            label="non", note="raison de test",
        )
        for _ in range(9):
            instance = manager.next_instance(strategy_name="test_strategy")["instance"]
            if instance is None:
                break
            manager.label(
                strategy_name="test_strategy", shape=instance["shape"], node=instance["node"], label="oui",
            )
        progress = manager.progress(strategy_name="test_strategy")
        if not progress["eligible_for_prompt"]:
            self.skipTest("not enough real candidates in this fixture to reach the prompt gate")
        prompt = manager.build_prompt(strategy_name="test_strategy", template="TEMPLATE")
        self.assertIn("Aucun filtre", prompt)
        self.assertIn("raison de test", prompt)

    def test_existing_filter_embeds_its_own_source(self) -> None:
        manager, root = self._setup(with_filter=True)
        self._write_candles(root, 10)
        manager.scan("test_strategy")
        instance = manager.next_instance(strategy_name="test_strategy")["instance"]
        manager.label(
            strategy_name="test_strategy", shape=instance["shape"], node=instance["node"],
            label="non", note="raison de test",
        )
        for _ in range(9):
            instance = manager.next_instance(strategy_name="test_strategy")["instance"]
            if instance is None:
                break
            manager.label(
                strategy_name="test_strategy", shape=instance["shape"], node=instance["node"], label="oui",
            )
        progress = manager.progress(strategy_name="test_strategy")
        if not progress["eligible_for_prompt"]:
            self.skipTest("not enough real candidates in this fixture to reach the prompt gate")
        prompt = manager.build_prompt(strategy_name="test_strategy", template="TEMPLATE")
        self.assertIn("FILTER_INFO", prompt)
        self.assertNotIn("Aucun filtre", prompt)


class AutoRefineJobTests(_RefinementTestBase, unittest.IsolatedAsyncioTestCase):
    # See test_concept_refinement.py's AutoRefineJobTests -- identical
    # mechanism, mirrored here for strategy_name. Uses with_filter=False
    # (build_prompt's placeholder-source path -- already exercised by
    # BuildPromptTests above) since it needs no filter_id plumbing.

    def _setup_eligible(self) -> tuple[StrategyFilterRefinementManager, Path]:
        manager, root = self._setup(with_filter=False, claude_code_command=["claude"])
        self._write_candles(root, 10)
        manager.scan("test_strategy")
        return manager, root

    def _label(self, manager: StrategyFilterRefinementManager, count: int, no_count: int) -> None:
        labeled = 0
        for i in range(count):
            instance = manager.next_instance(strategy_name="test_strategy")["instance"]
            if instance is None:
                break
            label, note = ("non", f"nuance manquée numéro {i}") if i < no_count else ("oui", "")
            manager.label(
                strategy_name="test_strategy", shape=instance["shape"], node=instance["node"],
                label=label, note=note,
            )
            labeled += 1
        if labeled < count:
            self.skipTest("not enough real trade candidates in this fixture to reach the prompt gate")

    async def _wait_for_done(self, manager: StrategyFilterRefinementManager, job) -> dict[str, object]:
        for _ in range(100):
            status = refinement.scan_job_status(manager.jobs, job.job_id)
            if status["done"]:
                return status
            await asyncio.sleep(0.02)
        self.fail("job never completed")

    def test_returns_none_below_eligibility(self) -> None:
        manager, _root = self._setup_eligible()
        self._label(manager, MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        self.assertIsNone(manager.start_auto_refine_job(strategy_name="test_strategy", template="TEMPLATE"))

    async def test_fires_once_at_eligibility_and_imports_auto_suffixed_file(self) -> None:
        manager, root = self._setup_eligible()
        self._label(manager, MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        with patch(
            "polymarket_btc.data_collection.control.strategy_filter_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {"filename": "new_filter.py", "content": "FILTER_INFO = {}\n"}
            instance = manager.next_instance(strategy_name="test_strategy")["instance"]
            if instance is None:
                self.skipTest("not enough real trade candidates in this fixture to reach the prompt gate")
            manager.label(
                strategy_name="test_strategy", shape=instance["shape"], node=instance["node"], label="oui",
            )
            job = manager.start_auto_refine_job(strategy_name="test_strategy", template="TEMPLATE")
            self.assertIsNotNone(job)
            status = await self._wait_for_done(manager, job)
            self.assertIsNone(status["error"])
            self.assertEqual(status["result"]["filename"], "new_filter_auto.py")
            self.assertTrue((root / "filter_profiles" / "new_filter_auto.py").is_file())

    async def test_failure_releases_claim_so_the_next_label_retries(self) -> None:
        manager, _root = self._setup_eligible()
        self._label(manager, MIN_TOTAL_FOR_PROMPT - 1, no_count=1)
        with patch(
            "polymarket_btc.data_collection.control.strategy_filter_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.side_effect = ValueError("Claude Code a échoué")
            instance = manager.next_instance(strategy_name="test_strategy")["instance"]
            if instance is None:
                self.skipTest("not enough real trade candidates in this fixture to reach the prompt gate")
            manager.label(
                strategy_name="test_strategy", shape=instance["shape"], node=instance["node"], label="oui",
            )
            job = manager.start_auto_refine_job(strategy_name="test_strategy", template="TEMPLATE")
            status = await self._wait_for_done(manager, job)
            self.assertIn("échoué", status["error"])

            mock_generate.side_effect = None
            mock_generate.return_value = {"filename": "new_filter.py", "content": "FILTER_INFO = {}\n"}
            retry_job = manager.start_auto_refine_job(strategy_name="test_strategy", template="TEMPLATE")
            self.assertIsNotNone(retry_job)
            retry_status = await self._wait_for_done(manager, retry_job)
            self.assertIsNone(retry_status["error"])


class SyntheticExampleTests(_RefinementTestBase, unittest.IsolatedAsyncioTestCase):
    # generate_synthetic_instance calls run_backtest (~1-3s even on this
    # small fixed scenario -- confirmed by hand, well past "instant"), so
    # unlike concept/microsystem's own synchronous synthetic route, this
    # one always runs as a background job -- IsolatedAsyncioTestCase,
    # same pattern this file's other job-based tests already use.

    async def _wait_for_done(self, manager: StrategyFilterRefinementManager, job) -> dict[str, object]:
        for _ in range(300):
            status = refinement.scan_job_status(manager.jobs, job.job_id)
            if status["done"]:
                return status
            await asyncio.sleep(0.05)
        self.fail("job never completed")

    async def test_enter_once_strategy_produces_a_synthetic_trade(self) -> None:
        # The exact fixture this file's other tests already establish
        # (enter_once + fixed_sltp) reliably takes one long entry near the
        # start of build_synthetic_candle_set's range and rides it through
        # the breakout -- confirmed by hand to close as a "win" by the end
        # of the fixed synthetic window even without hitting the 5% TP.
        manager, _root = self._setup(with_filter=False)
        job = manager.start_synthetic_job(strategy_name="test_strategy")
        status = await self._wait_for_done(manager, job)
        self.assertIsNone(status["error"])
        result = status["result"]
        self.assertEqual(result["shape"], "trade")
        self.assertEqual(result["node"]["direction"], "long")
        self.assertEqual(result["window"]["key"], "binance_futures_kline")
        self.assertTrue(result["window"]["candles"])
        self.assertIsNotNone(result["window"]["replay"])
        self.assertTrue(result["synthetic"])

    async def test_missing_execution_or_management_raises_clearly(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        (root / "concepts").mkdir(parents=True)
        (root / "concepts" / "kline_reader.py").write_text(_KLINE_READER_CONCEPT, encoding="utf-8")
        (root / "microsystems").mkdir(parents=True)
        (root / "execution_profiles").mkdir(parents=True)
        (root / "management_profiles").mkdir(parents=True)
        (root / "filter_profiles").mkdir(parents=True)
        runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            plugins_dir=root / "plugins", concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems", execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles", filter_dir=root / "filter_profiles",
        )
        strategies = StrategyManager(strategies_dir=root / "strategies", runs=runs)
        strategies.save_strategy(
            name="incomplete_strategy",
            concepts=[{"instance_id": "c1", "concept_id": "kline_reader", "config": {}, "data_bindings": {}}],
            microsystems=[], execution=None, management=None, filter=None,
        )
        manager = StrategyFilterRefinementManager(feedback_dir=root / "filter_feedback", runs=runs, strategies=strategies)
        job = manager.start_synthetic_job(strategy_name="incomplete_strategy")
        status = await self._wait_for_done(manager, job)
        self.assertIn("profil d'exécution et de gestion", status["error"])

    async def test_unknown_strategy_raises_clearly(self) -> None:
        manager, _root = self._setup(with_filter=False)
        job = manager.start_synthetic_job(strategy_name="no_such_strategy")
        status = await self._wait_for_done(manager, job)
        self.assertIn("unknown strategy", status["error"])


if __name__ == "__main__":
    unittest.main()
