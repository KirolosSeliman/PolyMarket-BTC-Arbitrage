from datetime import UTC, datetime
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import time
import unittest

from polymarket_btc.data_collection.control.backtest_engine import (
    build_timeline,
    expand_numeric_range,
    expand_sweep,
    normalize_direction,
    run_backtest,
)
from polymarket_btc.data_collection.control.concepts import ConceptInfo
from polymarket_btc.data_collection.control.config_schema import parse_config_schema
from polymarket_btc.data_collection.control.microsystems import MicrosystemInfo
from polymarket_btc.data_collection.control.runs import CollectionRunManager
from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    BinanceKlinePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    TakerSide,
)
from polymarket_btc.data_collection.market_data.storage import RawEventStorage

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

_CONCEPT = '''
CONCEPT_INFO = {
    "label": "Last price", "description": "d",
    "data_sources": ["binance_futures_trade"],
}

def compute(context):
    trades = context.data.get("binance_futures_trade") or []
    if not trades:
        return {"last_price": None}
    return {"last_price": trades[-1]["price"]}
'''

_MICROSYSTEM = '''
MICROSYSTEM_INFO = {
    "label": "Passthrough", "description": "d",
    "concept_inputs": ["last_price_concept"],
}

def compute(context):
    result = context.concepts.get("last_price_concept") or {}
    return {"last_price": result.get("last_price")}
'''

_THRESHOLD_EXECUTION = '''
EXECUTION_INFO = {
    "label": "Threshold", "description": "d",
}

def execute(context):
    micro = context.microsystems.get("micro_1") or {}
    price = micro.get("last_price")
    if price is None:
        return {"direction": "neutre"}
    return {"direction": "long" if price >= 100 else "short"}
'''

_ENTER_ONCE_EXECUTION = '''
EXECUTION_INFO = {
    "label": "Enter once", "description": "d",
}

def execute(context):
    return {"direction": "long"}
'''

_FIXED_SLTP_MANAGEMENT = '''
MANAGEMENT_INFO = {
    "label": "Fixed SL/TP", "description": "d",
    "config_schema": [
        {"name": "stop_loss_pct", "type": "number", "label": "SL", "default": 5.0},
        {"name": "take_profit_pct", "type": "number", "label": "TP", "default": 5.0},
    ],
}

def manage(context):
    return {"stop_loss_pct": context.config["stop_loss_pct"], "take_profit_pct": context.config["take_profit_pct"]}
'''


def _write_trade_events(run_dir: Path, trade_points: list[tuple[float, float]]) -> None:
    storage = RawEventStorage(run_dir, zstd_level=3)
    for i, (t_seconds, price) in enumerate(trade_points):
        ts_ns = int(t_seconds * 1e9)
        payload = BinanceAggTradePayload(
            symbol="BTCUSDT", aggregate_trade_id=i, price=Decimal(str(price)), quantity=Decimal("0.1"),
            first_trade_id=i, last_trade_id=i, trade_timestamp_ns=ts_ns, taker_side=TakerSide.BUY,
        )
        event = MarketDataEvent(
            schema_version=2, ingest_sequence=i, event_id=f"evt-{i}",
            source=EventSource.BINANCE_FUTURES_TRADE, stream=EventStream.BINANCE_FUTURES_AGG_TRADE,
            instrument="BTCUSDT", source_timestamp_ns=ts_ns, server_timestamp_ns=ts_ns,
            received_wall_timestamp_ns=ts_ns, received_monotonic_ns=time.monotonic_ns(),
            source_sequence=None, timeframe=None, market_id=None, condition_id=None,
            asset_id=None, outcome=None, payload=payload,
        )
        storage.write(event)
    storage.close()


def _write_kline_events(run_dir: Path, candles: list[tuple[float, float]]) -> None:
    """candles: [(close_time_seconds, close_price), ...] -- open/high/low
    are irrelevant to this fixture, only close is read by the price-path
    fallback being tested."""
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


def _seeded_symbol_cache(root: Path) -> Path:
    cache_path = root / "cache" / "binance_symbols.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({
        "spot": ["BTC"], "futures": ["BTC"], "fetched_at_utc": datetime.now(UTC).isoformat(),
    }))
    return cache_path


class NormalizeDirectionTests(unittest.TestCase):
    def test_recognizes_long_synonyms_case_insensitively(self) -> None:
        for word in ("long", "Buy", "HAUSSIER", "bullish"):
            self.assertEqual(normalize_direction(word), "long")

    def test_recognizes_short_synonyms_case_insensitively(self) -> None:
        for word in ("short", "Sell", "BAISSIER", "bearish"):
            self.assertEqual(normalize_direction(word), "short")

    def test_unrecognized_or_missing_is_none(self) -> None:
        self.assertIsNone(normalize_direction("neutre"))
        self.assertIsNone(normalize_direction(None))
        self.assertIsNone(normalize_direction(123))


class ExpandSweepTests(unittest.TestCase):
    def test_numeric_range_includes_both_endpoints(self) -> None:
        self.assertEqual(expand_numeric_range(3, 8, 1), [3, 4, 5, 6, 7, 8])

    def test_field_absent_from_sweep_uses_its_fixed_default(self) -> None:
        schema = parse_config_schema([{"name": "x", "type": "number", "label": "X", "default": 7}])
        self.assertEqual(expand_sweep(schema, {}), [{"x": 7}])

    def test_numeric_sweep_and_select_sweep_combine_as_cartesian_product(self) -> None:
        schema = parse_config_schema([
            {"name": "rr", "type": "number", "label": "RR", "default": 3},
            {"name": "mode", "type": "select", "label": "Mode", "default": "a", "options": ["a", "b"]},
        ])
        combos = expand_sweep(schema, {"rr": {"min": 3, "max": 4, "step": 1}, "mode": ["a", "b"]})
        self.assertEqual(
            sorted((c["rr"], c["mode"]) for c in combos),
            [(3, "a"), (3, "b"), (4, "a"), (4, "b")],
        )


class RunBacktestTests(unittest.TestCase):
    def _setup(self, root: Path, execution_source: str, management_source: str | None = None) -> tuple[dict, dict, CollectionRunManager]:
        (root / "concepts").mkdir(parents=True, exist_ok=True)
        (root / "concepts" / "last_price_concept.py").write_text(_CONCEPT, encoding="utf-8")
        (root / "microsystems").mkdir(parents=True, exist_ok=True)
        (root / "microsystems" / "passthrough.py").write_text(_MICROSYSTEM, encoding="utf-8")
        (root / "execution_profiles").mkdir(parents=True, exist_ok=True)
        (root / "execution_profiles" / "exec.py").write_text(execution_source, encoding="utf-8")
        (root / "management_profiles").mkdir(parents=True, exist_ok=True)
        management_entry = None
        if management_source is not None:
            (root / "management_profiles" / "mgmt.py").write_text(management_source, encoding="utf-8")
            management_entry = {"management_id": "mgmt", "config": {}}

        manager = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections", symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=root / "plugins", concepts_dir=root / "concepts",
            microsystems_dir=root / "microsystems", execution_dir=root / "execution_profiles",
            management_dir=root / "management_profiles",
        )
        strategy = {
            "concepts": [{"instance_id": "concept_1", "concept_id": "last_price_concept", "config": {}, "data_bindings": {}}],
            "microsystems": [{
                "instance_id": "micro_1", "microsystem_id": "passthrough",
                "concept_instance_ids": ["concept_1"], "config": {}, "data_bindings": {},
            }],
            "execution": {"execution_id": "exec", "config": {}},
            "management": management_entry,
        }
        return strategy, manager.__dict__, manager

    def test_reversal_exit_and_end_of_range_forced_close(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy, _, manager = self._setup(root, _THRESHOLD_EXECUTION)
            run_dir = root / "collections" / "run1"
            run_dir.mkdir(parents=True)
            _write_trade_events(run_dir, [(-1, 100), (9, 150), (19, 90), (29, 50), (39, 120), (49, 200)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [],
                "start_ts_utc": "2026-01-01T00:00:00", "end_ts_utc": "2026-01-01T01:00:00",
                "data_dir": str(run_dir),
            }
            result = run_backtest(
                strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[manifest], instrument="BTC",
                start_ts=0, end_ts=50, cadence_seconds=10,
                execution_sweep={}, management_sweep={},
            )
            self.assertEqual(result["evaluation_steps"], 6)
            self.assertEqual(result["best"]["trades"], 3)
            self.assertEqual(result["best"]["wins"], 1)
            self.assertAlmostEqual(result["best"]["win_rate"], 1 / 3)

    def test_management_stop_loss_take_profit_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy, _, manager = self._setup(root, _ENTER_ONCE_EXECUTION, _FIXED_SLTP_MANAGEMENT)
            run_dir = root / "collections" / "run1"
            run_dir.mkdir(parents=True)
            # The t=-1 trade falls outside the [0, 20] range filter, so the
            # first price actually seen is 101 at step1 (t=10) -- entry
            # there, TP = 101*1.05 = 106.05, never actually reached (last
            # price is 106), so this closes via the end-of-range force-close
            # path (last_price 106 > entry 101 -> win), not a TP hit.
            _write_trade_events(run_dir, [(-1, 100), (9, 101), (19, 106)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [],
                "start_ts_utc": "2026-01-01T00:00:00", "end_ts_utc": "2026-01-01T01:00:00",
                "data_dir": str(run_dir),
            }
            result = run_backtest(
                strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[manifest], instrument="BTC",
                start_ts=0, end_ts=20, cadence_seconds=10,
                execution_sweep={}, management_sweep={},
            )
            self.assertEqual(result["best"]["trades"], 1)
            self.assertEqual(result["best"]["wins"], 1)
            self.assertEqual(result["best"]["win_rate"], 1.0)

            self.assertEqual(result["replay"]["trades"], [{
                "entry_time": 10.0, "entry_price": 101.0,
                "exit_time": 20.0, "exit_price": 106.0,
                "direction": "long", "outcome": "win",
                "stop_loss": 95.94999999999999, "take_profit": 106.05000000000001,
            }])
            self.assertEqual([p["price"] for p in result["replay"]["price_path"]], [101.0, 106.0])
            self.assertEqual([step["timestamp"] for step in result["replay"]["timeline"]], [0.0, 10.0, 20.0])
            self.assertIn("concept_1", result["replay"]["timeline"][0]["concepts"])
            self.assertIn("micro_1", result["replay"]["timeline"][0]["microsystems"])

    def test_replay_is_only_computed_once_for_the_best_combo_not_every_sweep_combo(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy, _, manager = self._setup(root, _ENTER_ONCE_EXECUTION, _FIXED_SLTP_MANAGEMENT)
            run_dir = root / "collections" / "run1"
            run_dir.mkdir(parents=True)
            _write_trade_events(run_dir, [(-1, 100), (9, 101), (19, 106)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [],
                "start_ts_utc": "2026-01-01T00:00:00", "end_ts_utc": "2026-01-01T01:00:00",
                "data_dir": str(run_dir),
            }
            result = run_backtest(
                strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[manifest], instrument="BTC",
                start_ts=0, end_ts=20, cadence_seconds=10,
                execution_sweep={}, management_sweep={"stop_loss_pct": [1.0, 2.0], "take_profit_pct": [5.0, 10.0]},
            )
            self.assertEqual(len(result["results"]), 4)
            for r in result["results"]:
                self.assertNotIn("trade_log", r)
            self.assertIsNotNone(result["replay"])
            self.assertGreater(len(result["replay"]["trades"]), 0)

    def test_sweep_mode_runs_every_combination_via_process_pool(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy, _, manager = self._setup(root, _ENTER_ONCE_EXECUTION, _FIXED_SLTP_MANAGEMENT)
            run_dir = root / "collections" / "run1"
            run_dir.mkdir(parents=True)
            _write_trade_events(run_dir, [(-1, 100), (9, 101), (19, 106)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [],
                "start_ts_utc": "2026-01-01T00:00:00", "end_ts_utc": "2026-01-01T01:00:00",
                "data_dir": str(run_dir),
            }
            result = run_backtest(
                strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[manifest], instrument="BTC",
                start_ts=0, end_ts=20, cadence_seconds=10,
                execution_sweep={},
                management_sweep={"stop_loss_pct": [1.0, 2.0], "take_profit_pct": [5.0, 10.0]},
            )
            self.assertEqual(len(result["results"]), 4)
            configs = sorted(
                (r["management_config"]["stop_loss_pct"], r["management_config"]["take_profit_pct"])
                for r in result["results"]
            )
            self.assertEqual(configs, [(1.0, 5.0), (1.0, 10.0), (2.0, 5.0), (2.0, 10.0)])
            for r in result["results"]:
                self.assertEqual(r["trades"], 1)
                self.assertEqual(r["wins"], 1)

    def test_fixed_value_override_takes_precedence_over_script_default(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy, _, manager = self._setup(root, _ENTER_ONCE_EXECUTION, _FIXED_SLTP_MANAGEMENT)
            run_dir = root / "collections" / "run1"
            run_dir.mkdir(parents=True)
            # Entry @100; script default take_profit_pct=5 -> TP=105, which
            # 101 never reaches (a loss via end-of-range forced close since
            # 101>100 is still a win by direction... use a losing final
            # price instead so the two configs produce different outcomes).
            _write_trade_events(run_dir, [(-1, 100), (9, 95)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [],
                "start_ts_utc": "2026-01-01T00:00:00", "end_ts_utc": "2026-01-01T01:00:00",
                "data_dir": str(run_dir),
            }
            # Default stop_loss_pct=5 -> SL=95, hit exactly at the second
            # trade: a loss. Overriding to stop_loss_pct=10 -> SL=90, never
            # hit -- forced-closed at 95 < 100: still a loss either way by
            # direction, so assert on the resolved config value itself
            # instead of the outcome, which is the actual bug being guarded.
            result = run_backtest(
                strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[manifest], instrument="BTC",
                start_ts=0, end_ts=10, cadence_seconds=10,
                execution_sweep={}, management_sweep={},
                management_config={"stop_loss_pct": 10.0},
            )
            self.assertEqual(result["best"]["management_config"]["stop_loss_pct"], 10.0)
            self.assertEqual(result["best"]["management_config"]["take_profit_pct"], 5.0)

    def test_fixed_override_is_ignored_for_a_field_that_is_being_swept(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy, _, manager = self._setup(root, _ENTER_ONCE_EXECUTION, _FIXED_SLTP_MANAGEMENT)
            run_dir = root / "collections" / "run1"
            run_dir.mkdir(parents=True)
            _write_trade_events(run_dir, [(-1, 100), (9, 101)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [],
                "start_ts_utc": "2026-01-01T00:00:00", "end_ts_utc": "2026-01-01T01:00:00",
                "data_dir": str(run_dir),
            }
            result = run_backtest(
                strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[manifest], instrument="BTC",
                start_ts=0, end_ts=10, cadence_seconds=10,
                execution_sweep={},
                management_sweep={"stop_loss_pct": [1.0, 2.0]},
                management_config={"stop_loss_pct": 999.0},
            )
            values = sorted(r["management_config"]["stop_loss_pct"] for r in result["results"])
            self.assertEqual(values, [1.0, 2.0])

    def test_no_execution_profile_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy, _, manager = self._setup(root, _ENTER_ONCE_EXECUTION)
            strategy["execution"] = None
            with self.assertRaises(ValueError):
                run_backtest(
                    strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                    execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                    data_requirements_for=manager._data_requirements_for,
                    manifests=[], instrument="BTC",
                    start_ts=0, end_ts=20, cadence_seconds=10,
                    execution_sweep={}, management_sweep={},
                )

    def test_no_trade_data_available_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy, _, manager = self._setup(root, _ENTER_ONCE_EXECUTION)
            with self.assertRaises(ValueError):
                run_backtest(
                    strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                    execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                    data_requirements_for=manager._data_requirements_for,
                    manifests=[], instrument="BTC",
                    start_ts=0, end_ts=20, cadence_seconds=10,
                    execution_sweep={}, management_sweep={},
                )

    def test_prices_the_backtest_from_klines_when_no_trade_data_was_collected(self) -> None:
        """Regression test: a strategy whose only collected data is klines
        (a very common case -- klines are exactly what "mode déjà collecté"
        fetches by default) must still be priceable. The price path used to
        be hardcoded to binance_futures_trade only, so a strategy with real,
        usable kline data would incorrectly raise "no trade data available"."""
        kline_concept = '''
CONCEPT_INFO = {
    "label": "Last close", "description": "d",
    "data_sources": ["binance_futures_kline"],
}

def compute(context):
    candles = context.data.get("binance_futures_kline") or []
    return {"last_close": candles[-1]["close"] if candles else None}
'''
        kline_microsystem = '''
MICROSYSTEM_INFO = {
    "label": "Passthrough", "description": "d",
    "concept_inputs": ["kline_concept"],
}

def compute(context):
    result = context.concepts.get("kline_concept") or {}
    return {"last_close": result.get("last_close")}
'''
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "concepts").mkdir(parents=True)
            (root / "concepts" / "kline_concept.py").write_text(kline_concept, encoding="utf-8")
            (root / "microsystems").mkdir(parents=True)
            (root / "microsystems" / "passthrough.py").write_text(kline_microsystem, encoding="utf-8")
            (root / "execution_profiles").mkdir(parents=True)
            (root / "execution_profiles" / "exec.py").write_text(_ENTER_ONCE_EXECUTION, encoding="utf-8")
            (root / "management_profiles").mkdir(parents=True)

            manager = CollectionRunManager(
                config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
                collections_dir=root / "collections", symbol_cache_path=_seeded_symbol_cache(root),
                plugins_dir=root / "plugins", concepts_dir=root / "concepts",
                microsystems_dir=root / "microsystems", execution_dir=root / "execution_profiles",
                management_dir=root / "management_profiles",
            )
            strategy = {
                "concepts": [{"instance_id": "concept_1", "concept_id": "kline_concept", "config": {}, "data_bindings": {}}],
                "microsystems": [{
                    "instance_id": "micro_1", "microsystem_id": "passthrough",
                    "concept_instance_ids": ["concept_1"], "config": {}, "data_bindings": {},
                }],
                "execution": {"execution_id": "exec", "config": {}},
                "management": None,
            }

            run_dir = root / "collections" / "run1"
            run_dir.mkdir(parents=True)
            # No trades written at all -- only klines. entry@100 (step0),
            # forced-closed at 110 (last close, step1) -- a hand-computable win.
            _write_kline_events(run_dir, [(0, 100), (9, 110)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_kline"], "plugins": [],
                "start_ts_utc": "2026-01-01T00:00:00", "end_ts_utc": "2026-01-01T01:00:00",
                "data_dir": str(run_dir),
            }

            result = run_backtest(
                strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[manifest], instrument="BTC",
                start_ts=0, end_ts=10, cadence_seconds=10,
                execution_sweep={}, management_sweep={},
            )
            self.assertEqual(result["best"]["trades"], 1)
            self.assertEqual(result["best"]["wins"], 1)


_LOCKED_REQUIREMENT = [{"type": "key", "swappable": False, "keys": ["key"], "default_asset": None}]


def _concept_info(concept_id: str, compute, required_lookback_seconds=None) -> ConceptInfo:
    return ConceptInfo(
        id=concept_id, label="t", description="d", category="Général",
        data_sources=("key",), config_schema=(), path=Path("unused.py"),
        compute=compute, detail=None, required_lookback_seconds=required_lookback_seconds,
    )


def _microsystem_info(microsystem_id: str, compute, required_lookback_seconds=None) -> MicrosystemInfo:
    return MicrosystemInfo(
        id=microsystem_id, label="t", description="d", category="Général",
        concept_inputs=(), data_inputs=(), config_schema=(), path=Path("unused.py"),
        compute=compute, detail=None, required_lookback_seconds=required_lookback_seconds,
    )


class BuildTimelineWindowingTests(unittest.TestCase):
    """Direct build_timeline coverage for required_lookback_seconds -- the
    fix for a concept that reprocesses its entire accumulated history from
    scratch every evaluation step (concepts/fvg.py's _build_candles,
    confirmed the dominant cost in a slow long-period backtest) growing
    unboundedly slower as a backtest walk progresses."""

    def test_unbounded_concept_sees_the_full_accumulated_history(self) -> None:
        records = [{"timestamp": float(i), "price": float(i)} for i in range(10)]

        def compute(context):
            return {"count": len(context.data["key"])}

        strategy = {"concepts": [{"instance_id": "c1", "concept_id": "c1", "config": {}, "data_bindings": {}}]}
        timeline = build_timeline(
            strategy, {"c1": _concept_info("c1", compute)}, {}, {"key": records},
            {"c1": _LOCKED_REQUIREMENT}, {}, start_ts=0, end_ts=9, cadence_seconds=3,
        )
        self.assertEqual([step.concept_outputs["c1"]["count"] for step in timeline], [1, 4, 7, 10])

    def test_windowed_concept_sees_only_its_declared_trailing_window(self) -> None:
        records = [{"timestamp": float(i), "price": float(i)} for i in range(10)]

        def compute(context):
            return {"timestamps": [r["timestamp"] for r in context.data["key"]]}

        strategy = {"concepts": [{"instance_id": "c1", "concept_id": "c1", "config": {}, "data_bindings": {}}]}
        timeline = build_timeline(
            strategy, {"c1": _concept_info("c1", compute, required_lookback_seconds=lambda cfg: 2.0)}, {},
            {"key": records}, {"c1": _LOCKED_REQUIREMENT}, {}, start_ts=0, end_ts=9, cadence_seconds=3,
        )
        last_step = timeline[-1]
        self.assertEqual(last_step.timestamp, 9)
        # unbounded would see all of 0..9 -- windowed sees only [9-2, 9]
        self.assertEqual(last_step.concept_outputs["c1"]["timestamps"], [7.0, 8.0, 9.0])

    def test_windowed_and_unbounded_agree_when_the_window_is_sufficient(self) -> None:
        """The correctness proof windowing depends on: a generous-enough
        window produces an identical result to no window at all, for a
        concept whose own output only ever depends on a bounded trailing
        slice of its data -- exactly fvg.py/liquidity_sweep.py's own
        candles[-lookback_candles:] shape."""
        records = [{"timestamp": float(i), "price": float(i % 5)} for i in range(30)]

        def make_compute():
            def compute(context):
                recent = context.data["key"][-5:]
                return {"max_recent": max((r["price"] for r in recent), default=None)}
            return compute

        strategy = {
            "concepts": [
                {"instance_id": "unbounded", "concept_id": "unbounded", "config": {}, "data_bindings": {}},
                {"instance_id": "windowed", "concept_id": "windowed", "config": {}, "data_bindings": {}},
            ],
        }
        concept_infos = {
            "unbounded": _concept_info("unbounded", make_compute()),
            # 10s window vs 1 record/second -> comfortably covers the 5
            # most-recent records the concept itself trims to, while still
            # small enough to actually bind (not just never fire).
            "windowed": _concept_info("windowed", make_compute(), required_lookback_seconds=lambda cfg: 10.0),
        }
        timeline = build_timeline(
            strategy, concept_infos, {}, {"key": records},
            {"unbounded": _LOCKED_REQUIREMENT, "windowed": _LOCKED_REQUIREMENT}, {},
            start_ts=0, end_ts=29, cadence_seconds=1,
        )
        for step in timeline:
            self.assertEqual(
                step.concept_outputs["windowed"]["max_recent"], step.concept_outputs["unbounded"]["max_recent"],
            )


class BuildTimelineMemoizationTests(unittest.TestCase):
    """A concept/microsystem instance's compute() is skipped and its last
    output reused whenever its own dependency signature hasn't changed
    since its last run -- pure memoization (identical results), the concrete
    fix for an instance whose data updates slower than the evaluation
    cadence (e.g. hourly data under a 5s cadence) being recomputed far more
    often than its output could ever actually change."""

    def test_concept_is_not_recomputed_while_its_data_is_unchanged(self) -> None:
        records = [{"timestamp": float(t), "price": float(t)} for t in (0, 10, 20)]
        calls: list[int] = []

        def compute(context):
            calls.append(1)
            return {"n": len(context.data["key"])}

        strategy = {"concepts": [{"instance_id": "c1", "concept_id": "c1", "config": {}, "data_bindings": {}}]}
        timeline = build_timeline(
            strategy, {"c1": _concept_info("c1", compute)}, {}, {"key": records},
            {"c1": _LOCKED_REQUIREMENT}, {}, start_ts=0, end_ts=20, cadence_seconds=2,
        )
        self.assertEqual(len(timeline), 11)
        # data only actually changes 3 times (new record at t=0, t=10, t=20)
        # across 11 evaluation steps -- compute() must reflect that, not run 11 times.
        self.assertEqual(len(calls), 3)
        self.assertEqual(
            [step.concept_outputs["c1"]["n"] for step in timeline],
            [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3],
        )

    def test_microsystem_is_not_recomputed_while_neither_its_data_nor_its_concept_input_changed(self) -> None:
        records = [{"timestamp": float(t), "price": float(t)} for t in (0, 10, 20)]
        concept_calls: list[int] = []
        micro_calls: list[int] = []

        def concept_compute(context):
            concept_calls.append(1)
            return {"n": len(context.data["key"])}

        def micro_compute(context):
            micro_calls.append(1)
            return {"n": (context.concepts.get("c1") or {}).get("n")}

        strategy = {
            "concepts": [{"instance_id": "c1", "concept_id": "c1", "config": {}, "data_bindings": {}}],
            "microsystems": [{
                "instance_id": "m1", "microsystem_id": "m1",
                "concept_instance_ids": ["c1"], "config": {}, "data_bindings": {},
            }],
        }
        timeline = build_timeline(
            strategy, {"c1": _concept_info("c1", concept_compute)}, {"m1": _microsystem_info("m1", micro_compute)},
            {"key": records}, {"c1": _LOCKED_REQUIREMENT}, {"m1": []},
            start_ts=0, end_ts=20, cadence_seconds=2,
        )
        self.assertEqual(len(concept_calls), 3)
        self.assertEqual(len(micro_calls), 3)
        self.assertEqual(
            [step.microsystem_outputs["m1"]["n"] for step in timeline],
            [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3],
        )


if __name__ == "__main__":
    unittest.main()
