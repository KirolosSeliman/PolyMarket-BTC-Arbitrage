"""Behavioral tests for the real, user-facing ICT scripts committed at the
repo root (concepts/fvg.py, concepts/liquidity_sweep.py, microsystems/
fvg_sweep_reversal.py) -- unlike the rest of the suite, these aren't
throwaway fixtures written for a test, they're the actual content a real
strategy runs. Covers the switch from reconstructing candles out of raw
trades to reading pre-built binance_futures_kline records directly: the
detection math itself (_detect_fvgs, _find_swings, ...) is untouched, only
how candles are obtained changed, so these tests focus on that seam --
real kline-shaped input in, a correctly detected candle_seconds and FVG/
sweep out.
"""

from __future__ import annotations

from pathlib import Path
import unittest

from polymarket_btc.data_collection.control.concepts import ConceptContext, discover_concepts
from polymarket_btc.data_collection.control.management import ManagementContext, discover_management_profiles
from polymarket_btc.data_collection.control.microsystems import MicrosystemContext, discover_microsystems

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _noop_log(_message: str) -> None:
    pass


def _kline_record(open_time: float, o: float, h: float, l: float, c: float, close_time: float | None = None) -> dict:
    """Matches read_records' own kline extractor shape (backtest_data.py's
    _ACCESS_EXTRACTORS[BinanceKlinePayload]) -- what a concept's
    context.data["binance_futures_kline"] actually contains."""
    return {
        "open": o, "high": h, "low": l, "close": c,
        "volume": 1.0, "open_time": open_time,
        "close_time": close_time if close_time is not None else open_time + 59.999,
        "timestamp": close_time if close_time is not None else open_time + 59.999,
        "is_closed": True,
    }


class FvgConceptTests(unittest.TestCase):
    def _compute(self, records: list[dict], config: dict | None = None) -> dict:
        infos = {info.id: info for info in discover_concepts(REPOSITORY_ROOT / "concepts")}
        info = infos["fvg"]
        context = ConceptContext(
            data={"binance_futures_kline": records}, config=config or {}, log=_noop_log,
        )
        return info.compute(context)

    def test_detects_a_bullish_fvg_from_real_kline_records_and_the_right_candle_width(self) -> None:
        records = [
            _kline_record(0, 100, 101, 99, 100.5),
            _kline_record(60, 100.5, 105, 100, 104),   # displacement: close > open
            _kline_record(120, 104, 106, 102, 105),    # low(102) > candle1 high(101) -> bullish FVG
        ]
        result = self._compute(records)
        self.assertEqual(result["candle_seconds"], 60)
        self.assertEqual(len(result["fvgs"]), 1)
        fvg = result["fvgs"][0]
        self.assertEqual(fvg["direction"], "bullish")
        self.assertEqual(fvg["low"], 101)
        self.assertEqual(fvg["high"], 102)
        self.assertEqual(fvg["formed_at"], 120)

    def test_data_source_is_kline_not_trade(self) -> None:
        infos = {info.id: info for info in discover_concepts(REPOSITORY_ROOT / "concepts")}
        self.assertEqual(infos["fvg"].data_sources, ("binance_futures_kline",))

    def test_no_config_field_named_candle_seconds_anymore(self) -> None:
        infos = {info.id: info for info in discover_concepts(REPOSITORY_ROOT / "concepts")}
        names = {field.name for field in infos["fvg"].config_schema}
        self.assertNotIn("candle_seconds", names)

    def test_fewer_than_two_candles_yields_no_candle_seconds(self) -> None:
        result = self._compute([_kline_record(0, 100, 101, 99, 100.5)])
        self.assertIsNone(result["candle_seconds"])
        self.assertEqual(result["fvgs"], [])


class LiquiditySweepConceptTests(unittest.TestCase):
    def _compute(self, records: list[dict], config: dict | None = None) -> dict:
        infos = {info.id: info for info in discover_concepts(REPOSITORY_ROOT / "concepts")}
        info = infos["liquidity_sweep"]
        context = ConceptContext(
            data={"binance_futures_kline": records}, config=config or {}, log=_noop_log,
        )
        return info.compute(context)

    def test_detects_a_confirmed_buyside_sweep_from_real_kline_records(self) -> None:
        records = [
            _kline_record(0, 99, 100, 98, 99.5),
            _kline_record(60, 99.5, 105, 99, 100),      # swing high (105 > neighbors' highs)
            _kline_record(120, 100, 101, 99.5, 100.5),
            _kline_record(180, 100.5, 107, 100, 104),   # wicks above 105, closes back below -> sweep
        ]
        result = self._compute(records, config={"swing_lookback": 1})
        self.assertEqual(result["candle_seconds"], 60)
        self.assertEqual(len(result["sweeps"]), 1)
        sweep = result["sweeps"][0]
        self.assertEqual(sweep["direction"], "bearish")  # buyside liquidity swept -> bearish signal
        self.assertEqual(sweep["level"], 105)
        self.assertEqual(sweep["swept_at"], 180)

    def test_data_source_is_kline_not_trade(self) -> None:
        infos = {info.id: info for info in discover_concepts(REPOSITORY_ROOT / "concepts")}
        self.assertEqual(infos["liquidity_sweep"].data_sources, ("binance_futures_kline",))


class FvgSweepReversalMicrosystemTests(unittest.TestCase):
    def _compute(self, fvg_result: dict, sweep_result: dict, config: dict | None = None) -> dict:
        infos = {info.id: info for info in discover_microsystems(REPOSITORY_ROOT / "microsystems")}
        info = infos["fvg_sweep_reversal"]
        context = MicrosystemContext(
            concepts={"fvg": fvg_result, "liquidity_sweep": sweep_result},
            data={}, config=config or {}, log=_noop_log,
        )
        return info.compute(context)

    def test_reads_candle_seconds_from_the_concept_outputs_not_its_own_config(self) -> None:
        infos = {info.id: info for info in discover_microsystems(REPOSITORY_ROOT / "microsystems")}
        names = {field.name for field in infos["fvg_sweep_reversal"].config_schema}
        self.assertNotIn("candle_seconds", names)

        # candle_seconds=30 here, taken only from the concept outputs below --
        # a setup only lines up correctly if the microsystem actually used it.
        fvg_result = {
            "candle_seconds": 30,
            "fvgs": [
                {"direction": "bearish", "high": 100, "low": 99, "formed_at": 0},   # initial move down
                {"direction": "bullish", "high": 106, "low": 105, "formed_at": 150},  # reversal: swept_at(60) + 3*30
            ],
        }
        sweep_result = {
            "candle_seconds": 30,
            "sweeps": [{"direction": "bullish", "level": 99, "swept_at": 60}],
        }
        result = self._compute(fvg_result, sweep_result)
        self.assertEqual(len(result["setups"]), 1)
        self.assertEqual(result["dernier_signal"], "haussier")

    def test_last_price_is_propagated_from_the_concept_outputs(self) -> None:
        """An execution profile only ever receives context.microsystems, never
        context.concepts (see fvg_rebound_entry.py) -- without this, no
        execution profile downstream of this microsystem could ever confirm a
        rebound against the current price."""
        result = self._compute({"last_price": 101.5}, {"last_price": 202.0})
        self.assertEqual(result["last_price"], 101.5)  # fvg's own last_price wins when both are present

    def test_last_price_falls_back_to_the_sweep_concept_when_fvg_lacks_it(self) -> None:
        result = self._compute({}, {"last_price": 202.0})
        self.assertEqual(result["last_price"], 202.0)

    def test_missing_candle_seconds_from_both_concepts_yields_no_setups(self) -> None:
        result = self._compute(
            {"fvgs": [{"direction": "bearish", "high": 100, "low": 99, "formed_at": 0}]},
            {"sweeps": [{"direction": "bullish", "level": 99, "swept_at": 60}]},
        )
        self.assertEqual(result, {"setups": [], "dernier_signal": "neutre", "last_price": None})


class FvgStopLegTargetManagementTests(unittest.TestCase):
    """The backtest engine only ever recognizes stop_loss_pct/take_profit_pct
    from a management profile's return value (see simulate_combo in
    backtest_engine.py and docs/nouveau_management_prompt.md's "Forme
    reconnue" section) -- this profile computes real ICT price levels (an
    entry-candle wick, a liquidity-pool target) internally, which is the
    whole point of it, but has to re-express them as a % distance from the
    entry price at the very end, or the engine silently ignores them and
    every trade rides unmanaged to a signal flip or the end of the range."""

    def _compute(self, execution: dict, microsystems: dict, config: dict | None = None) -> dict:
        infos = {info.id: info for info in discover_management_profiles(REPOSITORY_ROOT / "management_profiles")}
        info = infos["fvg_stop_leg_target"]
        context = ManagementContext(execution=execution, microsystems=microsystems, config=config or {}, log=_noop_log)
        return info.manage(context)

    def test_returns_stop_loss_pct_and_take_profit_pct_not_absolute_levels(self) -> None:
        result = self._compute(
            execution={"direction": "haussier", "fvg": {"direction": "bullish", "high": 110, "low": 100, "fill_pct": 0}},
            microsystems={"m1": {"last_price": 105, "active_pools_above": [{"side": "buyside", "level": 130}]}},
        )
        self.assertNotIn("stop_loss", result)
        self.assertNotIn("take_profit", result)
        self.assertIn("stop_loss_pct", result)
        self.assertIn("take_profit_pct", result)
        # Unconfirmed (price hasn't advanced 20% beyond the zone) -> SL sits
        # on the far edge of the zone (100), 5/105 below the 105 entry.
        self.assertAlmostEqual(result["stop_loss_pct"], (5 / 105) * 100, places=6)
        # TP targets the buyside pool at 130, 25/105 above the 105 entry.
        self.assertAlmostEqual(result["take_profit_pct"], (25 / 105) * 100, places=6)

    def test_neutral_direction_returns_none_for_both_pct_fields(self) -> None:
        result = self._compute(execution={"direction": "neutre"}, microsystems={})
        self.assertEqual(result, {"stop_loss_pct": None, "take_profit_pct": None, "reward_risk_ratio": None})

    def test_reward_risk_below_minimum_returns_none_pct_fields(self) -> None:
        result = self._compute(
            execution={"direction": "haussier", "fvg": {"direction": "bullish", "high": 110, "low": 100, "fill_pct": 0}},
            microsystems={"m1": {"last_price": 105, "active_pools_above": [{"side": "buyside", "level": 106}]}},
            config={"min_reward_risk_ratio": 5},
        )
        self.assertIsNone(result["stop_loss_pct"])
        self.assertIsNone(result["take_profit_pct"])
        self.assertIsNotNone(result["reward_risk_ratio"])


if __name__ == "__main__":
    unittest.main()
