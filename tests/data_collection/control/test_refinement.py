import tempfile
import unittest
from pathlib import Path

from polymarket_btc.data_collection.control import refinement


class WalkAndPredicateTests(unittest.TestCase):
    def test_zone_requires_direction_ordered_high_low_and_numeric_formed_at(self) -> None:
        self.assertTrue(refinement.looks_like_zone({"direction": "bullish", "high": 2, "low": 1, "formed_at": 5.0}))
        self.assertFalse(refinement.looks_like_zone({"direction": "bullish", "high": 1, "low": 2, "formed_at": 5.0}))
        self.assertFalse(refinement.looks_like_zone({"direction": "sideways", "high": 2, "low": 1, "formed_at": 5.0}))
        self.assertFalse(refinement.looks_like_zone({"direction": "bullish", "high": 2, "low": 1}))

    def test_bool_never_satisfies_the_numeric_guard(self) -> None:
        # Python's bool is an int subclass -- a naive isinstance(x, (int, float))
        # port of JS's typeof x === "number" would wrongly accept True/False.
        self.assertFalse(refinement.looks_like_zone({"direction": "bullish", "high": True, "low": 1, "formed_at": 5.0}))
        self.assertFalse(refinement.looks_like_level({"level": True, "formed_at": 5.0}))

    def test_level_accepts_either_formed_at_or_swept_at(self) -> None:
        self.assertTrue(refinement.looks_like_level({"level": 1.5, "formed_at": 5.0}))
        self.assertTrue(refinement.looks_like_level({"level": 1.5, "swept_at": 5.0}))
        self.assertFalse(refinement.looks_like_level({"level": 1.5}))

    def test_walk_finds_nodes_arbitrarily_nested_regardless_of_key_names(self) -> None:
        output = {
            "sweeps": [{"level": 10.0, "swept_at": 3.0, "direction": "bullish"}],
            "meta": {"nested": {"deep_pool": {"level": 20.0, "formed_at": 1.0}}},
        }
        found = refinement.walk_for_annotations(output)
        shapes = sorted(shape for shape, _node in found)
        self.assertEqual(shapes, ["level", "level"])

    def test_walk_depth_cap_stops_runaway_recursion(self) -> None:
        node: dict = {"level": 1.0, "formed_at": 1.0}
        wrapped = node
        for _ in range(10):
            wrapped = {"inner": wrapped}
        found = refinement.walk_for_annotations(wrapped)
        self.assertEqual(found, [])  # buried past depth 6, never reached


# A compound "setup" node, shaped like a real microsystem's own detected
# pattern (microsystems/fvg_sweep_reversal.py): an initial FVG (zone), a
# sweep (level, formed far earlier than it resolves), and a reversal FVG
# (zone) -- three sub-fields, well past the >=2 bar looks_like_setup_entry
# requires.
_SETUP_NODE = {
    "signal": "haussier",
    "initial_fvg": {"direction": "bearish", "high": 105.0, "low": 104.0, "formed_at": 10.0},
    "sweep": {"direction": "bullish", "level": 100.0, "formed_at": 20.0, "swept_at": 50.0},
    "reversal_fvg": {"direction": "bullish", "high": 101.0, "low": 100.5, "formed_at": 55.0},
    "detected_at": 55.0,
}


class SetupDetectionTests(unittest.TestCase):
    def test_looks_like_setup_entry_requires_at_least_two_zone_or_level_subfields(self) -> None:
        self.assertTrue(refinement.looks_like_setup_entry(_SETUP_NODE))
        only_one = {"initial_fvg": _SETUP_NODE["initial_fvg"], "note": "not enough on its own"}
        self.assertFalse(refinement.looks_like_setup_entry(only_one))

    def test_a_zone_or_level_itself_is_never_also_a_setup(self) -> None:
        self.assertFalse(refinement.looks_like_setup_entry(_SETUP_NODE["initial_fvg"]))
        self.assertFalse(refinement.looks_like_setup_entry(_SETUP_NODE["sweep"]))

    def test_find_setup_candidates_collects_whole_setups_not_fragments(self) -> None:
        output = {"setups": [_SETUP_NODE], "dernier_signal": "haussier", "last_price": 101.0}
        found = refinement.find_setup_candidates(output)
        self.assertEqual(len(found), 1)
        self.assertIs(found[0], _SETUP_NODE)

    def test_find_setup_candidates_does_not_descend_into_a_matched_setups_own_subfields(self) -> None:
        # If it did, initial_fvg/reversal_fvg (both zone-shaped) would also
        # separately satisfy looks_like_setup_entry's own zone/level guard
        # rejection incorrectly, or at minimum this would silently start
        # treating the setup's own parts as further setups.
        found = refinement.find_setup_candidates({"setups": [_SETUP_NODE]})
        self.assertEqual(len(found), 1)

    def test_find_setup_candidates_finds_multiple_sibling_setups(self) -> None:
        second = {
            "signal": "baissier",
            "initial_fvg": {"direction": "bullish", "high": 200.0, "low": 199.0, "formed_at": 60.0},
            "sweep": {"direction": "bearish", "level": 205.0, "formed_at": 65.0, "swept_at": 70.0},
        }
        found = refinement.find_setup_candidates({"setups": [_SETUP_NODE, second]})
        self.assertEqual(len(found), 2)


class TriggerTimestampTests(unittest.TestCase):
    def test_zone_always_triggers_on_formed_at(self) -> None:
        self.assertEqual(refinement.trigger_timestamp("zone", {"formed_at": 7.0}), 7.0)

    def test_level_prefers_swept_at_over_formed_at_when_both_present(self) -> None:
        # The sweep-trigger fix: formed_at (pool origin) can be far earlier
        # than swept_at (resolution) -- triggering on formed_at alone would
        # mean the sweep itself is never captured.
        self.assertEqual(refinement.trigger_timestamp("level", {"formed_at": 1.0, "swept_at": 99.0}), 99.0)

    def test_level_falls_back_to_formed_at_when_never_swept(self) -> None:
        self.assertEqual(refinement.trigger_timestamp("level", {"formed_at": 1.0}), 1.0)

    def test_setup_triggers_on_the_latest_of_its_own_subnodes(self) -> None:
        # initial_fvg formed_at=10, sweep swept_at=50 (preferred over its
        # own formed_at=20), reversal_fvg formed_at=55 -- latest is 55, not
        # the setup's own earliest piece (10).
        self.assertEqual(refinement.trigger_timestamp("setup", _SETUP_NODE), 55.0)

    def test_setup_with_no_numeric_subnodes_returns_none(self) -> None:
        self.assertIsNone(refinement.trigger_timestamp("setup", {"note": "empty"}))


class InstanceKeyTests(unittest.TestCase):
    def test_stable_under_extra_or_reordered_volatile_fields(self) -> None:
        base = {"direction": "bullish", "high": 2.0, "low": 1.0, "formed_at": 5.0}
        with_extra = {**base, "fill_pct": 42.0, "status": "mitigated"}
        reordered = {"formed_at": 5.0, "low": 1.0, "high": 2.0, "direction": "bullish", "status": "untouched"}
        key_a = refinement.instance_key("fvg", "zone", base)
        key_b = refinement.instance_key("fvg", "zone", with_extra)
        key_c = refinement.instance_key("fvg", "zone", reordered)
        self.assertEqual(key_a, key_b)
        self.assertEqual(key_a, key_c)

    def test_distinct_high_low_or_level_gives_distinct_keys(self) -> None:
        a = refinement.instance_key("fvg", "zone", {"direction": "bullish", "high": 2.0, "low": 1.0, "formed_at": 5.0})
        b = refinement.instance_key("fvg", "zone", {"direction": "bullish", "high": 3.0, "low": 1.0, "formed_at": 5.0})
        self.assertNotEqual(a, b)

    def test_distinct_owner_id_gives_distinct_keys_for_an_otherwise_identical_node(self) -> None:
        node = {"direction": "bullish", "high": 2.0, "low": 1.0, "formed_at": 5.0}
        self.assertNotEqual(refinement.instance_key("fvg", "zone", node), refinement.instance_key("other", "zone", node))

    def test_setup_key_stable_regardless_of_subnode_dict_key_order(self) -> None:
        reordered_setup = {
            "reversal_fvg": _SETUP_NODE["reversal_fvg"], "sweep": _SETUP_NODE["sweep"],
            "initial_fvg": _SETUP_NODE["initial_fvg"], "signal": "haussier", "detected_at": 55.0,
        }
        self.assertEqual(
            refinement.instance_key("micro_1", "setup", _SETUP_NODE),
            refinement.instance_key("micro_1", "setup", reordered_setup),
        )

    def test_setup_key_distinct_from_a_setup_with_a_different_subnode(self) -> None:
        changed = {**_SETUP_NODE, "sweep": {**_SETUP_NODE["sweep"], "level": 999.0}}
        self.assertNotEqual(
            refinement.instance_key("micro_1", "setup", _SETUP_NODE),
            refinement.instance_key("micro_1", "setup", changed),
        )

    def test_trade_key_is_entry_time_and_direction_ignoring_other_fields(self) -> None:
        base = {"entry_time": 100.0, "direction": "long", "entry_price": 101.0, "outcome": "win"}
        # exit_price/outcome differ but entry_time+direction don't -- same key,
        # matching the plan's own "already unique within one strategy's
        # deterministic replay" reasoning for why no richer hashing is needed.
        different_outcome = {**base, "entry_price": 999.0, "outcome": "loss"}
        self.assertEqual(
            refinement.instance_key("strat_1", "trade", base),
            refinement.instance_key("strat_1", "trade", different_outcome),
        )

    def test_trade_key_distinct_for_different_entry_time_or_direction(self) -> None:
        base = {"entry_time": 100.0, "direction": "long"}
        other_time = {"entry_time": 200.0, "direction": "long"}
        other_direction = {"entry_time": 100.0, "direction": "short"}
        key_base = refinement.instance_key("strat_1", "trade", base)
        self.assertNotEqual(key_base, refinement.instance_key("strat_1", "trade", other_time))
        self.assertNotEqual(key_base, refinement.instance_key("strat_1", "trade", other_direction))


class KeyInstrumentTests(unittest.TestCase):
    def test_bare_key_defaults_to_btc(self) -> None:
        self.assertEqual(refinement.key_instrument("binance_futures_kline"), "BTCUSDT")

    def test_suffixed_key_uses_its_own_asset(self) -> None:
        self.assertEqual(refinement.key_instrument("binance_futures_kline:ETH"), "ETHUSDT")


class AppendLabelValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.feedback_dir = Path(self._tmp.name)

    def test_setup_is_an_accepted_shape(self) -> None:
        progress = refinement.append_label(
            self.feedback_dir, "micro_1", shape="setup", node=_SETUP_NODE, label="oui",
        )
        self.assertEqual(progress["total"], 1)

    def test_trade_is_an_accepted_shape(self) -> None:
        trade = {"entry_time": 100.0, "direction": "long", "entry_price": 101.0, "outcome": "win"}
        progress = refinement.append_label(
            self.feedback_dir, "some_strategy", shape="trade", node=trade, label="oui",
        )
        self.assertEqual(progress["total"], 1)

    def test_unknown_shape_rejected(self) -> None:
        with self.assertRaises(ValueError):
            refinement.append_label(self.feedback_dir, "micro_1", shape="nonsense", node={}, label="oui")


class ScanJobTests(unittest.TestCase):
    def test_run_scan_job_and_status_round_trip(self) -> None:
        import asyncio

        async def go() -> None:
            jobs: dict = {}
            job = refinement.run_scan_job(jobs, lambda on_progress: {"added": 3}, name_prefix="test-job")
            await job.task
            status = refinement.scan_job_status(jobs, job.job_id)
            self.assertTrue(status["done"])
            self.assertIsNone(status["error"])
            self.assertEqual(status["result"], {"added": 3})

        asyncio.run(go())

    def test_run_scan_job_captures_exception_as_error(self) -> None:
        import asyncio

        def boom(_on_progress):
            raise ValueError("scan failed")

        async def go() -> None:
            jobs: dict = {}
            job = refinement.run_scan_job(jobs, boom)
            await job.task
            status = refinement.scan_job_status(jobs, job.job_id)
            self.assertTrue(status["done"])
            self.assertEqual(status["error"], "scan failed")
            self.assertIsNone(status["result"])

        asyncio.run(go())

    def test_scan_job_status_unknown_id_returns_none(self) -> None:
        self.assertIsNone(refinement.scan_job_status({}, "no-such-id"))


if __name__ == "__main__":
    unittest.main()
