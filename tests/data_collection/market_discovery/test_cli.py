import io
import json
import unittest
import asyncio
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from unittest.mock import patch

from polymarket_btc.data_collection.market_discovery import cli
from polymarket_btc.data_collection.market_discovery.models import ResolveOutcome, ResolveResult, Timeframe
from polymarket_btc.data_collection.market_discovery.transition_log import TransitionLogError
from tests.data_collection.market_discovery.fixtures import market


START = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)


class FakeResolver:
    results: dict[Timeframe, ResolveResult] = {}

    def __init__(self, client: object) -> None:
        pass

    async def resolve_current_market(self, timeframe: Timeframe) -> ResolveResult:
        return self.results[timeframe]


def resolved(outcome: ResolveOutcome, timeframe: Timeframe) -> ResolveResult:
    if outcome is ResolveOutcome.FOUND:
        return ResolveResult(outcome, timeframe, START, market(timeframe, START))
    return ResolveResult(outcome, timeframe, START, error="failure" if outcome is ResolveOutcome.ERROR else "missing")


class CliTests(unittest.TestCase):
    def run_current(self, outcomes: tuple[ResolveOutcome, ResolveOutcome]) -> tuple[int, dict[str, object]]:
        FakeResolver.results = {timeframe: resolved(outcome, timeframe) for timeframe, outcome in zip(Timeframe, outcomes)}
        output = io.StringIO()
        with patch.object(cli, "MarketResolver", FakeResolver), redirect_stdout(output):
            code = cli.main(["current"])
        return code, json.loads(output.getvalue())

    def test_current_exit_codes_and_json(self) -> None:
        for outcomes, expected in (
            ((ResolveOutcome.FOUND, ResolveOutcome.FOUND), 0),
            ((ResolveOutcome.FOUND, ResolveOutcome.NOT_FOUND), 1),
            ((ResolveOutcome.ERROR, ResolveOutcome.FOUND), 2),
        ):
            with self.subTest(outcomes=outcomes):
                code, payload = self.run_current(outcomes)
                self.assertEqual(code, expected)
                self.assertEqual(set(payload), {"5m", "15m"})
                self.assertTrue(payload["5m"]["expected_start_utc"].endswith("Z"))

    def test_run_respects_transition_log_and_handles_interrupt(self) -> None:
        captured: list[object] = []

        class Logger:
            def __init__(self, path: object) -> None:
                captured.append(path)

        def interrupt(coroutine: object) -> None:
            coroutine.close()
            raise KeyboardInterrupt

        with (
            patch.object(cli, "TransitionLogger", Logger),
            patch.object(cli.asyncio, "run", side_effect=interrupt),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(cli.main(["run", "--transition-log", "custom.jsonl"]), 0)
        self.assertEqual(str(captured[0]), "custom.jsonl")

    def test_old_options_are_rejected(self) -> None:
        for option in ("--once", "--watch", "--fixture", "--network-audit", "--validate-config", "--iterations"):
            with self.subTest(option=option), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    cli.main([option])

    def test_snapshot_serialization_is_jsonl_compatible(self) -> None:
        value = cli._to_jsonable(market(Timeframe.FIVE_MINUTES, START))
        self.assertEqual(value["timeframe"], "5m")
        self.assertTrue(value["start_time_utc"].endswith("Z"))

    def test_run_prints_snapshots_as_json_lines(self) -> None:
        class Runner:
            def __init__(self, resolver, controller, logger, on_state) -> None:
                self.on_state = on_state

            async def run_forever(self) -> None:
                self.on_state(market(Timeframe.FIVE_MINUTES, START))

        output = io.StringIO()
        with patch.object(cli, "MarketDiscoveryRunner", Runner), redirect_stdout(output):
            asyncio.run(cli._run(object()))
        row = json.loads(output.getvalue())
        self.assertEqual(row["timeframe"], "5m")

    def test_run_returns_exit_code_three_on_transition_log_error(self) -> None:
        def fail(coroutine: object) -> None:
            coroutine.close()
            raise TransitionLogError("failed to persist transition log")

        stderr = io.StringIO()
        with patch.object(cli.asyncio, "run", side_effect=fail), redirect_stderr(stderr):
            code = cli.main(["run"])
        self.assertEqual(code, 3)
        self.assertIn("transition log error: failed to persist transition log", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_run_help_documents_home_log_path(self) -> None:
        help_text = cli._parser()._subparsers._group_actions[0].choices["run"].format_help()
        self.assertIn(
            "~/.polymarket-btc/market_discovery/transitions.jsonl",
            "".join(help_text.split()),
        )
