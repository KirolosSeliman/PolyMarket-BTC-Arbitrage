import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from polymarket_btc.data_collection.market_discovery.models import Timeframe, TransitionResult
from polymarket_btc.data_collection.market_discovery.transition_log import TransitionLogger
from tests.data_collection.market_discovery.fixtures import market


START = datetime(2026, 7, 21, 10, 5, tzinfo=UTC)


def result(success: bool) -> TransitionResult:
    new = market(Timeframe.FIVE_MINUTES, START, market_id="new") if success else None
    return TransitionResult(
        success,
        Timeframe.FIVE_MINUTES,
        market(Timeframe.FIVE_MINUTES, START - timedelta(minutes=5), market_id="old"),
        new,
        START,
        START - timedelta(seconds=5),
        START + timedelta(seconds=1) if success else None,
        13 if success else 21,
        1000 if success else None,
        None if success else "market_not_found",
    )


class TransitionLoggerTests(unittest.TestCase):
    def test_append_creates_parent_and_preserves_existing_lines(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "transitions.jsonl"
            logger = TransitionLogger(path)
            logger.append(result(True))
            logger.append(result(False))
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["status"], "success")
        self.assertEqual(rows[1]["status"], "failed")
        self.assertEqual(rows[0]["up_token_id"], "token-up")
        self.assertIsNone(rows[1]["up_token_id"])
        self.assertEqual(rows[0]["transition_delay_ms"], 1000)
        self.assertTrue(rows[0]["resolved_at_utc"].endswith("Z"))
        self.assertNotIn("payload", rows[0])
        self.assertEqual(set(rows[0]), {
            "event_type", "timeframe", "status", "expected_start_utc", "search_started_at_utc",
            "resolved_at_utc", "attempt_count", "transition_delay_ms", "previous_market_id",
            "new_market_id", "condition_id", "up_token_id", "down_token_id", "last_error",
        })
