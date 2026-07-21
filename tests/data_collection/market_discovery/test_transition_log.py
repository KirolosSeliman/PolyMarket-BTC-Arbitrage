import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from polymarket_btc.data_collection.market_discovery.models import Timeframe, TransitionResult
from polymarket_btc.data_collection.market_discovery.transition_log import (
    TransitionLogError,
    TransitionLogger,
)
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
    def assert_wrapped_os_error(self, action, path: Path) -> None:
        with self.assertRaises(TransitionLogError) as raised:
            action()
        self.assertIn(str(path), str(raised.exception))
        self.assertIsInstance(raised.exception.__cause__, OSError)

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

    def test_default_transition_log_path_does_not_depend_on_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake_home = root / "home"
            other_cwd = root / "cwd"
            other_cwd.mkdir()
            original_cwd = Path.cwd()
            try:
                os.chdir(other_cwd)
                with patch.object(Path, "home", return_value=fake_home):
                    logger = TransitionLogger()
            finally:
                os.chdir(original_cwd)
        self.assertEqual(
            logger.path,
            (fake_home / ".polymarket-btc" / "market_discovery" / "transitions.jsonl").resolve(),
        )
        self.assertFalse(logger.path.is_relative_to(other_cwd))

    def test_explicit_transition_log_path_is_resolved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expanded = Path(directory) / "custom.jsonl"
            with patch.object(Path, "expanduser", return_value=expanded):
                logger = TransitionLogger(Path("~/custom.jsonl"))
        self.assertEqual(logger.path, expanded.resolve())

    def test_append_wraps_mkdir_failure(self) -> None:
        path = Path("failure/mkdir.jsonl").resolve()
        logger = TransitionLogger(path)
        with patch.object(Path, "mkdir", side_effect=OSError("mkdir failed")):
            self.assert_wrapped_os_error(lambda: logger.append(result(True)), path)

    def test_append_wraps_open_failure(self) -> None:
        path = Path("failure/open.jsonl").resolve()
        logger = TransitionLogger(path)
        with (
            patch.object(Path, "mkdir"),
            patch.object(Path, "open", side_effect=OSError("open failed")),
        ):
            self.assert_wrapped_os_error(lambda: logger.append(result(True)), path)

    def test_append_wraps_write_failure(self) -> None:
        path = Path("failure/write.jsonl").resolve()
        logger = TransitionLogger(path)
        handle = unittest.mock.MagicMock()
        handle.__enter__.return_value = handle
        handle.write.side_effect = OSError("write failed")
        with patch.object(Path, "mkdir"), patch.object(Path, "open", return_value=handle):
            self.assert_wrapped_os_error(lambda: logger.append(result(True)), path)

    def test_append_wraps_flush_failure(self) -> None:
        path = Path("failure/flush.jsonl").resolve()
        logger = TransitionLogger(path)
        handle = unittest.mock.MagicMock()
        handle.__enter__.return_value = handle
        handle.flush.side_effect = OSError("flush failed")
        with patch.object(Path, "mkdir"), patch.object(Path, "open", return_value=handle):
            self.assert_wrapped_os_error(lambda: logger.append(result(True)), path)

    def test_append_wraps_fsync_failure(self) -> None:
        path = Path("failure/fsync.jsonl").resolve()
        logger = TransitionLogger(path)
        handle = unittest.mock.MagicMock()
        handle.__enter__.return_value = handle
        with (
            patch.object(Path, "mkdir"),
            patch.object(Path, "open", return_value=handle),
            patch("polymarket_btc.data_collection.market_discovery.transition_log.os.fsync", side_effect=OSError("fsync failed")),
        ):
            self.assert_wrapped_os_error(lambda: logger.append(result(True)), path)
