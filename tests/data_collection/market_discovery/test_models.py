import unittest
from datetime import UTC, datetime, timedelta, timezone

from polymarket_btc.data_collection.market_discovery.models import (
    DiscoveryState,
    MarketWindow,
    ResolveOutcome,
    ResolveResult,
    Timeframe,
    TransitionResult,
)


class ModelTests(unittest.TestCase):
    def test_supported_timeframes_are_exact(self) -> None:
        self.assertEqual(
            [(item.value, item.duration_seconds, item.slug_prefix) for item in Timeframe],
            [("5m", 300, "btc-updown-5m"), ("15m", 900, "btc-updown-15m")],
        )
        with self.assertRaises(ValueError):
            Timeframe("1h")

    def test_discovery_states_are_exact(self) -> None:
        self.assertEqual(
            [item.value for item in DiscoveryState],
            ["active", "searching_next", "next_ready", "transition_failed"],
        )

    def test_market_window_normalizes_aware_datetimes_to_utc(self) -> None:
        offset = timezone(timedelta(hours=-4))
        window = MarketWindow(
            Timeframe.FIVE_MINUTES,
            "m",
            "c",
            "slug",
            datetime(2026, 7, 21, 6, 0, tzinfo=offset),
            datetime(2026, 7, 21, 6, 5, tzinfo=offset),
            "up",
            "down",
            "source",
        )
        self.assertEqual(window.start_time_utc, datetime(2026, 7, 21, 10, 0, tzinfo=UTC))
        self.assertIs(window.end_time_utc.tzinfo, UTC)

    def test_market_window_rejects_naive_datetime_and_duplicate_tokens(self) -> None:
        values = (Timeframe.FIVE_MINUTES, "m", "c", "slug")
        with self.assertRaises(ValueError):
            MarketWindow(*values, datetime(2026, 7, 21), datetime.now(UTC), "up", "down", "source")
        with self.assertRaises(ValueError):
            MarketWindow(*values, datetime.now(UTC), datetime.now(UTC), "same", "same", "source")

    def test_resolve_result_invariants(self) -> None:
        start = datetime.now(UTC)
        with self.assertRaises(ValueError):
            ResolveResult(ResolveOutcome.FOUND, Timeframe.FIVE_MINUTES, start)
        with self.assertRaises(ValueError):
            ResolveResult(ResolveOutcome.ERROR, Timeframe.FIVE_MINUTES, start)

    def test_transition_result_invariants(self) -> None:
        start = datetime.now(UTC)
        with self.assertRaises(ValueError):
            TransitionResult(True, Timeframe.FIVE_MINUTES, None, None, start, start, None, 1, None, None)
        with self.assertRaises(ValueError):
            TransitionResult(False, Timeframe.FIVE_MINUTES, None, None, start, start, None, 1, None, None)


if __name__ == "__main__":
    unittest.main()
