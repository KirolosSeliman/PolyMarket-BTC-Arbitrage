from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from tests.data_collection.market_discovery.fixtures import (
    OBSERVED_AT,
    btc_5m_payload,
    default_config,
)

from polymarket_btc.data_collection.market_discovery.models import DiscoveryStatus
from polymarket_btc.data_collection.market_discovery.normalizer import normalize_gamma_market
from polymarket_btc.data_collection.market_discovery.selector import select_markets


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def candidate(
    start: datetime,
    market_id: str = "2951004",
    closed: bool = False,
):
    end = start + timedelta(minutes=5)
    epoch = int(start.timestamp())
    payload = btc_5m_payload(
        id=market_id,
        slug=f"btc-updown-5m-{epoch}",
        eventStartTime=iso_z(start),
        endDate=iso_z(end),
        closed=closed,
        conditionId=f"0xcondition{market_id}",
        questionID=f"0xquestion{market_id}",
    )
    return normalize_gamma_market(payload, default_config(), OBSERVED_AT)


class SelectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = default_config()
        self.start = datetime(2026, 7, 17, 19, 50, tzinfo=UTC)

    def test_selects_single_current_market(self) -> None:
        result = select_markets([candidate(self.start)], self.start + timedelta(minutes=2), self.config)

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)
        self.assertIsNotNone(result.selected_market)
        self.assertEqual(result.selected_market.market_id, "2951004")
        self.assertEqual(result.valid_candidate_count, 1)

    def test_returns_no_match_when_only_candidate_is_future(self) -> None:
        result = select_markets([candidate(self.start)], self.start - timedelta(milliseconds=1), self.config)

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertIsNone(result.selected_market)
        self.assertIsNotNone(result.next_market)

    def test_returns_ambiguous_when_multiple_markets_match_current_window(self) -> None:
        result = select_markets(
            [candidate(self.start, "1"), candidate(self.start, "2")],
            self.start + timedelta(minutes=1),
            self.config,
        )

        self.assertEqual(result.status, DiscoveryStatus.AMBIGUOUS)
        self.assertIsNone(result.selected_market)
        self.assertEqual(result.valid_candidate_count, 2)

    def test_invalid_closed_candidate_is_not_selected(self) -> None:
        result = select_markets([candidate(self.start, closed=True)], self.start + timedelta(minutes=1), self.config)

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertIsNone(result.selected_market)
        self.assertEqual(result.rejected_candidates[0].reason, "closed_market")

    def test_start_boundary_is_inclusive(self) -> None:
        result = select_markets([candidate(self.start)], self.start, self.config)

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)

    def test_one_millisecond_before_start_is_not_current(self) -> None:
        result = select_markets([candidate(self.start)], self.start - timedelta(milliseconds=1), self.config)

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)

    def test_end_boundary_is_exclusive(self) -> None:
        result = select_markets([candidate(self.start)], self.start + timedelta(minutes=5), self.config)

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertIsNone(result.selected_market)

    def test_one_millisecond_before_end_is_current(self) -> None:
        result = select_markets(
            [candidate(self.start)],
            self.start + timedelta(minutes=5) - timedelta(milliseconds=1),
            self.config,
        )

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)

    def test_detects_next_market_after_selected_current_market(self) -> None:
        result = select_markets(
            [candidate(self.start), candidate(self.start + timedelta(minutes=5), "next")],
            self.start + timedelta(minutes=1),
            self.config,
        )

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)
        self.assertEqual(result.selected_market.market_id, "2951004")
        self.assertEqual(result.next_market.market_id, "next")

    def test_next_market_is_none_when_absent(self) -> None:
        result = select_markets([candidate(self.start)], self.start + timedelta(minutes=1), self.config)

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)
        self.assertIsNone(result.next_market)


if __name__ == "__main__":
    unittest.main()
