from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from tests.data_collection.market_discovery.fixtures import default_config, payload_for_window

from polymarket_btc.data_collection.market_discovery.models import DiscoveryStatus
from polymarket_btc.data_collection.market_discovery.service import (
    MarketDiscoveryService,
    compute_five_minute_window_start,
)

class FakeGammaClient:
    def __init__(self, payloads_by_slug: dict[str, dict]) -> None:
        self.payloads_by_slug = payloads_by_slug
        self.requested_slugs: list[str] = []

    def fetch_market_by_slug(self, slug: str):
        self.requested_slugs.append(slug)
        return self.payloads_by_slug.get(slug)


class MarketDiscoveryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = default_config()
        self.start = datetime(2026, 7, 17, 19, 50, tzinfo=UTC)
        self.next_start = self.start + timedelta(minutes=5)

    def test_compute_window_start_floors_to_five_minute_boundary(self) -> None:
        now = datetime(2026, 7, 17, 19, 54, 59, 999000, tzinfo=UTC)

        result = compute_five_minute_window_start(now)

        self.assertEqual(result, self.start)

    def test_compute_window_start_uses_exact_boundary(self) -> None:
        now = datetime(2026, 7, 17, 19, 55, tzinfo=UTC)

        result = compute_five_minute_window_start(now)

        self.assertEqual(result, self.next_start)

    def test_discover_once_fetches_current_and_next_slugs(self) -> None:
        current_slug = f"btc-updown-5m-{int(self.start.timestamp())}"
        next_slug = f"btc-updown-5m-{int(self.next_start.timestamp())}"
        gamma = FakeGammaClient(
            {
                current_slug: payload_for_window(self.start, "current"),
                next_slug: payload_for_window(self.next_start, "next"),
            }
        )
        service = MarketDiscoveryService(self.config, gamma_client=gamma)

        result = service.discover_once(self.start + timedelta(minutes=1))

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)
        self.assertEqual(result.selected_market.market_id, "current")
        self.assertEqual(result.next_market.market_id, "next")
        self.assertEqual(gamma.requested_slugs, [current_slug, next_slug])

    def test_discover_once_reports_no_match_when_provider_has_no_current_market(self) -> None:
        gamma = FakeGammaClient({})
        service = MarketDiscoveryService(self.config, gamma_client=gamma)

        result = service.discover_once(self.start + timedelta(minutes=1))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertIsNone(result.selected_market)
        self.assertEqual(result.candidate_count, 0)

    def test_transition_detection_does_not_duplicate_same_market(self) -> None:
        current_slug = f"btc-updown-5m-{int(self.start.timestamp())}"
        next_slug = f"btc-updown-5m-{int(self.next_start.timestamp())}"
        gamma = FakeGammaClient(
            {
                current_slug: payload_for_window(self.start, "current"),
                next_slug: payload_for_window(self.next_start, "next"),
            }
        )
        service = MarketDiscoveryService(self.config, gamma_client=gamma)

        first = service.discover_once(self.start + timedelta(minutes=1))
        second = service.discover_once(self.start + timedelta(minutes=2))
        third = service.discover_once(self.next_start + timedelta(minutes=1))
        fourth = service.discover_once(self.next_start + timedelta(minutes=2))

        self.assertFalse(first.transition_detected)
        self.assertFalse(second.transition_detected)
        self.assertTrue(third.transition_detected)
        self.assertEqual(third.transition_from_market_id, "current")
        self.assertEqual(third.transition_to_market_id, "next")
        self.assertFalse(fourth.transition_detected)


if __name__ == "__main__":
    unittest.main()
