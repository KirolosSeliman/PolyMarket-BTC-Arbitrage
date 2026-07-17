from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from typing import Any

from tests.data_collection.market_discovery.fixtures import btc_5m_payload, iso_z

from polymarket_btc.data_collection.market_discovery.config import MarketDiscoveryConfig
from polymarket_btc.data_collection.market_discovery.discovery import (
    build_btc_5m_slug,
    discover_current_market,
    floor_to_five_minute_start,
)
from polymarket_btc.data_collection.market_discovery.gamma_client import ProviderUnavailableError
from polymarket_btc.data_collection.market_discovery.models import DiscoveryStatus


class FakeClient:
    def __init__(self, payloads: dict[str, object] | None = None, error: Exception | None = None) -> None:
        self.payloads = payloads or {}
        self.error = error
        self.slugs: list[str] = []

    def get_market_by_slug(self, slug: str) -> object | None:
        self.slugs.append(slug)
        if self.error:
            raise self.error
        return self.payloads.get(slug)


def config() -> MarketDiscoveryConfig:
    return MarketDiscoveryConfig(
        gamma_base_url="https://gamma-api.polymarket.com",
        request_timeout_seconds=3,
        max_retries=1,
        retry_delay_seconds=0.5,
    )


class DiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 7, 17, 19, 50, tzinfo=UTC)
        self.now = self.start + timedelta(minutes=1)
        self.slug = build_btc_5m_slug(self.start)

    def discover(self, payload: dict[str, Any] | None, now: datetime | None = None):
        client = FakeClient({self.slug: payload} if payload else {})
        return discover_current_market(now or self.now, config=config(), gamma_client=client)

    def test_five_minute_flooring(self) -> None:
        now = datetime(2026, 7, 17, 19, 54, 59, 999000, tzinfo=UTC)

        self.assertEqual(floor_to_five_minute_start(now), self.start)

    def test_build_slug_uses_window_start_epoch(self) -> None:
        self.assertEqual(build_btc_5m_slug(self.start), "btc-updown-5m-1784317800")

    def test_valid_current_market_returns_concise_result(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start))

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)
        self.assertIsNone(result.reason)
        self.assertIsNotNone(result.market)
        assert result.market is not None
        self.assertEqual(result.market.market_id, "2951004")
        self.assertEqual(result.market.condition_id, "0xece31a80d3b21df6f05b30a27ea9542ee89a66945cdcbecaeaf3ced75307be68")
        self.assertEqual(result.market.slug, self.slug)
        self.assertEqual(result.market.start_time_utc, self.start)
        self.assertEqual(result.market.end_time_utc, self.start + timedelta(minutes=5))
        self.assertEqual(
            result.market.up_token_id,
            "48859776499838806404537537842106177463066316513734253200298308734307495640443",
        )
        self.assertEqual(
            result.market.down_token_id,
            "61478559042419582453760709973693988619232016626791716178709596040362979422358",
        )

    def test_no_market_at_current_slug_returns_no_match(self) -> None:
        result = self.discover(None)

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "no_market_at_expected_slug")
        self.assertIsNone(result.market)

    def test_provider_error_returns_provider_unavailable(self) -> None:
        client = FakeClient(error=ProviderUnavailableError("timeout"))

        result = discover_current_market(self.now, config=config(), gamma_client=client)

        self.assertEqual(result.status, DiscoveryStatus.PROVIDER_UNAVAILABLE)
        self.assertEqual(result.reason, "timeout")
        self.assertIsNone(result.market)

    def test_rejects_naive_datetime(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            discover_current_market(datetime(2026, 7, 17, 19, 51), config=config(), gamma_client=FakeClient())

    def test_before_start_uses_previous_slug_and_returns_no_match(self) -> None:
        client = FakeClient({self.slug: btc_5m_payload(start=self.start)})

        result = discover_current_market(self.start - timedelta(milliseconds=1), config=config(), gamma_client=client)

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(client.slugs, ["btc-updown-5m-1784317500"])

    def test_exact_start_is_selected(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start), self.start)

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)

    def test_one_millisecond_before_end_is_selected(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start), self.start + timedelta(minutes=5, milliseconds=-1))

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)

    def test_exact_end_uses_next_slug_and_returns_no_match(self) -> None:
        client = FakeClient({self.slug: btc_5m_payload(start=self.start)})

        result = discover_current_market(self.start + timedelta(minutes=5), config=config(), gamma_client=client)

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(client.slugs, ["btc-updown-5m-1784318100"])

    def test_rejects_closed_market(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, closed=True))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "closed_market")

    def test_rejects_archived_market(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, archived=True))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "archived_market")

    def test_rejects_inactive_market(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, active=False))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "inactive_market")

    def test_rejects_order_book_disabled(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, enableOrderBook=False))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "order_book_disabled")

    def test_rejects_wrong_duration(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, endDate=iso_z(self.start + timedelta(minutes=15))))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "wrong_duration")

    def test_rejects_slug_start_mismatch(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, eventStartTime=iso_z(self.start + timedelta(minutes=5))))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "slug_start_mismatch")

    def test_rejects_wrong_resolution_source_but_accepts_trailing_slash(self) -> None:
        accepted = self.discover(btc_5m_payload(start=self.start, resolutionSource="https://data.chain.link/streams/btc-usd/"))
        rejected = self.discover(btc_5m_payload(start=self.start, resolutionSource="https://example.com/btc"))

        self.assertEqual(accepted.status, DiscoveryStatus.SELECTED)
        self.assertEqual(rejected.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(rejected.reason, "wrong_resolution_source")

    def test_rejects_missing_market_id(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, id=""))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "missing_market_id")

    def test_rejects_missing_condition_id(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, conditionId=""))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "missing_condition_id")

    def test_rejects_missing_tokens(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, token_ids=None))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "missing_token_ids")

    def test_accepts_outcome_and_token_arrays(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, outcomes=["Up", "Down"], token_ids=["up", "down"]))

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)
        assert result.market is not None
        self.assertEqual(result.market.up_token_id, "up")
        self.assertEqual(result.market.down_token_id, "down")

    def test_reversed_outcomes_map_tokens_by_source_index(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, outcomes=["Down", "Up"], token_ids=["down", "up"]))

        self.assertEqual(result.status, DiscoveryStatus.SELECTED)
        assert result.market is not None
        self.assertEqual(result.market.up_token_id, "up")
        self.assertEqual(result.market.down_token_id, "down")

    def test_rejects_unknown_outcome(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, outcomes=["Up", "Flat"]))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "unknown_outcome")

    def test_rejects_duplicate_outcome(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, outcomes=["Up", "Up"]))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "duplicate_outcome")

    def test_rejects_duplicate_token(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, token_ids=["same", "same"]))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "duplicate_token_id")

    def test_rejects_mismatched_outcome_and_token_lengths(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, outcomes=["Up", "Down"], token_ids=["only-up"]))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "outcome_token_count_mismatch")

    def test_rejects_empty_token(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, token_ids=["", "down"]))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "empty_token_id")

    def test_rejects_invalid_timestamp(self) -> None:
        result = self.discover(btc_5m_payload(start=self.start, eventStartTime="not-a-date"))

        self.assertEqual(result.status, DiscoveryStatus.NO_MATCH)
        self.assertEqual(result.reason, "invalid_timestamp")

    def test_ambiguous_when_client_returns_multiple_candidates(self) -> None:
        start = self.start

        class MultiClient:
            def get_market_by_slug(self, slug: str) -> list[dict[str, Any]]:
                return [btc_5m_payload(start=start, market_id="one"), btc_5m_payload(start=start, market_id="two")]

        result = discover_current_market(self.now, config=config(), gamma_client=MultiClient())

        self.assertEqual(result.status, DiscoveryStatus.AMBIGUOUS)
        self.assertEqual(result.reason, "multiple_markets_at_expected_slug")


if __name__ == "__main__":
    unittest.main()
