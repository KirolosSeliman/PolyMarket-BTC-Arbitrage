from __future__ import annotations

import unittest
from datetime import UTC, datetime

from tests.data_collection.market_discovery.fixtures import (
    OBSERVED_AT,
    btc_5m_payload,
    default_config,
)

from polymarket_btc.data_collection.market_discovery.normalizer import normalize_gamma_market


class NormalizerTests(unittest.TestCase):
    def test_accepts_valid_btc_5m_payload_with_json_string_lists(self) -> None:
        result = normalize_gamma_market(btc_5m_payload(), default_config(), OBSERVED_AT)

        self.assertTrue(result.is_valid)
        self.assertIsNotNone(result.market)
        market = result.market
        assert market is not None
        self.assertEqual(market.market_id, "2951004")
        self.assertEqual(market.event_id, "711207")
        self.assertEqual(market.condition_id, "0xece31a80d3b21df6f05b30a27ea9542ee89a66945cdcbecaeaf3ced75307be68")
        self.assertEqual(market.question_id, "0x058d3259facd2b70783285327813c1c591944095d53f2fc1bdb47f7ce1ccb17a")
        self.assertEqual(market.start_time_utc, datetime(2026, 7, 17, 19, 50, tzinfo=UTC))
        self.assertEqual(market.end_time_utc, datetime(2026, 7, 17, 19, 55, tzinfo=UTC))
        self.assertEqual(market.duration_seconds, 300)
        self.assertEqual([outcome.normalized_name for outcome in market.outcomes], ["Up", "Down"])
        self.assertEqual(market.outcomes[0].source_name, "Up")
        self.assertEqual(
            market.outcomes[0].token_id,
            "48859776499838806404537537842106177463066316513734253200298308734307495640443",
        )

    def test_accepts_outcomes_and_tokens_as_arrays(self) -> None:
        payload = btc_5m_payload(outcomes=["up", "DOWN"], clobTokenIds=["token-up", "token-down"])

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertTrue(result.is_valid)
        market = result.market
        assert market is not None
        self.assertEqual([outcome.normalized_name for outcome in market.outcomes], ["Up", "Down"])
        self.assertEqual([outcome.token_id for outcome in market.outcomes], ["token-up", "token-down"])

    def test_accepts_reversed_outcome_order_by_mapping_tokens_to_source_indexes(self) -> None:
        payload = btc_5m_payload(outcomes=["Down", "Up"], clobTokenIds=["token-down", "token-up"])

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertTrue(result.is_valid)
        market = result.market
        assert market is not None
        self.assertEqual([outcome.normalized_name for outcome in market.outcomes], ["Up", "Down"])
        self.assertEqual([outcome.token_id for outcome in market.outcomes], ["token-up", "token-down"])
        self.assertEqual([outcome.source_name for outcome in market.outcomes], ["Up", "Down"])

    def test_rejects_mismatched_outcome_and_token_counts(self) -> None:
        payload = btc_5m_payload(outcomes='["Up", "Down"]', clobTokenIds='["token-up"]')

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection.reason, "outcome_token_count_mismatch")

    def test_rejects_duplicate_outcomes(self) -> None:
        payload = btc_5m_payload(outcomes='["Up", "Up"]')

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection.reason, "duplicate_outcome")

    def test_rejects_duplicate_token_ids(self) -> None:
        payload = btc_5m_payload(clobTokenIds='["same-token", "same-token"]')

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection.reason, "duplicate_token_id")

    def test_rejects_unknown_outcomes(self) -> None:
        payload = btc_5m_payload(outcomes='["Up", "Flat"]')

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection.reason, "unknown_outcome")

    def test_rejects_missing_condition_id(self) -> None:
        payload = btc_5m_payload(conditionId="")

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection.reason, "missing_condition_id")

    def test_rejects_missing_token_ids(self) -> None:
        payload = btc_5m_payload(clobTokenIds=None)

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection.reason, "missing_token_ids")

    def test_rejects_invalid_event_start_time(self) -> None:
        payload = btc_5m_payload(eventStartTime="not-a-date")

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection.reason, "invalid_timestamp")

    def test_rejects_closed_markets(self) -> None:
        payload = btc_5m_payload(closed=True)

        result = normalize_gamma_market(payload, default_config(), OBSERVED_AT)

        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection.reason, "closed_market")


if __name__ == "__main__":
    unittest.main()
