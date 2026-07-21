import unittest
from datetime import UTC, datetime, timedelta, timezone

from polymarket_btc.data_collection.market_discovery.client import GammaUnavailable
from polymarket_btc.data_collection.market_discovery.models import ResolveOutcome, Timeframe
from polymarket_btc.data_collection.market_discovery.resolver import (
    MarketResolver,
    build_market_slug,
    floor_to_window_start,
    parse_gamma_datetime,
)
from tests.data_collection.market_discovery.fixtures import market_payload


START = datetime(2026, 7, 21, 10, 0, tzinfo=UTC)


class FakeClient:
    def __init__(self, response: object) -> None:
        self.response = response
        self.slugs: list[str] = []

    async def get_market_by_slug(self, slug: str):
        self.slugs.append(slug)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class ResolverTests(unittest.IsolatedAsyncioTestCase):
    def test_flooring_and_slugs_for_both_timeframes(self) -> None:
        cases = (
            (Timeframe.FIVE_MINUTES, datetime(2026, 7, 21, 10, 4, 59, 999000, tzinfo=UTC), START),
            (Timeframe.FIVE_MINUTES, datetime(2026, 7, 21, 10, 5, tzinfo=UTC), START + timedelta(minutes=5)),
            (Timeframe.FIFTEEN_MINUTES, datetime(2026, 7, 21, 10, 14, 59, 999000, tzinfo=UTC), START),
            (Timeframe.FIFTEEN_MINUTES, datetime(2026, 7, 21, 10, 15, tzinfo=UTC), START + timedelta(minutes=15)),
        )
        for timeframe, value, expected in cases:
            with self.subTest(timeframe=timeframe, value=value):
                self.assertEqual(floor_to_window_start(value, timeframe), expected)
                self.assertEqual(build_market_slug(timeframe, expected), f"{timeframe.slug_prefix}-{int(expected.timestamp())}")

    def test_naive_datetime_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            floor_to_window_start(datetime(2026, 7, 21), Timeframe.FIVE_MINUTES)
        with self.assertRaises(ValueError):
            parse_gamma_datetime("2026-07-21T10:00:00", "field")

    def test_gamma_timestamp_accepts_z_and_offsets(self) -> None:
        self.assertEqual(parse_gamma_datetime("2026-07-21T10:00:00Z", "field"), START)
        self.assertEqual(
            parse_gamma_datetime("2026-07-21T06:00:00-04:00", "field"),
            START,
        )

    async def test_valid_market_for_both_timeframes_and_future_window(self) -> None:
        for timeframe in Timeframe:
            with self.subTest(timeframe=timeframe):
                payload = market_payload(timeframe, START)
                result = await MarketResolver(FakeClient(payload)).resolve_market(timeframe, START)
                self.assertEqual(result.outcome, ResolveOutcome.FOUND)
                self.assertEqual(result.market.up_token_id, "token-up")
                self.assertEqual(result.market.down_token_id, "token-down")

    async def test_not_found_and_provider_error_are_distinct(self) -> None:
        missing = await MarketResolver(FakeClient(None)).resolve_market(Timeframe.FIVE_MINUTES, START)
        failed = await MarketResolver(FakeClient(GammaUnavailable("offline"))).resolve_market(Timeframe.FIVE_MINUTES, START)
        self.assertEqual(missing.outcome, ResolveOutcome.NOT_FOUND)
        self.assertEqual(failed.outcome, ResolveOutcome.ERROR)
        self.assertEqual(failed.error, "offline")

    async def test_business_validation_rejections(self) -> None:
        cases = {
            "missing id": {"id": ""},
            "missing condition": {"conditionId": None},
            "wrong slug": {"slug": "other"},
            "inactive": {"active": False},
            "closed": {"closed": True},
            "archived": {"archived": True},
            "order book disabled": {"enableOrderBook": False},
            "wrong start": {"eventStartTime": "2026-07-21T10:00:01Z"},
            "wrong end": {"endDate": "2026-07-21T10:06:00Z"},
            "wrong source": {"resolutionSource": "https://example.com"},
            "unknown outcome": {"outcomes": ["Up", "Maybe"]},
            "duplicate outcome": {"outcomes": ["Up", "UP"]},
            "missing outcome": {"outcomes": ["Up"]},
            "empty token": {"clobTokenIds": ["up", " "]},
            "duplicate token": {"clobTokenIds": ["same", "same"]},
            "token count": {"clobTokenIds": ["up"]},
        }
        for label, override in cases.items():
            with self.subTest(label=label):
                payload = market_payload(Timeframe.FIVE_MINUTES, START, **override)
                result = await MarketResolver(FakeClient(payload)).resolve_market(Timeframe.FIVE_MINUTES, START)
                self.assertEqual(result.outcome, ResolveOutcome.NOT_FOUND)
                self.assertTrue(result.error)

    async def test_wrong_duration_is_rejected_for_each_timeframe(self) -> None:
        for timeframe in Timeframe:
            wrong_end = START + timedelta(seconds=timeframe.duration_seconds + 1)
            payload = market_payload(timeframe, START, endDate=wrong_end.isoformat())
            result = await MarketResolver(FakeClient(payload)).resolve_market(timeframe, START)
            self.assertEqual(result.outcome, ResolveOutcome.NOT_FOUND)

    async def test_numeric_identifiers_are_normalized_to_strings(self) -> None:
        payload = market_payload(Timeframe.FIVE_MINUTES, START, market_id=123, conditionId=456)
        result = await MarketResolver(FakeClient(payload)).resolve_market(Timeframe.FIVE_MINUTES, START)
        self.assertEqual((result.market.market_id, result.market.condition_id), ("123", "456"))

    async def test_outcomes_and_tokens_accept_lists_strings_and_reversed_order(self) -> None:
        combinations = (
            ('["Up", "Down"]', '["up", "down"]'),
            (["Up", "Down"], ["up", "down"]),
            (["Down", "Up"], ["down", "up"]),
        )
        for outcomes, tokens in combinations:
            payload = market_payload(Timeframe.FIVE_MINUTES, START, outcomes=outcomes, token_ids=tokens)
            result = await MarketResolver(FakeClient(payload)).resolve_market(Timeframe.FIVE_MINUTES, START)
            self.assertEqual(result.outcome, ResolveOutcome.FOUND)
            self.assertEqual((result.market.up_token_id, result.market.down_token_id), ("up", "down"))

    async def test_source_normalization_is_narrow(self) -> None:
        payload = market_payload(
            Timeframe.FIVE_MINUTES,
            START,
            resolutionSource="HTTPS://DATA.CHAIN.LINK/streams/btc-usd/",
        )
        result = await MarketResolver(FakeClient(payload)).resolve_market(Timeframe.FIVE_MINUTES, START)
        self.assertEqual(result.outcome, ResolveOutcome.FOUND)

    async def test_resolve_current_uses_current_window_in_non_utc_timezone(self) -> None:
        client = FakeClient(market_payload(Timeframe.FIVE_MINUTES, START))
        value = START.astimezone(timezone(timedelta(hours=-4)))
        result = await MarketResolver(client).resolve_current_market(Timeframe.FIVE_MINUTES, value)
        self.assertEqual(result.outcome, ResolveOutcome.FOUND)
