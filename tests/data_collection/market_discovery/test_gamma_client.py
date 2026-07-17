from __future__ import annotations

import unittest
from typing import Any

from tests.data_collection.market_discovery.fixtures import default_config

from polymarket_btc.data_collection.common.http import HttpStatusError, HttpTimeoutError
from polymarket_btc.data_collection.market_discovery.gamma_client import (
    GammaClient,
    ProviderRequestError,
    ProviderUnavailableError,
)


class FakeTransport:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, int]] = []

    def get_json(self, url: str, timeout_seconds: int) -> Any:
        self.calls.append((url, timeout_seconds))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class GammaClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = default_config()
        self.sleeps: list[float] = []

    def client(self, transport: FakeTransport) -> GammaClient:
        return GammaClient(self.config, http_client=transport, sleep=self.sleeps.append)

    def test_fetch_market_by_slug_returns_payload(self) -> None:
        transport = FakeTransport([{"id": "1"}])

        result = self.client(transport).fetch_market_by_slug("btc-updown-5m-1784317800")

        self.assertEqual(result, {"id": "1"})
        self.assertIn("/markets/slug/btc-updown-5m-1784317800", transport.calls[0][0])
        self.assertEqual(transport.calls[0][1], 10)

    def test_fetch_market_by_slug_returns_none_on_404(self) -> None:
        transport = FakeTransport([HttpStatusError(404, "not found")])

        result = self.client(transport).fetch_market_by_slug("missing")

        self.assertIsNone(result)
        self.assertEqual(len(transport.calls), 1)

    def test_retries_500_then_succeeds(self) -> None:
        transport = FakeTransport([HttpStatusError(500, "server error"), {"id": "ok"}])

        result = self.client(transport).fetch_market_by_slug("btc-updown-5m-1784317800")

        self.assertEqual(result, {"id": "ok"})
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(self.sleeps, [1.0])

    def test_retries_429_then_succeeds(self) -> None:
        transport = FakeTransport([HttpStatusError(429, "rate limited"), {"id": "ok"}])

        result = self.client(transport).fetch_market_by_slug("btc-updown-5m-1784317800")

        self.assertEqual(result, {"id": "ok"})
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(self.sleeps, [1.0])

    def test_400_is_not_retried(self) -> None:
        transport = FakeTransport([HttpStatusError(400, "bad request")])

        with self.assertRaises(ProviderRequestError):
            self.client(transport).fetch_market_by_slug("bad")

        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(self.sleeps, [])

    def test_timeout_is_converted_after_bounded_retries(self) -> None:
        transport = FakeTransport(
            [
                HttpTimeoutError("timeout"),
                HttpTimeoutError("timeout"),
                HttpTimeoutError("timeout"),
                HttpTimeoutError("timeout"),
            ]
        )

        with self.assertRaises(ProviderUnavailableError):
            self.client(transport).fetch_market_by_slug("btc-updown-5m-1784317800")

        self.assertEqual(len(transport.calls), 4)
        self.assertEqual(self.sleeps, [1.0, 2.0, 4.0])

    def test_search_flattens_events_and_markets(self) -> None:
        transport = FakeTransport(
            [
                {
                    "events": [
                        {"markets": [{"id": "event-market"}]},
                    ],
                    "markets": [{"id": "direct-market"}],
                }
            ]
        )

        result = self.client(transport).search_btc_five_minute_markets(limit=5)

        self.assertEqual(result, [{"id": "event-market"}, {"id": "direct-market"}])
        self.assertIn("/public-search?", transport.calls[0][0])
        self.assertIn("bitcoin+up+down+5m", transport.calls[0][0])


if __name__ == "__main__":
    unittest.main()
