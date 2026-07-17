from __future__ import annotations

import unittest
from typing import Any

from polymarket_btc.data_collection.common.http import HttpStatusError, HttpTimeoutError, HttpTransportError
from polymarket_btc.data_collection.market_discovery.config import MarketDiscoveryConfig
from polymarket_btc.data_collection.market_discovery.gamma_client import (
    GammaClient,
    ProviderRequestError,
    ProviderUnavailableError,
)


class FakeTransport:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, float]] = []

    def get_json(self, url: str, timeout_seconds: float) -> Any:
        self.calls.append((url, timeout_seconds))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def config() -> MarketDiscoveryConfig:
    return MarketDiscoveryConfig(
        gamma_base_url="https://gamma-api.polymarket.com",
        request_timeout_seconds=3,
        max_retries=1,
        retry_delay_seconds=0.5,
    )


class ClientTests(unittest.TestCase):
    def make_client(self, responses: list[Any], sleeps: list[float] | None = None) -> tuple[GammaClient, FakeTransport]:
        transport = FakeTransport(responses)
        sleep = sleeps.append if sleeps is not None else (lambda _: None)
        client = GammaClient(config(), http_client=transport, sleep=sleep)
        return client, transport

    def test_successful_market_response(self) -> None:
        client, transport = self.make_client([{"id": "1"}])

        result = client.get_market_by_slug("btc-updown-5m-1")

        self.assertEqual(result, {"id": "1"})
        self.assertIn("/markets/slug/btc-updown-5m-1", transport.calls[0][0])
        self.assertEqual(transport.calls[0][1], 3)

    def test_404_returns_none(self) -> None:
        client, transport = self.make_client([HttpStatusError(404, "missing")])

        self.assertIsNone(client.get_market_by_slug("missing"))
        self.assertEqual(len(transport.calls), 1)

    def test_429_retries_then_succeeds(self) -> None:
        sleeps: list[float] = []
        client, transport = self.make_client([HttpStatusError(429, "rate"), {"id": "ok"}], sleeps)

        self.assertEqual(client.get_market_by_slug("slug"), {"id": "ok"})
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(sleeps, [0.5])

    def test_500_retries_then_succeeds(self) -> None:
        sleeps: list[float] = []
        client, transport = self.make_client([HttpStatusError(500, "server"), {"id": "ok"}], sleeps)

        self.assertEqual(client.get_market_by_slug("slug"), {"id": "ok"})
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(sleeps, [0.5])

    def test_timeout_retries_then_succeeds(self) -> None:
        sleeps: list[float] = []
        client, transport = self.make_client([HttpTimeoutError("timeout"), {"id": "ok"}], sleeps)

        self.assertEqual(client.get_market_by_slug("slug"), {"id": "ok"})
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(sleeps, [0.5])

    def test_exhausted_retry_returns_provider_unavailable(self) -> None:
        client, transport = self.make_client([HttpTimeoutError("timeout"), HttpTimeoutError("timeout")])

        with self.assertRaises(ProviderUnavailableError):
            client.get_market_by_slug("slug")

        self.assertEqual(len(transport.calls), 2)

    def test_400_is_not_retried(self) -> None:
        client, transport = self.make_client([HttpStatusError(400, "bad")])

        with self.assertRaises(ProviderRequestError):
            client.get_market_by_slug("bad")

        self.assertEqual(len(transport.calls), 1)

    def test_malformed_json_transport_error_is_provider_unavailable_after_retry(self) -> None:
        client, transport = self.make_client([HttpTransportError("not json"), HttpTransportError("not json")])

        with self.assertRaises(ProviderUnavailableError):
            client.get_market_by_slug("slug")

        self.assertEqual(len(transport.calls), 2)

    def test_invalid_response_type_is_provider_request_error(self) -> None:
        client, _ = self.make_client([["not", "a", "market"]])

        with self.assertRaises(ProviderRequestError):
            client.get_market_by_slug("slug")


if __name__ == "__main__":
    unittest.main()
