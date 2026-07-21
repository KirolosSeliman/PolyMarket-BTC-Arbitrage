import json
import http.client
import socket
import unittest
from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from polymarket_btc.data_collection.market_discovery.client import (
    GammaClient,
    GammaInvalidResponse,
    GammaUnavailable,
    HTTP_TIMEOUT_SECONDS,
)


class Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


class GammaClientTests(unittest.IsolatedAsyncioTestCase):
    def test_http_timeout_is_exactly_one_second(self) -> None:
        self.assertEqual(HTTP_TIMEOUT_SECONDS, 1.0)

    async def test_valid_object_uses_one_encoded_request_and_fixed_timeout(self) -> None:
        calls: list[tuple[object, float]] = []

        def open_request(request: object, timeout: float) -> Response:
            calls.append((request, timeout))
            return Response(json.dumps({"id": "1"}).encode())

        with patch("urllib.request.urlopen", side_effect=open_request):
            result = await GammaClient().get_market_by_slug("slug with/slash")
        self.assertEqual(result, {"id": "1"})
        self.assertEqual(len(calls), 1)
        request, timeout = calls[0]
        self.assertEqual(timeout, HTTP_TIMEOUT_SECONDS)
        self.assertTrue(request.full_url.endswith("slug%20with%2Fslash"))
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("User-agent"), "polymarket-btc-market-discovery/1.0")

    async def test_404_returns_none(self) -> None:
        error = HTTPError("url", 404, "missing", {}, BytesIO())
        with patch("urllib.request.urlopen", side_effect=error):
            self.assertIsNone(await GammaClient().get_market_by_slug("slug"))

    async def test_http_errors_are_classified_without_retry(self) -> None:
        for status, expected in ((400, GammaInvalidResponse), (429, GammaInvalidResponse), (500, GammaUnavailable)):
            with self.subTest(status=status):
                error = HTTPError("url", status, "failure", {}, BytesIO())
                with patch("urllib.request.urlopen", side_effect=error) as opener:
                    with self.assertRaises(expected):
                        await GammaClient().get_market_by_slug("slug")
                    self.assertEqual(opener.call_count, 1)

    async def test_transport_and_timeout_are_unavailable(self) -> None:
        for error in (
            URLError("offline"),
            socket.timeout("slow"),
            TimeoutError("slow"),
            http.client.IncompleteRead(b"partial"),
            http.client.BadStatusLine("bad status"),
        ):
            with self.subTest(error=type(error).__name__):
                with patch("urllib.request.urlopen", side_effect=error):
                    with self.assertRaises(GammaUnavailable):
                        await GammaClient().get_market_by_slug("slug")

    async def test_invalid_json_or_non_object_is_invalid_response(self) -> None:
        for payload in (b"not json", b"[]"):
            with self.subTest(payload=payload):
                with patch("urllib.request.urlopen", return_value=Response(payload)):
                    with self.assertRaises(GammaInvalidResponse):
                        await GammaClient().get_market_by_slug("slug")
