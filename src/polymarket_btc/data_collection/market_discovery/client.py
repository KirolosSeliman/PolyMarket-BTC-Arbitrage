"""Minimal asynchronous Gamma market-by-slug client."""

import asyncio
import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from typing import Any


GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
HTTP_TIMEOUT_SECONDS = 0.45


class GammaClientError(RuntimeError):
    pass


class GammaNotFound(GammaClientError):
    pass


class GammaUnavailable(GammaClientError):
    pass


class GammaInvalidResponse(GammaClientError):
    pass


class GammaClient:
    async def get_market_by_slug(self, slug: str) -> Mapping[str, Any] | None:
        return await asyncio.to_thread(self._get_market_by_slug, slug)

    def _get_market_by_slug(self, slug: str) -> Mapping[str, Any] | None:
        encoded_slug = urllib.parse.quote(slug, safe="")
        request = urllib.request.Request(
            f"{GAMMA_BASE_URL}/markets/slug/{encoded_slug}",
            headers={
                "Accept": "application/json",
                "User-Agent": "polymarket-btc-market-discovery/1.0",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            if 400 <= exc.code <= 499:
                raise GammaInvalidResponse(f"Gamma HTTP {exc.code}") from exc
            raise GammaUnavailable(f"Gamma HTTP {exc.code}") from exc
        except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
            raise GammaUnavailable(f"Gamma unavailable: {exc}") from exc
        except Exception as exc:
            raise GammaUnavailable(f"Gamma protocol failure: {exc}") from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GammaInvalidResponse("Gamma returned invalid JSON") from exc
        if not isinstance(payload, Mapping):
            raise GammaInvalidResponse("Gamma response must be a JSON object")
        return payload
