from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Mapping
from urllib.parse import urlencode

from polymarket_btc.data_collection.common.http import (
    HttpStatusError,
    HttpTimeoutError,
    HttpTransportError,
    JsonHttpClient,
)
from polymarket_btc.data_collection.market_discovery.config import MarketDiscoveryConfig


class GammaClientError(RuntimeError):
    pass


class ProviderRequestError(GammaClientError):
    pass


class ProviderUnavailableError(GammaClientError):
    pass


class GammaClient:
    def __init__(
        self,
        config: MarketDiscoveryConfig,
        *,
        http_client: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._http_client = http_client or JsonHttpClient()
        self._sleep = sleep

    def fetch_market_by_slug(self, slug: str) -> Mapping[str, Any] | None:
        response = self._get(f"/markets/slug/{slug}", allow_404=True)
        if response is None:
            return None
        if not isinstance(response, Mapping):
            raise ProviderRequestError("Gamma market-by-slug response must be an object")
        return response

    def search_btc_five_minute_markets(self, limit: int = 20) -> list[Mapping[str, Any]]:
        response = self._get(
            "/public-search",
            params={
                "q": self._config.provider.search_query,
                "limit_per_type": str(limit),
                "events_status": "active",
                "search_profiles": "false",
                "cache": "false",
            },
        )
        if not isinstance(response, Mapping):
            raise ProviderRequestError("Gamma public-search response must be an object")

        markets: list[Mapping[str, Any]] = []
        for event in _mapping_list(response.get("events")):
            markets.extend(_mapping_list(event.get("markets")))
        markets.extend(_mapping_list(response.get("markets")))
        return markets

    def _get(
        self,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        allow_404: bool = False,
    ) -> Any | None:
        url = self._build_url(path, params)
        total_attempts = self._config.polling.max_retries + 1
        for attempt_index in range(total_attempts):
            try:
                return self._http_client.get_json(
                    url,
                    timeout_seconds=self._config.polling.request_timeout_seconds,
                )
            except HttpStatusError as error:
                if allow_404 and error.status_code == 404:
                    return None
                if not _is_retryable_status(error.status_code):
                    raise ProviderRequestError(str(error)) from error
                if attempt_index == total_attempts - 1:
                    raise ProviderUnavailableError(str(error)) from error
                self._sleep(_retry_delay(self._config, attempt_index))
            except (HttpTimeoutError, HttpTransportError) as error:
                if attempt_index == total_attempts - 1:
                    raise ProviderUnavailableError(str(error)) from error
                self._sleep(_retry_delay(self._config, attempt_index))
        raise ProviderUnavailableError("Gamma request failed")

    def _build_url(self, path: str, params: Mapping[str, str] | None = None) -> str:
        url = f"{self._config.provider.gamma_base_url}{path}"
        if params:
            return f"{url}?{urlencode(params)}"
        return url


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _retry_delay(config: MarketDiscoveryConfig, attempt_index: int) -> float:
    delay = config.polling.retry_base_delay_seconds * (2**attempt_index)
    return min(delay, config.polling.retry_max_delay_seconds)


def _mapping_list(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]
