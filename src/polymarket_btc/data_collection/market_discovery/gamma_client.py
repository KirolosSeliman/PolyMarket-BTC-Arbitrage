from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Mapping
from urllib.parse import quote

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

    def get_market_by_slug(self, slug: str) -> Mapping[str, Any] | None:
        response = self._get_json(f"/markets/slug/{quote(slug, safe='')}", allow_404=True)
        if response is None:
            return None
        if not isinstance(response, Mapping):
            raise ProviderRequestError("Gamma market-by-slug response must be an object")
        return response

    def _get_json(self, path: str, *, allow_404: bool = False) -> Any | None:
        url = f"{self._config.gamma_base_url}{path}"
        total_attempts = self._config.max_retries + 1

        for attempt_index in range(total_attempts):
            try:
                return self._http_client.get_json(
                    url,
                    timeout_seconds=self._config.request_timeout_seconds,
                )
            except HttpStatusError as error:
                if allow_404 and error.status_code == 404:
                    return None
                if not _is_retryable_status(error.status_code):
                    raise ProviderRequestError(str(error)) from error
                if attempt_index == total_attempts - 1:
                    raise ProviderUnavailableError(str(error)) from error
                self._sleep(self._config.retry_delay_seconds)
            except (HttpTimeoutError, HttpTransportError) as error:
                if attempt_index == total_attempts - 1:
                    raise ProviderUnavailableError(str(error)) from error
                self._sleep(self._config.retry_delay_seconds)

        raise ProviderUnavailableError("Gamma request failed")


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599
