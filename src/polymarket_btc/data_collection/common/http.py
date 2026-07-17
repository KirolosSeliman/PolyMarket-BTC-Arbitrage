from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class HttpStatusError(Exception):
    status_code: int
    body: str

    def __str__(self) -> str:
        return f"HTTP {self.status_code}: {self.body}"


class HttpTimeoutError(TimeoutError):
    pass


class HttpTransportError(RuntimeError):
    pass


class JsonHttpClient:
    def get_json(self, url: str, timeout_seconds: int) -> Any:
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "polymarket-btc-market-discovery/0.1",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise HttpStatusError(error.code, body) from error
        except (TimeoutError, socket.timeout) as error:
            raise HttpTimeoutError("request timed out") from error
        except urllib.error.URLError as error:
            if isinstance(error.reason, TimeoutError | socket.timeout):
                raise HttpTimeoutError("request timed out") from error
            raise HttpTransportError(str(error.reason)) from error

        try:
            return json.loads(body)
        except json.JSONDecodeError as error:
            raise HttpTransportError("response body is not valid JSON") from error
