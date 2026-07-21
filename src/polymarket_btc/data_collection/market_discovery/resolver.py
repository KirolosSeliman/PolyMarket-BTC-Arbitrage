"""UTC window calculation and strict Gamma payload validation."""

import json
import urllib.parse
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from .client import GammaClient, GammaInvalidResponse, GammaUnavailable
from .models import MarketWindow, ResolveOutcome, ResolveResult, Timeframe


EXPECTED_RESOLUTION_SOURCE = "https://data.chain.link/streams/btc-usd"


class MarketValidationError(ValueError):
    pass


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def floor_to_window_start(now_utc: datetime, timeframe: Timeframe) -> datetime:
    epoch = int(ensure_utc(now_utc).timestamp())
    start_epoch = epoch - (epoch % timeframe.duration_seconds)
    return datetime.fromtimestamp(start_epoch, UTC)


def build_market_slug(timeframe: Timeframe, start_time_utc: datetime) -> str:
    start = ensure_utc(start_time_utc)
    return f"{timeframe.slug_prefix}-{int(start.timestamp())}"


def parse_gamma_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty ISO 8601 string")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} is not a valid ISO 8601 datetime") from exc
    return ensure_utc(parsed)


def _required_id(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise MarketValidationError(f"{field} is missing or invalid")
    normalized = str(value).strip()
    if not normalized:
        raise MarketValidationError(f"{field} is empty")
    return normalized


def _json_list(value: object, field: str) -> list[object]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise MarketValidationError(f"{field} is not a valid JSON list") from exc
    if not isinstance(value, list):
        raise MarketValidationError(f"{field} must be a list")
    return value


def _normalized_source(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MarketValidationError("resolutionSource is missing")
    parsed = urllib.parse.urlsplit(value.strip())
    if parsed.query or parsed.fragment:
        raise MarketValidationError("resolutionSource contains unsupported components")
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", "")
    )


def validate_market_payload(
    payload: Mapping[str, Any],
    timeframe: Timeframe,
    expected_start_utc: datetime,
    expected_slug: str,
) -> MarketWindow:
    expected_start = ensure_utc(expected_start_utc)
    market_id = _required_id(payload, "id")
    condition_id = _required_id(payload, "conditionId")
    if payload.get("slug") != expected_slug:
        raise MarketValidationError("slug does not match expected market")
    if payload.get("active") is not True:
        raise MarketValidationError("market is not active")
    if payload.get("closed") is True:
        raise MarketValidationError("market is closed")
    if payload.get("archived") is True:
        raise MarketValidationError("market is archived")
    if payload.get("enableOrderBook") is not True:
        raise MarketValidationError("market order book is disabled")
    try:
        start = parse_gamma_datetime(payload.get("eventStartTime"), "eventStartTime")
        end = parse_gamma_datetime(payload.get("endDate"), "endDate")
    except ValueError as exc:
        raise MarketValidationError(str(exc)) from exc
    expected_end = expected_start + timedelta(seconds=timeframe.duration_seconds)
    if start != expected_start:
        raise MarketValidationError("eventStartTime does not match expected start")
    if end != expected_end:
        raise MarketValidationError("endDate does not match expected end")
    if build_market_slug(timeframe, start) != expected_slug:
        raise MarketValidationError("slug timestamp does not match eventStartTime")
    source = _normalized_source(payload.get("resolutionSource"))
    if source != EXPECTED_RESOLUTION_SOURCE:
        raise MarketValidationError("resolutionSource is not the expected Chainlink stream")
    outcomes = _json_list(payload.get("outcomes"), "outcomes")
    tokens = _json_list(payload.get("clobTokenIds"), "clobTokenIds")
    if len(outcomes) != 2 or len(tokens) != 2 or len(outcomes) != len(tokens):
        raise MarketValidationError("outcomes and clobTokenIds must contain exactly two entries")
    token_values: list[str] = []
    normalized_outcomes: list[str] = []
    for outcome, token in zip(outcomes, tokens):
        if not isinstance(outcome, str) or outcome.strip().lower() not in {"up", "down"}:
            raise MarketValidationError("outcomes must contain only Up and Down")
        if not isinstance(token, str) or not token.strip():
            raise MarketValidationError("token IDs must be non-empty strings")
        normalized_outcomes.append(outcome.strip().lower())
        token_values.append(token.strip())
    if set(normalized_outcomes) != {"up", "down"}:
        raise MarketValidationError("outcomes must contain one Up and one Down")
    if len(set(token_values)) != 2:
        raise MarketValidationError("token IDs must be unique")
    token_by_outcome = dict(zip(normalized_outcomes, token_values))
    return MarketWindow(
        timeframe,
        market_id,
        condition_id,
        expected_slug,
        start,
        end,
        token_by_outcome["up"],
        token_by_outcome["down"],
        source,
    )


class MarketResolver:
    def __init__(self, client: GammaClient) -> None:
        self._client = client

    async def resolve_market(self, timeframe: Timeframe, expected_start_utc: datetime) -> ResolveResult:
        expected_start = ensure_utc(expected_start_utc)
        slug = build_market_slug(timeframe, expected_start)
        try:
            payload = await self._client.get_market_by_slug(slug)
        except (GammaUnavailable, GammaInvalidResponse) as exc:
            return ResolveResult(ResolveOutcome.ERROR, timeframe, expected_start, error=str(exc))
        if payload is None:
            return ResolveResult(ResolveOutcome.NOT_FOUND, timeframe, expected_start)
        try:
            market = validate_market_payload(payload, timeframe, expected_start, slug)
        except (MarketValidationError, ValueError) as exc:
            return ResolveResult(ResolveOutcome.NOT_FOUND, timeframe, expected_start, error=str(exc))
        return ResolveResult(ResolveOutcome.FOUND, timeframe, expected_start, market=market)

    async def resolve_current_market(
        self,
        timeframe: Timeframe,
        now_utc: datetime | None = None,
    ) -> ResolveResult:
        current = datetime.now(UTC) if now_utc is None else ensure_utc(now_utc)
        return await self.resolve_market(timeframe, floor_to_window_start(current, timeframe))
