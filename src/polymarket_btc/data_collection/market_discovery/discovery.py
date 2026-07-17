from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from polymarket_btc.data_collection.common.time import parse_utc_datetime, utc_now
from polymarket_btc.data_collection.market_discovery.config import MarketDiscoveryConfig, default_config
from polymarket_btc.data_collection.market_discovery.gamma_client import (
    GammaClient,
    GammaClientError,
)
from polymarket_btc.data_collection.market_discovery.models import (
    DiscoveredMarket,
    DiscoveryResult,
    DiscoveryStatus,
)


BTC_5M_SLUG_PREFIX = "btc-updown-5m"
EXPECTED_DURATION = timedelta(minutes=5)
EXPECTED_RESOLUTION_SOURCE = "https://data.chain.link/streams/btc-usd"
EXPECTED_OUTCOMES = ("Up", "Down")
SLUG_RE = re.compile(r"^btc-updown-5m-(?P<start_epoch>\d+)$")


def floor_to_five_minute_start(now_utc: datetime) -> datetime:
    now = _require_aware_utc(now_utc)
    epoch = int(now.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % 300), tz=UTC)


def build_btc_5m_slug(start_time_utc: datetime) -> str:
    start = _require_aware_utc(start_time_utc)
    return f"{BTC_5M_SLUG_PREFIX}-{int(start.timestamp())}"


def discover_current_market(
    now_utc: datetime | None = None,
    *,
    config: MarketDiscoveryConfig | None = None,
    gamma_client: Any | None = None,
) -> DiscoveryResult:
    observed_at = _require_aware_utc(now_utc or utc_now())
    expected_start = floor_to_five_minute_start(observed_at)
    expected_slug = build_btc_5m_slug(expected_start)
    resolved_config = config or default_config()
    client = gamma_client or GammaClient(resolved_config)

    try:
        payload = client.get_market_by_slug(expected_slug)
    except GammaClientError as error:
        return DiscoveryResult(
            DiscoveryStatus.PROVIDER_UNAVAILABLE,
            reason=str(error) or error.__class__.__name__,
        )

    if payload is None:
        return DiscoveryResult(DiscoveryStatus.NO_MATCH, reason="no_market_at_expected_slug")

    if not isinstance(payload, Mapping):
        return DiscoveryResult(
            DiscoveryStatus.PROVIDER_UNAVAILABLE,
            reason="Gamma market-by-slug response must be an object",
        )

    validation = _validate_payload(payload, expected_slug, observed_at)
    if validation.market is None:
        return DiscoveryResult(DiscoveryStatus.NO_MATCH, reason=validation.reason)
    return DiscoveryResult(DiscoveryStatus.SELECTED, market=validation.market)


@dataclass(frozen=True)
class _Validation:
    market: DiscoveredMarket | None = None
    reason: str | None = None


def _validate_payload(
    payload: Mapping[str, Any],
    expected_slug: str,
    observed_at: datetime,
) -> _Validation:
    market_id = _required_text(payload.get("id"))
    if market_id is None:
        return _Validation(reason="missing_market_id")

    condition_id = _required_text(payload.get("conditionId"))
    if condition_id is None:
        return _Validation(reason="missing_condition_id")

    slug = _required_text(payload.get("slug"))
    if slug != expected_slug:
        return _Validation(reason="slug_mismatch")

    if payload.get("active") is not True:
        return _Validation(reason="inactive_market")
    if payload.get("closed") is True:
        return _Validation(reason="closed_market")
    if payload.get("archived") is True:
        return _Validation(reason="archived_market")
    if payload.get("enableOrderBook") is not True:
        return _Validation(reason="order_book_disabled")

    try:
        start_time = parse_utc_datetime(payload.get("eventStartTime"), "eventStartTime")
        end_time = parse_utc_datetime(payload.get("endDate"), "endDate")
    except (TypeError, ValueError):
        return _Validation(reason="invalid_timestamp")

    slug_match = SLUG_RE.match(slug)
    if slug_match is None:
        return _Validation(reason="slug_mismatch")
    if int(slug_match.group("start_epoch")) != int(start_time.timestamp()):
        return _Validation(reason="slug_start_mismatch")

    if end_time - start_time != EXPECTED_DURATION:
        return _Validation(reason="wrong_duration")

    if not (start_time <= observed_at < end_time):
        return _Validation(reason="not_current_window")

    if not _same_resolution_source(payload.get("resolutionSource")):
        return _Validation(reason="wrong_resolution_source")

    token_mapping = _extract_token_mapping(payload)
    if isinstance(token_mapping, str):
        return _Validation(reason=token_mapping)

    return _Validation(
        market=DiscoveredMarket(
            market_id=market_id,
            condition_id=condition_id,
            slug=slug,
            start_time_utc=start_time,
            end_time_utc=end_time,
            up_token_id=token_mapping["Up"],
            down_token_id=token_mapping["Down"],
            resolution_source=EXPECTED_RESOLUTION_SOURCE,
        )
    )


def _extract_token_mapping(payload: Mapping[str, Any]) -> dict[str, str] | str:
    outcomes = _parse_collection(payload.get("outcomes"), missing_reason="missing_outcomes")
    if isinstance(outcomes, str):
        return outcomes

    token_ids = _parse_collection(payload.get("clobTokenIds"), missing_reason="missing_token_ids")
    if isinstance(token_ids, str):
        return token_ids

    if len(outcomes) != len(token_ids):
        return "outcome_token_count_mismatch"

    mapped: dict[str, str] = {}
    seen_tokens: set[str] = set()
    for raw_outcome, raw_token_id in zip(outcomes, token_ids, strict=True):
        outcome = _normalize_outcome(raw_outcome)
        if outcome is None:
            return "unknown_outcome"
        if outcome in mapped:
            return "duplicate_outcome"

        if not isinstance(raw_token_id, str):
            return "invalid_token_id"
        token_id = raw_token_id.strip()
        if not token_id:
            return "empty_token_id"
        if token_id in seen_tokens:
            return "duplicate_token_id"

        mapped[outcome] = token_id
        seen_tokens.add(token_id)

    if set(mapped) != set(EXPECTED_OUTCOMES):
        return "missing_expected_outcome"

    return mapped


def _parse_collection(value: object, *, missing_reason: str) -> list[object] | str:
    if value is None:
        return missing_reason
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return "invalid_json_collection"
        if not isinstance(decoded, list):
            return "invalid_json_collection"
        return decoded
    if isinstance(value, list):
        return value
    return "invalid_json_collection"


def _normalize_outcome(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if normalized == "up":
        return "Up"
    if normalized == "down":
        return "Down"
    return None


def _same_resolution_source(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    return _normalize_url(value) == _normalize_url(EXPECTED_RESOLUTION_SOURCE)


def _normalize_url(value: str) -> str:
    parsed = urlparse(value.strip())
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"


def _required_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _require_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)
