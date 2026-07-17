from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Mapping, Sequence

from polymarket_btc.data_collection.common.time import parse_utc_datetime
from polymarket_btc.data_collection.market_discovery.config import MarketDiscoveryConfig
from polymarket_btc.data_collection.market_discovery.models import (
    CandidateValidation,
    MarketDescriptor,
    OutcomeDescriptor,
    RejectedCandidate,
)


SLUG_PATTERN = re.compile(r"^btc-updown-5m-(?P<start_epoch>\d+)$")


def normalize_gamma_market(
    payload: Mapping[str, Any],
    config: MarketDiscoveryConfig,
    observed_at_utc: datetime,
) -> CandidateValidation:
    market_id = _optional_str(payload.get("id"))
    slug = _optional_str(payload.get("slug"))

    def reject(reason: str, message: str) -> CandidateValidation:
        return CandidateValidation(
            rejection=RejectedCandidate(
                reason=reason,
                message=message,
                market_id=market_id,
                slug=slug,
            ),
            raw_payload=payload,
        )

    if not market_id:
        return reject("missing_market_id", "Gamma market is missing id")
    if not slug:
        return reject("missing_slug", "Gamma market is missing slug")

    slug_match = SLUG_PATTERN.fullmatch(slug)
    if slug_match is None:
        return reject("slug_mismatch", "Gamma market slug does not match BTC 5m pattern")

    condition_id = _optional_str(payload.get("conditionId"))
    if config.selection.require_condition_id and not condition_id:
        return reject("missing_condition_id", "Gamma market is missing conditionId")

    if config.selection.reject_closed and payload.get("closed") is True:
        return reject("closed_market", "Gamma market is closed")
    if config.selection.reject_archived and payload.get("archived") is True:
        return reject("archived_market", "Gamma market is archived")
    if payload.get("active") is not True:
        return reject("inactive_market", "Gamma market is not active")
    if config.selection.require_order_book and payload.get("enableOrderBook") is not True:
        return reject("order_book_disabled", "Gamma market does not have enableOrderBook=true")

    resolution_source = _optional_str(payload.get("resolutionSource"))
    if config.target.resolution_source and resolution_source != config.target.resolution_source:
        return reject("resolution_source_mismatch", "Gamma market resolutionSource does not match target")

    try:
        start_time_utc = parse_utc_datetime(payload.get("eventStartTime"), "eventStartTime")
        end_time_utc = parse_utc_datetime(payload.get("endDate"), "endDate")
    except (TypeError, ValueError):
        return reject("invalid_timestamp", "Gamma market has invalid eventStartTime or endDate")

    duration_seconds = int((end_time_utc - start_time_utc).total_seconds())
    if duration_seconds != config.target.duration_seconds:
        return reject("duration_mismatch", "Gamma market duration does not match configured target")
    if duration_seconds <= 0:
        return reject("invalid_time_window", "Gamma market endDate must be after eventStartTime")

    slug_epoch = int(slug_match.group("start_epoch"))
    if slug_epoch != int(start_time_utc.timestamp()):
        return reject("slug_timestamp_mismatch", "Gamma market slug timestamp does not match eventStartTime")

    try:
        outcomes = _parse_string_sequence(payload.get("outcomes"), "outcomes")
    except ValueError as error:
        return reject("invalid_outcomes", str(error))

    try:
        token_ids = _parse_string_sequence(payload.get("clobTokenIds"), "clobTokenIds")
    except ValueError as error:
        reason = "missing_token_ids" if payload.get("clobTokenIds") is None else "invalid_token_ids"
        return reject(reason, str(error))

    if config.selection.require_token_ids and not token_ids:
        return reject("missing_token_ids", "Gamma market is missing clobTokenIds")
    if len(outcomes) != len(token_ids):
        return reject("outcome_token_count_mismatch", "outcomes and clobTokenIds lengths differ")

    mapped_by_name: dict[str, OutcomeDescriptor] = {}
    normalized_names: set[str] = set()
    seen_tokens: set[str] = set()
    expected = set(config.target.expected_outcomes)
    for source_name, token_id in zip(outcomes, token_ids, strict=True):
        normalized_name = _normalize_outcome_name(source_name)
        if normalized_name not in expected:
            return reject("unknown_outcome", f"unexpected outcome: {source_name}")
        if normalized_name in normalized_names:
            return reject("duplicate_outcome", f"duplicate outcome: {normalized_name}")
        if not token_id.strip():
            return reject("missing_token_ids", "token ID must not be empty")
        if token_id in seen_tokens:
            return reject("duplicate_token_id", "token IDs must be unique")
        normalized_names.add(normalized_name)
        seen_tokens.add(token_id)
        mapped_by_name[normalized_name] = (
            OutcomeDescriptor(
                normalized_name=normalized_name,
                source_name=source_name,
                token_id=token_id,
            )
        )

    if normalized_names != expected:
        return reject("missing_expected_outcome", "market does not contain exactly Up and Down outcomes")
    mapped_outcomes = tuple(mapped_by_name[name] for name in config.target.expected_outcomes)

    event_id = _extract_event_id(payload.get("events"))
    question_id = _optional_str(payload.get("questionID"))
    question = _optional_str(payload.get("question")) or ""
    description = _optional_str(payload.get("description"))

    return CandidateValidation(
        market=MarketDescriptor(
            provider=config.provider.name,
            event_id=event_id,
            market_id=market_id,
            condition_id=condition_id or "",
            question_id=question_id,
            slug=slug,
            question=question,
            description=description,
            resolution_source=resolution_source,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            duration_seconds=duration_seconds,
            active=payload.get("active") is True,
            closed=payload.get("closed") is True,
            archived=payload.get("archived") is True,
            enable_order_book=payload.get("enableOrderBook") is True,
            outcomes=mapped_outcomes,
            discovered_at_utc=observed_at_utc,
        ),
        raw_payload=payload,
    )


def _parse_string_sequence(value: object, field_name: str) -> tuple[str, ...]:
    if value is None:
        raise ValueError(f"{field_name} is required")

    decoded: object
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(f"{field_name} must be a JSON array string") from error
    else:
        decoded = value

    if not isinstance(decoded, Sequence) or isinstance(decoded, (str, bytes, bytearray)):
        raise ValueError(f"{field_name} must be an array")
    if not all(isinstance(item, str) for item in decoded):
        raise ValueError(f"{field_name} must contain strings only")
    return tuple(item.strip() for item in decoded)


def _normalize_outcome_name(value: str) -> str:
    lowered = value.strip().lower()
    if lowered == "up":
        return "Up"
    if lowered == "down":
        return "Down"
    return value.strip()


def _extract_event_id(events: object) -> str | None:
    if isinstance(events, Mapping):
        return _optional_str(events.get("id"))
    if isinstance(events, Sequence) and not isinstance(events, (str, bytes, bytearray)):
        if not events:
            return None
        first = events[0]
        if isinstance(first, Mapping):
            return _optional_str(first.get("id"))
    return None


def _optional_str(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None
