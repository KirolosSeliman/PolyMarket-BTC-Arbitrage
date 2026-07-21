from datetime import UTC, datetime, timedelta

from polymarket_btc.data_collection.market_discovery.models import MarketWindow, Timeframe
from polymarket_btc.data_collection.market_discovery.resolver import build_market_slug


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def market_payload(
    timeframe: Timeframe,
    start_time_utc: datetime,
    *,
    market_id: str = "market-1",
    outcomes: object = '["Up", "Down"]',
    token_ids: object = '["token-up", "token-down"]',
    **overrides: object,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": market_id,
        "conditionId": "condition-1",
        "slug": build_market_slug(timeframe, start_time_utc),
        "eventStartTime": iso_z(start_time_utc),
        "endDate": iso_z(start_time_utc + timedelta(seconds=timeframe.duration_seconds)),
        "resolutionSource": "https://data.chain.link/streams/btc-usd",
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "outcomes": outcomes,
        "clobTokenIds": token_ids,
    }
    payload.update(overrides)
    return payload


def market(
    timeframe: Timeframe = Timeframe.FIVE_MINUTES,
    start: datetime = datetime(2026, 7, 21, 10, 0, tzinfo=UTC),
    *,
    market_id: str = "market-1",
) -> MarketWindow:
    return MarketWindow(
        timeframe=timeframe,
        market_id=market_id,
        condition_id="condition-1",
        slug=build_market_slug(timeframe, start),
        start_time_utc=start,
        end_time_utc=start + timedelta(seconds=timeframe.duration_seconds),
        up_token_id="token-up",
        down_token_id="token-down",
        resolution_source="https://data.chain.link/streams/btc-usd",
    )
