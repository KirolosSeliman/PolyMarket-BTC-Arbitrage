from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def btc_5m_payload(
    *,
    start: datetime | None = None,
    market_id: str = "2951004",
    outcomes: object = '["Up", "Down"]',
    token_ids: object = (
        '["48859776499838806404537537842106177463066316513734253200298308734307495640443", '
        '"61478559042419582453760709973693988619232016626791716178709596040362979422358"]'
    ),
    **overrides: Any,
) -> dict[str, Any]:
    start = start or datetime(2026, 7, 17, 19, 50, tzinfo=UTC)
    end = start + timedelta(minutes=5)
    payload: dict[str, Any] = {
        "id": market_id,
        "question": "Bitcoin Up or Down - July 17, 3:50PM-3:55PM ET",
        "conditionId": "0xece31a80d3b21df6f05b30a27ea9542ee89a66945cdcbecaeaf3ced75307be68",
        "slug": f"btc-updown-5m-{int(start.timestamp())}",
        "resolutionSource": "https://data.chain.link/streams/btc-usd",
        "endDate": iso_z(end),
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "outcomes": outcomes,
        "clobTokenIds": token_ids,
        "eventStartTime": iso_z(start),
    }
    payload.update(overrides)
    return payload
