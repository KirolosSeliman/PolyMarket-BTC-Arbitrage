from __future__ import annotations

import copy
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from polymarket_btc.data_collection.market_discovery.config import (  # noqa: E402
    MarketDiscoveryConfig,
    load_market_discovery_config,
)


OBSERVED_AT = datetime(2026, 7, 17, 19, 52, 0, tzinfo=UTC)


def default_config() -> MarketDiscoveryConfig:
    return load_market_discovery_config(Path("config/data_collection/market_discovery.yaml"))


def btc_5m_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": "2951004",
        "question": "Bitcoin Up or Down - July 17, 3:50PM-3:55PM ET",
        "conditionId": "0xece31a80d3b21df6f05b30a27ea9542ee89a66945cdcbecaeaf3ced75307be68",
        "slug": "btc-updown-5m-1784317800",
        "resolutionSource": "https://data.chain.link/streams/btc-usd",
        "startDate": "2026-07-16T19:57:35.875131Z",
        "endDate": "2026-07-17T19:55:00Z",
        "active": True,
        "closed": False,
        "archived": False,
        "enableOrderBook": True,
        "acceptingOrders": True,
        "questionID": "0x058d3259facd2b70783285327813c1c591944095d53f2fc1bdb47f7ce1ccb17a",
        "outcomes": '["Up", "Down"]',
        "clobTokenIds": (
            '["48859776499838806404537537842106177463066316513734253200298308734307495640443", '
            '"61478559042419582453760709973693988619232016626791716178709596040362979422358"]'
        ),
        "eventStartTime": "2026-07-17T19:50:00Z",
        "createdAt": "2026-07-16T19:56:41.278508Z",
        "updatedAt": "2026-07-17T19:45:26.037253Z",
        "events": [
            {
                "id": "711207",
                "slug": "btc-updown-5m-1784317800",
                "title": "Bitcoin Up or Down - July 17, 3:50PM-3:55PM ET",
                "startDate": "2026-07-16T19:57:35.875131Z",
                "endDate": "2026-07-17T19:55:00Z",
                "active": True,
                "closed": False,
            }
        ],
    }
    result = copy.deepcopy(payload)
    result.update(overrides)
    return result


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def payload_for_window(start: datetime, market_id: str) -> dict[str, Any]:
    end = start + timedelta(minutes=5)
    return btc_5m_payload(
        id=market_id,
        slug=f"btc-updown-5m-{int(start.timestamp())}",
        eventStartTime=iso_z(start),
        endDate=iso_z(end),
        conditionId=f"0xcondition{market_id}",
        questionID=f"0xquestion{market_id}",
    )
