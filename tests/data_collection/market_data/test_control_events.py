from decimal import Decimal
import unittest

from polymarket_btc.data_collection.market_discovery import Timeframe
from polymarket_btc.data_collection.market_data.models import (
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    MarketWindowPayload,
    MarketWindowStatePayload,
    ReplayIntegrityError,
    SnapshotTickPayload,
    SourceHealthSnapshot,
    SourceStatusPayload,
    event_from_dict,
)


def _event_dict(stream: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": 2,
        "ingest_sequence": 11,
        "event_id": f"test:{stream}",
        "source": "market_discovery",
        "stream": stream,
        "instrument": "BTC-5m",
        "source_timestamp_ns": 1,
        "server_timestamp_ns": None,
        "received_wall_timestamp_ns": 2,
        "received_monotonic_ns": 3,
        "source_sequence": "4",
        "source_session_id": "session-1",
        "timeframe": "5m",
        "market_id": None,
        "condition_id": None,
        "asset_id": None,
        "outcome": None,
        "payload": payload,
    }


class ControlEventModelTests(unittest.TestCase):
    def test_v1_event_remains_readable_without_source_session_id(self) -> None:
        value = _event_dict(
            "chainlink_price",
            {"symbol": "btc/usd", "price": "67000.25"},
        )
        value["schema_version"] = 1
        value.pop("source_session_id")

        event = event_from_dict(value)

        self.assertIsNone(event.source_session_id)
        self.assertEqual(event.payload, ChainlinkPricePayload("btc/usd", Decimal("67000.25")))

    def test_unknown_event_schema_is_rejected_explicitly(self) -> None:
        value = _event_dict(
            "chainlink_price",
            {"symbol": "btc/usd", "price": "67000.25"},
        )
        value["schema_version"] = 99

        with self.assertRaisesRegex(ReplayIntegrityError, "schema version"):
            event_from_dict(value)

    def test_source_status_payload_round_trips_v2_fields(self) -> None:
        value = _event_dict(
            "source_status",
            {
                "source": "polymarket_clob",
                "connected": True,
                "session_id": "clob-session",
                "reconnect_count": 2,
                "reason": None,
            },
        )

        event = event_from_dict(value)

        self.assertEqual(
            event.payload,
            SourceStatusPayload(
                EventSource.POLYMARKET_CLOB,
                True,
                "clob-session",
                2,
                None,
            ),
        )
        self.assertEqual(event.source_session_id, "session-1")

    def test_snapshot_tick_payload_restores_immutable_health_checkpoint(self) -> None:
        value = _event_dict(
            "snapshot_tick",
            {
                "snapshot_sequence": 8,
                "scheduled_timestamp_ns": 123_000,
                "health": [
                    [
                        "binance_spot",
                        {
                            "connected": True,
                            "stale": False,
                            "current_session_id": "binance-session",
                            "last_message_timestamp_ns": 120_000,
                            "age_ms": 3,
                            "reconnect_count": 1,
                            "invalid_count": 2,
                            "duplicate_count": 3,
                            "stale_session_count": 4,
                            "divergence_count": 5,
                            "protocol_error_count": 6,
                        },
                    ]
                ],
            },
        )

        event = event_from_dict(value)

        self.assertEqual(
            event.payload,
            SnapshotTickPayload(
                8,
                123_000,
                (
                    (
                        EventSource.BINANCE_SPOT,
                        SourceHealthSnapshot(
                            True,
                            False,
                            "binance-session",
                            120_000,
                            3,
                            1,
                            2,
                            3,
                            4,
                            5,
                            6,
                        ),
                    ),
                ),
            ),
        )

    def test_market_window_state_keeps_current_and_next_markets(self) -> None:
        current = {
            "timeframe": "5m",
            "market_id": "market-a",
            "condition_id": "condition-a",
            "slug": "market-a",
            "start_timestamp_ns": 100,
            "end_timestamp_ns": 200,
            "up_token_id": "a-up",
            "down_token_id": "a-down",
            "resolution_source": "chainlink",
        }
        next_market = {
            **current,
            "market_id": "market-b",
            "condition_id": "condition-b",
            "slug": "market-b",
            "start_timestamp_ns": 200,
            "end_timestamp_ns": 300,
            "up_token_id": "b-up",
            "down_token_id": "b-down",
        }
        value = _event_dict(
            "market_window_state",
            {
                "state": "next_ready",
                "timeframe": "5m",
                "current_market": current,
                "next_market": next_market,
                "expected_transition_timestamp_ns": 200,
                "updated_at_timestamp_ns": 150,
                "attempt_count": 1,
                "last_error": None,
            },
        )

        event = event_from_dict(value)

        self.assertEqual(
            event.payload,
            MarketWindowStatePayload(
                "next_ready",
                Timeframe.FIVE_MINUTES,
                MarketWindowPayload(
                    Timeframe.FIVE_MINUTES,
                    "market-a",
                    "condition-a",
                    "market-a",
                    100,
                    200,
                    "a-up",
                    "a-down",
                    "chainlink",
                ),
                MarketWindowPayload(
                    Timeframe.FIVE_MINUTES,
                    "market-b",
                    "condition-b",
                    "market-b",
                    200,
                    300,
                    "b-up",
                    "b-down",
                    "chainlink",
                ),
                200,
                150,
                1,
                None,
            ),
        )

    def test_new_market_data_event_accepts_schema_v2(self) -> None:
        event = MarketDataEvent(
            schema_version=2,
            ingest_sequence=1,
            event_id="test",
            source=EventSource.CHAINLINK_RTDS,
            stream=EventStream.CHAINLINK_PRICE,
            instrument="BTC/USD",
            source_timestamp_ns=1,
            server_timestamp_ns=2,
            received_wall_timestamp_ns=3,
            received_monotonic_ns=4,
            source_sequence="1",
            source_session_id=None,
            timeframe=None,
            market_id=None,
            condition_id=None,
            asset_id=None,
            outcome=None,
            payload=ChainlinkPricePayload("btc/usd", Decimal("1")),
        )

        self.assertEqual(event.schema_version, 2)


if __name__ == "__main__":
    unittest.main()
