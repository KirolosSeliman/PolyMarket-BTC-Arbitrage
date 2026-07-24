import unittest

from polymarket_btc.data_collection.market_data.health import HealthRegistry
from polymarket_btc.data_collection.market_data.models import EventSource


class HealthRegistryTests(unittest.TestCase):
    def test_one_registry_produces_consistent_snapshot_and_health_payload(self) -> None:
        registry = HealthRegistry()
        registry.record_connection(EventSource.POLYMARKET_CLOB, "session-1", 10)
        registry.record_reconnect(EventSource.POLYMARKET_CLOB)
        registry.record_invalid(EventSource.POLYMARKET_CLOB)
        registry.record_duplicate(EventSource.POLYMARKET_CLOB)
        registry.record_stale_session(EventSource.POLYMARKET_CLOB)
        registry.record_divergence(EventSource.POLYMARKET_CLOB)
        registry.record_protocol_error(EventSource.POLYMARKET_CLOB)
        registry.update_runtime(
            gateway_state="running",
            ready=True,
            not_ready_reasons=(),
            raw_events_written=3,
            snapshots_written=2,
        )

        snapshot = registry.source_snapshot(EventSource.POLYMARKET_CLOB, 20)
        payload = registry.to_health_file_payload(20)

        self.assertEqual(snapshot.current_session_id, "session-1")
        self.assertEqual(snapshot.reconnect_count, 1)
        self.assertEqual(snapshot.invalid_count, 1)
        self.assertEqual(snapshot.duplicate_count, 1)
        self.assertEqual(snapshot.stale_session_count, 1)
        self.assertEqual(snapshot.divergence_count, 1)
        self.assertEqual(snapshot.protocol_error_count, 1)
        self.assertEqual(payload["sources"]["polymarket_clob"]["reconnect_count"], 1)
        self.assertTrue(payload["ready"])

    def test_disconnect_immediately_clears_readiness(self) -> None:
        registry = HealthRegistry()
        registry.update_runtime(
            gateway_state="running",
            ready=True,
            not_ready_reasons=(),
        )
        registry.record_connection(EventSource.POLYMARKET_CLOB, "session-1", 10)
        registry.record_disconnection(
            EventSource.POLYMARKET_CLOB, "session-1", "closed"
        )

        self.assertFalse(registry.to_health_file_payload(20)["ready"])


if __name__ == "__main__":
    unittest.main()
