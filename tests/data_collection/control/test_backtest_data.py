from decimal import Decimal
from pathlib import Path
import tempfile
import time
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_btc.data_collection.control.backtest_data import combined_coverage, key_coverage, narrowest_key, read_records
from polymarket_btc.data_collection.market_data.health import HealthRegistry
from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    SnapshotTickPayload,
    TakerSide,
)
from polymarket_btc.data_collection.market_data.reducer import MarketDataReducer
from polymarket_btc.data_collection.market_data.state import StateStore
from polymarket_btc.data_collection.market_data.storage import ParquetSnapshotWriter, RawEventStorage


class KeyCoverageTests(unittest.TestCase):
    def test_uses_per_key_source_coverage_when_present(self) -> None:
        manifests = [{
            "sources": ["binance_futures_kline", "binance_futures_kline:ETH"], "plugins": [],
            "start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-19T00:00:00",
            "source_coverage": {
                "binance_futures_kline": {"start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-19T00:00:00"},
                "binance_futures_kline:ETH": {"start_ts_utc": "2026-08-18T00:00:00", "end_ts_utc": "2026-08-19T00:00:00"},
            },
        }]
        self.assertEqual(
            key_coverage("binance_futures_kline", manifests),
            [("2026-08-01T00:00:00", "2026-08-19T00:00:00")],
        )
        self.assertEqual(
            key_coverage("binance_futures_kline:ETH", manifests),
            [("2026-08-18T00:00:00", "2026-08-19T00:00:00")],
        )

    def test_falls_back_to_manifest_level_range_when_source_coverage_absent(self) -> None:
        """A manifest written before export_run computed per-key coverage --
        still correct for the (common) single-source-per-run case."""
        manifests = [{
            "sources": ["chainlink"], "plugins": [],
            "start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-02T00:00:00",
        }]
        self.assertEqual(key_coverage("chainlink", manifests), [("2026-08-01T00:00:00", "2026-08-02T00:00:00")])

    def test_unions_across_multiple_manifests_for_the_same_key(self) -> None:
        manifests = [
            {
                "sources": ["chainlink"], "plugins": [],
                "start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-02T00:00:00",
            },
            {
                "sources": ["chainlink"], "plugins": [],
                "start_ts_utc": "2026-08-02T00:00:00", "end_ts_utc": "2026-08-03T00:00:00",
            },
        ]
        self.assertEqual(key_coverage("chainlink", manifests), [("2026-08-01T00:00:00", "2026-08-03T00:00:00")])

    def test_key_never_collected_yields_no_coverage(self) -> None:
        self.assertEqual(key_coverage("binance_futures_trade", [{"sources": ["chainlink"], "plugins": []}]), [])


class CombinedCoverageTests(unittest.TestCase):
    def test_intersects_two_keys_with_different_ranges_down_to_their_overlap(self) -> None:
        """The exact reported bug scenario: one required key covers 1-19
        août, another only 18-19 -- the combined, honest answer is 18-19,
        not the wider 1-19 a run-level range would wrongly suggest."""
        manifests = [{
            "sources": ["binance_futures_kline", "binance_futures_kline:ETH"], "plugins": [],
            "start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-19T00:00:00",
            "source_coverage": {
                "binance_futures_kline": {"start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-19T00:00:00"},
                "binance_futures_kline:ETH": {"start_ts_utc": "2026-08-18T00:00:00", "end_ts_utc": "2026-08-19T00:00:00"},
            },
        }]
        self.assertEqual(
            combined_coverage({"binance_futures_kline", "binance_futures_kline:ETH"}, manifests),
            [("2026-08-18T00:00:00", "2026-08-19T00:00:00")],
        )

    def test_any_required_key_with_no_coverage_makes_the_whole_thing_empty(self) -> None:
        manifests = [{
            "sources": ["chainlink"], "plugins": [],
            "start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-19T00:00:00",
        }]
        self.assertEqual(combined_coverage({"chainlink", "binance_futures_trade"}, manifests), [])

    def test_empty_required_keys_yields_no_coverage(self) -> None:
        self.assertEqual(combined_coverage(set(), [{"sources": ["x"], "plugins": []}]), [])

    def test_non_overlapping_ranges_intersect_to_nothing(self) -> None:
        manifests = [
            {
                "sources": ["a"], "plugins": [],
                "start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-02T00:00:00",
            },
            {
                "sources": ["b"], "plugins": [],
                "start_ts_utc": "2026-08-05T00:00:00", "end_ts_utc": "2026-08-06T00:00:00",
            },
        ]
        self.assertEqual(combined_coverage({"a", "b"}, manifests), [])


class NarrowestKeyTests(unittest.TestCase):
    def test_picks_the_key_with_the_smallest_total_span(self) -> None:
        manifests = [{
            "sources": ["binance_futures_kline", "binance_futures_kline:ETH"], "plugins": [],
            "start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-19T00:00:00",
            "source_coverage": {
                "binance_futures_kline": {"start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-19T00:00:00"},
                "binance_futures_kline:ETH": {"start_ts_utc": "2026-08-18T00:00:00", "end_ts_utc": "2026-08-19T00:00:00"},
            },
        }]
        self.assertEqual(
            narrowest_key({"binance_futures_kline", "binance_futures_kline:ETH"}, manifests),
            "binance_futures_kline:ETH",
        )

    def test_a_key_with_zero_coverage_is_the_narrowest_by_definition(self) -> None:
        manifests = [{
            "sources": ["chainlink"], "plugins": [],
            "start_ts_utc": "2026-08-01T00:00:00", "end_ts_utc": "2026-08-19T00:00:00",
        }]
        self.assertEqual(narrowest_key({"chainlink", "binance_futures_trade"}, manifests), "binance_futures_trade")

    def test_empty_required_keys_yields_none(self) -> None:
        self.assertIsNone(narrowest_key(set(), [{"sources": ["x"], "plugins": []}]))


def _write_trade_events(run_dir: Path, trade_points: list[tuple[float, float]]) -> None:
    storage = RawEventStorage(run_dir, zstd_level=3)
    for i, (t_seconds, price) in enumerate(trade_points):
        ts_ns = int(t_seconds * 1e9)
        payload = BinanceAggTradePayload(
            symbol="BTCUSDT", aggregate_trade_id=i, price=Decimal(str(price)), quantity=Decimal("0.1"),
            first_trade_id=i, last_trade_id=i, trade_timestamp_ns=ts_ns, taker_side=TakerSide.BUY,
        )
        event = MarketDataEvent(
            schema_version=2, ingest_sequence=i, event_id=f"evt-{i}",
            source=EventSource.BINANCE_FUTURES_TRADE, stream=EventStream.BINANCE_FUTURES_AGG_TRADE,
            instrument="BTCUSDT", source_timestamp_ns=ts_ns, server_timestamp_ns=ts_ns,
            received_wall_timestamp_ns=ts_ns, received_monotonic_ns=time.monotonic_ns(),
            source_sequence=None, timeframe=None, market_id=None, condition_id=None,
            asset_id=None, outcome=None, payload=payload,
        )
        storage.write(event)
    storage.close()


class ReadRecordsAccessModeTests(unittest.TestCase):
    def test_reads_ordered_time_filtered_trade_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run1"
            run_dir.mkdir()
            _write_trade_events(run_dir, [(-1, 100), (9, 150), (19, 90)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [], "data_dir": str(run_dir),
            }
            records = read_records("binance_futures_trade", 0, 20_000_000_000, [manifest], instrument="BTCUSDT")
            self.assertEqual([r["price"] for r in records], [150.0, 90.0])
            self.assertEqual([r["timestamp"] for r in records], [9.0, 19.0])

    def test_wrong_instrument_yields_no_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run1"
            run_dir.mkdir()
            _write_trade_events(run_dir, [(0, 100)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [], "data_dir": str(run_dir),
            }
            records = read_records("binance_futures_trade", 0, 10_000_000_000, [manifest], instrument="ETHUSDT")
            self.assertEqual(records, [])

    def test_unknown_key_yields_no_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory) / "run1"
            run_dir.mkdir()
            manifest = {"mode": "access", "sources": ["my_plugin"], "plugins": ["my_plugin"], "data_dir": str(run_dir)}
            records = read_records("my_plugin", 0, 10_000_000_000, [manifest], instrument="BTCUSDT")
            self.assertEqual(records, [])


class ReadRecordsCollectModeTests(unittest.TestCase):
    def _build_dataset(self, directory: Path) -> None:
        registry = HealthRegistry()
        state = StateStore(health_registry=registry)
        reducer = MarketDataReducer(state)
        price_event = MarketDataEvent(
            2, 1, "chainlink-1", EventSource.CHAINLINK_RTDS,
            EventStream.CHAINLINK_PRICE, "BTC/USD", 1_000_000_000, 1_000_000_000, 1_000_000_000, 1, "1",
            None, None, None, None, None,
            ChainlinkPricePayload("btc/usd", Decimal("67000.25")),
        )
        reducer.apply(price_event)
        health = registry.all_source_snapshots(1_000_000_000)
        tick_event = MarketDataEvent(
            2, 2, "tick-1", EventSource.MARKET_DISCOVERY,
            EventStream.SNAPSHOT_TICK, "gateway", 1_000_000_000, None, 1_000_000_000, 2, "1",
            None, None, None, None, None,
            SnapshotTickPayload(1, 1_000_000_000, health),
        )
        snapshot = reducer.apply(tick_event)
        writer = ParquetSnapshotWriter(directory, zstd_level=3)
        writer.write(snapshot)
        writer.close()
        parts = sorted(directory.glob("snapshots/**/*.parquet"))
        combined = pa.concat_tables([pq.read_table(p) for p in parts])
        pq.write_table(combined, directory / "dataset.parquet", compression="zstd")

    def test_reads_chainlink_price_from_dataset_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            self._build_dataset(run_dir)
            manifest = {"mode": "collect", "sources": ["chainlink"], "plugins": [], "data_dir": str(run_dir)}
            records = read_records("chainlink", 0, 2_000_000_000, [manifest], instrument="BTCUSDT")
            self.assertEqual(records, [{"price": 67000.25, "timestamp": 1.0}])

    def test_no_dataset_file_yields_no_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = {"mode": "collect", "sources": ["chainlink"], "plugins": [], "data_dir": directory}
            records = read_records("chainlink", 0, 2_000_000_000, [manifest], instrument="BTCUSDT")
            self.assertEqual(records, [])


if __name__ == "__main__":
    unittest.main()
