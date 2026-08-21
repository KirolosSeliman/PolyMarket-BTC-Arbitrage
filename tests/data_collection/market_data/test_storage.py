from decimal import Decimal
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow.parquet as pq
import zstandard

from polymarket_btc.data_collection.market_data.models import (
    ChainlinkPricePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
)
from polymarket_btc.data_collection.market_data.state import StateStore
from polymarket_btc.data_collection.market_data.storage import (
    ParquetSnapshotWriter,
    RawEventStorage,
    recover_partial_files,
    snapshot_from_parquet_row,
    snapshot_to_parquet_row,
)


def event(sequence: int) -> MarketDataEvent:
    return MarketDataEvent(
        1, sequence, f"event-{sequence}",
        EventSource.CHAINLINK_RTDS, EventStream.CHAINLINK_PRICE, "BTC/USD",
        sequence, sequence, sequence, sequence, str(sequence),
        None, None, None, None, None,
        ChainlinkPricePayload("btc/usd", Decimal("67234.5000")),
    )


def event_with_distinct_timestamps(sequence: int, *, source_ns: int, received_ns: int) -> MarketDataEvent:
    """Unlike event() above (which sets every timestamp field to the same
    value), a real access-mode historical fetch has these genuinely differ
    -- source_ns is the event's own real-world moment (e.g. a kline's
    close_time), received_ns is when the local fetcher actually wrote it,
    which for a bulk historical fetch can be months later than source_ns."""
    return MarketDataEvent(
        1, sequence, f"event-{sequence}",
        EventSource.CHAINLINK_RTDS, EventStream.CHAINLINK_PRICE, "BTC/USD",
        source_ns, source_ns, received_ns, received_ns, str(sequence),
        None, None, None, None, None,
        ChainlinkPricePayload("btc/usd", Decimal("67234.5000")),
    )


class RawStorageTests(unittest.TestCase):
    def test_jsonl_zstd_manifest_and_decimal_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            storage.write(event(1))
            storage.write(event(2))
            manifests = storage.close()
            self.assertEqual(len(manifests), 1)
            manifest_path = manifests[0]
            manifest = json.loads(manifest_path.read_text())
            compressed = manifest_path.with_name(
                Path(manifest["relative_path"]).name
            )
            payload = zstandard.ZstdDecompressor().decompress(compressed.read_bytes())
            rows = [json.loads(line) for line in payload.splitlines()]
            self.assertEqual(rows[0]["payload"]["price"], "67234.5000")
            self.assertEqual(manifest["event_count"], 2)
            self.assertEqual(
                hashlib.sha256(compressed.read_bytes()).hexdigest(),
                manifest["sha256"],
            )

    def test_manifest_tracks_source_timestamps_separately_from_receipt_time(self) -> None:
        """The exact bug this fixes: an access-mode bulk historical fetch
        writes events *today* whose own source_timestamp_ns is from months
        ago -- the manifest must record both ranges distinctly, not only
        received_timestamp_ns (which used to be the only field, making
        runs.py's per-key source_coverage report roughly "now" instead of
        the actual historical period a collection covers)."""
        a_year_ago_ns = 1_700_000_000_000_000_000
        now_ns = 1_730_000_000_000_000_000
        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            storage.write(event_with_distinct_timestamps(1, source_ns=a_year_ago_ns, received_ns=now_ns))
            storage.write(event_with_distinct_timestamps(2, source_ns=a_year_ago_ns + 60, received_ns=now_ns + 1))
            manifests = storage.close()
            manifest = json.loads(manifests[0].read_text())
            self.assertEqual(manifest["first_source_timestamp_ns"], a_year_ago_ns)
            self.assertEqual(manifest["last_source_timestamp_ns"], a_year_ago_ns + 60)
            self.assertEqual(manifest["first_received_timestamp_ns"], now_ns)
            self.assertEqual(manifest["last_received_timestamp_ns"], now_ns + 1)

    def test_source_timestamp_tracking_uses_min_max_not_arrival_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            # written out of source-time order -- min/max must still be correct
            storage.write(event_with_distinct_timestamps(1, source_ns=500, received_ns=1))
            storage.write(event_with_distinct_timestamps(2, source_ns=100, received_ns=2))
            storage.write(event_with_distinct_timestamps(3, source_ns=300, received_ns=3))
            manifests = storage.close()
            manifest = json.loads(manifests[0].read_text())
            self.assertEqual(manifest["first_source_timestamp_ns"], 100)
            self.assertEqual(manifest["last_source_timestamp_ns"], 500)

    def test_a_segments_first_event_may_have_ingest_sequence_zero(self) -> None:
        # `segment.first_sequence or event.ingest_sequence` used to be the
        # write() logic -- falsy for ingest_sequence == 0, so it silently
        # kept overwriting first_sequence on every later write instead of
        # staying pinned at 0, and close()'s validation-vs-tracked-metadata
        # check would then reject the segment outright. Only ever latent in
        # the live gateway path (its counter starts at 1), but a zero-based
        # counter (e.g. a fresh itertools.count() per access-mode run) hits
        # it on the very first event of whichever segment happens first.
        with tempfile.TemporaryDirectory() as directory:
            storage = RawEventStorage(Path(directory), zstd_level=3)
            storage.write(event(0))
            storage.write(event(1))
            storage.write(event(2))
            manifests = storage.close()
            manifest = json.loads(manifests[0].read_text())
            self.assertEqual(manifest["first_ingest_sequence"], 0)
            self.assertEqual(manifest["last_ingest_sequence"], 2)
            self.assertEqual(manifest["event_count"], 3)

    def test_recovery_truncates_partial_line_and_finalizes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw" / "date=2026-01-01" / "hour=00"
            path = path / "source=chainlink_rtds" / "stream=chainlink_price"
            path.mkdir(parents=True)
            partial = path / "part-1-test.jsonl.partial"
            from polymarket_btc.data_collection.market_data.models import json_dumps
            partial.write_bytes((json_dumps(event(1)) + "\n{\"broken\":").encode())
            manifests = recover_partial_files(Path(directory), zstd_level=3)
            self.assertEqual(len(manifests), 1)
            self.assertFalse(partial.exists())

    def test_recovery_restores_orphan_compressed_segment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            storage = RawEventStorage(root, zstd_level=3)
            storage.write(event(1))
            manifest = storage.close()[0]
            compressed = manifest.parent / json.loads(manifest.read_text())["relative_path"]
            partial = compressed.with_suffix(compressed.suffix + ".partial")
            compressed.replace(partial)
            manifest.unlink()
            recovered = recover_partial_files(root, zstd_level=3)
            self.assertTrue(recovered)
            self.assertTrue(compressed.exists())
            self.assertTrue(compressed.with_name(compressed.name.replace(".jsonl.zst", ".manifest.json")).exists())

    def test_recovery_restores_orphan_parquet_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = StateStore().snapshot(1_750_000_000_000_000_000, 1)
            writer = ParquetSnapshotWriter(root, zstd_level=3)
            manifest = writer.write(snapshot)
            manifests = writer.close()
            manifest = manifests[0]
            parquet = manifest.with_name(manifest.name.replace(".manifest.json", ".parquet"))
            partial = parquet.with_suffix(parquet.suffix + ".partial")
            parquet.replace(partial)
            manifest.unlink()
            recovered = recover_partial_files(root, zstd_level=3)
            self.assertTrue(recovered)
            self.assertTrue(parquet.exists())
            self.assertTrue(manifest.exists())


class ParquetStorageTests(unittest.TestCase):
    def test_parquet_has_fixed_structured_depth_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            snapshot = StateStore().snapshot(1_750_000_000_000_000_000, 1)
            writer = ParquetSnapshotWriter(Path(directory), zstd_level=3)
            writer.write(snapshot)
            manifest_path = writer.close()[0]
            manifest = json.loads(manifest_path.read_text())
            parquet_path = manifest_path.with_name(
                manifest_path.name.replace(".manifest.json", ".parquet")
            )
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, 1)
            self.assertIn("binance_depth_bids", table.schema.names)
            self.assertEqual(manifest["row_count"], 1)

    def test_parquet_round_trip_restores_full_snapshot_contract(self) -> None:
        snapshot = StateStore().snapshot(1_750_000_000_000_000_000, 1)
        restored = snapshot_from_parquet_row(snapshot_to_parquet_row(snapshot))
        self.assertEqual(restored, snapshot)


if __name__ == "__main__":
    unittest.main()
