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
)


def event(sequence: int) -> MarketDataEvent:
    return MarketDataEvent(
        1, sequence, f"event-{sequence}",
        EventSource.CHAINLINK_RTDS, EventStream.CHAINLINK_PRICE, "BTC/USD",
        sequence, sequence, sequence, sequence, str(sequence),
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


if __name__ == "__main__":
    unittest.main()
