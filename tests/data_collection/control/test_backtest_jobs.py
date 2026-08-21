from datetime import UTC, datetime
from decimal import Decimal
import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest

from polymarket_btc.data_collection.control.backtest_jobs import BacktestJobManager
from polymarket_btc.data_collection.control.runs import CollectionRunManager
from polymarket_btc.data_collection.market_data.models import (
    BinanceAggTradePayload,
    EventSource,
    EventStream,
    MarketDataEvent,
    TakerSide,
)
from polymarket_btc.data_collection.market_data.storage import RawEventStorage

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

_CONCEPT = '''
CONCEPT_INFO = {
    "label": "Last price", "description": "d",
    "data_sources": ["binance_futures_trade"],
}

def compute(context):
    trades = context.data.get("binance_futures_trade") or []
    return {"last_price": trades[-1]["price"] if trades else None}
'''

_MICROSYSTEM = '''
MICROSYSTEM_INFO = {
    "label": "Passthrough", "description": "d",
    "concept_inputs": ["last_price_concept"],
}

def compute(context):
    result = context.concepts.get("last_price_concept") or {}
    return {"last_price": result.get("last_price")}
'''

_EXECUTION = '''
EXECUTION_INFO = {"label": "Enter once", "description": "d"}

def execute(context):
    return {"direction": "long"}
'''


def _seeded_symbol_cache(root: Path) -> Path:
    cache_path = root / "cache" / "binance_symbols.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({
        "spot": ["BTC"], "futures": ["BTC"], "fetched_at_utc": datetime.now(UTC).isoformat(),
    }))
    return cache_path


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


class BacktestJobManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_then_poll_reaches_done_with_correct_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "concepts").mkdir()
            (root / "concepts" / "last_price_concept.py").write_text(_CONCEPT, encoding="utf-8")
            (root / "microsystems").mkdir()
            (root / "microsystems" / "passthrough.py").write_text(_MICROSYSTEM, encoding="utf-8")
            (root / "execution_profiles").mkdir()
            (root / "execution_profiles" / "exec.py").write_text(_EXECUTION, encoding="utf-8")
            (root / "management_profiles").mkdir()

            run_dir = root / "collections" / "run1"
            run_dir.mkdir(parents=True)
            _write_trade_events(run_dir, [(0, 100), (9, 105)])
            manifest = {
                "mode": "access", "sources": ["binance_futures_trade"], "plugins": [], "data_dir": str(run_dir),
            }
            manager = CollectionRunManager(
                config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
                collections_dir=root / "collections", symbol_cache_path=_seeded_symbol_cache(root),
                plugins_dir=root / "plugins", concepts_dir=root / "concepts",
                microsystems_dir=root / "microsystems", execution_dir=root / "execution_profiles",
                management_dir=root / "management_profiles",
            )
            strategy = {
                "concepts": [{"instance_id": "concept_1", "concept_id": "last_price_concept", "config": {}, "data_bindings": {}}],
                "microsystems": [{
                    "instance_id": "micro_1", "microsystem_id": "passthrough",
                    "concept_instance_ids": ["concept_1"], "config": {}, "data_bindings": {},
                }],
                "execution": {"execution_id": "exec", "config": {}},
                "management": None,
            }

            jobs = BacktestJobManager()
            job = jobs.start(
                strategy=strategy, concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[manifest], instrument="BTC", start_ts=0, end_ts=10, cadence_seconds=10,
                execution_sweep={}, management_sweep={},
            )
            self.assertFalse(jobs.status(job.job_id)["done"])
            self.assertEqual(jobs.status(job.job_id)["progress"], 0.0)

            for _ in range(100):
                status = jobs.status(job.job_id)
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(status["done"])
            self.assertEqual(status["progress"], 1.0)
            self.assertIsNone(status["error"])
            self.assertEqual(status["result"]["best"]["trades"], 1)
            self.assertEqual(status["result"]["best"]["wins"], 1)

    async def test_unknown_job_id_returns_none(self) -> None:
        jobs = BacktestJobManager()
        self.assertIsNone(jobs.status("nope"))

    async def test_failure_is_reported_not_raised(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "concepts").mkdir()
            (root / "microsystems").mkdir()
            (root / "execution_profiles").mkdir()
            (root / "management_profiles").mkdir()
            manager = CollectionRunManager(
                config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
                collections_dir=root / "collections", symbol_cache_path=_seeded_symbol_cache(root),
                plugins_dir=root / "plugins", concepts_dir=root / "concepts",
                microsystems_dir=root / "microsystems", execution_dir=root / "execution_profiles",
                management_dir=root / "management_profiles",
            )
            jobs = BacktestJobManager()
            job = jobs.start(
                strategy={"concepts": [], "microsystems": [], "execution": None, "management": None},
                concepts_dir=root / "concepts", microsystems_dir=root / "microsystems",
                execution_dir=root / "execution_profiles", management_dir=root / "management_profiles",
                data_requirements_for=manager._data_requirements_for,
                manifests=[], instrument="BTC", start_ts=0, end_ts=10, cadence_seconds=10,
                execution_sweep={}, management_sweep={},
            )
            for _ in range(100):
                status = jobs.status(job.job_id)
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(status["done"])
            self.assertEqual(status["progress"], 1.0)
            self.assertIsNotNone(status["error"])
            self.assertIsNone(status["result"])


if __name__ == "__main__":
    unittest.main()
