# polymarket-btc

Read-only discovery and durable collection of public BTC market data from
Polymarket and Binance.

## Installation

Python 3.11 or 3.13 is supported.

```shell
python -m pip install -e .
python -m polymarket_btc.data_collection.market_data.cli validate-config --config config/market_data.toml
```

## Market Discovery

```shell
python -m polymarket_btc.data_collection.market_discovery.cli current
python -m polymarket_btc.data_collection.market_discovery.cli run
```

See [Market Discovery](docs/data_collection/market_discovery.md).

## Market Data Gateway

```shell
python -m polymarket_btc.data_collection.market_data.cli run --config config/market_data.toml
```

The gateway is read-only. It publishes immutable in-process snapshots and
persists raw JSONL/Zstandard events plus Parquet snapshots. See
[Market Data Gateway](docs/data_collection/market_data.md).

Live and replay paths share one `StateStore`/`MarketDataReducer`: books are
asset-keyed, CLOB sessions invalidate on reconnect, and `SnapshotTick` emits
four deterministic snapshots per second. Raw replay is streaming and verifies
v1/v2 manifests; Parquet schema v2 preserves the complete snapshot contract.
Writes use bounded queues, atomic manifests, and quarantine-based crash
recovery. The strict smoke reports fatal errors, readiness, storage hashes,
health-file validity, and queue drain.

## Docker

```shell
docker compose up --build -d
docker compose down
```

## Tests

```shell
python -m unittest
python -m compileall src tests
```

The 24-hour operational validation is intentionally outside CI and offline
tests. Until it runs on the target host, report:
`validation opérationnelle 24 h non exécutée`.
