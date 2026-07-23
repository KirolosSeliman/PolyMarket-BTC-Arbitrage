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
