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

## Live View

```shell
python -m polymarket_btc.data_collection.market_data.cli live --config config/market_data.toml
```

Runs the gateway and serves a browser dashboard on <http://127.0.0.1:8765/>
(`--host`, `--port`). Snapshots reach the page over Server-Sent Events at the
gateway's own cadence — four per second — so the view carries exactly what the
reducer publishes, with no extra polling and no third-party dependency.

The page shows Binance spot microstructure and rolling trade windows, the
futures perpetual (last trade, mark, funding, open interest, long/short,
liquidations), Chainlink RTDS with its basis against spot, both bucketed DOM
ladders, 1-minute klines and rolling 24h statistics for both markets, a
recent-trades tape for both markets, and the Polymarket 5 m/15 m order books
with the complementary-pair arbitrage legs (`buy_both_cost`,
`sell_both_credit`). Per-source health chips carry connection, staleness, and
error counters.

Binance geo-restricts the USDT-M Futures **aggTrade/markPrice/kline/ticker**
WebSocket pushes in some jurisdictions (observed from CA- and US-egress IPs)
while leaving `depth`/`bookTicker` WebSocket and all REST market-data
endpoints open. Those four feeds poll REST instead
(`sources/binance_futures_rest_streams.py`): mark price/funding every 1 s,
the forming 1-minute kline every 2 s, the 24h ticker every 3 s, and an
incremental `fromId`-cursored aggTrade poll every 2 s -- all well under
Binance's per-IP weight budget. If your deployment host's WebSocket isn't
geo-restricted, `sources/binance_futures_{trade,kline,ticker24h,mark_price}.py`
still exist as tested, unused-by-default alternatives.

Endpoints: `/` the dashboard, `/stream` the SSE feed, `/frame.json` a single
current frame, `/health.json` the gateway health payload. Every frame is
display-only — floats, not the `Decimal` values used for storage and replay.

## Control Panel

```shell
python -m polymarket_btc.data_collection.market_data.cli control --config config/market_data.toml
```

Serves a small control panel on <http://127.0.0.1:8780/> with three
destinations: Live Trading (placeholder), Backtest (dataset picker over past
collection runs), and Data Collection (`/collect`) — check off which
built-in feeds to gather (grouped by asset kind → asset → spot/futures, plus
an "other" bucket for Polymarket/Chainlink), pick a duration or run until
stopped, and it exports one consolidated `dataset.parquet` + `manifest.json`
per run under `data/collections/<run_id>/`.

Drop a `.py` file in `plugins/` to add a custom feed (any data at all — news,
another asset, order flow, on-chain, macro, anything) without touching the
gateway's code; it's auto-discovered and shown under "Extensions ajoutées",
grouped by whatever `category` it declares. See
[`docs/nouveau_plugin_prompt.md`](docs/nouveau_plugin_prompt.md) for a
copy-paste prompt that lets a context-free AI write one for you —
`plugins/example_funding_history.py` is a working reference.

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
