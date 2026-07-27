# Market Data Gateway

## Purpose and safety boundary

The gateway is a single-process, read-only `asyncio` service. It receives
public market data and never authenticates, signs, submits, cancels, or settles
orders. Custom endpoints are rejected unless
`allow_custom_endpoints = true` is explicitly configured.

It combines:

- Polymarket RTDS Chainlink BTC/USD updates;
- Binance Spot BTCUSDT aggregate trades, best bid/ask, and full depth-20
  snapshots;
- Polymarket CLOB market-channel data for the active and preloaded 5-minute
  and 15-minute Up/Down tokens;
- the existing Market Discovery state machine.

## Architecture

Each source validates messages into immutable typed events. A bounded event bus
fans every accepted event to a single state reducer and the raw writer.
Backpressure that outlives the configured deadline is fatal; the service never
samples or silently drops accepted events. A separate bounded queue carries
Market Discovery transitions to the CLOB subscription manager.

The reducer is the only live-state writer. The snapshot publisher emits an
immutable `MarketDataSnapshot` every 250 ms by default. Slow subscribers have
bounded queues and are disconnected instead of blocking collection.

## Exact public endpoints

- RTDS: `wss://ws-live-data.polymarket.com`
- Binance: `wss://stream.binance.com:9443/stream`
- CLOB: `wss://ws-subscriptions-clob.polymarket.com/ws/market`

Binance subscribes to `btcusdt@aggTrade`, `btcusdt@bookTicker`, and
`btcusdt@depth20@100ms` on one combined connection with microsecond timestamps.
The depth stream is treated as a complete replacement snapshot, never as a
delta. Binance is proactively reconnected before its 24-hour server limit.

RTDS sends the documented subscription and an application `PING` every five
seconds. CLOB sends an application `PING` every ten seconds and dynamically
subscribes or unsubscribes token IDs as Market Discovery transitions.

## Configuration

The development configuration is
[`config/market_data.toml`](../../config/market_data.toml). Startup validates
all sections, queue capacities, source names, streams, intervals, URLs, and
storage parameters before opening any network connection.

Important settings:

- `snapshot_interval_ms`: strategic publication interval;
- `put_timeout_seconds`: maximum bounded-queue backpressure;
- source freshness thresholds;
- raw and Parquet rotation limits;
- `data_dir` and the atomic health-file path.

No secret is required or supported.

## Data contract and timestamps

Canonical prices and quantities use `Decimal`. JSON serializes decimals as
strings and timestamps as integer nanoseconds. Each event includes source,
server when available, local wall-clock, and local monotonic timestamps.
Binance millisecond or requested microsecond timestamps are normalized to
nanoseconds.

Event IDs are deterministic from source identifiers and message sequences.
The gateway assigns a strictly increasing ingest sequence only after parsing.
Deduplication is per stream. Invalid, stale, out-of-order, crossed, malformed,
NaN, infinite, oversized, and unknown messages do not replace live state.

## Market transitions

Current and next 5-minute and 15-minute markets remain independent. Next-token
books may be collected early but are not exposed as strategy-ready before
activation. On transition, the new context is activated before old tokens are
unsubscribed. Old subscriptions remain until both new books initialize or the
ten-second grace period expires. Reconnection invalidates local CLOB books and
requires fresh full `book` snapshots.

`ready_for_strategy` is false unless required sources are connected and fresh,
both active markets exist, and every exposed Up/Down book is initialized,
coherent, and unresolved. `not_ready_reasons` provides explicit causes.

## Storage and recovery

Raw accepted events are appended as JSONL to `.partial` files, flushed at the
configured event/time limits, fsynced periodically, compressed with Zstandard,
and finalized with a manifest and SHA-256 digest. Startup recovers non-empty
partial files and quarantines invalid ones. It never overwrites a completed
segment.

Parquet stores fixed-schema snapshot rows with Zstandard compression and
structured depth levels. Snapshot files rotate by time or row count and are
finalized atomically with manifests. The `data` directory must be placed on
persistent storage.

The `StateStore` is the sole owner of live books. CLOB books are indexed by
`asset_id`, while market/condition IDs and the CLOB session ID remain part of
each immutable snapshot. A reconnect invalidates old-session books, so stale
messages cannot restore readiness. Live ingestion and replay use the same
reducer and deterministic `SNAPSHOT_TICK` events (four snapshots per second).

Raw schema versions 1 and 2 remain replay-compatible. Startup recovery handles
partial JSONL, compressed segments, manifests, and Parquet files by either
finalizing them deterministically or moving them to `data/quarantine/` with a
reason sidecar. Parquet schema v2 stores the complete public snapshot contract
alongside structured depth fields. A dedicated bounded Parquet queue and
worker thread write rows; queue saturation is fatal rather than a silent drop.

Replay verifies manifests, hashes, JSON records, schema versions, duplicate
IDs, and contiguous ingest sequences before reconstructing snapshots:

```shell
python -m polymarket_btc.data_collection.market_data.cli replay \
  --input data/raw --output data/replay --speed 0
```

## CLI

```shell
python -m polymarket_btc.data_collection.market_data.cli validate-config --config config/market_data.toml
python -m polymarket_btc.data_collection.market_data.cli run --config config/market_data.toml
python -m polymarket_btc.data_collection.market_data.cli smoke --config config/market_data.toml --duration-seconds 120
python -m polymarket_btc.data_collection.market_data.cli status --health-file data/runtime/health.json
python -m polymarket_btc.data_collection.market_data.cli replay --input data/raw --output data/replay --speed 0
```

Exit code 0 means success/healthy, 1 means unhealthy or an incomplete smoke
test, 2 means invalid configuration/usage, and 3 means storage or replay
integrity failure.

## Health and troubleshooting

The service atomically rewrites `data/runtime/health.json`. It reports gateway
state, readiness causes, connections, reconnects, invalid and duplicate
messages, divergence, queue sizes/capacities, active token IDs, and persisted
event/snapshot counts.

`HealthRegistry` is the single source for source health and fatal diagnostics;
the same immutable checkpoint is attached to snapshots and the atomic health
file. `market_resolved` remains a control event and can resolve a market even
when the message has no asset ID.

- `*_disconnected`: inspect DNS, TLS, firewall, and endpoint reachability.
- `*_stale`: verify source traffic and system time.
- `*_book_uninitialized`: wait for a full CLOB `book` snapshot; reconnect if
  it persists.
- `*_book_incoherent`: the local and published best bid/ask diverged three
  times; a fresh full book is required.
- exit 3 or `storage_error`: stop the service and preserve the data directory
  for recovery; do not discard partial files.
- queue saturation: storage is not keeping up. Increase durable I/O capacity;
  never hide the condition by dropping events.

## Docker

The image runs as an unprivileged user, contains no credentials, and exposes no
network service.

```shell
docker compose config
docker compose up --build -d
docker compose ps
python -m polymarket_btc.data_collection.market_data.cli status --health-file data/runtime/health.json
docker compose down
```

Compose mounts `./data`, rotates container logs, applies a healthcheck, and
allows 30 seconds for graceful shutdown.

## Capacity and expected volume

Volume depends on Binance and CLOB activity. Capacity planning should assume
hundreds of Binance events per second plus four CLOB books and 4 snapshots/s.
At 500–600 events/s, raw JSON may reach tens of gigabytes per day before
compression. Monitor actual compressed bytes, inode usage, fsync latency,
queue occupancy, and retention externally. This service deliberately performs
no deletion or retention.

## Offline benchmark

Run the synthetic benchmark for at least five minutes:

```shell
python scripts/benchmark_market_data.py --duration-seconds 300
```

It reports event throughput, event-to-state p99, snapshot jitter p99, storage
progress, and memory growth after warmup. A passing result requires at least
the configured synthetic input rate, event-to-state p99 below 50 ms, snapshot
jitter p99 below 100 ms, storage keeping pace, and no sustained post-warmup
memory growth.

## 24-hour soak

The soak is intentionally excluded from standard CI:

```shell
python scripts/soak_market_data.py --config config/market_data.toml --duration-seconds 86400
```

Run it on the deployment-class host with persistent storage. Preserve its JSON
report and all health/storage manifests. Acceptance requires zero silent loss,
zero fatal backpressure, no continuous memory growth, observed Binance
renewal, at least 95% of expected snapshots, valid manifests/hashes, and every
gap documented. Technical deployability does not replace this operational
validation.

## Limits

- Public upstream availability, schemas, throttling, and clock quality remain
  external dependencies.
- Market valuation and trading decisions are outside this service.
- No retention policy is implemented.
- A live 120-second smoke and a 24-hour soak must be repeated in the target
network and storage environment before operational launch.

## Migration procedure

1. Stop the previous collector cleanly and keep its data directory intact.
2. Start this gateway against the same persistent `data_dir`; recovery verifies
   old manifests before opening network connections.
3. Run `validate-config`, then replay a copy of the raw directory and compare
   its deterministic snapshots with the stored Parquet rows.
4. Run the strict 120-second smoke and inspect health, queue drain, and hashes
   before enabling production traffic.

The 24-hour operational validation is not run in CI or offline tests. Until it
is executed on the target host, report: `validation opérationnelle 24 h non exécutée`.
