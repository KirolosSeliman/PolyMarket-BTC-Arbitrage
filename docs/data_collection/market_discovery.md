# Market Discovery

Market Discovery resolves only the deterministic Polymarket BTC Up/Down
markets for the 5-minute and 15-minute timeframes. It is read-only and uses
the public Gamma market-by-slug endpoint.

## Windows and slugs

UTC timestamps are floored to an exact timeframe boundary. The expected slugs
are:

- `btc-updown-5m-{start_epoch}` for a 300-second window;
- `btc-updown-15m-{start_epoch}` for a 900-second window.

Only these two timeframes exist. There is no YAML configuration, fuzzy search,
candidate selection, or slug fallback.

## Strict validation

A Gamma response is accepted only when:

- `id` and `conditionId` are present and non-empty;
- `slug` exactly matches the expected slug;
- `active` and `enableOrderBook` are `true`;
- `closed` and `archived` are not `true`;
- `eventStartTime` equals the expected UTC boundary;
- `endDate` is exactly 300 or 900 seconds later;
- `resolutionSource` is the Chainlink BTC/USD stream;
- outcomes contain exactly one Up and one Down;
- the two non-empty, unique token IDs are mapped by outcome index.

The source may have a trailing slash, and only its scheme and hostname are
case-normalized. Invalid business metadata is `not_found`. Timeouts, transport
errors, non-404 HTTP errors, invalid JSON, and non-object JSON are `error`.
Gamma requests use a fixed 1.0-second timeout and have no internal retry.

## Controlled transitions

Each timeframe has an independent worker. At startup it makes at most three
current-market attempts, exactly 0.5 second apart, recalculating the current UTC
window each time. A market that expires while its startup request is running is
rejected. After startup, the worker searches the next exact slug from T-5
seconds through T+5 seconds. The target interval is 0.5 second, giving at most
21 theoretical attempts.

Requests never overlap within one timeframe. If a request overruns a slot,
elapsed slots are skipped without a catch-up burst. The 1.0-second HTTP timeout
can therefore skip a 0.5-second slot. A 5m request and a 15m request may run
concurrently.

A market found before T becomes `NEXT_READY`; the previous market remains
current only until its end time. At T, the ready market becomes `ACTIVE` with
zero delay. A market found from T through T+5 becomes `ACTIVE` immediately and
records its transition delay.

The expired market is removed exactly at T, even while a Gamma request is in
flight, and is never reused. If a market is already `NEXT_READY`, it becomes
`ACTIVE` directly at T without an intermediate searching state. If the expected
market is still unavailable after the T+5 attempt, the worker emits one
`TRANSITION_FAILED`, stores one failed transition, and schedules the next
timeframe boundary. It does not continue searching the failed window.

## Transition log

Final transition results are appended to:

`~/.polymarket-btc/market_discovery/transitions.jsonl`

Each line contains one success or failure, UTC timestamps, attempt count,
delay, market IDs, condition ID, Up/Down token IDs, and the last error. The
file never stores attempts, snapshots, or raw Gamma payloads. Writes are
flushed and synchronized to disk. `--transition-log` accepts an explicit path,
which is expanded and resolved to an absolute path. The default never depends
on the current working directory.

Any directory, open, write, flush, or `fsync` failure raises an explicit
transition-log error. This is fatal: the other worker is cancelled, the runner
stops, and the CLI exits with code 3 without a traceback. Transition data is
never lost silently.

## Commands

Resolve both current markets once:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli current
```

Run both workers until interrupted:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli run
```

Choose another append-only log:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli run \
  --transition-log data/custom-transitions.jsonl
```

## Limits

Correct operation depends on the published slug format, Gamma availability,
and an accurate server clock. Market Discovery does not collect the order
book, DOM, limits, volumes, trades, BTC price, probabilities, orders,
positions, or any trading data.
