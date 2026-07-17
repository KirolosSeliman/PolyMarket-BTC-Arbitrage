# Market Discovery V1

Market Discovery V1 is a read-only current-market resolver for Polymarket BTC
Up/Down five-minute markets. It discovers metadata only. It does not trade,
place orders, sign messages, use a wallet, calculate edge, or manage capital.

## Production Flow

```text
UTC clock
  -> floor current time to five-minute boundary
  -> build btc-updown-5m-{start_epoch}
  -> GET https://gamma-api.polymarket.com/markets/slug/{slug}
  -> strict payload validation
  -> concise DiscoveryResult
```

The direct slug lookup follows Polymarket's documented Gamma slug lookup path:
`GET /markets/slug/{slug}`. Broad search, watch mode, next-market preloading,
fixture mode, and multiple-candidate selection are intentionally not production
CLI behavior in V1. The direct slug contract has only `selected`, `no_match`,
and `provider_unavailable` statuses.

## Validation Rules

Discovery fails closed unless the payload satisfies all rules:

- `slug` equals the expected current UTC slug.
- `eventStartTime <= now_utc < endDate`.
- `endDate - eventStartTime == 300 seconds`.
- slug epoch equals `eventStartTime`.
- `active` is true.
- `closed` and `archived` are not true.
- `enableOrderBook` is true.
- `id` and `conditionId` are present.
- `resolutionSource` equals `https://data.chain.link/streams/btc-usd`, allowing a trailing slash.
- `outcomes` and `clobTokenIds` parse as arrays with equal length.
- outcomes are exactly one `Up` and one `Down`.
- token IDs are non-empty and unique.
- outcome tokens are mapped by source index, not by assumed position.

## Config

Market Discovery uses built-in runtime defaults when `--config` is omitted:

- Gamma base URL: `https://gamma-api.polymarket.com`
- request timeout: `3.0` seconds
- max retries: `1`
- retry delay: `0.5` seconds

The YAML file is an explicit optional override and a checked example artifact:

```text
config/data_collection/market_discovery.yaml
```

Supported keys:

```yaml
version: 1

market_discovery:
  gamma_base_url: https://gamma-api.polymarket.com
  request_timeout_seconds: 3
  max_retries: 1
  retry_delay_seconds: 0.5
```

Unknown keys are rejected at startup.

## CLI

Validate built-in defaults:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --validate-config --json
```

Validate an explicit YAML override:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --validate-config --json
```

Run discovery with built-in defaults:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --json
```

Run discovery with an explicit YAML override:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --json
```

Normalized selected output:

```json
{
  "market": {
    "condition_id": "0x...",
    "down_token_id": "...",
    "end_time_utc": "2026-07-17T19:55:00Z",
    "market_id": "2951004",
    "resolution_source": "https://data.chain.link/streams/btc-usd",
    "slug": "btc-updown-5m-1784317800",
    "start_time_utc": "2026-07-17T19:50:00Z",
    "up_token_id": "..."
  },
  "reason": null,
  "status": "selected"
}
```

Exit codes:

- `0`: selected current market.
- `1`: no match.
- `2`: invalid config or provider unavailable.

## Optional CLOB Smoke Check

The development-only smoke script first runs Market Discovery V1, then reads
the public CLOB order book endpoint for the discovered Up and Down token IDs:
`GET https://clob.polymarket.com/book?token_id={token_id}`.

```powershell
python scripts/clob_token_smoke.py
python scripts/clob_token_smoke.py --config config/data_collection/market_discovery.yaml
```

The script verifies that each response is an object, that `asset_id` matches the
requested token, and that `market`, `bids`, `asks`, and `hash` are present. It
does not authenticate, sign, post, cancel, or trade.

## Tests

Offline tests cover:

- config acceptance and strict unknown-key rejection;
- Gamma direct slug success, 404, retry, timeout, malformed JSON, and no-retry 400 behavior;
- UTC five-minute flooring and slug construction;
- active/closed/archived/order-book guards;
- exact start and end boundary behavior;
- duration, slug/start, timestamp, and resolution-source validation;
- outcome/token parsing from JSON strings and arrays;
- reversed source outcome order;
- unknown/duplicate outcomes, duplicate/empty tokens, and count mismatch;
- concise CLI JSON and exit codes.

## Twelve-Window Live Validation

Before promoting V1 beyond read-only discovery, run this procedure for 12
consecutive five-minute windows, covering one full hour:

1. At each UTC five-minute boundary plus 5 to 30 seconds, run:

   ```powershell
   python -m polymarket_btc.data_collection.market_discovery.cli --json
   ```

2. Record the current UTC time, expected slug, exit code, selected market ID,
   condition ID, Up token, Down token, and reason when no market is selected.
3. For every selected market, confirm the slug epoch equals `eventStartTime`,
   the window is exactly 300 seconds, the selected time is inside
   `eventStartTime <= now_utc < endDate`, and the status flags are tradable.
4. Confirm that outcomes and token IDs are mapped by source index and that Up
   and Down token IDs are different.
5. Optionally run `python scripts/clob_token_smoke.py` in the same window and
   record whether both token order books return matching `asset_id` values.
6. Any `no_match`, provider outage, token mismatch, or timestamp mismatch must
   be treated as a failed window and investigated before launch.

With the default config, one Gamma discovery attempt can take up to about 6.5
seconds in the worst case: two three-second HTTP attempts plus one 0.5-second
retry delay. The optional CLOB smoke check adds up to about six more seconds
for two token book reads. The 12-window procedure takes one hour of wall-clock
time by design.

## Limits

- This module depends on the observed BTC five-minute slug format. If
  Polymarket changes that format, discovery fails closed.
- It validates market metadata only; it does not prove liquidity, fillability,
  profitability, or trading safety.
- The optional CLOB smoke check verifies token/order-book connectivity only.
  It is not an order execution test.

## References

- Polymarket Fetching Markets: https://docs.polymarket.com/market-data/fetching-markets
- Polymarket List Markets API reference: https://docs.polymarket.com/api-reference/markets/list-markets
- Polymarket Get Order Book API reference: https://docs.polymarket.com/api-reference/market-data/get-order-book
- Polymarket Orderbook guide: https://docs.polymarket.com/trading/orderbook
