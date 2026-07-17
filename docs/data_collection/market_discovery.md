# Market Discovery

Market Discovery observes and describes Polymarket BTC Up/Down five-minute
markets. It does not calculate probabilities, evaluate edge, manage capital,
sign messages, place orders, or make trading decisions.

## Architecture

```text
Polymarket Gamma API
        |
Gamma Client
        |
Payload Normalizer
        |
Candidate Validator
        |
Active Market Selector
        |
MarketDescriptor
        |
Future collectors
```

The repository now has a general data collection namespace:

- `polymarket_btc.data_collection.common`: shared UTC time and HTTP utilities.
- `polymarket_btc.data_collection.market_discovery`: BTC five-minute market discovery.

Future collectors should consume `MarketDescriptor` instead of depending on
raw Gamma payload fields, title parsing, slug parsing, or pagination details.

## Endpoint Choice

The primary one-shot service computes the expected BTC five-minute slug from
the current UTC five-minute boundary and calls:

```text
GET https://gamma-api.polymarket.com/markets/slug/{slug}
```

Observed BTC five-minute slugs use:

```text
btc-updown-5m-{event_start_epoch}
```

The optional network audit uses:

```text
GET https://gamma-api.polymarket.com/public-search
```

to inspect recent candidate records. Broad event and market listing endpoints
are useful for reconnaissance, but they are not sufficient by themselves for
safe active selection because default ordering can include old records and
`active=true` can appear on markets that are already closed.

Keyset pagination exists at `/markets/keyset` and returns `markets` plus
`next_cursor`. Offset pagination must not be used on that endpoint.

## Reliable Fields

For observed BTC five-minute records, the selector uses:

- `slug`
- `eventStartTime`
- `endDate`
- `conditionId`
- `questionID`
- `outcomes`
- `clobTokenIds`
- `active`
- `closed`
- `archived`
- `enableOrderBook`
- `resolutionSource`

`startDate` is not used as the five-minute window start. In observed BTC 5m
payloads, it is the listing or creation time from the previous day. The actual
window is:

```text
eventStartTime <= now_utc < endDate
```

The start boundary is inclusive. The end boundary is exclusive.

## Outcome And Token Mapping

Gamma has been observed returning `outcomes` and `clobTokenIds` as JSON-encoded
strings. The normalizer also accepts already-decoded arrays for testability and
future compatibility.

The normalizer:

- parses both collections;
- requires equal lengths;
- normalizes outcome names while preserving source names;
- maps each outcome to the token at the same source index;
- rejects duplicate outcomes;
- rejects duplicate or empty token IDs;
- rejects unknown outcomes;
- outputs outcomes in stable `Up`, `Down` order.

It never assumes the first token is `Up` unless the source outcome at index 0 is
actually `Up`.

## Configuration

Default config:

```text
config/data_collection/market_discovery.yaml
```

The config contains static rules only. It does not contain dynamic market IDs,
condition IDs, or token IDs.

Important units:

- `duration_seconds`: seconds, fixed to `300` for this module.
- `interval_seconds`: seconds between watch-mode polls.
- `request_timeout_seconds`: per-request timeout.
- `retry_base_delay_seconds` and `retry_max_delay_seconds`: retry backoff bounds.

Unknown keys are rejected so accidental misspellings fail at startup.

## CLI

Validate config:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --validate-config
```

Run one live discovery cycle:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --once --json
```

Run watch mode:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --watch --iterations 3 --json
```

Run optional public-search audit:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --network-audit --json
```

Run deterministic fixture mode:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --once --fixture tests/data_collection/market_discovery/fixtures/btc_5m_current_next.json --now 2026-07-17T19:51:00Z --json
```

Exit codes:

- `0`: selected market or successful config validation.
- `1`: no selected market, ambiguity, or provider unavailable result.
- `2`: invalid config, invalid arguments, invalid time, or provider audit error.

## Tests

Offline tests cover:

- valid and invalid YAML config;
- JSON-string and array payload fields;
- outcome/token count mismatch;
- duplicate outcomes and duplicate tokens;
- unknown outcomes;
- missing condition IDs and token IDs;
- invalid timestamps;
- closed markets;
- reversed outcome order;
- start-inclusive and end-exclusive boundaries;
- future active markets;
- ambiguous current matches;
- next-market detection;
- 429 and 500 retry;
- 400 no-retry behavior;
- timeout conversion;
- transition de-duplication;
- CLI fixture and config behavior.

## Known Limits

- The current production path relies on the observed BTC five-minute slug
  template. If Polymarket changes that format, discovery fails closed.
- The optional search audit is for inspection; it is not the primary selection
  path.
- No order book, price, volatility, probability, or trading collector is
  implemented in this module.
