# polymarket-btc

Read-only discovery of the current Polymarket BTC Up/Down markets for the
5-minute and 15-minute timeframes.

Startup resolves each timeframe up to three times, 0.5 second apart. Gamma
requests use a fixed 1.0-second timeout. During transitions, the expired market
is removed exactly at its end time even when a Gamma request is still running.

## Install

```powershell
python -m pip install -e .
```

## Resolve current markets

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli current
```

## Run continuously

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli run
```

## Use another transition log

By default, transitions are durably appended to
`~/.polymarket-btc/market_discovery/transitions.jsonl`. A persistence failure
stops both workers and exits with code 3; transition loss is never ignored.

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli run \
  --transition-log data/custom-transitions.jsonl
```

## Tests

```powershell
python -m unittest
```

See `docs/data_collection/market_discovery.md` for the exact discovery and
transition contract.
