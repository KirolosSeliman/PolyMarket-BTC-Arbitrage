# polymarket-btc

Read-only discovery of the current Polymarket BTC Up/Down markets for the
5-minute and 15-minute timeframes.

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
