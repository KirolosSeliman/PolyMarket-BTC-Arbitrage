# polymarket_BTC

Read-only data collection utilities for Polymarket BTC markets.

The first implemented component is Market Discovery for BTC Up/Down
five-minute markets. It discovers and validates market metadata only. It does
not trade, place orders, sign messages, use a wallet, calculate probability, or
manage capital.

## Market Discovery

Default config:

```text
config/data_collection/market_discovery.yaml
```

Validate config:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --validate-config
```

Run one discovery cycle:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --once --json
```

Run tests:

```powershell
python -m unittest
```

See `docs/data_collection/market_discovery.md` for architecture, selection
rules, endpoint rationale, CLI usage, tests, and limitations.
