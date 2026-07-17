# polymarket-btc

Read-only Polymarket BTC market data utilities.

The current production surface is Market Discovery V1: one command that computes
the current UTC five-minute BTC Up/Down slug, fetches that single Gamma market,
validates it strictly, and returns a concise selected/no-match result.

It does not trade, sign messages, use a wallet, place orders, calculate edge, or
manage capital.

## Commands

Validate built-in config defaults:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --validate-config --json
```

Validate an explicit YAML override:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --validate-config --json
```

Run one discovery lookup with built-in defaults:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --json
```

Run with an explicit YAML override:

```powershell
python -m polymarket_btc.data_collection.market_discovery.cli --config config/data_collection/market_discovery.yaml --json
```

Run tests:

```powershell
python -m unittest
```

Optional development-only CLOB token smoke check:

```powershell
python scripts/clob_token_smoke.py
python scripts/clob_token_smoke.py --config config/data_collection/market_discovery.yaml
```

See `docs/data_collection/market_discovery.md` for validation rules, failure
behavior, live validation procedure, and known limits.
