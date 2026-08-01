from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from polymarket_btc.data_collection.market_data.binance_symbol_catalog import SymbolCatalog
from polymarket_btc.data_collection.market_data.config import load_config
from polymarket_btc.data_collection.market_data.gateway import (
    SOURCE_CATALOG,
    MarketDataGateway,
    build_extra_catalog,
    parse_source_key,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _config(data_dir: Path):
    config = load_config(REPOSITORY_ROOT / "config" / "market_data.toml")
    return replace(
        config,
        storage=replace(config.storage, data_dir=data_dir),
        health=replace(config.health, health_file=data_dir / "runtime" / "health.json"),
    )


class ParseSourceKeyTests(unittest.TestCase):
    def test_plain_key_has_no_symbol(self) -> None:
        self.assertEqual(parse_source_key("binance_spot"), ("binance_spot", None))

    def test_compound_key_splits_base_and_symbol(self) -> None:
        self.assertEqual(parse_source_key("binance_spot:ETH"), ("binance_spot", "ETH"))
        self.assertEqual(
            parse_source_key("binance_futures_liquidations:ETH"),
            ("binance_futures_liquidations", "ETH"),
        )


def _fake_catalog(**kwargs) -> SymbolCatalog:
    return SymbolCatalog(
        spot=kwargs.get("spot", frozenset({"BTC", "ETH", "SOL"})),
        futures=kwargs.get("futures", frozenset({"BTC", "ETH", "SOL"})),
        fetched_at_utc="2026-01-01T00:00:00+00:00",
    )


class BuildExtraCatalogTests(unittest.TestCase):
    def test_generates_an_entry_per_symbol_per_wired_base_key(self) -> None:
        extra = build_extra_catalog(_fake_catalog())
        expected_keys = {
            "binance_spot:ETH", "binance_spot_dom:ETH", "binance_futures_dom:ETH",
            "binance_futures_mark_price:ETH", "binance_futures_liquidations:ETH",
            "binance_futures_trade:ETH", "binance_futures_kline:ETH", "binance_futures_ticker:ETH",
            "binance_spot:SOL", "binance_futures_kline:SOL",
        }
        self.assertTrue(expected_keys.issubset(extra.keys()))
        for key in ("binance_spot:ETH", "binance_futures_kline:SOL"):
            base_key, symbol = key.split(":")
            self.assertEqual(extra[key].asset, symbol)
            # Same tag as BTC's equivalent entry, so "Par tag" groups them together.
            self.assertEqual(extra[key].tag, SOURCE_CATALOG[base_key].tag)

    def test_btc_itself_is_never_duplicated_into_the_extra_catalog(self) -> None:
        extra = build_extra_catalog(_fake_catalog())
        self.assertFalse(any(key.endswith(":BTC") for key in extra))

    def test_open_interest_long_short_gets_extra_entries_but_stays_access_only(self) -> None:
        # No parameterized *live* source class exists for it, but its
        # mode="access" template is safe to generalize: runs.py enforces
        # mode server-side, so a generated entry can only ever run through
        # the historical fetcher, never through live construction.
        extra = build_extra_catalog(_fake_catalog())
        oi_keys = [key for key in extra if key.startswith("binance_futures_open_interest_long_short:")]
        self.assertTrue(oi_keys)
        for key in oi_keys:
            self.assertEqual(extra[key].mode, "access")
            self.assertEqual(extra[key].historical_limit_days, 30)

    def test_spot_only_symbol_gets_only_spot_scoped_entries(self) -> None:
        extra = build_extra_catalog(_fake_catalog(spot=frozenset({"BTC", "DOGE"}), futures=frozenset({"BTC"})))
        doge_keys = {key for key in extra if key.endswith(":DOGE")}
        self.assertEqual(doge_keys, {"binance_spot:DOGE", "binance_spot_dom:DOGE"})

    def test_futures_only_symbol_gets_only_futures_scoped_entries(self) -> None:
        extra = build_extra_catalog(_fake_catalog(spot=frozenset({"BTC"}), futures=frozenset({"BTC", "1000PEPE"})))
        pepe_keys = {key for key in extra if key.endswith(":1000PEPE")}
        self.assertEqual(pepe_keys, {
            "binance_futures_dom:1000PEPE", "binance_futures_mark_price:1000PEPE",
            "binance_futures_liquidations:1000PEPE", "binance_futures_trade:1000PEPE",
            "binance_futures_kline:1000PEPE", "binance_futures_ticker:1000PEPE",
            "binance_futures_open_interest_long_short:1000PEPE",
        })


class ExtraSourcesConstructionTests(unittest.TestCase):
    def test_no_extra_sources_when_enabled_sources_is_none(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gateway = MarketDataGateway(_config(Path(directory)))
            self.assertEqual(gateway.extra_sources, {})

    def test_no_extra_sources_when_only_btc_keys_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gateway = MarketDataGateway(
                _config(Path(directory)), enabled_sources=frozenset({"binance_spot", "chainlink"}),
            )
            self.assertEqual(gateway.extra_sources, {})

    def test_eth_compound_keys_populate_extra_sources_with_resolved_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gateway = MarketDataGateway(
                _config(Path(directory)),
                enabled_sources=frozenset({
                    "binance_spot:ETH", "binance_futures_kline:ETH", "binance_futures_liquidations:ETH",
                }),
            )
            self.assertIn("binance_spot:ETH", gateway.extra_sources)
            self.assertEqual(gateway.extra_sources["binance_spot:ETH"].symbol, "ETHUSDT")
            self.assertIn("binance_futures_kline:ETH", gateway.extra_sources)
            # Liquidations is one shared instance keyed synthetically, not
            # one per requested symbol.
            self.assertIn("binance_futures_liquidations:*", gateway.extra_sources)
            liquidations = gateway.extra_sources["binance_futures_liquidations:*"]
            self.assertEqual(liquidations._symbol_filters, frozenset({"ETHUSDT"}))

    def test_btc_sources_always_constructed_regardless_of_extra_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            gateway = MarketDataGateway(
                _config(Path(directory)), enabled_sources=frozenset({"binance_spot:ETH"}),
            )
            self.assertEqual(gateway.binance.symbol, "BTCUSDT")
            self.assertEqual(gateway.futures_depth.instrument, "BTCUSDT")


class ExtraSourceSpawnIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_extra_source_task_is_spawned_and_cleanly_stopped(self) -> None:
        # No BTC live source spawns either (compound keys don't match any
        # plain BTC key). The liquidations source's own connect() is patched
        # out so this stays a pure task-lifecycle check, not a real network call.
        with tempfile.TemporaryDirectory() as directory:
            config = _config(Path(directory))
            config = replace(config, service=replace(config.service, snapshot_interval_ms=50))
            gateway = MarketDataGateway(
                config,
                start_live_sources=True,
                start_market_discovery=False,
                enabled_sources=frozenset({"binance_futures_liquidations:ETH"}),
            )
            module = "polymarket_btc.data_collection.market_data.sources.binance_futures_liquidations.connect"
            with patch(module, side_effect=ConnectionRefusedError("no network in tests")):
                async with gateway:
                    task_names = {task.get_name() for task in gateway._tasks}
            extra_task_names = {name for name in task_names if name.startswith("market-data-extra-")}
            self.assertEqual(extra_task_names, {"market-data-extra-binance_futures_liquidations:*"})


if __name__ == "__main__":
    unittest.main()
