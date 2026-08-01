from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from polymarket_btc.data_collection.market_data.binance_symbol_catalog import (
    SymbolCatalog,
    fetch_futures_usdt_perp_symbols,
    fetch_spot_usdt_symbols,
    load_cached_or_fetch,
    validate_symbol,
)


class _Response:
    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


_SPOT_PAYLOAD = {
    "symbols": [
        {"symbol": "BTCUSDT", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "BTC", "isSpotTradingAllowed": True},
        {"symbol": "ETHUSDT", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "ETH", "isSpotTradingAllowed": True},
        {"symbol": "OLDUSDT", "status": "BREAK", "quoteAsset": "USDT", "baseAsset": "OLD", "isSpotTradingAllowed": True},
        {"symbol": "BTCEUR", "status": "TRADING", "quoteAsset": "EUR", "baseAsset": "BTC", "isSpotTradingAllowed": True},
        {"symbol": "WEIRDUSDT", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "WEIRD", "isSpotTradingAllowed": False},
    ]
}
_FUTURES_PAYLOAD = {
    "symbols": [
        {"symbol": "BTCUSDT", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "BTC", "contractType": "PERPETUAL"},
        {"symbol": "ETHUSDT", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "ETH", "contractType": "PERPETUAL"},
        {"symbol": "BTCUSDT_240329", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "BTC", "contractType": "CURRENT_QUARTER"},
        {"symbol": "SOLBUSD", "status": "TRADING", "quoteAsset": "BUSD", "baseAsset": "SOL", "contractType": "PERPETUAL"},
    ]
}


class FetchFilteringTests(unittest.TestCase):
    def test_spot_fetch_filters_status_quote_and_permission(self) -> None:
        with patch("urllib.request.urlopen", return_value=_Response(_SPOT_PAYLOAD)):
            symbols = fetch_spot_usdt_symbols()
        self.assertEqual(symbols, frozenset({"BTC", "ETH"}))

    def test_futures_fetch_filters_status_quote_and_contract_type(self) -> None:
        with patch("urllib.request.urlopen", return_value=_Response(_FUTURES_PAYLOAD)):
            symbols = fetch_futures_usdt_perp_symbols()
        self.assertEqual(symbols, frozenset({"BTC", "ETH"}))


class CacheRoundTripTests(unittest.TestCase):
    def test_fetches_and_writes_cache_when_absent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "binance_symbols.json"
            responses = {
                "https://api.binance.com/api/v3/exchangeInfo": _Response(_SPOT_PAYLOAD),
                "https://fapi.binance.com/fapi/v1/exchangeInfo": _Response(_FUTURES_PAYLOAD),
            }

            def fake_urlopen(request, timeout=None):
                return responses[request.full_url]

            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                catalog = load_cached_or_fetch(cache_path, max_age_seconds=3600)

            self.assertEqual(catalog.spot, frozenset({"BTC", "ETH"}))
            self.assertEqual(catalog.futures, frozenset({"BTC", "ETH"}))
            self.assertTrue(cache_path.is_file())

    def test_fresh_cache_is_served_without_any_network_call(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "binance_symbols.json"
            cache_path.write_text(json.dumps({
                "spot": ["BTC"], "futures": ["BTC"],
                "fetched_at_utc": datetime.now(UTC).isoformat(),
            }))

            def fail_if_called(*args, **kwargs):
                raise AssertionError("should not touch the network for a fresh cache")

            with patch("urllib.request.urlopen", side_effect=fail_if_called):
                catalog = load_cached_or_fetch(cache_path, max_age_seconds=3600)
            self.assertEqual(catalog.spot, frozenset({"BTC"}))

    def test_stale_cache_triggers_a_refetch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "binance_symbols.json"
            stale = (datetime.now(UTC) - timedelta(days=2)).isoformat()
            cache_path.write_text(json.dumps({
                "spot": ["OLD"], "futures": ["OLD"], "fetched_at_utc": stale,
            }))
            responses = {
                "https://api.binance.com/api/v3/exchangeInfo": _Response(_SPOT_PAYLOAD),
                "https://fapi.binance.com/fapi/v1/exchangeInfo": _Response(_FUTURES_PAYLOAD),
            }

            def fake_urlopen(request, timeout=None):
                return responses[request.full_url]

            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                catalog = load_cached_or_fetch(cache_path, max_age_seconds=3600)
            self.assertEqual(catalog.spot, frozenset({"BTC", "ETH"}))

    def test_force_refetches_even_with_a_fresh_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "binance_symbols.json"
            cache_path.write_text(json.dumps({
                "spot": ["OLD"], "futures": ["OLD"],
                "fetched_at_utc": datetime.now(UTC).isoformat(),
            }))
            responses = {
                "https://api.binance.com/api/v3/exchangeInfo": _Response(_SPOT_PAYLOAD),
                "https://fapi.binance.com/fapi/v1/exchangeInfo": _Response(_FUTURES_PAYLOAD),
            }

            def fake_urlopen(request, timeout=None):
                return responses[request.full_url]

            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                catalog = load_cached_or_fetch(cache_path, max_age_seconds=3600, force=True)
            self.assertEqual(catalog.spot, frozenset({"BTC", "ETH"}))

    def test_network_failure_falls_back_to_stale_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "binance_symbols.json"
            stale = (datetime.now(UTC) - timedelta(days=2)).isoformat()
            cache_path.write_text(json.dumps({
                "spot": ["OLD"], "futures": ["OLD"], "fetched_at_utc": stale,
            }))

            def fail_urlopen(request, timeout=None):
                raise OSError("network unreachable")

            with patch("urllib.request.urlopen", side_effect=fail_urlopen):
                catalog = load_cached_or_fetch(cache_path, max_age_seconds=3600)
            self.assertEqual(catalog.spot, frozenset({"OLD"}))

    def test_network_failure_with_no_cache_at_all_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "binance_symbols.json"

            def fail_urlopen(request, timeout=None):
                raise OSError("network unreachable")

            with patch("urllib.request.urlopen", side_effect=fail_urlopen):
                with self.assertRaises(OSError):
                    load_cached_or_fetch(cache_path, max_age_seconds=3600)


class ValidateSymbolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog = SymbolCatalog(
            spot=frozenset({"BTC", "ETH"}), futures=frozenset({"BTC", "ETH", "SOL"}),
            fetched_at_utc=datetime.now(UTC).isoformat(),
        )

    def test_accepts_known_symbol_case_insensitively(self) -> None:
        self.assertEqual(validate_symbol("eth", self.catalog, "spot"), "ETH")

    def test_rejects_symbol_not_in_the_requested_market(self) -> None:
        with self.assertRaises(ValueError):
            validate_symbol("SOL", self.catalog, "spot")

    def test_rejects_completely_unknown_symbol(self) -> None:
        with self.assertRaises(ValueError):
            validate_symbol("NOTREAL", self.catalog, "futures")


if __name__ == "__main__":
    unittest.main()
