import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from polymarket_btc.data_collection.control.runs import CollectionRunManager
from polymarket_btc.data_collection.control.server import ControlPanelServer

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _seeded_symbol_cache(root: Path) -> Path:
    """Pre-seeds a fresh, minimal Binance symbol-catalog cache so
    CollectionRunManager.available_sources()/start() never touch the real
    network in these tests -- see test_symbol_catalog.py for that module's
    own tests."""
    cache_path = root / "cache" / "binance_symbols.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps({
        "spot": ["BTC", "ETH"], "futures": ["BTC", "ETH"], "fetched_at_utc": datetime.now(UTC).isoformat(),
    }))
    return cache_path


class ControlPanelServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=REPOSITORY_ROOT / "plugins",
        )
        self.server = ControlPanelServer(runs=self.runs, host="127.0.0.1", port=0)
        await self.server.start()

    async def asyncTearDown(self) -> None:
        if self.runs.current is not None and self.runs.current.ended_at_ns is None:
            self.runs.stop()
            if self.runs.current.task is not None:
                await asyncio.wait_for(
                    asyncio.gather(self.runs.current.task, return_exceptions=True), timeout=15
                )
        await self.server.stop()
        self._tmp.cleanup()

    async def _request(
        self, method: str, path: str, *, json_body: object = None
    ) -> tuple[str, dict]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        body = b"" if json_body is None else json.dumps(json_body).encode()
        lines = [f"{method} {path} HTTP/1.1", "Host: localhost"]
        if body:
            lines.append("Content-Type: application/json")
            lines.append(f"Content-Length: {len(body)}")
        request = ("\r\n".join(lines) + "\r\n\r\n").encode() + body
        writer.write(request)
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(-1), timeout=5)
        writer.close()
        await writer.wait_closed()
        head, _, resp_body = raw.partition(b"\r\n\r\n")
        head_text = head.decode("latin-1")
        parsed = json.loads(resp_body) if resp_body.strip().startswith((b"{", b"[")) else {}
        return head_text, parsed

    async def test_pages_are_served(self) -> None:
        for path, needle in (
            ("/", "Centre de contrôle"),
            ("/live", "<body"),
            ("/backtest", "Backtest"),
            ("/collect", "Collecte de données"),
        ):
            head, _ = await self._request("GET", path)
            self.assertIn("200 OK", head)

    async def test_unknown_path_is_404(self) -> None:
        head, _ = await self._request("GET", "/nope")
        self.assertIn("404", head)

    async def test_sources_and_plugins_endpoints(self) -> None:
        _head, sources = await self._request("GET", "/api/sources")
        keys = [row["key"] for row in sources["sources"]]
        self.assertIn("binance_spot", keys)
        self.assertIn("polymarket", keys)
        spot = next(row for row in sources["sources"] if row["key"] == "binance_spot")
        self.assertEqual(spot["asset_kind"], "crypto")
        self.assertEqual(spot["asset"], "BTC")
        self.assertEqual(spot["market"], "spot")
        self.assertEqual(spot["tag"], "Flux combiné")
        self.assertEqual(spot["mode"], "collect")
        polymarket = next(row for row in sources["sources"] if row["key"] == "polymarket")
        self.assertIsNone(polymarket["asset_kind"])
        self.assertIsNone(polymarket["asset"])
        self.assertIsNone(polymarket["tag"])

        eth_spot = next(row for row in sources["sources"] if row["key"] == "binance_spot:ETH")
        self.assertEqual(eth_spot["asset"], "ETH")
        self.assertEqual(eth_spot["market"], "spot")
        self.assertEqual(eth_spot["tag"], spot["tag"])  # same tag as BTC's -- groups together in "Par tag"

        _head, plugins = await self._request("GET", "/api/plugins")
        ids = [p["id"] for p in plugins["plugins"]]
        self.assertIn("example_funding_history", ids)
        example = next(p for p in plugins["plugins"] if p["id"] == "example_funding_history")
        self.assertEqual(example["category"], "Crypto")
        self.assertEqual(example["mode"], "collect")

    async def test_status_and_runs_are_empty_before_any_collection(self) -> None:
        _head, status = await self._request("GET", "/api/collect/status")
        self.assertIsNone(status["run"])
        _head, runs = await self._request("GET", "/api/runs")
        self.assertEqual(runs["runs"], [])

    async def test_start_rejects_missing_or_empty_sources(self) -> None:
        head, body = await self._request(
            "POST", "/api/collect/start", json_body={"plugins": []}
        )
        self.assertNotIn("200", head)
        self.assertIn("error", body)

        head, body = await self._request(
            "POST", "/api/collect/start", json_body={"sources": []}
        )
        self.assertNotIn("200", head)
        self.assertIn("error", body)

    async def test_stop_without_a_running_collection_errors(self) -> None:
        head, body = await self._request("POST", "/api/collect/stop")
        self.assertIn("409", head)
        self.assertIn("error", body)

    async def test_start_then_status_then_stop_round_trip(self) -> None:
        head, body = await self._request(
            "POST", "/api/collect/start",
            json_body={"sources": ["chainlink"], "plugins": [], "duration_seconds": None},
        )
        self.assertIn("200 OK", head)
        run_id = body["run_id"]

        _head, status = await self._request("GET", "/api/collect/status")
        self.assertEqual(status["run"]["run_id"], run_id)
        self.assertTrue(status["run"]["running"])

        head, body = await self._request("POST", "/api/collect/stop")
        self.assertIn("200 OK", head)

        for _ in range(200):
            _head, status = await self._request("GET", "/api/collect/status")
            if not status["run"]["running"]:
                break
            await asyncio.sleep(0.05)
        self.assertFalse(status["run"]["running"])
        self.assertIsNotNone(status["run"]["export"])

        _head, runs = await self._request("GET", "/api/runs")
        self.assertEqual(runs["runs"][0]["run_id"], run_id)


class PluginImportAndPromptTests(unittest.IsolatedAsyncioTestCase):
    """Isolated plugins_dir -- must never write into the repo's real plugins/
    while exercising the import endpoint."""

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.plugins_dir = root / "plugins"
        self.prompt_path = root / "prompt.md"
        self.prompt_path.write_text("# Prompt de test\ncontenu.", encoding="utf-8")
        self.runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=self.plugins_dir,
        )
        self.server = ControlPanelServer(
            runs=self.runs, host="127.0.0.1", port=0, prompt_doc_path=self.prompt_path,
        )
        await self.server.start()

    async def asyncTearDown(self) -> None:
        await self.server.stop()
        self._tmp.cleanup()

    async def _request(
        self, method: str, path: str, *, json_body: object = None
    ) -> tuple[str, dict]:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        body = b"" if json_body is None else json.dumps(json_body).encode()
        lines = [f"{method} {path} HTTP/1.1", "Host: localhost"]
        if body:
            lines.append("Content-Type: application/json")
            lines.append(f"Content-Length: {len(body)}")
        request = ("\r\n".join(lines) + "\r\n\r\n").encode() + body
        writer.write(request)
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(-1), timeout=5)
        writer.close()
        await writer.wait_closed()
        head, _, resp_body = raw.partition(b"\r\n\r\n")
        head_text = head.decode("latin-1")
        parsed = json.loads(resp_body) if resp_body.strip().startswith((b"{", b"[")) else {}
        return head_text, parsed

    async def test_plugin_prompt_endpoint_returns_document_content(self) -> None:
        head, body = await self._request("GET", "/api/plugin-prompt")
        self.assertIn("200 OK", head)
        self.assertIn("Prompt de test", body["content"])

    async def test_plugin_prompt_endpoint_404s_when_unconfigured(self) -> None:
        server = ControlPanelServer(runs=self.runs, host="127.0.0.1", port=0)
        await server.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            writer.write(b"GET /api/plugin-prompt HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            raw = await asyncio.wait_for(reader.read(-1), timeout=5)
            writer.close()
            await writer.wait_closed()
            self.assertIn(b"404", raw)
        finally:
            await server.stop()

    async def test_import_writes_a_new_plugin_visible_via_api(self) -> None:
        head, body = await self._request(
            "POST", "/api/plugins/import",
            json_body={
                "filename": "test_import.py",
                "content": 'PLUGIN_INFO = {"label": "Import", "description": "..."}\n'
                           "async def run(context):\n    pass\n",
            },
        )
        self.assertIn("200 OK", head)
        self.assertTrue(body["recognized"])
        self.assertTrue((self.plugins_dir / "test_import.py").is_file())

        _head, plugins = await self._request("GET", "/api/plugins")
        self.assertIn("test_import", [p["id"] for p in plugins["plugins"]])

    async def test_import_rejects_bad_filename(self) -> None:
        head, body = await self._request(
            "POST", "/api/plugins/import",
            json_body={"filename": "../escape.py", "content": "# x"},
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_import_conflict_requires_overwrite_flag(self) -> None:
        await self._request(
            "POST", "/api/plugins/import",
            json_body={"filename": "dup.py", "content": "# v1"},
        )
        head, body = await self._request(
            "POST", "/api/plugins/import",
            json_body={"filename": "dup.py", "content": "# v2"},
        )
        self.assertIn("409", head)
        self.assertTrue(body["exists"])
        self.assertEqual((self.plugins_dir / "dup.py").read_text(encoding="utf-8"), "# v1")

        head, _body = await self._request(
            "POST", "/api/plugins/import",
            json_body={"filename": "dup.py", "content": "# v2", "overwrite": True},
        )
        self.assertIn("200 OK", head)
        self.assertEqual((self.plugins_dir / "dup.py").read_text(encoding="utf-8"), "# v2")

    async def test_symbols_refresh_bypasses_the_cache_and_updates_it(self) -> None:
        class _Response:
            def __init__(self, payload: object) -> None:
                self._body = json.dumps(payload).encode()

            def read(self) -> bytes:
                return self._body

            def __enter__(self):
                return self

            def __exit__(self, *exc_info: object) -> None:
                return None

        responses = {
            "https://api.binance.com/api/v3/exchangeInfo": _Response({"symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "BTC", "isSpotTradingAllowed": True},
                {"symbol": "SOLUSDT", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "SOL", "isSpotTradingAllowed": True},
            ]}),
            "https://fapi.binance.com/fapi/v1/exchangeInfo": _Response({"symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING", "quoteAsset": "USDT", "baseAsset": "BTC", "contractType": "PERPETUAL"},
            ]}),
        }

        def fake_urlopen(request, timeout=None):
            return responses[request.full_url]

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            head, body = await self._request("POST", "/api/symbols/refresh")
        self.assertIn("200 OK", head)
        self.assertEqual(body["spot_count"], 2)
        self.assertEqual(body["futures_count"], 1)

        cached = json.loads(self.runs.symbol_cache_path.read_text(encoding="utf-8"))
        self.assertEqual(sorted(cached["spot"]), ["BTC", "SOL"])

    async def test_access_mode_start_status_round_trip_via_http(self) -> None:
        self.plugins_dir.mkdir(parents=True, exist_ok=True)
        (self.plugins_dir / "access_plugin.py").write_text(
            'PLUGIN_INFO = {"label": "Accès", "description": "...", "mode": "access"}\n'
            "async def run(context):\n"
            "    (context.data_dir / 'out.jsonl').write_text('{}')\n",
            encoding="utf-8",
        )
        head, body = await self._request(
            "POST", "/api/collect/start",
            json_body={
                "sources": [], "plugins": ["access_plugin"], "mode": "access",
                "start_ts": 1_700_000_000_000, "end_ts": 1_700_100_000_000,
            },
        )
        self.assertIn("200 OK", head)
        run_id = body["run_id"]

        for _ in range(200):
            _head, status = await self._request("GET", "/api/collect/status")
            if not status["run"]["running"]:
                break
            await asyncio.sleep(0.05)
        self.assertEqual(status["run"]["run_id"], run_id)
        self.assertEqual(status["run"]["mode"], "access")
        self.assertIsNotNone(status["run"]["start_ts_utc"])
        self.assertEqual(status["run"]["export"]["plugin_files"], ["out.jsonl"])

    async def test_access_mode_validation_error_is_400_not_409(self) -> None:
        head, body = await self._request(
            "POST", "/api/collect/start",
            json_body={"sources": ["chainlink"], "plugins": [], "mode": "access"},
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_unknown_mode_is_400(self) -> None:
        head, body = await self._request(
            "POST", "/api/collect/start",
            json_body={"sources": [], "plugins": [], "mode": "bogus"},
        )
        self.assertIn("400", head)
        self.assertIn("error", body)


if __name__ == "__main__":
    unittest.main()
