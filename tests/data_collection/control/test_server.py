import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from polymarket_btc.data_collection.control.runs import CollectionRunManager
from polymarket_btc.data_collection.control.server import ControlPanelServer
from polymarket_btc.data_collection.control.strategies import StrategyManager

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
            concepts_dir=REPOSITORY_ROOT / "concepts",
            microsystems_dir=REPOSITORY_ROOT / "microsystems",
            execution_dir=REPOSITORY_ROOT / "execution_profiles",
            management_dir=REPOSITORY_ROOT / "management_profiles",
        )
        self.strategies = StrategyManager(
            strategies_dir=REPOSITORY_ROOT / "strategies", runs=self.runs,
        )
        self.server = ControlPanelServer(runs=self.runs, strategies=self.strategies, host="127.0.0.1", port=0)
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
            ("/", "Choisir une stratégie"),
            ("/menu", "Centre de contrôle"),
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

    async def test_delete_run_removes_it_from_the_list(self) -> None:
        _head, start_body = await self._request(
            "POST", "/api/collect/start",
            json_body={"sources": ["chainlink"], "plugins": [], "duration_seconds": None},
        )
        run_id = start_body["run_id"]
        await self._request("POST", "/api/collect/stop")
        for _ in range(200):
            _head, status = await self._request("GET", "/api/collect/status")
            if not status["run"]["running"]:
                break
            await asyncio.sleep(0.05)

        head, body = await self._request("POST", "/api/runs/delete", json_body={"run_id": run_id})
        self.assertIn("200 OK", head)
        self.assertTrue(body["ok"])

        _head, runs = await self._request("GET", "/api/runs")
        self.assertEqual(runs["runs"], [])

    async def test_delete_run_missing_run_id_is_400(self) -> None:
        head, body = await self._request("POST", "/api/runs/delete", json_body={})
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_delete_run_unknown_run_id_is_404(self) -> None:
        head, body = await self._request("POST", "/api/runs/delete", json_body={"run_id": "no-such-run"})
        self.assertIn("404", head)
        self.assertIn("error", body)

    async def test_delete_run_currently_running_is_409(self) -> None:
        _head, start_body = await self._request(
            "POST", "/api/collect/start",
            json_body={"sources": ["chainlink"], "plugins": [], "duration_seconds": None},
        )
        run_id = start_body["run_id"]
        head, body = await self._request("POST", "/api/runs/delete", json_body={"run_id": run_id})
        self.assertIn("409", head)
        self.assertIn("error", body)


class PluginImportAndPromptTests(unittest.IsolatedAsyncioTestCase):
    """Isolated plugins_dir -- must never write into the repo's real plugins/
    while exercising the import endpoint."""

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.plugins_dir = root / "plugins"
        self.prompt_path = root / "prompt.md"
        self.prompt_path.write_text("# Prompt de test\ncontenu.", encoding="utf-8")
        self.concepts_dir = root / "concepts"
        self.microsystems_dir = root / "microsystems"
        self.execution_dir = root / "execution_profiles"
        self.management_dir = root / "management_profiles"
        self.concept_prompt_path = root / "concept_prompt.md"
        self.concept_prompt_path.write_text("# Prompt de concept de test\ncontenu.", encoding="utf-8")
        self.microsystem_prompt_path = root / "microsystem_prompt.md"
        self.microsystem_prompt_path.write_text("# Prompt de microsystème de test\ncontenu.", encoding="utf-8")
        self.execution_prompt_path = root / "execution_prompt.md"
        self.execution_prompt_path.write_text("# Prompt d'exécution de test\ncontenu.", encoding="utf-8")
        self.management_prompt_path = root / "management_prompt.md"
        self.management_prompt_path.write_text("# Prompt de gestion de test\ncontenu.", encoding="utf-8")
        self.runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=self.plugins_dir,
            concepts_dir=self.concepts_dir,
            microsystems_dir=self.microsystems_dir,
            execution_dir=self.execution_dir,
            management_dir=self.management_dir,
        )
        self.strategies_dir = root / "strategies"
        self.strategies = StrategyManager(strategies_dir=self.strategies_dir, runs=self.runs)
        self.server = ControlPanelServer(
            runs=self.runs, strategies=self.strategies, host="127.0.0.1", port=0,
            prompt_doc_path=self.prompt_path,
            concept_prompt_doc_path=self.concept_prompt_path,
            microsystem_prompt_doc_path=self.microsystem_prompt_path,
            execution_prompt_doc_path=self.execution_prompt_path,
            management_prompt_doc_path=self.management_prompt_path,
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
        server = ControlPanelServer(runs=self.runs, strategies=self.strategies, host="127.0.0.1", port=0)
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

    async def test_strategy_builder_page_is_served(self) -> None:
        head, _body = await self._request("GET", "/strategy/build")
        self.assertIn("200 OK", head)

    async def test_concepts_microsystems_execution_management_list_endpoints_start_empty(self) -> None:
        _head, concepts = await self._request("GET", "/api/concepts")
        self.assertEqual(concepts["concepts"], [])
        _head, microsystems = await self._request("GET", "/api/microsystems")
        self.assertEqual(microsystems["microsystems"], [])
        _head, execution = await self._request("GET", "/api/execution-profiles")
        self.assertEqual(execution["execution_profiles"], [])
        _head, management = await self._request("GET", "/api/management-profiles")
        self.assertEqual(management["management_profiles"], [])

    async def test_concept_import_and_list_round_trip(self) -> None:
        head, body = await self._request(
            "POST", "/api/concepts/import",
            json_body={
                "filename": "test_concept.py",
                "content": 'CONCEPT_INFO = {"label": "Test", "description": "...", "data_sources": ["chainlink"]}\n'
                           "def compute(context):\n    pass\n",
            },
        )
        self.assertIn("200 OK", head)
        self.assertTrue(body["recognized"])
        _head, concepts = await self._request("GET", "/api/concepts")
        self.assertIn("test_concept", [c["id"] for c in concepts["concepts"]])

    async def test_microsystem_import_and_list_round_trip(self) -> None:
        head, body = await self._request(
            "POST", "/api/microsystems/import",
            json_body={
                "filename": "test_micro.py",
                "content": 'MICROSYSTEM_INFO = {"label": "Test", "description": "...", "concept_inputs": ["x"]}\n'
                           "def compute(context):\n    pass\n",
            },
        )
        self.assertIn("200 OK", head)
        self.assertTrue(body["recognized"])
        _head, microsystems = await self._request("GET", "/api/microsystems")
        self.assertIn("test_micro", [m["id"] for m in microsystems["microsystems"]])

    async def test_execution_profile_import_and_list_round_trip(self) -> None:
        head, body = await self._request(
            "POST", "/api/execution-profiles/import",
            json_body={
                "filename": "test_exec.py",
                "content": 'EXECUTION_INFO = {"label": "Test", "description": "..."}\n'
                           "def execute(context):\n    pass\n",
            },
        )
        self.assertIn("200 OK", head)
        self.assertTrue(body["recognized"])
        _head, execution = await self._request("GET", "/api/execution-profiles")
        self.assertIn("test_exec", [e["id"] for e in execution["execution_profiles"]])

    async def test_management_profile_import_and_list_round_trip(self) -> None:
        head, body = await self._request(
            "POST", "/api/management-profiles/import",
            json_body={
                "filename": "test_management.py",
                "content": 'MANAGEMENT_INFO = {"label": "Test", "description": "..."}\n'
                           "def manage(context):\n    pass\n",
            },
        )
        self.assertIn("200 OK", head)
        self.assertTrue(body["recognized"])
        _head, management = await self._request("GET", "/api/management-profiles")
        self.assertIn("test_management", [m["id"] for m in management["management_profiles"]])

    async def test_view_plugin_source_found_and_not_found(self) -> None:
        await self._request(
            "POST", "/api/plugins/import",
            json_body={"filename": "viewable.py", "content": 'PLUGIN_INFO = {"label": "x", "description": "y"}\nasync def run(context): pass\n'},
        )
        head, body = await self._request("POST", "/api/plugins/view", json_body={"id": "viewable"})
        self.assertIn("200 OK", head)
        self.assertIn("PLUGIN_INFO", body["content"])

        head, body = await self._request("POST", "/api/plugins/view", json_body={"id": "nope"})
        self.assertIn("404", head)

    async def test_view_concept_source_found_and_not_found(self) -> None:
        await self._request(
            "POST", "/api/concepts/import",
            json_body={
                "filename": "viewable.py",
                "content": 'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["chainlink"]}\ndef compute(context): pass\n',
            },
        )
        head, body = await self._request("POST", "/api/concepts/view", json_body={"id": "viewable"})
        self.assertIn("200 OK", head)
        self.assertIn("CONCEPT_INFO", body["content"])

        head, body = await self._request("POST", "/api/concepts/view", json_body={"id": "nope"})
        self.assertIn("404", head)

    async def test_concept_prompt_endpoint_embeds_selection(self) -> None:
        head, body = await self._request(
            "POST", "/api/concept-prompt", json_body={"sources": ["chainlink"], "plugins": []},
        )
        self.assertIn("200 OK", head)
        self.assertIn("Prompt de concept de test", body["content"])
        self.assertIn("chainlink", body["content"])

    async def test_concept_prompt_endpoint_400_on_empty_selection(self) -> None:
        head, body = await self._request(
            "POST", "/api/concept-prompt", json_body={"sources": [], "plugins": []},
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_concept_prompt_endpoint_400_on_malformed_body(self) -> None:
        head, body = await self._request(
            "POST", "/api/concept-prompt", json_body={"sources": "not a list", "plugins": []},
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_microsystem_prompt_endpoint_embeds_selection(self) -> None:
        head, body = await self._request(
            "POST", "/api/microsystem-prompt",
            json_body={"concepts": [], "sources": ["chainlink"], "plugins": []},
        )
        self.assertIn("200 OK", head)
        self.assertIn("Prompt de microsystème de test", body["content"])

    async def test_execution_prompt_endpoint_returns_static_document(self) -> None:
        head, body = await self._request("GET", "/api/execution-prompt")
        self.assertIn("200 OK", head)
        self.assertIn("Prompt d'exécution de test", body["content"])

    async def test_management_prompt_endpoint_returns_static_document(self) -> None:
        head, body = await self._request("GET", "/api/management-prompt")
        self.assertIn("200 OK", head)
        self.assertIn("Prompt de gestion de test", body["content"])

    async def test_strategies_list_starts_with_only_the_repo_seed_absent(self) -> None:
        # Isolated strategies_dir -- the real repo's model_base_polymarket.json
        # must not leak into this test's view.
        _head, body = await self._request("GET", "/api/strategies")
        self.assertEqual(body["strategies"], [])

    async def test_save_and_list_strategy_round_trip(self) -> None:
        head, body = await self._request(
            "POST", "/api/strategies",
            json_body={"name": "my_strategy", "concepts": [], "microsystems": [], "execution": None, "management": None},
        )
        self.assertIn("200 OK", head)
        self.assertEqual(body["name"], "my_strategy")
        _head, listed = await self._request("GET", "/api/strategies")
        self.assertIn("my_strategy", [s["name"] for s in listed["strategies"]])

    async def test_save_strategy_unknown_concept_id_is_400(self) -> None:
        head, body = await self._request(
            "POST", "/api/strategies",
            json_body={
                "name": "bad", "concepts": [{"instance_id": "c1", "concept_id": "nope", "config": {}}],
                "microsystems": [], "execution": None, "management": None,
            },
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_concepts_endpoint_exposes_data_requirements(self) -> None:
        await self._request(
            "POST", "/api/concepts/import",
            json_body={
                "filename": "zscore.py",
                "content": 'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
                           "def compute(context):\n    pass\n",
            },
        )
        _head, body = await self._request("GET", "/api/concepts")
        concept = next(c for c in body["concepts"] if c["id"] == "zscore")
        self.assertEqual(concept["data_requirements"][0]["type"], "Bougies")
        self.assertTrue(concept["data_requirements"][0]["swappable"])

    async def test_save_strategy_wrong_type_data_binding_is_400(self) -> None:
        await self._request(
            "POST", "/api/concepts/import",
            json_body={
                "filename": "zscore.py",
                "content": 'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
                           "def compute(context):\n    pass\n",
            },
        )
        head, body = await self._request(
            "POST", "/api/strategies",
            json_body={
                "name": "bad",
                "concepts": [{
                    "instance_id": "c1", "concept_id": "zscore", "config": {},
                    "data_bindings": {"Bougies": "chainlink"},
                }],
                "microsystems": [], "execution": None, "management": None,
            },
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_save_strategy_duplicate_name_is_409(self) -> None:
        await self._request(
            "POST", "/api/strategies",
            json_body={"name": "dup", "concepts": [], "microsystems": [], "execution": None, "management": None},
        )
        head, body = await self._request(
            "POST", "/api/strategies",
            json_body={"name": "dup", "concepts": [], "microsystems": [], "execution": None, "management": None},
        )
        self.assertIn("409", head)
        self.assertTrue(body["exists"])

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

    async def test_get_strategy_unknown_is_404(self) -> None:
        head, _body = await self._request("GET", "/api/strategy?name=nope")
        self.assertIn("404", head)

    async def test_get_strategy_missing_query_param_is_400(self) -> None:
        head, _body = await self._request("GET", "/api/strategy")
        self.assertIn("400", head)

    async def test_get_strategy_returns_full_definition(self) -> None:
        await self._request(
            "POST", "/api/strategies",
            json_body={"name": "full", "concepts": [], "microsystems": [], "execution": None, "management": None},
        )
        _head, body = await self._request("GET", "/api/strategy?name=full")
        self.assertEqual(body["name"], "full")
        self.assertEqual(body["concepts"], [])

    async def test_backtest_eligibility_unknown_strategy_is_404(self) -> None:
        head, _body = await self._request("GET", "/api/backtest/eligibility?strategy=nope")
        self.assertIn("404", head)

    async def test_backtest_eligibility_missing_query_param_is_400(self) -> None:
        head, _body = await self._request("GET", "/api/backtest/eligibility")
        self.assertIn("400", head)

    async def test_backtest_eligibility_reports_missing_execution_management_and_data(self) -> None:
        await self._request(
            "POST", "/api/strategies",
            json_body={"name": "bare", "concepts": [], "microsystems": [], "execution": None, "management": None},
        )
        _head, body = await self._request("GET", "/api/backtest/eligibility?strategy=bare")
        self.assertTrue(body["missing_execution"])
        self.assertTrue(body["missing_management"])
        self.assertEqual(body["coverage"], [])

    async def test_backtest_eligibility_reports_missing_data_keys_when_nothing_collected(self) -> None:
        await self._request(
            "POST", "/api/concepts/import",
            json_body={
                "filename": "lp.py",
                "content": 'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_trade"]}\n'
                           "def compute(context):\n    pass\n",
            },
        )
        await self._request(
            "POST", "/api/execution-profiles/import",
            json_body={"filename": "e.py", "content": 'EXECUTION_INFO = {"label": "x", "description": "y"}\ndef execute(context):\n    pass\n'},
        )
        await self._request(
            "POST", "/api/management-profiles/import",
            json_body={"filename": "m.py", "content": 'MANAGEMENT_INFO = {"label": "x", "description": "y"}\ndef manage(context):\n    pass\n'},
        )
        await self._request(
            "POST", "/api/strategies",
            json_body={
                "name": "wired",
                "concepts": [{"instance_id": "c1", "concept_id": "lp", "config": {}}],
                "microsystems": [], "execution": {"execution_id": "e", "config": {}},
                "management": {"management_id": "m", "config": {}},
            },
        )
        _head, body = await self._request("GET", "/api/backtest/eligibility?strategy=wired")
        self.assertFalse(body["missing_execution"])
        self.assertFalse(body["missing_management"])
        self.assertIn("binance_futures_trade", body["missing_data_keys"])
        self.assertEqual(body["coverage"], [])

    async def test_backtest_run_unknown_strategy_is_404(self) -> None:
        head, _body = await self._request(
            "POST", "/api/backtest/run",
            json_body={
                "strategy": "nope", "instrument": "BTC", "start_ts": 0, "end_ts": 10, "cadence_seconds": 10,
            },
        )
        self.assertIn("404", head)

    async def test_backtest_run_invalid_range_is_400(self) -> None:
        await self._request(
            "POST", "/api/strategies",
            json_body={"name": "any", "concepts": [], "microsystems": [], "execution": None, "management": None},
        )
        head, body = await self._request(
            "POST", "/api/backtest/run",
            json_body={
                "strategy": "any", "instrument": "BTC", "start_ts": 10, "end_ts": 5, "cadence_seconds": 10,
            },
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_backtest_status_unknown_job_is_404(self) -> None:
        head, _body = await self._request("GET", "/api/backtest/status?id=nope")
        self.assertIn("404", head)

    async def test_backtest_run_and_status_round_trip(self) -> None:
        await self._request(
            "POST", "/api/concepts/import",
            json_body={
                "filename": "lp.py",
                "content": (
                    'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_trade"]}\n'
                    "def compute(context):\n"
                    '    trades = context.data.get("binance_futures_trade") or []\n'
                    '    return {"last_price": trades[-1]["price"] if trades else None}\n'
                ),
            },
        )
        await self._request(
            "POST", "/api/microsystems/import",
            json_body={
                "filename": "pt.py",
                "content": (
                    'MICROSYSTEM_INFO = {"label": "x", "description": "y", "concept_inputs": ["lp"]}\n'
                    "def compute(context):\n"
                    '    result = context.concepts.get("lp") or {}\n'
                    '    return {"last_price": result.get("last_price")}\n'
                ),
            },
        )
        await self._request(
            "POST", "/api/execution-profiles/import",
            json_body={
                "filename": "e.py",
                "content": 'EXECUTION_INFO = {"label": "x", "description": "y"}\ndef execute(context):\n    return {"direction": "long"}\n',
            },
        )
        await self._request(
            "POST", "/api/strategies",
            json_body={
                "name": "roundtrip",
                "concepts": [{"instance_id": "c1", "concept_id": "lp", "config": {}}],
                "microsystems": [{
                    "instance_id": "m1", "microsystem_id": "pt", "concept_instance_ids": ["c1"], "config": {},
                }],
                "execution": {"execution_id": "e", "config": {}},
                "management": None,
            },
        )

        run_dir = self.runs.collections_dir / "run1"
        run_dir.mkdir(parents=True)
        from decimal import Decimal
        import time as time_module

        from polymarket_btc.data_collection.market_data.models import (
            BinanceAggTradePayload, EventSource, EventStream, MarketDataEvent, TakerSide,
        )
        from polymarket_btc.data_collection.market_data.storage import RawEventStorage

        storage = RawEventStorage(run_dir, zstd_level=3)
        for i, (t_seconds, price) in enumerate([(0, 100), (9, 105)]):
            ts_ns = int(t_seconds * 1e9)
            payload = BinanceAggTradePayload(
                symbol="BTCUSDT", aggregate_trade_id=i, price=Decimal(str(price)), quantity=Decimal("0.1"),
                first_trade_id=i, last_trade_id=i, trade_timestamp_ns=ts_ns, taker_side=TakerSide.BUY,
            )
            event = MarketDataEvent(
                schema_version=2, ingest_sequence=i, event_id=f"evt-{i}",
                source=EventSource.BINANCE_FUTURES_TRADE, stream=EventStream.BINANCE_FUTURES_AGG_TRADE,
                instrument="BTCUSDT", source_timestamp_ns=ts_ns, server_timestamp_ns=ts_ns,
                received_wall_timestamp_ns=ts_ns, received_monotonic_ns=time_module.monotonic_ns(),
                source_sequence=None, timeframe=None, market_id=None, condition_id=None,
                asset_id=None, outcome=None, payload=payload,
            )
            storage.write(event)
        storage.close()
        manifest = {
            "run_id": "run1", "mode": "access", "sources": ["binance_futures_trade"], "plugins": [],
            "start_ts_utc": "2026-01-01T00:00:00", "end_ts_utc": "2026-01-01T01:00:00",
            "data_dir": str(run_dir), "error": None, "duration_seconds": None,
            "snapshot_row_count": 0, "dataset_file": None, "plugin_files": [], "raw_files": [],
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        _head, eligibility = await self._request("GET", "/api/backtest/eligibility?strategy=roundtrip")
        self.assertFalse(eligibility["missing_execution"])
        self.assertEqual(eligibility["missing_data_keys"], [])
        self.assertEqual(eligibility["coverage"], [["2026-01-01T00:00:00", "2026-01-01T01:00:00"]])

        head, body = await self._request(
            "POST", "/api/backtest/run",
            json_body={
                "strategy": "roundtrip", "instrument": "BTC",
                "start_ts": 0, "end_ts": 10, "cadence_seconds": 10,
            },
        )
        self.assertIn("200 OK", head)
        job_id = body["job_id"]

        status = None
        for _ in range(100):
            _head, status = await self._request("GET", f"/api/backtest/status?id={job_id}")
            if status["done"]:
                break
            await asyncio.sleep(0.02)
        self.assertTrue(status["done"])
        self.assertIsNone(status["error"])
        self.assertEqual(status["result"]["best"]["trades"], 1)
        self.assertEqual(status["result"]["best"]["wins"], 1)


if __name__ == "__main__":
    unittest.main()
