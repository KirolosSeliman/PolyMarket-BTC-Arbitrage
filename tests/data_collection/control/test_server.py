import asyncio
from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from polymarket_btc.data_collection.control.concept_generation import ConceptGenerationManager
from polymarket_btc.data_collection.control.concept_refinement import ConceptRefinementManager
from polymarket_btc.data_collection.control.microsystem_refinement import MicrosystemRefinementManager
from polymarket_btc.data_collection.control.runs import CollectionRunManager
from polymarket_btc.data_collection.control.server import ControlPanelServer
from polymarket_btc.data_collection.control.strategies import StrategyManager
from polymarket_btc.data_collection.control.strategy_filter_refinement import StrategyFilterRefinementManager

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
            filter_dir=REPOSITORY_ROOT / "filter_profiles",
        )
        self.strategies = StrategyManager(
            strategies_dir=REPOSITORY_ROOT / "strategies", runs=self.runs,
        )
        self.concept_feedback = ConceptRefinementManager(
            feedback_dir=root / "concept_feedback", runs=self.runs,
        )
        self.microsystem_feedback = MicrosystemRefinementManager(
            feedback_dir=root / "microsystem_feedback", runs=self.runs,
        )
        self.filter_feedback = StrategyFilterRefinementManager(
            feedback_dir=root / "filter_feedback", runs=self.runs, strategies=self.strategies,
        )
        self.server = ControlPanelServer(
            runs=self.runs, strategies=self.strategies, concept_feedback=self.concept_feedback,
            microsystem_feedback=self.microsystem_feedback, filter_feedback=self.filter_feedback,
            host="127.0.0.1", port=0,
        )
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

    async def test_start_rejects_an_unknown_kline_interval(self) -> None:
        head, body = await self._request(
            "POST", "/api/collect/start",
            json_body={
                "sources": ["binance_futures_kline"], "plugins": [], "mode": "access",
                "kline_interval": "10m",
            },
        )
        self.assertIn("400", head)
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

    async def test_delete_run_filesystem_failure_is_a_clean_500_not_a_dropped_connection(self) -> None:
        """Left uncaught, an OSError from shutil.rmtree (permission denied,
        a locked file, ...) would hit _handle_connection's generic except
        Exception and close the connection with no response at all --
        indistinguishable from a network failure client-side, and nothing
        like the clear error message the UI is built to show."""
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

        with patch(
            "polymarket_btc.data_collection.control.runs.CollectionRunManager.delete_run",
            side_effect=OSError("permission denied"),
        ):
            head, body = await self._request("POST", "/api/runs/delete", json_body={"run_id": run_id})
        self.assertIn("500", head)
        self.assertIn("error", body)

    async def test_strategy_example_preview_runs_the_real_fvg_concept_with_real_candles(self) -> None:
        """Uses the repo's own shipped fvg concept (reads binance_futures_kline
        directly, not synthesized trades) end-to-end through the real HTTP
        route -- the builder's per-concept "i" preview sends exactly this
        shape: one concept instance alone, no microsystems/execution/
        management."""
        head, body = await self._request(
            "POST", "/api/strategies/example",
            json_body={
                "concepts": [{"instance_id": "c1", "concept_id": "fvg", "config": {}}],
                "microsystems": [], "execution": None, "management": None,
            },
        )
        self.assertIn("200 OK", head)
        self.assertIsNotNone(body["candles"])
        self.assertGreater(len(body["candles"]), 0)
        self.assertGreater(body["evaluation_steps"], 0)
        self.assertIn("c1", body["timeline"][-1]["concepts"])
        self.assertEqual(body["trades"], [])

    async def test_strategy_example_preview_unknown_concept_is_400(self) -> None:
        head, body = await self._request(
            "POST", "/api/strategies/example",
            json_body={
                "concepts": [{"instance_id": "c1", "concept_id": "nope", "config": {}}],
                "microsystems": [], "execution": None, "management": None,
            },
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_strategy_example_preview_invalid_body_shape_is_400(self) -> None:
        head, body = await self._request(
            "POST", "/api/strategies/example", json_body={"concepts": "not-a-list"},
        )
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_strategy_example_preview_seed_changes_the_scenario(self) -> None:
        body_for = {
            "concepts": [{"instance_id": "c1", "concept_id": "fvg", "config": {}}],
            "microsystems": [], "execution": None, "management": None,
        }
        _head, default_seed = await self._request("POST", "/api/strategies/example", json_body=body_for)
        _head, explicit_same_seed = await self._request(
            "POST", "/api/strategies/example", json_body={**body_for, "seed": 42},
        )
        _head, other_seed = await self._request(
            "POST", "/api/strategies/example", json_body={**body_for, "seed": 7},
        )
        self.assertEqual(default_seed["candles"], explicit_same_seed["candles"])
        self.assertNotEqual(default_seed["candles"], other_seed["candles"])

    async def test_strategy_example_preview_invalid_seed_is_400(self) -> None:
        head, body = await self._request(
            "POST", "/api/strategies/example",
            json_body={"concepts": [], "microsystems": [], "execution": None, "management": None, "seed": "nope"},
        )
        self.assertIn("400", head)
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
        self.filter_dir = root / "filter_profiles"
        self.concept_prompt_path = root / "concept_prompt.md"
        self.concept_prompt_path.write_text("# Prompt de concept de test\ncontenu.", encoding="utf-8")
        self.microsystem_prompt_path = root / "microsystem_prompt.md"
        self.microsystem_prompt_path.write_text("# Prompt de microsystème de test\ncontenu.", encoding="utf-8")
        self.execution_prompt_path = root / "execution_prompt.md"
        self.execution_prompt_path.write_text("# Prompt d'exécution de test\ncontenu.", encoding="utf-8")
        self.management_prompt_path = root / "management_prompt.md"
        self.management_prompt_path.write_text("# Prompt de gestion de test\ncontenu.", encoding="utf-8")
        self.filter_prompt_path = root / "filter_prompt.md"
        self.filter_prompt_path.write_text("# Prompt de filtre de test\ncontenu.", encoding="utf-8")
        self.runs = CollectionRunManager(
            config_path=REPOSITORY_ROOT / "config" / "market_data.toml",
            collections_dir=root / "collections",
            symbol_cache_path=_seeded_symbol_cache(root),
            plugins_dir=self.plugins_dir,
            concepts_dir=self.concepts_dir,
            microsystems_dir=self.microsystems_dir,
            execution_dir=self.execution_dir,
            management_dir=self.management_dir,
            filter_dir=self.filter_dir,
        )
        self.strategies_dir = root / "strategies"
        self.strategies = StrategyManager(strategies_dir=self.strategies_dir, runs=self.runs)
        self.concept_feedback = ConceptRefinementManager(
            feedback_dir=root / "concept_feedback", runs=self.runs,
        )
        self.microsystem_feedback = MicrosystemRefinementManager(
            feedback_dir=root / "microsystem_feedback", runs=self.runs,
        )
        self.filter_feedback = StrategyFilterRefinementManager(
            feedback_dir=root / "filter_feedback", runs=self.runs, strategies=self.strategies,
        )
        self.concept_generation = ConceptGenerationManager(
            runs=self.runs, command=["claude"], timeout_seconds=30,
        )
        self.server = ControlPanelServer(
            runs=self.runs, strategies=self.strategies, concept_feedback=self.concept_feedback,
            microsystem_feedback=self.microsystem_feedback, filter_feedback=self.filter_feedback,
            concept_generation=self.concept_generation,
            host="127.0.0.1", port=0,
            prompt_doc_path=self.prompt_path,
            concept_prompt_doc_path=self.concept_prompt_path,
            microsystem_prompt_doc_path=self.microsystem_prompt_path,
            execution_prompt_doc_path=self.execution_prompt_path,
            management_prompt_doc_path=self.management_prompt_path,
            filter_prompt_doc_path=self.filter_prompt_path,
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
        server = ControlPanelServer(
            runs=self.runs, strategies=self.strategies, concept_feedback=self.concept_feedback,
            microsystem_feedback=self.microsystem_feedback, filter_feedback=self.filter_feedback,
            host="127.0.0.1", port=0,
        )
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

    async def test_delete_strategy_removes_it_from_the_list(self) -> None:
        await self._request(
            "POST", "/api/strategies",
            json_body={"name": "to_delete", "concepts": [], "microsystems": [], "execution": None, "management": None},
        )
        head, body = await self._request("POST", "/api/strategies/delete", json_body={"name": "to_delete"})
        self.assertIn("200 OK", head)
        self.assertTrue(body["ok"])
        _head, listed = await self._request("GET", "/api/strategies")
        self.assertNotIn("to_delete", [s["name"] for s in listed["strategies"]])

    async def test_delete_strategy_missing_name_is_400(self) -> None:
        head, body = await self._request("POST", "/api/strategies/delete", json_body={})
        self.assertIn("400", head)
        self.assertIn("error", body)

    async def test_delete_strategy_unknown_name_is_404(self) -> None:
        head, body = await self._request("POST", "/api/strategies/delete", json_body={"name": "nope"})
        self.assertIn("404", head)
        self.assertIn("error", body)

    async def test_delete_strategy_filesystem_failure_is_a_clean_500_not_a_dropped_connection(self) -> None:
        await self._request(
            "POST", "/api/strategies",
            json_body={"name": "will_fail", "concepts": [], "microsystems": [], "execution": None, "management": None},
        )
        with patch(
            "polymarket_btc.data_collection.control.strategies.StrategyManager.delete_strategy",
            side_effect=OSError("permission denied"),
        ):
            head, body = await self._request("POST", "/api/strategies/delete", json_body={"name": "will_fail"})
        self.assertIn("500", head)
        self.assertIn("error", body)

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

    async def test_concept_refine_page_serves_200(self) -> None:
        head, _body = await self._request("GET", "/concept/refine?id=whatever")
        self.assertIn("200 OK", head)

    def _write_zone_concept_fixture(self, up_candle_count: int) -> None:
        """Writes a concept (via the filesystem directly, not the import
        endpoint -- equivalent end state, less noise in a test that's
        mostly about the /api/concept-refine/* routes) that emits one zone
        candidate per up-candle, plus enough real collected kline data to
        find up_candle_count of them."""
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        (self.concepts_dir / "zone_route_test.py").write_text(
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n"
            '    candles = context.data.get("binance_futures_kline") or []\n'
            "    zones = []\n"
            "    for c in candles:\n"
            '        if c["close"] > c["open"]:\n'
            '            zones.append({"direction": "bullish", "high": c["high"], "low": c["low"], "formed_at": c["timestamp"]})\n'
            '    return {"zones": zones}\n',
            encoding="utf-8",
        )
        run_dir = self.runs.collections_dir / "run1"
        from decimal import Decimal
        import time as time_module

        from polymarket_btc.data_collection.market_data.models import (
            BinanceKlinePayload, EventSource, EventStream, MarketDataEvent,
        )
        from polymarket_btc.data_collection.market_data.storage import RawEventStorage

        storage = RawEventStorage(run_dir, zstd_level=3)
        candles = []
        t = 0.0
        for i in range(up_candle_count):
            candles.append((t, 100, 101, 99, 99))  # down filler
            t += 60.0
            candles.append((t, 99, 103 + i, 99, 102))  # up
            t += 60.0
        for i, (ts, o, h, low, c) in enumerate(candles):
            close_ns = int(ts * 1e9)
            payload = BinanceKlinePayload(
                market="futures", interval="1m", open_time_ns=close_ns - 60_000_000_000, close_time_ns=close_ns,
                open=Decimal(str(o)), high=Decimal(str(h)), low=Decimal(str(low)), close=Decimal(str(c)),
                base_volume=Decimal("1"), quote_volume=Decimal("1"), trade_count=1, is_closed=True,
            )
            event = MarketDataEvent(
                schema_version=2, ingest_sequence=i, event_id=f"kline-{i}",
                source=EventSource.BINANCE_FUTURES_KLINE, stream=EventStream.BINANCE_KLINE,
                instrument="BTCUSDT", source_timestamp_ns=close_ns, server_timestamp_ns=close_ns,
                received_wall_timestamp_ns=close_ns, received_monotonic_ns=time_module.monotonic_ns(),
                source_sequence=None, timeframe=None, market_id=None, condition_id=None,
                asset_id=None, outcome=None, payload=payload,
            )
            storage.write(event)
        storage.close()
        from datetime import UTC, datetime
        manifest = {
            "mode": "access", "sources": ["binance_futures_kline"], "plugins": [],
            # Must bound the *actual* candle timestamps above (epoch-relative,
            # not a real calendar date) -- combined_coverage/read_records
            # only ever look at this declared range, not the raw data itself,
            # so a mismatch here silently scans zero records.
            "start_ts_utc": datetime.fromtimestamp(-60.0, tz=UTC).isoformat(),
            "end_ts_utc": datetime.fromtimestamp(t + 60.0, tz=UTC).isoformat(),
            "data_dir": str(run_dir),
        }
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    async def _run_scan_and_wait(self, concept_id: str) -> dict:
        head, body = await self._request(
            "POST", "/api/concept-refine/scan", json_body={"concept_id": concept_id},
        )
        self.assertIn("200 OK", head)
        job_id = body["job_id"]
        status = None
        for _ in range(200):
            _head, status = await self._request("GET", f"/api/concept-refine/scan-status?job_id={job_id}")
            if status["done"]:
                break
            await asyncio.sleep(0.02)
        self.assertTrue(status["done"])
        return status

    async def test_concept_refine_full_round_trip(self) -> None:
        self._write_zone_concept_fixture(up_candle_count=12)
        status = await self._run_scan_and_wait("zone_route_test")
        self.assertIsNone(status["error"])
        self.assertEqual(status["result"]["added"], 12)

        # Below the gate: 9 "oui" only -- no "non" at all yet.
        for _ in range(9):
            _head, next_body = await self._request(
                "POST", "/api/concept-refine/next", json_body={"concept_id": "zone_route_test"},
            )
            instance = next_body["instance"]
            await self._request(
                "POST", "/api/concept-refine/label",
                json_body={
                    "concept_id": "zone_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "oui",
                },
            )
        head, prompt_body = await self._request(
            "POST", "/api/concept-refine/prompt", json_body={"concept_id": "zone_route_test"},
        )
        self.assertIn("400 Bad Request", head)

        # The 10th example, labeled "non" -- now both gates (>=10 total,
        # >=1 non) are met.
        _head, next_body = await self._request(
            "POST", "/api/concept-refine/next", json_body={"concept_id": "zone_route_test"},
        )
        instance = next_body["instance"]
        head, label_body = await self._request(
            "POST", "/api/concept-refine/label",
            json_body={
                "concept_id": "zone_route_test", "shape": instance["shape"], "node": instance["node"],
                "trigger_ts": instance["trigger_ts"], "label": "non", "note": "il manque la nuance X",
            },
        )
        self.assertIn("200 OK", head)
        self.assertTrue(label_body["eligible_for_prompt"])

        head, prompt_body = await self._request(
            "POST", "/api/concept-refine/prompt", json_body={"concept_id": "zone_route_test"},
        )
        self.assertIn("200 OK", head)
        self.assertIn("il manque la nuance X", prompt_body["content"])

    async def test_concept_refine_label_triggers_auto_refine_job_exactly_once(self) -> None:
        # claude_code_command defaults to None (disabled) on the shared
        # fixture's concept_feedback -- enable it in place for this test
        # only, without losing the manager's own feedback_dir/state.
        self.server.concept_feedback.claude_code_command = ["claude"]
        self._write_zone_concept_fixture(up_candle_count=12)
        await self._run_scan_and_wait("zone_route_test")
        for _ in range(9):
            _head, next_body = await self._request(
                "POST", "/api/concept-refine/next", json_body={"concept_id": "zone_route_test"},
            )
            instance = next_body["instance"]
            head, label_body = await self._request(
                "POST", "/api/concept-refine/label",
                json_body={
                    "concept_id": "zone_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "oui",
                },
            )
            self.assertNotIn("auto_refine_job_id", label_body)  # not eligible yet

        with patch(
            "polymarket_btc.data_collection.control.concept_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {"filename": "zone_route_test.py", "content": "CONCEPT_INFO = {}\n"}
            _head, next_body = await self._request(
                "POST", "/api/concept-refine/next", json_body={"concept_id": "zone_route_test"},
            )
            instance = next_body["instance"]
            head, label_body = await self._request(
                "POST", "/api/concept-refine/label",
                json_body={
                    "concept_id": "zone_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "non", "note": "il manque la nuance X",
                },
            )
            self.assertIn("200 OK", head)
            job_id = label_body["auto_refine_job_id"]
            status = None
            for _ in range(200):
                _head, status = await self._request("GET", f"/api/concept-refine/scan-status?job_id={job_id}")
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(status["done"])
            self.assertIsNone(status["error"])
            self.assertEqual(status["result"]["filename"], "zone_route_test_auto.py")
            self.assertTrue((self.concepts_dir / "zone_route_test_auto.py").is_file())

            # A further "oui" label, still eligible, must not refire.
            mock_generate.reset_mock()
            _head, next_body = await self._request(
                "POST", "/api/concept-refine/next", json_body={"concept_id": "zone_route_test"},
            )
            instance = next_body["instance"]
            _head, label_body = await self._request(
                "POST", "/api/concept-refine/label",
                json_body={
                    "concept_id": "zone_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "oui",
                },
            )
            self.assertNotIn("auto_refine_job_id", label_body)
            mock_generate.assert_not_called()

    async def test_concept_refine_label_non_without_note_is_400(self) -> None:
        self._write_zone_concept_fixture(up_candle_count=1)
        await self._run_scan_and_wait("zone_route_test")
        _head, next_body = await self._request(
            "POST", "/api/concept-refine/next", json_body={"concept_id": "zone_route_test"},
        )
        instance = next_body["instance"]
        head, body = await self._request(
            "POST", "/api/concept-refine/label",
            json_body={
                "concept_id": "zone_route_test", "shape": instance["shape"], "node": instance["node"],
                "label": "non", "note": "",
            },
        )
        self.assertIn("400 Bad Request", head)

    async def test_concept_refine_scan_unknown_concept_reports_error_via_status(self) -> None:
        status = await self._run_scan_and_wait("no_such_concept")
        self.assertIsNotNone(status["error"])
        self.assertIn("no_such_concept", status["error"])

    async def test_concept_refine_next_missing_concept_id_is_400(self) -> None:
        head, _body = await self._request("POST", "/api/concept-refine/next", json_body={})
        self.assertIn("400 Bad Request", head)

    async def test_microsystem_refine_page_serves_200(self) -> None:
        head, _body = await self._request("GET", "/microsystem/refine?id=whatever")
        self.assertIn("200 OK", head)

    def _write_setup_microsystem_fixture(self, up_candle_count: int) -> None:
        """Mirrors _write_zone_concept_fixture, with an extra microsystem
        layer wired to the same zone concept -- each up-candle's zone
        becomes one compound "setup" candidate (entry_zone +
        confirmation_level), the shape /api/microsystem-refine/* judges as
        a whole, not fragmented into its own zone/level sub-fields."""
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        (self.concepts_dir / "zone_route_test.py").write_text(
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n"
            '    candles = context.data.get("binance_futures_kline") or []\n'
            "    zones = []\n"
            "    for c in candles:\n"
            '        if c["close"] > c["open"]:\n'
            '            zones.append({"direction": "bullish", "high": c["high"], "low": c["low"], "formed_at": c["timestamp"]})\n'
            '    return {"zones": zones}\n',
            encoding="utf-8",
        )
        self.microsystems_dir.mkdir(parents=True, exist_ok=True)
        (self.microsystems_dir / "setup_route_test.py").write_text(
            'MICROSYSTEM_INFO = {"label": "x", "description": "y", "concept_inputs": ["zone_route_test"]}\n'
            "def compute(context):\n"
            '    zone_result = context.concepts.get("zone_route_test") or {}\n'
            '    zones = zone_result.get("zones") or []\n'
            "    setups = []\n"
            "    for z in zones:\n"
            "        setups.append({\n"
            '            "signal": "haussier", "entry_zone": z,\n'
            '            "confirmation_level": {\n'
            '                "direction": "bullish", "level": z["low"],\n'
            '                "formed_at": z["formed_at"], "swept_at": z["formed_at"] + 30.0,\n'
            "            },\n"
            "        })\n"
            '    return {"setups": setups}\n',
            encoding="utf-8",
        )
        run_dir = self.runs.collections_dir / "run1"
        from decimal import Decimal
        import time as time_module

        from polymarket_btc.data_collection.market_data.models import (
            BinanceKlinePayload, EventSource, EventStream, MarketDataEvent,
        )
        from polymarket_btc.data_collection.market_data.storage import RawEventStorage

        storage = RawEventStorage(run_dir, zstd_level=3)
        candles = []
        t = 0.0
        for i in range(up_candle_count):
            candles.append((t, 100, 101, 99, 99))  # down filler
            t += 60.0
            candles.append((t, 99, 103 + i, 99, 102))  # up
            t += 60.0
        for i, (ts, o, h, low, c) in enumerate(candles):
            close_ns = int(ts * 1e9)
            payload = BinanceKlinePayload(
                market="futures", interval="1m", open_time_ns=close_ns - 60_000_000_000, close_time_ns=close_ns,
                open=Decimal(str(o)), high=Decimal(str(h)), low=Decimal(str(low)), close=Decimal(str(c)),
                base_volume=Decimal("1"), quote_volume=Decimal("1"), trade_count=1, is_closed=True,
            )
            event = MarketDataEvent(
                schema_version=2, ingest_sequence=i, event_id=f"kline-{i}",
                source=EventSource.BINANCE_FUTURES_KLINE, stream=EventStream.BINANCE_KLINE,
                instrument="BTCUSDT", source_timestamp_ns=close_ns, server_timestamp_ns=close_ns,
                received_wall_timestamp_ns=close_ns, received_monotonic_ns=time_module.monotonic_ns(),
                source_sequence=None, timeframe=None, market_id=None, condition_id=None,
                asset_id=None, outcome=None, payload=payload,
            )
            storage.write(event)
        storage.close()
        from datetime import UTC, datetime
        manifest = {
            "mode": "access", "sources": ["binance_futures_kline"], "plugins": [],
            "start_ts_utc": datetime.fromtimestamp(-60.0, tz=UTC).isoformat(),
            "end_ts_utc": datetime.fromtimestamp(t + 60.0, tz=UTC).isoformat(),
            "data_dir": str(run_dir),
        }
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    async def _run_microsystem_scan_and_wait(self, microsystem_id: str) -> dict:
        head, body = await self._request(
            "POST", "/api/microsystem-refine/scan", json_body={"microsystem_id": microsystem_id},
        )
        self.assertIn("200 OK", head)
        job_id = body["job_id"]
        status = None
        for _ in range(200):
            _head, status = await self._request("GET", f"/api/microsystem-refine/scan-status?job_id={job_id}")
            if status["done"]:
                break
            await asyncio.sleep(0.02)
        self.assertTrue(status["done"])
        return status

    async def test_microsystem_refine_full_round_trip(self) -> None:
        self._write_setup_microsystem_fixture(up_candle_count=12)
        status = await self._run_microsystem_scan_and_wait("setup_route_test")
        self.assertIsNone(status["error"])
        self.assertEqual(status["result"]["added"], 12)

        # Below the gate: 9 "oui" only -- no "non" at all yet.
        for _ in range(9):
            _head, next_body = await self._request(
                "POST", "/api/microsystem-refine/next", json_body={"microsystem_id": "setup_route_test"},
            )
            instance = next_body["instance"]
            self.assertEqual(instance["shape"], "setup")
            await self._request(
                "POST", "/api/microsystem-refine/label",
                json_body={
                    "microsystem_id": "setup_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "oui",
                },
            )
        head, _prompt_body = await self._request(
            "POST", "/api/microsystem-refine/prompt", json_body={"microsystem_id": "setup_route_test"},
        )
        self.assertIn("400 Bad Request", head)

        # The 10th example, labeled "non" -- now both gates (>=10 total,
        # >=1 non) are met.
        _head, next_body = await self._request(
            "POST", "/api/microsystem-refine/next", json_body={"microsystem_id": "setup_route_test"},
        )
        instance = next_body["instance"]
        head, label_body = await self._request(
            "POST", "/api/microsystem-refine/label",
            json_body={
                "microsystem_id": "setup_route_test", "shape": instance["shape"], "node": instance["node"],
                "trigger_ts": instance["trigger_ts"], "label": "non", "note": "il manque la nuance Y",
            },
        )
        self.assertIn("200 OK", head)
        self.assertTrue(label_body["eligible_for_prompt"])

        head, prompt_body = await self._request(
            "POST", "/api/microsystem-refine/prompt", json_body={"microsystem_id": "setup_route_test"},
        )
        self.assertIn("200 OK", head)
        self.assertIn("il manque la nuance Y", prompt_body["content"])

    async def test_microsystem_refine_label_triggers_auto_refine_job(self) -> None:
        # Lighter than the concept version above -- the underlying manager
        # logic is fully covered in test_microsystem_refinement.py; this
        # only confirms the server wiring (label route -> job id -> status
        # route) actually connects for this flow too.
        self.server.microsystem_feedback.claude_code_command = ["claude"]
        self._write_setup_microsystem_fixture(up_candle_count=12)
        await self._run_microsystem_scan_and_wait("setup_route_test")
        for _ in range(9):
            _head, next_body = await self._request(
                "POST", "/api/microsystem-refine/next", json_body={"microsystem_id": "setup_route_test"},
            )
            instance = next_body["instance"]
            await self._request(
                "POST", "/api/microsystem-refine/label",
                json_body={
                    "microsystem_id": "setup_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "oui",
                },
            )
        with patch(
            "polymarket_btc.data_collection.control.microsystem_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {"filename": "setup_route_test.py", "content": "MICROSYSTEM_INFO = {}\n"}
            _head, next_body = await self._request(
                "POST", "/api/microsystem-refine/next", json_body={"microsystem_id": "setup_route_test"},
            )
            instance = next_body["instance"]
            _head, label_body = await self._request(
                "POST", "/api/microsystem-refine/label",
                json_body={
                    "microsystem_id": "setup_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "non", "note": "il manque la nuance Y",
                },
            )
            job_id = label_body["auto_refine_job_id"]
            status = None
            for _ in range(200):
                _head, status = await self._request("GET", f"/api/microsystem-refine/scan-status?job_id={job_id}")
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(status["done"])
            self.assertIsNone(status["error"])
            self.assertEqual(status["result"]["filename"], "setup_route_test_auto.py")
            self.assertTrue((self.microsystems_dir / "setup_route_test_auto.py").is_file())

    async def test_microsystem_refine_label_non_without_note_is_400(self) -> None:
        self._write_setup_microsystem_fixture(up_candle_count=1)
        await self._run_microsystem_scan_and_wait("setup_route_test")
        _head, next_body = await self._request(
            "POST", "/api/microsystem-refine/next", json_body={"microsystem_id": "setup_route_test"},
        )
        instance = next_body["instance"]
        head, _body = await self._request(
            "POST", "/api/microsystem-refine/label",
            json_body={
                "microsystem_id": "setup_route_test", "shape": instance["shape"], "node": instance["node"],
                "label": "non", "note": "",
            },
        )
        self.assertIn("400 Bad Request", head)

    async def test_microsystem_refine_scan_unknown_microsystem_reports_error_via_status(self) -> None:
        status = await self._run_microsystem_scan_and_wait("no_such_microsystem")
        self.assertIsNotNone(status["error"])
        self.assertIn("no_such_microsystem", status["error"])

    async def test_microsystem_refine_next_missing_microsystem_id_is_400(self) -> None:
        head, _body = await self._request("POST", "/api/microsystem-refine/next", json_body={})
        self.assertIn("400 Bad Request", head)

    async def test_filter_profiles_endpoint_lists_imported_profiles(self) -> None:
        head, body = await self._request(
            "POST", "/api/filter-profiles/import",
            json_body={
                "filename": "reward_risk_route_test.py",
                "content": 'FILTER_INFO = {"label": "x", "description": "y"}\ndef filter(context):\n    return None\n',
            },
        )
        self.assertIn("200 OK", head)
        head, body = await self._request("GET", "/api/filter-profiles")
        self.assertIn("200 OK", head)
        self.assertIn("reward_risk_route_test", [f["id"] for f in body["filter_profiles"]])

    async def test_filter_prompt_endpoint_returns_document_content(self) -> None:
        head, body = await self._request("GET", "/api/filter-prompt")
        self.assertIn("200 OK", head)
        self.assertIn("Prompt de filtre de test", body["content"])

    async def test_strategy_filter_refine_page_serves_200(self) -> None:
        head, _body = await self._request("GET", "/strategy-filter/refine?strategy=whatever")
        self.assertIn("200 OK", head)

    def _write_filter_refine_fixture(self, candle_count: int) -> None:
        """A strategy that always opens a long (ENTER_ONCE-style execution,
        unconditional) with fixed 5% SL/TP -- one trivial kline-reading
        concept (so backtest_eligibility's own required-keys coverage isn't
        vacuously empty, see combined_coverage's own "not required_keys ->
        []" rule) plus real kline data giving /api/strategy-filter-refine/*
        real trade candidates to find. Price alternates 100/106 every
        candle: each step's TP (105.0) or SL (95.0-ish, relative to
        whichever price a trade most recently opened at) is crossed by the
        very next candle, so a trade closes and a new one opens on almost
        every single step -- plenty of real, deterministic candidates."""
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        (self.concepts_dir / "kline_reader_route_test.py").write_text(
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n"
            '    return {"last_close": None}\n',
            encoding="utf-8",
        )
        self.execution_dir.mkdir(parents=True, exist_ok=True)
        (self.execution_dir / "enter_once_route_test.py").write_text(
            'EXECUTION_INFO = {"label": "x", "description": "y"}\n'
            "def execute(context):\n"
            '    return {"direction": "long"}\n',
            encoding="utf-8",
        )
        self.management_dir.mkdir(parents=True, exist_ok=True)
        (self.management_dir / "fixed_sltp_route_test.py").write_text(
            "MANAGEMENT_INFO = {\n"
            '    "label": "x", "description": "y",\n'
            '    "config_schema": [\n'
            '        {"name": "stop_loss_pct", "type": "number", "label": "SL", "default": 5.0},\n'
            '        {"name": "take_profit_pct", "type": "number", "label": "TP", "default": 5.0},\n'
            "    ],\n"
            "}\n"
            "def manage(context):\n"
            '    return {"stop_loss_pct": 5.0, "take_profit_pct": 5.0}\n',
            encoding="utf-8",
        )
        run_dir = self.runs.collections_dir / "run1"
        from decimal import Decimal
        import time as time_module

        from polymarket_btc.data_collection.market_data.models import (
            BinanceKlinePayload, EventSource, EventStream, MarketDataEvent,
        )
        from polymarket_btc.data_collection.market_data.storage import RawEventStorage

        storage = RawEventStorage(run_dir, zstd_level=3)
        t = 0.0
        for i in range(candle_count):
            price = 100.0 if i % 2 == 0 else 106.0
            close_ns = int(t * 1e9)
            payload = BinanceKlinePayload(
                market="futures", interval="1m", open_time_ns=close_ns - 60_000_000_000, close_time_ns=close_ns,
                open=Decimal(str(price)), high=Decimal(str(price)), low=Decimal(str(price)), close=Decimal(str(price)),
                base_volume=Decimal("1"), quote_volume=Decimal("1"), trade_count=1, is_closed=True,
            )
            event = MarketDataEvent(
                schema_version=2, ingest_sequence=i, event_id=f"kline-{i}",
                source=EventSource.BINANCE_FUTURES_KLINE, stream=EventStream.BINANCE_KLINE,
                instrument="BTCUSDT", source_timestamp_ns=close_ns, server_timestamp_ns=close_ns,
                received_wall_timestamp_ns=close_ns, received_monotonic_ns=time_module.monotonic_ns(),
                source_sequence=None, timeframe=None, market_id=None, condition_id=None,
                asset_id=None, outcome=None, payload=payload,
            )
            storage.write(event)
            t += 60.0
        storage.close()
        from datetime import UTC, datetime
        manifest = {
            "mode": "access", "sources": ["binance_futures_kline"], "plugins": [],
            "start_ts_utc": datetime.fromtimestamp(-60.0, tz=UTC).isoformat(),
            "end_ts_utc": datetime.fromtimestamp(t + 60.0, tz=UTC).isoformat(),
            "data_dir": str(run_dir),
        }
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        self.strategies.save_strategy(
            name="filter_route_test",
            concepts=[{
                "instance_id": "concept_1", "concept_id": "kline_reader_route_test",
                "config": {}, "data_bindings": {},
            }],
            microsystems=[],
            execution={"execution_id": "enter_once_route_test", "config": {}},
            management={"management_id": "fixed_sltp_route_test", "config": {}},
        )

    async def _run_filter_scan_and_wait(self, strategy_name: str) -> dict:
        head, body = await self._request(
            "POST", "/api/strategy-filter-refine/scan", json_body={"strategy_name": strategy_name},
        )
        self.assertIn("200 OK", head)
        job_id = body["job_id"]
        status = None
        for _ in range(300):
            _head, status = await self._request("GET", f"/api/strategy-filter-refine/scan-status?job_id={job_id}")
            if status["done"]:
                break
            await asyncio.sleep(0.02)
        self.assertTrue(status["done"])
        return status

    async def test_filter_refine_full_round_trip(self) -> None:
        self._write_filter_refine_fixture(candle_count=30)
        status = await self._run_filter_scan_and_wait("filter_route_test")
        self.assertIsNone(status["error"])
        self.assertGreaterEqual(status["result"]["added"], 10)

        for _ in range(9):
            _head, next_body = await self._request(
                "POST", "/api/strategy-filter-refine/next", json_body={"strategy_name": "filter_route_test"},
            )
            instance = next_body["instance"]
            self.assertEqual(instance["shape"], "trade")
            await self._request(
                "POST", "/api/strategy-filter-refine/label",
                json_body={
                    "strategy_name": "filter_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "oui",
                },
            )
        head, _prompt_body = await self._request(
            "POST", "/api/strategy-filter-refine/prompt", json_body={"strategy_name": "filter_route_test"},
        )
        self.assertIn("400 Bad Request", head)

        _head, next_body = await self._request(
            "POST", "/api/strategy-filter-refine/next", json_body={"strategy_name": "filter_route_test"},
        )
        instance = next_body["instance"]
        head, label_body = await self._request(
            "POST", "/api/strategy-filter-refine/label",
            json_body={
                "strategy_name": "filter_route_test", "shape": instance["shape"], "node": instance["node"],
                "trigger_ts": instance["trigger_ts"], "label": "non", "note": "ce trade n'aurait pas dû être pris",
            },
        )
        self.assertIn("200 OK", head)
        self.assertTrue(label_body["eligible_for_prompt"])

        head, prompt_body = await self._request(
            "POST", "/api/strategy-filter-refine/prompt", json_body={"strategy_name": "filter_route_test"},
        )
        self.assertIn("200 OK", head)
        self.assertIn("ce trade n'aurait pas dû être pris", prompt_body["content"])
        # No filter configured for this strategy yet -- confirms the
        # "build one from nothing" placeholder path, not a source read.
        self.assertIn("Aucun filtre", prompt_body["content"])

    async def test_filter_refine_label_triggers_auto_refine_job(self) -> None:
        # Lighter than the concept version above -- see that test's own
        # comment; this only confirms the server wiring for this flow too.
        self.server.filter_feedback.claude_code_command = ["claude"]
        self._write_filter_refine_fixture(candle_count=30)
        await self._run_filter_scan_and_wait("filter_route_test")
        for _ in range(9):
            _head, next_body = await self._request(
                "POST", "/api/strategy-filter-refine/next", json_body={"strategy_name": "filter_route_test"},
            )
            instance = next_body["instance"]
            await self._request(
                "POST", "/api/strategy-filter-refine/label",
                json_body={
                    "strategy_name": "filter_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "oui",
                },
            )
        with patch(
            "polymarket_btc.data_collection.control.strategy_filter_refinement.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {"filename": "new_filter.py", "content": "FILTER_INFO = {}\n"}
            _head, next_body = await self._request(
                "POST", "/api/strategy-filter-refine/next", json_body={"strategy_name": "filter_route_test"},
            )
            instance = next_body["instance"]
            _head, label_body = await self._request(
                "POST", "/api/strategy-filter-refine/label",
                json_body={
                    "strategy_name": "filter_route_test", "shape": instance["shape"], "node": instance["node"],
                    "trigger_ts": instance["trigger_ts"], "label": "non", "note": "ce trade n'aurait pas dû être pris",
                },
            )
            job_id = label_body["auto_refine_job_id"]
            status = None
            for _ in range(200):
                _head, status = await self._request(
                    "GET", f"/api/strategy-filter-refine/scan-status?job_id={job_id}",
                )
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(status["done"])
            self.assertIsNone(status["error"])
            self.assertEqual(status["result"]["filename"], "new_filter_auto.py")
            self.assertTrue((self.filter_dir / "new_filter_auto.py").is_file())

    async def test_filter_refine_label_non_without_note_is_400(self) -> None:
        self._write_filter_refine_fixture(candle_count=4)
        await self._run_filter_scan_and_wait("filter_route_test")
        _head, next_body = await self._request(
            "POST", "/api/strategy-filter-refine/next", json_body={"strategy_name": "filter_route_test"},
        )
        instance = next_body["instance"]
        head, _body = await self._request(
            "POST", "/api/strategy-filter-refine/label",
            json_body={
                "strategy_name": "filter_route_test", "shape": instance["shape"], "node": instance["node"],
                "label": "non", "note": "",
            },
        )
        self.assertIn("400 Bad Request", head)

    async def test_filter_refine_scan_unknown_strategy_reports_error_via_status(self) -> None:
        status = await self._run_filter_scan_and_wait("no_such_strategy")
        self.assertIsNotNone(status["error"])

    async def test_filter_refine_next_missing_strategy_name_is_400(self) -> None:
        head, _body = await self._request("POST", "/api/strategy-filter-refine/next", json_body={})
        self.assertIn("400 Bad Request", head)

    async def test_builder_duplicate_success_round_trip(self) -> None:
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        (self.concepts_dir / "zscore_route_test.py").write_text(
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n    return None\n",
            encoding="utf-8",
        )
        self.strategies.save_strategy(
            name="builder_route_test",
            concepts=[{"instance_id": "concept_1", "concept_id": "zscore_route_test", "config": {}}],
            microsystems=[], execution=None, management=None,
        )
        head, body = await self._request(
            "POST", "/api/builder/duplicate",
            json_body={
                "category": "concept", "source_id": "zscore_route_test",
                "new_filename": "zscore_route_test_builder_route_test.py", "strategy_name": "builder_route_test",
            },
        )
        self.assertIn("200 OK", head)
        self.assertEqual(body["new_id"], "zscore_route_test_builder_route_test")
        self.assertEqual(body["rebound_count"], 1)
        _head, strategy_body = await self._request("GET", "/api/strategy?name=builder_route_test")
        self.assertEqual(strategy_body["concepts"][0]["concept_id"], "zscore_route_test_builder_route_test")

    async def test_builder_duplicate_missing_fields_is_400(self) -> None:
        head, _body = await self._request("POST", "/api/builder/duplicate", json_body={"category": "concept"})
        self.assertIn("400 Bad Request", head)

    async def test_builder_duplicate_unknown_strategy_is_404(self) -> None:
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        (self.concepts_dir / "zscore_route_test2.py").write_text(
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n    return None\n",
            encoding="utf-8",
        )
        head, _body = await self._request(
            "POST", "/api/builder/duplicate",
            json_body={
                "category": "concept", "source_id": "zscore_route_test2",
                "new_filename": "zscore_route_test2_copy.py", "strategy_name": "no_such_strategy",
            },
        )
        self.assertIn("404 Not Found", head)

    async def test_builder_duplicate_filename_collision_is_409(self) -> None:
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        (self.concepts_dir / "zscore_route_test3.py").write_text(
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n    return None\n",
            encoding="utf-8",
        )
        (self.concepts_dir / "already_exists.py").write_text("# taken", encoding="utf-8")
        self.strategies.save_strategy(
            name="builder_route_test2",
            concepts=[{"instance_id": "concept_1", "concept_id": "zscore_route_test3", "config": {}}],
            microsystems=[], execution=None, management=None,
        )
        head, body = await self._request(
            "POST", "/api/builder/duplicate",
            json_body={
                "category": "concept", "source_id": "zscore_route_test3",
                "new_filename": "already_exists.py", "strategy_name": "builder_route_test2",
            },
        )
        self.assertIn("409 Conflict", head)
        self.assertTrue(body["exists"])

    async def test_builder_delete_success(self) -> None:
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        (self.concepts_dir / "zscore_delete_test.py").write_text(
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n    return None\n",
            encoding="utf-8",
        )
        head, body = await self._request(
            "POST", "/api/builder/delete", json_body={"category": "concept", "source_id": "zscore_delete_test"},
        )
        self.assertIn("200 OK", head)
        self.assertEqual(body, {"category": "concept", "id": "zscore_delete_test"})
        self.assertFalse((self.concepts_dir / "zscore_delete_test.py").exists())

    async def test_builder_delete_missing_fields_is_400(self) -> None:
        head, _body = await self._request("POST", "/api/builder/delete", json_body={"category": "concept"})
        self.assertIn("400 Bad Request", head)

    async def test_builder_delete_unknown_source_is_404(self) -> None:
        head, _body = await self._request(
            "POST", "/api/builder/delete", json_body={"category": "concept", "source_id": "no_such_concept"},
        )
        self.assertIn("404 Not Found", head)

    async def test_builder_delete_referenced_concept_is_400(self) -> None:
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        (self.concepts_dir / "zscore_delete_test2.py").write_text(
            'CONCEPT_INFO = {"label": "x", "description": "y", "data_sources": ["binance_futures_kline"]}\n'
            "def compute(context):\n    return None\n",
            encoding="utf-8",
        )
        self.strategies.save_strategy(
            name="delete_route_test",
            concepts=[{"instance_id": "concept_1", "concept_id": "zscore_delete_test2", "config": {}}],
            microsystems=[], execution=None, management=None,
        )
        head, body = await self._request(
            "POST", "/api/builder/delete", json_body={"category": "concept", "source_id": "zscore_delete_test2"},
        )
        self.assertIn("400 Bad Request", head)
        self.assertIn("delete_route_test", body["error"])
        self.assertTrue((self.concepts_dir / "zscore_delete_test2.py").exists())

    async def test_builder_page_serves_200(self) -> None:
        head, _body = await self._request("GET", "/builder")
        self.assertIn("200 OK", head)

    async def test_concept_prompt_generate_missing_prompt_is_400(self) -> None:
        head, _body = await self._request("POST", "/api/concept-prompt/generate", json_body={})
        self.assertIn("400 Bad Request", head)

    async def test_concept_prompt_generate_blank_prompt_is_400(self) -> None:
        # This route takes the exact final prompt the frontend already
        # built (and let the user preview/edit) via /api/concept-prompt --
        # a non-interactive `claude -p` call gets no back-and-forth, so an
        # empty prompt would have nothing to build from.
        head, _body = await self._request(
            "POST", "/api/concept-prompt/generate", json_body={"prompt": "   "},
        )
        self.assertIn("400 Bad Request", head)

    async def test_concept_prompt_generate_success_round_trip(self) -> None:
        well_formed = (
            "FILENAME: server_route_concept.py\n\n```python\n"
            'CONCEPT_INFO = {"label": "x", "description": "y", '
            '"data_sources": ["binance_futures_kline"]}\n\n'
            "def compute(context):\n    return {}\n```\n"
        )
        with patch(
            "polymarket_btc.data_collection.control.concept_generation.generate_concept_via_claude_code",
        ) as mock_generate:
            mock_generate.return_value = {
                "filename": "server_route_concept.py",
                "content": well_formed.split("```python\n")[1].split("```")[0],
            }
            head, body = await self._request(
                "POST", "/api/concept-prompt/generate",
                json_body={"prompt": "un prompt déjà construit et confirmé par l'utilisateur"},
            )
            self.assertIn("200 OK", head)
            job_id = body["job_id"]
            status = None
            for _ in range(200):
                _head, status = await self._request(
                    "GET", f"/api/concept-prompt/generate-status?job_id={job_id}",
                )
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
        self.assertTrue(status["done"])
        self.assertIsNone(status["error"])
        self.assertEqual(status["result"]["filename"], "server_route_concept.py")
        self.assertTrue((self.concepts_dir / "server_route_concept.py").is_file())

    async def test_concept_prompt_generate_status_unknown_job_is_404(self) -> None:
        head, _body = await self._request(
            "GET", "/api/concept-prompt/generate-status?job_id=no-such-job",
        )
        self.assertIn("404 Not Found", head)

    async def test_concept_prompt_generate_status_missing_job_id_is_400(self) -> None:
        head, _body = await self._request("GET", "/api/concept-prompt/generate-status")
        self.assertIn("400 Bad Request", head)

    async def test_concept_prompt_generate_not_configured_is_404(self) -> None:
        # Mirrors _import_generic's own "not configured" pattern (see
        # concept_prompt_doc_path is None) -- a server built without a
        # concept_generation manager (the default) must fail clearly, not
        # crash with an AttributeError.
        self.server.concept_generation = None
        head, _body = await self._request(
            "POST", "/api/concept-prompt/generate", json_body={"prompt": "un prompt"},
        )
        self.assertIn("404 Not Found", head)

    async def test_concept_prompt_expand_description_missing_text_is_400(self) -> None:
        head, _body = await self._request(
            "POST", "/api/concept-prompt/expand-description", json_body={},
        )
        self.assertIn("400 Bad Request", head)

    async def test_concept_prompt_expand_description_blank_text_is_400(self) -> None:
        head, _body = await self._request(
            "POST", "/api/concept-prompt/expand-description", json_body={"text": "   "},
        )
        self.assertIn("400 Bad Request", head)

    async def test_concept_prompt_expand_description_success_round_trip(self) -> None:
        with patch(
            "polymarket_btc.data_collection.control.concept_generation.expand_concept_description_via_claude_code",
        ) as mock_expand:
            mock_expand.return_value = "Une description complète du range breakout."
            head, body = await self._request(
                "POST", "/api/concept-prompt/expand-description", json_body={"text": "range breakout"},
            )
            self.assertIn("200 OK", head)
            job_id = body["job_id"]
            status = None
            for _ in range(200):
                _head, status = await self._request(
                    "GET", f"/api/concept-prompt/expand-description-status?job_id={job_id}",
                )
                if status["done"]:
                    break
                await asyncio.sleep(0.02)
        self.assertTrue(status["done"])
        self.assertIsNone(status["error"])
        self.assertEqual(status["result"]["description"], "Une description complète du range breakout.")

    async def test_concept_prompt_expand_description_status_unknown_job_is_404(self) -> None:
        head, _body = await self._request(
            "GET", "/api/concept-prompt/expand-description-status?job_id=no-such-job",
        )
        self.assertIn("404 Not Found", head)

    async def test_concept_prompt_expand_description_status_missing_job_id_is_400(self) -> None:
        head, _body = await self._request("GET", "/api/concept-prompt/expand-description-status")
        self.assertIn("400 Bad Request", head)

    async def test_concept_prompt_expand_description_not_configured_is_404(self) -> None:
        self.server.concept_generation = None
        head, _body = await self._request(
            "POST", "/api/concept-prompt/expand-description", json_body={"text": "range breakout"},
        )
        self.assertIn("404 Not Found", head)


if __name__ == "__main__":
    unittest.main()
