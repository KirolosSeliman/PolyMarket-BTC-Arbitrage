"""HTTP server for the control panel: the landing page (3 destinations),
the data-collection console, and a small JSON API the collection page polls.

Read-mostly like the live dashboard's server, but the collection endpoints
accept POST bodies to start/stop a run -- the one place in this project a
web request causes a side effect (spawning/stopping background collection),
so every mutating route is scoped to /api/collect/* and validated before
touching CollectionRunManager.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import json
import logging
from pathlib import Path
from urllib.parse import parse_qs

from ..common.http import (
    MalformedRequest,
    build_response,
    read_request,
)
from .backtest_jobs import BacktestJobManager
from .runs import CollectionRunManager
from .strategies import StrategyManager

_LOGGER = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
_REQUEST_TIMEOUT_SECONDS = 15.0

_PAGES = {
    "/": "strategy.html",
    "/strategy/build": "strategy_builder.html",
    "/menu": "index.html",
    "/live": "live.html",
    "/backtest": "backtest.html",
    "/collect": "collect.html",
}


class ControlPanelServer:
    def __init__(
        self,
        *,
        runs: CollectionRunManager,
        strategies: StrategyManager,
        host: str = "127.0.0.1",
        port: int = 8780,
        prompt_doc_path: Path | None = None,
        concept_prompt_doc_path: Path | None = None,
        microsystem_prompt_doc_path: Path | None = None,
        execution_prompt_doc_path: Path | None = None,
        management_prompt_doc_path: Path | None = None,
    ) -> None:
        self.runs = runs
        self.strategies = strategies
        self.host = host
        self.port = port
        self.prompt_doc_path = prompt_doc_path
        self.concept_prompt_doc_path = concept_prompt_doc_path
        self.microsystem_prompt_doc_path = microsystem_prompt_doc_path
        self.execution_prompt_doc_path = execution_prompt_doc_path
        self.management_prompt_doc_path = management_prompt_doc_path
        self.backtest_jobs = BacktestJobManager()
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.Task[None]] = set()

    @property
    def url(self) -> str:
        host = "127.0.0.1" if self.host in {"", "0.0.0.0", "::"} else self.host
        return f"http://{host}:{self.port}/"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_connection, self.host, self.port)
        for socket in self._server.sockets or ():
            self.port = socket.getsockname()[1]
            break
        _LOGGER.info("control panel listening on %s", self.url)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for task in tuple(self._connections):
            task.cancel()
        if self._connections:
            await asyncio.gather(*self._connections, return_exceptions=True)
        self._connections.clear()

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        try:
            await asyncio.wait_for(self._serve(reader, writer), timeout=_REQUEST_TIMEOUT_SECONDS)
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError, TimeoutError):
            pass
        except MalformedRequest:
            try:
                await self._write(writer, build_response("400 Bad Request", b"", "text/plain"))
            except (ConnectionResetError, BrokenPipeError):
                pass
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.exception("control panel connection failed")
        finally:
            if task is not None:
                self._connections.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        method, target, _headers, body = await read_request(reader)
        path, _, raw_query = target.partition("?")
        query = parse_qs(raw_query)
        if method == "GET":
            await self._write(writer, self._get(path, query))
        elif method == "POST":
            await self._write(writer, self._post(path, body))
        else:
            await self._write(writer, build_response("405 Method Not Allowed", b"", "text/plain"))

    def _get(self, path: str, query: dict[str, list[str]]) -> bytes:
        if path in _PAGES:
            return self._page(_PAGES[path])
        if path == "/api/sources":
            return self._json({"sources": self.runs.available_sources()})
        if path == "/api/plugins":
            return self._json({"plugins": self.runs.available_plugins()})
        if path == "/api/concepts":
            return self._json({"concepts": self.runs.available_concepts()})
        if path == "/api/microsystems":
            return self._json({"microsystems": self.runs.available_microsystems()})
        if path == "/api/execution-profiles":
            return self._json({"execution_profiles": self.runs.available_execution_profiles()})
        if path == "/api/management-profiles":
            return self._json({"management_profiles": self.runs.available_management_profiles()})
        if path == "/api/strategies":
            return self._json({"strategies": self.strategies.list_strategies()})
        if path == "/api/strategy":
            return self._get_strategy(query)
        if path == "/api/collect/status":
            return self._json({"run": self.runs.status()})
        if path == "/api/runs":
            return self._json({"runs": self.runs.list_runs()})
        if path == "/api/plugin-prompt":
            return self._prompt_doc(self.prompt_doc_path)
        if path == "/api/execution-prompt":
            return self._prompt_doc(self.execution_prompt_doc_path)
        if path == "/api/management-prompt":
            return self._prompt_doc(self.management_prompt_doc_path)
        if path == "/api/backtest/eligibility":
            return self._backtest_eligibility(query)
        if path == "/api/backtest/status":
            return self._backtest_status(query)
        return build_response("404 Not Found", b"not found", "text/plain; charset=utf-8")

    def _get_strategy(self, query: dict[str, list[str]]) -> bytes:
        names = query.get("name")
        if not names:
            return self._error("400 Bad Request", "name query parameter is required")
        strategy = self.strategies.load_strategy(names[0])
        if strategy is None:
            return self._error("404 Not Found", f"unknown strategy: {names[0]!r}")
        return self._json(strategy)

    def _backtest_eligibility(self, query: dict[str, list[str]]) -> bytes:
        names = query.get("strategy")
        if not names:
            return self._error("400 Bad Request", "strategy query parameter is required")
        result = self.strategies.backtest_eligibility(names[0])
        if result is None:
            return self._error("404 Not Found", f"unknown strategy: {names[0]!r}")
        return self._json(result)

    def _backtest_status(self, query: dict[str, list[str]]) -> bytes:
        ids = query.get("id")
        if not ids:
            return self._error("400 Bad Request", "id query parameter is required")
        status = self.backtest_jobs.status(ids[0])
        if status is None:
            return self._error("404 Not Found", f"unknown job id: {ids[0]!r}")
        return self._json(status)

    def _start_backtest(self, body: bytes) -> bytes:
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return self._error("400 Bad Request", "invalid JSON body")
        if not isinstance(payload, dict):
            return self._error("400 Bad Request", "body must be a JSON object")
        name = payload.get("strategy")
        if not isinstance(name, str) or not name:
            return self._error("400 Bad Request", "strategy must be a non-empty string")
        strategy = self.strategies.load_strategy(name)
        if strategy is None:
            return self._error("404 Not Found", f"unknown strategy: {name!r}")
        instrument = payload.get("instrument")
        start_ts = payload.get("start_ts")
        end_ts = payload.get("end_ts")
        cadence_seconds = payload.get("cadence_seconds")
        if not isinstance(instrument, str) or not instrument:
            return self._error("400 Bad Request", "instrument must be a non-empty string")
        if not isinstance(start_ts, (int, float)) or not isinstance(end_ts, (int, float)) or start_ts >= end_ts:
            return self._error("400 Bad Request", "start_ts must be a number less than end_ts")
        if not isinstance(cadence_seconds, (int, float)) or cadence_seconds <= 0:
            return self._error("400 Bad Request", "cadence_seconds must be a positive number")
        execution_sweep = payload.get("execution_sweep") or {}
        management_sweep = payload.get("management_sweep") or {}
        execution_config = payload.get("execution_config") or {}
        management_config = payload.get("management_config") or {}
        if not isinstance(execution_sweep, dict) or not isinstance(management_sweep, dict):
            return self._error("400 Bad Request", "execution_sweep and management_sweep must be objects")
        if not isinstance(execution_config, dict) or not isinstance(management_config, dict):
            return self._error("400 Bad Request", "execution_config and management_config must be objects")

        job = self.backtest_jobs.start(
            strategy=strategy,
            concepts_dir=self.runs.concepts_dir, microsystems_dir=self.runs.microsystems_dir,
            execution_dir=self.runs.execution_dir, management_dir=self.runs.management_dir,
            data_requirements_for=self.runs._data_requirements_for,
            manifests=self.runs.list_runs(), instrument=instrument,
            start_ts=float(start_ts), end_ts=float(end_ts), cadence_seconds=float(cadence_seconds),
            execution_config=execution_config, management_config=management_config,
            execution_sweep=execution_sweep, management_sweep=management_sweep,
        )
        return self._json({"job_id": job.job_id})

    def _post(self, path: str, body: bytes) -> bytes:
        if path == "/api/backtest/run":
            return self._start_backtest(body)
        if path == "/api/collect/start":
            return self._start(body)
        if path == "/api/collect/stop":
            return self._stop()
        if path == "/api/runs/delete":
            return self._delete_run(body)
        if path == "/api/plugins/import":
            return self._import_generic(body, self.runs.import_plugin_file)
        if path == "/api/concepts/import":
            return self._import_generic(body, self.runs.import_concept_file)
        if path == "/api/microsystems/import":
            return self._import_generic(body, self.runs.import_microsystem_file)
        if path == "/api/execution-profiles/import":
            return self._import_generic(body, self.runs.import_execution_profile_file)
        if path == "/api/management-profiles/import":
            return self._import_generic(body, self.runs.import_management_profile_file)
        if path == "/api/plugins/view":
            return self._view_source(body, self.runs.read_plugin_source)
        if path == "/api/concepts/view":
            return self._view_source(body, self.runs.read_concept_source)
        if path == "/api/concept-prompt":
            return self._concept_prompt(body)
        if path == "/api/microsystem-prompt":
            return self._microsystem_prompt(body)
        if path == "/api/strategies":
            return self._save_strategy(body)
        if path == "/api/symbols/refresh":
            return self._refresh_symbols()
        return build_response("404 Not Found", b"not found", "text/plain; charset=utf-8")

    def _refresh_symbols(self) -> bytes:
        """Bypasses the 24h cache TTL for a manual "refresh the crypto list"
        action -- picks up new Binance listings without waiting them out."""
        try:
            catalog = self.runs.refresh_symbol_catalog()
        except Exception as exc:
            return self._error("502 Bad Gateway", f"could not refresh symbol catalog: {exc!r}")
        return self._json({
            "spot_count": len(catalog.spot),
            "futures_count": len(catalog.futures),
            "fetched_at_utc": catalog.fetched_at_utc,
        })

    def _prompt_doc(self, path: Path | None) -> bytes:
        """Serves a static prompt document's raw text -- used for the
        plugin, execution and management prompts, the ones that carry no
        dynamic, selection-dependent context (see _concept_prompt/
        _microsystem_prompt for the ones that do)."""
        if path is None:
            return self._error("404 Not Found", "no prompt document configured")
        try:
            content = path.read_text(encoding="utf-8")
        except OSError as exc:
            return self._error("404 Not Found", f"prompt document unavailable: {exc}")
        return self._json({"content": content})

    def _import_generic(self, body: bytes, importer: Callable[..., dict[str, object]]) -> bytes:
        """Shared by every "drop a .py file in" import route (plugins,
        concepts, microsystems, execution profiles, management profiles) --
        identical body shape and error translation regardless of which
        directory it lands in."""
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return self._error("400 Bad Request", "invalid JSON body")
        if not isinstance(payload, dict):
            return self._error("400 Bad Request", "body must be a JSON object")
        filename = payload.get("filename")
        content = payload.get("content")
        overwrite = bool(payload.get("overwrite", False))
        if not isinstance(filename, str) or not isinstance(content, str):
            return self._error("400 Bad Request", "filename and content must be strings")
        try:
            result = importer(filename, content, overwrite=overwrite)
        except ValueError as exc:
            return self._error("400 Bad Request", str(exc))
        except FileExistsError as exc:
            return self._json({"error": str(exc), "exists": True}, status="409 Conflict")
        return self._json(result)

    def _view_source(self, body: bytes, reader: Callable[[str], dict[str, str]]) -> bytes:
        """Shared by /api/plugins/view and /api/concepts/view: {"id": "..."}
        in, {"id","filename","content"} out -- lets a concept/microsystem
        author see exactly what a data source/concept they depend on looks
        like before writing code against it."""
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return self._error("400 Bad Request", "invalid JSON body")
        if not isinstance(payload, dict) or not isinstance(payload.get("id"), str):
            return self._error("400 Bad Request", "body must be a JSON object with a string 'id'")
        try:
            result = reader(payload["id"])
        except FileNotFoundError as exc:
            return self._error("404 Not Found", str(exc))
        return self._json(result)

    @staticmethod
    def _string_list_field(payload: dict, key: str) -> list[str] | None:
        value = payload.get(key, [])
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            return None
        return value

    def _concept_prompt(self, body: bytes) -> bytes:
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return self._error("400 Bad Request", "invalid JSON body")
        if not isinstance(payload, dict):
            return self._error("400 Bad Request", "body must be a JSON object")
        sources = self._string_list_field(payload, "sources")
        plugins = self._string_list_field(payload, "plugins")
        if sources is None or plugins is None:
            return self._error("400 Bad Request", "sources and plugins must be lists of strings")
        if self.concept_prompt_doc_path is None:
            return self._error("404 Not Found", "no concept prompt document configured")
        try:
            template = self.concept_prompt_doc_path.read_text(encoding="utf-8")
        except OSError as exc:
            return self._error("404 Not Found", f"concept prompt document unavailable: {exc}")
        try:
            content = self.runs.build_concept_prompt(sources=sources, plugins=plugins, template=template)
        except ValueError as exc:
            return self._error("400 Bad Request", str(exc))
        return self._json({"content": content})

    def _microsystem_prompt(self, body: bytes) -> bytes:
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return self._error("400 Bad Request", "invalid JSON body")
        if not isinstance(payload, dict):
            return self._error("400 Bad Request", "body must be a JSON object")
        concepts = self._string_list_field(payload, "concepts")
        sources = self._string_list_field(payload, "sources")
        plugins = self._string_list_field(payload, "plugins")
        if concepts is None or sources is None or plugins is None:
            return self._error("400 Bad Request", "concepts, sources and plugins must be lists of strings")
        if self.microsystem_prompt_doc_path is None:
            return self._error("404 Not Found", "no microsystem prompt document configured")
        try:
            template = self.microsystem_prompt_doc_path.read_text(encoding="utf-8")
        except OSError as exc:
            return self._error("404 Not Found", f"microsystem prompt document unavailable: {exc}")
        try:
            content = self.runs.build_microsystem_prompt(
                concepts=concepts, sources=sources, plugins=plugins, template=template,
            )
        except ValueError as exc:
            return self._error("400 Bad Request", str(exc))
        return self._json({"content": content})

    def _save_strategy(self, body: bytes) -> bytes:
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return self._error("400 Bad Request", "invalid JSON body")
        if not isinstance(payload, dict):
            return self._error("400 Bad Request", "body must be a JSON object")
        name = payload.get("name")
        concepts = payload.get("concepts", [])
        microsystems = payload.get("microsystems", [])
        execution = payload.get("execution")
        management = payload.get("management")
        overwrite = bool(payload.get("overwrite", False))
        if not isinstance(name, str) or not name:
            return self._error("400 Bad Request", "name must be a non-empty string")
        if not isinstance(concepts, list) or not isinstance(microsystems, list):
            return self._error("400 Bad Request", "concepts and microsystems must be lists")
        if execution is not None and not isinstance(execution, dict):
            return self._error("400 Bad Request", "execution must be an object or null")
        if management is not None and not isinstance(management, dict):
            return self._error("400 Bad Request", "management must be an object or null")
        try:
            result = self.strategies.save_strategy(
                name=name, concepts=concepts, microsystems=microsystems,
                execution=execution, management=management, overwrite=overwrite,
            )
        except ValueError as exc:
            return self._error("400 Bad Request", str(exc))
        except FileExistsError as exc:
            return self._json({"error": str(exc), "exists": True}, status="409 Conflict")
        return self._json(result)

    def _start(self, body: bytes) -> bytes:
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return self._error("400 Bad Request", "invalid JSON body")
        if not isinstance(payload, dict):
            return self._error("400 Bad Request", "body must be a JSON object")
        sources = payload.get("sources")
        plugins = payload.get("plugins", [])
        duration = payload.get("duration_seconds")
        mode = payload.get("mode", "collect")
        start_ts = payload.get("start_ts")
        end_ts = payload.get("end_ts")
        if not isinstance(sources, list) or not all(isinstance(s, str) for s in sources):
            return self._error("400 Bad Request", "sources must be a list of strings")
        if not isinstance(plugins, list) or not all(isinstance(p, str) for p in plugins):
            return self._error("400 Bad Request", "plugins must be a list of strings")
        if duration is not None and (not isinstance(duration, (int, float)) or duration <= 0):
            return self._error("400 Bad Request", "duration_seconds must be a positive number or null")
        if mode not in ("collect", "access"):
            return self._error("400 Bad Request", "mode must be 'collect' or 'access'")
        for label, value in (("start_ts", start_ts), ("end_ts", end_ts)):
            if value is not None and not isinstance(value, (int, float)):
                return self._error("400 Bad Request", f"{label} must be an epoch-millisecond number or null")
        try:
            state = self.runs.start(
                sources=sources, plugins=plugins,
                duration_seconds=None if duration is None else float(duration),
                mode=mode,
                start_ts_ns=None if start_ts is None else int(start_ts * 1_000_000),
                end_ts_ns=None if end_ts is None else int(end_ts * 1_000_000),
            )
        except RuntimeError as exc:
            return self._error("409 Conflict", str(exc))
        except ValueError as exc:
            return self._error("400 Bad Request", str(exc))
        return self._json({"run_id": state.run_id})

    def _stop(self) -> bytes:
        try:
            self.runs.stop()
        except RuntimeError as exc:
            return self._error("409 Conflict", str(exc))
        return self._json({"ok": True})

    def _delete_run(self, body: bytes) -> bytes:
        try:
            payload = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return self._error("400 Bad Request", "invalid JSON body")
        run_id = payload.get("run_id") if isinstance(payload, dict) else None
        if not isinstance(run_id, str) or not run_id:
            return self._error("400 Bad Request", "body must be a JSON object with a non-empty string 'run_id'")
        try:
            self.runs.delete_run(run_id)
        except ValueError as exc:
            return self._error("400 Bad Request", str(exc))
        except FileNotFoundError as exc:
            return self._error("404 Not Found", str(exc))
        except RuntimeError as exc:
            return self._error("409 Conflict", str(exc))
        return self._json({"ok": True})

    def _page(self, name: str) -> bytes:
        try:
            content = (STATIC_DIR / name).read_bytes()
        except OSError:
            return build_response("500 Internal Server Error", b"page missing", "text/plain; charset=utf-8")
        return build_response(
            "200 OK", content, "text/html; charset=utf-8", extra=(("Cache-Control", "no-store"),)
        )

    @staticmethod
    def _json(payload: object, *, status: str = "200 OK") -> bytes:
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        return build_response(status, body, "application/json; charset=utf-8", extra=(("Cache-Control", "no-store"),))

    def _error(self, status: str, message: str) -> bytes:
        return self._json({"error": message}, status=status)

    @staticmethod
    async def _write(writer: asyncio.StreamWriter, payload: bytes) -> None:
        writer.write(payload)
        await writer.drain()


__all__ = ["ControlPanelServer"]
