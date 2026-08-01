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
import json
import logging
from pathlib import Path

from ..common.http import (
    MalformedRequest,
    build_response,
    read_request,
)
from .runs import CollectionRunManager

_LOGGER = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
_REQUEST_TIMEOUT_SECONDS = 15.0

_PAGES = {
    "/": "index.html",
    "/index.html": "index.html",
    "/live": "live.html",
    "/backtest": "backtest.html",
    "/collect": "collect.html",
}


class ControlPanelServer:
    def __init__(
        self,
        *,
        runs: CollectionRunManager,
        host: str = "127.0.0.1",
        port: int = 8780,
        prompt_doc_path: Path | None = None,
    ) -> None:
        self.runs = runs
        self.host = host
        self.port = port
        self.prompt_doc_path = prompt_doc_path
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
        path = target.split("?", 1)[0]
        if method == "GET":
            await self._write(writer, self._get(path))
        elif method == "POST":
            await self._write(writer, self._post(path, body))
        else:
            await self._write(writer, build_response("405 Method Not Allowed", b"", "text/plain"))

    def _get(self, path: str) -> bytes:
        if path in _PAGES:
            return self._page(_PAGES[path])
        if path == "/api/sources":
            return self._json({"sources": self.runs.available_sources()})
        if path == "/api/plugins":
            return self._json({"plugins": self.runs.available_plugins()})
        if path == "/api/collect/status":
            return self._json({"run": self.runs.status()})
        if path == "/api/runs":
            return self._json({"runs": self.runs.list_runs()})
        if path == "/api/plugin-prompt":
            return self._plugin_prompt()
        return build_response("404 Not Found", b"not found", "text/plain; charset=utf-8")

    def _post(self, path: str, body: bytes) -> bytes:
        if path == "/api/collect/start":
            return self._start(body)
        if path == "/api/collect/stop":
            return self._stop()
        if path == "/api/plugins/import":
            return self._import_plugin(body)
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

    def _plugin_prompt(self) -> bytes:
        if self.prompt_doc_path is None:
            return self._error("404 Not Found", "no prompt document configured")
        try:
            content = self.prompt_doc_path.read_text(encoding="utf-8")
        except OSError as exc:
            return self._error("404 Not Found", f"prompt document unavailable: {exc}")
        return self._json({"content": content})

    def _import_plugin(self, body: bytes) -> bytes:
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
            result = self.runs.import_plugin_file(filename, content, overwrite=overwrite)
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
