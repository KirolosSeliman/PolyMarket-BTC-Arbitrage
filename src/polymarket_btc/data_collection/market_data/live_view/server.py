"""Dependency-free HTTP + Server-Sent-Events server for the live dashboard.

Read-only: it exposes gateway state and never accepts input beyond a request
line. Every connected browser gets its own snapshot subscription, coalesced to
the newest frame so a slow tab falls behind in latency, not in correctness.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import json
import logging
from pathlib import Path
import time

from ..models import MarketDataSnapshot
from .frame import build_frame

_LOGGER = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

_MAX_REQUEST_LINE_BYTES = 8192
_MAX_HEADERS = 64
_SSE_KEEPALIVE_SECONDS = 15.0

_SECURITY_HEADERS = (
    ("X-Content-Type-Options", "nosniff"),
    ("Referrer-Policy", "no-referrer"),
)


def _response(
    status: str,
    body: bytes,
    content_type: str,
    *,
    extra: tuple[tuple[str, str], ...] = (),
) -> bytes:
    headers = [
        f"HTTP/1.1 {status}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
        "Connection: close",
        *(f"{name}: {value}" for name, value in _SECURITY_HEADERS),
        *(f"{name}: {value}" for name, value in extra),
    ]
    return ("\r\n".join(headers) + "\r\n\r\n").encode("utf-8") + body


def _chunk(payload: bytes) -> bytes:
    return b"%x\r\n%s\r\n" % (len(payload), payload)


class LiveViewServer:
    """Serves the dashboard page and streams snapshot frames over SSE."""

    def __init__(
        self,
        *,
        subscribe: Callable[[], asyncio.Queue[MarketDataSnapshot | None]],
        unsubscribe: Callable[[asyncio.Queue[MarketDataSnapshot | None]], None],
        latest_snapshot: Callable[[], MarketDataSnapshot],
        health: Callable[[], dict[str, object]],
        host: str = "127.0.0.1",
        port: int = 8765,
    ) -> None:
        self._subscribe = subscribe
        self._unsubscribe = unsubscribe
        self._latest_snapshot = latest_snapshot
        self._health = health
        self.host = host
        self.port = port
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.Task[None]] = set()
        self.client_count = 0
        self.frames_sent = 0

    @property
    def url(self) -> str:
        host = "127.0.0.1" if self.host in {"", "0.0.0.0", "::"} else self.host
        return f"http://{host}:{self.port}/"

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )
        for socket in self._server.sockets or ():
            self.port = socket.getsockname()[1]
            break
        _LOGGER.info("live view listening on %s", self.url)

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
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        try:
            await self._serve(reader, writer)
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception:  # a broken request must never take the gateway down
            _LOGGER.exception("live view connection failed")
        finally:
            if task is not None:
                self._connections.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError):
                pass

    async def _serve(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        request_line = await asyncio.wait_for(
            reader.readline(), timeout=_SSE_KEEPALIVE_SECONDS
        )
        if not request_line:
            return
        if len(request_line) > _MAX_REQUEST_LINE_BYTES:
            await self._write(writer, _response("414 URI Too Long", b"", "text/plain"))
            return
        parts = request_line.decode("latin-1").split()
        if len(parts) < 2:
            await self._write(writer, _response("400 Bad Request", b"", "text/plain"))
            return
        method, target = parts[0], parts[1]
        for _ in range(_MAX_HEADERS):
            header = await reader.readline()
            if header in (b"\r\n", b"\n", b""):
                break
        path = target.split("?", 1)[0]

        if method not in {"GET", "HEAD"}:
            await self._write(
                writer, _response("405 Method Not Allowed", b"", "text/plain")
            )
            return
        if path == "/stream":
            await self._stream(reader, writer)
            return
        await self._write(writer, self._static_response(path, head=method == "HEAD"))

    def _static_response(self, path: str, *, head: bool) -> bytes:
        if path in {"/", "/index.html"}:
            try:
                body = (STATIC_DIR / "index.html").read_bytes()
            except OSError:
                return _response(
                    "500 Internal Server Error",
                    b"dashboard asset missing",
                    "text/plain; charset=utf-8",
                )
            return _response(
                "200 OK",
                b"" if head else body,
                "text/html; charset=utf-8",
                extra=(("Cache-Control", "no-store"),),
            )
        if path == "/frame.json":
            frame = build_frame(self._latest_snapshot())
            return self._json_response(frame, head=head)
        if path == "/health.json":
            return self._json_response(self._health(), head=head)
        return _response("404 Not Found", b"not found", "text/plain; charset=utf-8")

    def _json_response(self, payload: object, *, head: bool) -> bytes:
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        return _response(
            "200 OK",
            b"" if head else body,
            "application/json; charset=utf-8",
            extra=(("Cache-Control", "no-store"),),
        )

    async def _stream(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        headers = "\r\n".join(
            [
                "HTTP/1.1 200 OK",
                "Content-Type: text/event-stream; charset=utf-8",
                "Cache-Control: no-store",
                "Transfer-Encoding: chunked",
                "X-Accel-Buffering: no",
                *(f"{name}: {value}" for name, value in _SECURITY_HEADERS),
            ]
        )
        await self._write(writer, (headers + "\r\n\r\n").encode("utf-8"))

        queue = self._subscribe()
        self.client_count += 1
        closed = asyncio.get_running_loop().create_future()
        watcher = asyncio.create_task(self._watch_eof(reader, closed))
        try:
            await self._send_frame(writer, build_frame(self._latest_snapshot()))
            while not closed.done():
                snapshot = await self._next_snapshot(queue, closed)
                if snapshot is None:
                    await self._write(writer, _chunk(b": keepalive\n\n"))
                    continue
                await self._send_frame(writer, build_frame(snapshot))
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            self._unsubscribe(queue)
            self.client_count -= 1

    @staticmethod
    async def _watch_eof(
        reader: asyncio.StreamReader,
        closed: asyncio.Future[None],
    ) -> None:
        try:
            while await reader.read(4096):
                pass
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if not closed.done():
                closed.set_result(None)

    @staticmethod
    async def _next_snapshot(
        queue: asyncio.Queue[MarketDataSnapshot | None],
        closed: asyncio.Future[None],
    ) -> MarketDataSnapshot | None:
        """Return the newest queued snapshot, or None on keepalive/shutdown."""
        getter = asyncio.ensure_future(queue.get())
        try:
            done, _pending = await asyncio.wait(
                {getter, closed},
                timeout=_SSE_KEEPALIVE_SECONDS,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            if not getter.done():
                getter.cancel()
        if getter not in done:
            return None
        snapshot = getter.result()
        if snapshot is None:
            raise ConnectionResetError("snapshot publisher stopped")
        while True:  # coalesce: a slow tab skips stale frames, never blocks the bus
            try:
                newer = queue.get_nowait()
            except asyncio.QueueEmpty:
                return snapshot
            if newer is None:
                return snapshot
            snapshot = newer

    async def _send_frame(
        self,
        writer: asyncio.StreamWriter,
        frame: dict[str, object],
    ) -> None:
        payload = json.dumps(frame, separators=(",", ":"), allow_nan=False)
        await self._write(writer, _chunk(f"data: {payload}\n\n".encode("utf-8")))
        self.frames_sent += 1

    @staticmethod
    async def _write(writer: asyncio.StreamWriter, payload: bytes) -> None:
        writer.write(payload)
        await writer.drain()


def server_for_gateway(gateway: object, host: str, port: int) -> LiveViewServer:
    """Wire a LiveViewServer onto a running MarketDataGateway."""
    return LiveViewServer(
        subscribe=gateway.publisher.subscribe,  # type: ignore[attr-defined]
        unsubscribe=gateway.publisher.unsubscribe,  # type: ignore[attr-defined]
        latest_snapshot=lambda: gateway.state.snapshot(  # type: ignore[attr-defined]
            time.time_ns(), gateway._tick_sequence
        ),
        health=lambda: dict(gateway.health),  # type: ignore[attr-defined]
        host=host,
        port=port,
    )


__all__ = ["LiveViewServer", "server_for_gateway"]
