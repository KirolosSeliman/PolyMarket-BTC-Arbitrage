"""Runs the market data gateway with the live dashboard attached."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import signal

from ..config import MarketDataConfig
from ..gateway import MarketDataGateway
from .server import LiveViewServer, server_for_gateway

_LOGGER = logging.getLogger(__name__)


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM"):
        handler = getattr(signal, name, None)
        if handler is None:
            continue
        try:
            loop.add_signal_handler(handler, stop.set)
        except NotImplementedError:
            pass


async def run_live_view(
    config: MarketDataConfig,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    duration_seconds: float | None = None,
) -> dict[str, object]:
    """Start the gateway plus the dashboard, and block until stopped."""
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    gateway = MarketDataGateway(config)
    server: LiveViewServer | None = None
    async with gateway:
        server = server_for_gateway(gateway, host, port)
        await server.start()
        print(f"live view ready: {server.url}", flush=True)
        stop_task = asyncio.create_task(stop.wait())
        fatal_task = asyncio.create_task(gateway.wait_for_fatal())
        try:
            done, _pending = await asyncio.wait(
                {stop_task, fatal_task},
                timeout=duration_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if fatal_task in done:
                await fatal_task
        finally:
            stop_task.cancel()
            fatal_task.cancel()
            await asyncio.gather(stop_task, fatal_task, return_exceptions=True)
            await server.stop()
    return {
        "url": server.url,
        "frames_sent": server.frames_sent,
        "data_dir": str(config.storage.data_dir),
    }


def run(
    config_path: Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    duration_seconds: float | None = None,
) -> dict[str, object]:
    from ..config import load_config

    return asyncio.run(
        run_live_view(
            load_config(config_path),
            host=host,
            port=port,
            duration_seconds=duration_seconds,
        )
    )


__all__ = ["run", "run_live_view"]
