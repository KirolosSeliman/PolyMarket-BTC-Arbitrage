"""Binance USDT-M Futures 1-minute kline stream (btcusdt@kline_1m)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from ..config import MarketDataConfig
from ..health import HealthRegistry
from ..models import EventSource, MarketDataEvent
from .binance_kline_ticker import parse_futures_kline_message
from .single_stream_source import SingleStreamSource


class BinanceFuturesKlineSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
        health_registry: HealthRegistry | None = None,
    ) -> None:
        self._inner = SingleStreamSource(
            config,
            publish,
            next_sequence,
            on_connection,
            health_registry,
            event_source=EventSource.BINANCE_FUTURES_KLINE,
            url=config.binance_futures.kline_url,
            parse_message=parse_futures_kline_message,
            recv_timeout=90.0,
        )
        self.health_registry = self._inner.health_registry

    @property
    def connected(self) -> bool:
        return self._inner.connected

    @property
    def reconnect_count(self) -> int:
        return self._inner.reconnect_count

    @property
    def invalid_count(self) -> int:
        return self._inner.invalid_count

    async def stop(self) -> None:
        await self._inner.stop()

    async def run(self) -> None:
        await self._inner.run()


__all__ = ["BinanceFuturesKlineSource"]
