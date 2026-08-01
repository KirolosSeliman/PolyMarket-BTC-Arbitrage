"""Binance USDT-M Futures aggregate trade stream (btcusdt@aggTrade).

Same wire shape and validation rigor as the Spot aggTrade stream (monotonic
aggregate-trade-ID enforcement per connection) -- this is what supplies the
futures "last traded price" instead of relying on the depth mid-price as a
proxy, plus the recent-trades tape the live view renders.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from ..config import MarketDataConfig
from ..health import HealthRegistry
from ..models import (
    BinanceAggTradePayload,
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    TakerSide,
    parse_decimal,
)
from .single_stream_source import SingleStreamSource


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidEventError(f"{field} must be a positive integer")
    return value


class FuturesTradeParser:
    """Holds per-connection monotonic-ID state, mirroring the Spot parser."""

    def __init__(self) -> None:
        self.last_agg_trade_id: int | None = None

    def parse(
        self,
        message: object,
        *,
        received_wall_timestamp_ns: int,
        received_monotonic_ns: int,
        ingest_sequence: int,
    ) -> MarketDataEvent:
        if not isinstance(message, dict) or message.get("e") != "aggTrade" or message.get("s") != "BTCUSDT":
            raise InvalidEventError("unexpected futures aggTrade message identity")
        aggregate_id = _positive_int(message.get("a"), "a")
        if self.last_agg_trade_id is not None and aggregate_id <= self.last_agg_trade_id:
            raise InvalidEventError("aggregate trade ID is not increasing")
        first_id = _positive_int(message.get("f"), "f")
        last_id = _positive_int(message.get("l"), "l")
        if first_id > last_id:
            raise InvalidEventError("first trade ID exceeds last trade ID")
        event_time_ms = _positive_int(message.get("E"), "E")
        trade_time_ms = _positive_int(message.get("T"), "T")
        if not isinstance(message.get("m"), bool):
            raise InvalidEventError("m must be boolean")
        try:
            price = parse_decimal(str(message.get("p")), "p", strictly_positive=True)
            quantity = parse_decimal(str(message.get("q")), "q", strictly_positive=True)
        except ValueError as exc:
            raise InvalidEventError(str(exc)) from exc
        self.last_agg_trade_id = aggregate_id
        trade_ns = trade_time_ms * 1_000_000
        return MarketDataEvent(
            schema_version=2,
            ingest_sequence=ingest_sequence,
            event_id=f"binance_futures:aggTrade:BTCUSDT:{aggregate_id}",
            source=EventSource.BINANCE_FUTURES_TRADE,
            stream=EventStream.BINANCE_FUTURES_AGG_TRADE,
            instrument="BTCUSDT",
            source_timestamp_ns=trade_ns,
            server_timestamp_ns=event_time_ms * 1_000_000,
            received_wall_timestamp_ns=received_wall_timestamp_ns,
            received_monotonic_ns=received_monotonic_ns,
            source_sequence=str(aggregate_id),
            timeframe=None,
            market_id=None,
            condition_id=None,
            asset_id=None,
            outcome=None,
            payload=BinanceAggTradePayload(
                "BTCUSDT",
                aggregate_id,
                price,
                quantity,
                first_id,
                last_id,
                trade_ns,
                TakerSide.SELL if message["m"] else TakerSide.BUY,
            ),
        )


class BinanceFuturesTradeSource:
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
            event_source=EventSource.BINANCE_FUTURES_TRADE,
            url=config.binance_futures.agg_trade_url,
            parse_message=FuturesTradeParser().parse,
            recv_timeout=30.0,
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


__all__ = ["BinanceFuturesTradeSource", "FuturesTradeParser"]
