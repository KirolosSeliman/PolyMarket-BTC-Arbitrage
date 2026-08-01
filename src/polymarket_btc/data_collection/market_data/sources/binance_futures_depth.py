"""Full-depth Binance USDT-M Futures (perpetual) order book.

Same reconstruction protocol as spot (see full_depth_reconstruction.py),
but futures diff events carry a "pu" field used for the continuity check
instead of the spot U-based convention -- handled transparently there.
"""
from __future__ import annotations

from ..models import EventSource, EventStream
from .depth_source_base import BaseFullDepthSource


class BinanceFuturesDepthSource(BaseFullDepthSource):
    event_source = EventSource.BINANCE_FUTURES_DEPTH
    dom_stream = EventStream.BINANCE_FUTURES_DOM_SNAPSHOT
    market_label = "futures"

    @property
    def instrument(self) -> str:
        return self._symbol_override or self._config.binance_futures.symbol.upper()

    def _ws_url(self) -> str:
        if self._symbol_override:
            return f"wss://fstream.binance.com/ws/{self.instrument.lower()}@depth@100ms"
        return self._config.binance_futures.depth_url

    def _rest_url(self) -> str:
        return f"https://fapi.binance.com/fapi/v1/depth?symbol={self.instrument}&limit=1000"
