"""Full-depth Binance Spot order book -- upgrade over the top-20 snapshot
from binance_spot.py's depth20 stream. Maintains a locally reconstructed
book covering the entire visible depth, which is what's needed to build a
DOM ladder spanning e.g. +/-$100 of the mid price rather than just the
best 20 levels.

This runs as an independent connection alongside BinanceSpotSource -- the
existing depth20/aggTrade/bookTicker collection is untouched.
"""
from __future__ import annotations

from ..models import EventSource, EventStream
from .depth_source_base import BaseFullDepthSource


class BinanceSpotFullDepthSource(BaseFullDepthSource):
    event_source = EventSource.BINANCE_SPOT_FULL_DEPTH
    dom_stream = EventStream.BINANCE_SPOT_DOM_SNAPSHOT
    market_label = "spot"

    @property
    def instrument(self) -> str:
        return self._symbol_override or self._config.binance.symbol.upper()

    def _ws_url(self) -> str:
        if self._symbol_override:
            return f"wss://stream.binance.com:9443/ws/{self.instrument.lower()}@depth@100ms"
        return self._config.binance.full_depth_url

    def _rest_url(self) -> str:
        return f"https://api.binance.com/api/v3/depth?symbol={self.instrument}&limit=5000"
