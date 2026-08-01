"""Generic full order book reconstruction via REST snapshot + WebSocket diff stream.

Implements Binance's documented procedure. It is the SAME protocol for Spot
and USDT-M Futures -- only the REST/WS endpoints differ (handled by the
callers in binance_spot_full_depth.py / binance_futures_depth.py), and
futures diff events carry an extra "pu" (previous final update ID) field
used for the ongoing continuity check instead of the spot convention
(U == previous_event.u + 1).

Steps:
1. Open the diff WebSocket and buffer every event.
2. Fetch a REST snapshot; record its lastUpdateId (U0).
3. Drop buffered events where event["u"] <= U0.
4. The first event to apply must satisfy event["U"] <= U0 + 1 <= event["u"].
5. Apply that event and every subsequent one, in order. Continuity:
   - Spot events (no "pu" key): require event["U"] == previous_event["u"] + 1.
   - Futures events (has "pu" key): require event["pu"] == previous_event["u"].
   Any break forces a resync (fresh snapshot, buffer flushed, restart at step 3).
6. A level with quantity "0" means "remove this price".

This module holds no network code and raises no InvalidEventError -- it is
a plain data structure shared by the two source modules, which are
responsible for translating ResyncRequired into a fresh REST call and for
all MarketDataEvent/validation concerns.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal


class ResyncRequired(Exception):
    """Raised when the diff sequence breaks; caller must fetch a fresh snapshot."""


@dataclass
class FullDepthOrderBook:
    symbol: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_update_id: int | None = None
    _buffer: list[dict] = field(default_factory=list)
    _synced: bool = False

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None
        self._buffer.clear()
        self._synced = False

    def buffer_event(self, event: dict) -> None:
        """Call for every diff event received while not yet synced."""
        self._buffer.append(event)

    def drain_buffer(self) -> list[dict]:
        buffered, self._buffer = self._buffer, []
        return buffered

    def apply_snapshot(self, snapshot: dict) -> None:
        self.bids = {Decimal(p): Decimal(q) for p, q in snapshot["bids"]}
        self.asks = {Decimal(p): Decimal(q) for p, q in snapshot["asks"]}
        self.last_update_id = int(snapshot["lastUpdateId"])
        self._buffer = [e for e in self._buffer if e["u"] > self.last_update_id]
        self._synced = False  # still need to find/apply the bracketing event

    def apply_event(self, event: dict) -> None:
        """Apply one diff event. Call apply_snapshot() first, then replay
        drained buffered events through this method, then call it live for
        every new event as it arrives."""
        if self.last_update_id is None:
            raise ResyncRequired("no snapshot applied yet")

        if not self._synced:
            if event["u"] <= self.last_update_id:
                return  # stale, predates the snapshot
            if not (event["U"] <= self.last_update_id + 1 <= event["u"]):
                raise ResyncRequired("first event does not bracket the snapshot")
            self._synced = True
        elif "pu" in event:
            if event["pu"] != self.last_update_id:
                raise ResyncRequired(
                    f"futures sequence gap: expected pu={self.last_update_id}, got {event['pu']}"
                )
        elif event["U"] != self.last_update_id + 1:
            raise ResyncRequired(
                f"spot sequence gap: expected U={self.last_update_id + 1}, got {event['U']}"
            )

        for price_str, qty_str in event["b"]:
            self._apply_level(self.bids, price_str, qty_str)
        for price_str, qty_str in event["a"]:
            self._apply_level(self.asks, price_str, qty_str)
        self.last_update_id = event["u"]

    @staticmethod
    def _apply_level(side: dict[Decimal, Decimal], price_str: str, qty_str: str) -> None:
        price = Decimal(price_str)
        qty = Decimal(qty_str)
        if qty == 0:
            side.pop(price, None)
        else:
            side[price] = qty

    @property
    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    @property
    def mid_price(self) -> Decimal | None:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2

    @property
    def ready(self) -> bool:
        return self._synced and bool(self.bids) and bool(self.asks)
