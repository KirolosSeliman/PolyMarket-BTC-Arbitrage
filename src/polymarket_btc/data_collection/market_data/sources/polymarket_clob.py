"""Polymarket CLOB market-channel parsing and local order books."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import hashlib
import json
import asyncio
import time
from collections.abc import Awaitable, Callable

from websockets.asyncio.client import connect

from polymarket_btc.data_collection.market_discovery import Timeframe

from ..models import (
    EventSource,
    EventStream,
    InvalidEventError,
    MarketDataEvent,
    Outcome,
    PolymarketBestBidAskPayload,
    PolymarketBookPayload,
    PolymarketLastTradePayload,
    PolymarketPriceChangePayload,
    PolymarketResolvedPayload,
    PolymarketTickSizePayload,
    PriceLevel,
    parse_decimal,
)
from ..config import MarketDataConfig
from .base import ExponentialBackoff


@dataclass(slots=True)
class ClobBookState:
    asset_id: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    last_trade: PolymarketLastTradePayload | None = None
    tick_size: Decimal | None = None
    initialized: bool = False
    coherent: bool = True
    resolved: bool = False
    divergence_count: int = 0
    last_event_timestamp_ns: int | None = None
    timeframe: Timeframe | None = None
    market_id: str | None = None
    condition_id: str | None = None
    outcome: Outcome | None = None
    book_hash: str | None = None


def initial_subscription(asset_ids: list[str]) -> dict[str, object]:
    return {
        "assets_ids": asset_ids,
        "type": "market",
        "custom_feature_enabled": True,
    }


def subscription_update(asset_ids: list[str], operation: str) -> dict[str, object]:
    if operation not in {"subscribe", "unsubscribe"}:
        raise ValueError("operation must be subscribe or unsubscribe")
    result: dict[str, object] = {"assets_ids": asset_ids, "operation": operation}
    if operation == "subscribe":
        result["custom_feature_enabled"] = True
    return result


def _timestamp(message: dict[str, object]) -> int:
    value = message.get("timestamp")
    try:
        timestamp_ms = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise InvalidEventError("CLOB timestamp is invalid") from exc
    if timestamp_ms <= 0:
        raise InvalidEventError("CLOB timestamp must be positive")
    return timestamp_ms * 1_000_000


def _event_id(message: dict[str, object], asset_id: str, event_type: str, timestamp_ns: int) -> str:
    supplied_hash = message.get("hash")
    if isinstance(supplied_hash, str) and supplied_hash:
        return f"polymarket:{event_type}:{asset_id}:{supplied_hash}"
    canonical = json.dumps(message, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(
        f"{asset_id}:{event_type}:{timestamp_ns}:{canonical}".encode()
    ).hexdigest()
    return f"polymarket:{event_type}:{asset_id}:{digest}"


def _book_levels(value: object, name: str) -> dict[Decimal, Decimal]:
    if not isinstance(value, list):
        raise InvalidEventError(f"{name} must be a list")
    result: dict[Decimal, Decimal] = {}
    for row in value:
        if not isinstance(row, dict):
            raise InvalidEventError(f"{name} level must be an object")
        try:
            price = parse_decimal(str(row.get("price")), f"{name}.price", strictly_positive=True)
            quantity = parse_decimal(str(row.get("size")), f"{name}.size", allow_zero=True)
        except ValueError as exc:
            raise InvalidEventError(str(exc)) from exc
        if price in result:
            raise InvalidEventError(f"duplicate {name} price")
        if quantity:
            result[price] = quantity
    return result


def _refresh(book: ClobBookState) -> None:
    book.best_bid = max(book.bids, default=None)
    book.best_ask = min(book.asks, default=None)
    if book.best_bid is not None and book.best_ask is not None and book.best_bid >= book.best_ask:
        book.coherent = False


def _make_event(
    book: ClobBookState,
    event_type: str,
    stream: EventStream,
    payload: object,
    message: dict[str, object],
    timestamp_ns: int,
    wall_ns: int,
    monotonic_ns: int,
    sequence: int,
) -> MarketDataEvent:
    return MarketDataEvent(
        schema_version=1,
        ingest_sequence=sequence,
        event_id=_event_id(message, book.asset_id, event_type, timestamp_ns),
        source=EventSource.POLYMARKET_CLOB,
        stream=stream,
        instrument=book.asset_id,
        source_timestamp_ns=timestamp_ns,
        server_timestamp_ns=None,
        received_wall_timestamp_ns=wall_ns,
        received_monotonic_ns=monotonic_ns,
        source_sequence=message.get("hash") if isinstance(message.get("hash"), str) else None,
        timeframe=book.timeframe,
        market_id=book.market_id,
        condition_id=book.condition_id or (
            str(message["market"]) if message.get("market") is not None else None
        ),
        asset_id=book.asset_id,
        outcome=book.outcome,
        payload=payload,  # type: ignore[arg-type]
    )


def apply_clob_message(
    message: object,
    books: dict[str, ClobBookState],
    received_wall_timestamp_ns: int,
    received_monotonic_ns: int,
    ingest_sequence_start: int,
) -> tuple[MarketDataEvent, ...]:
    if not isinstance(message, dict):
        raise InvalidEventError("CLOB message must be an object")
    event_type = message.get("event_type")
    timestamp_ns = _timestamp(message)
    if event_type == "price_change":
        changes = message.get("price_changes")
        if not isinstance(changes, list) or not changes:
            raise InvalidEventError("price_changes must be non-empty")
        events: list[MarketDataEvent] = []
        for offset, change in enumerate(changes):
            if not isinstance(change, dict):
                raise InvalidEventError("price change must be an object")
            asset_id = str(change.get("asset_id", ""))
            book = books.get(asset_id)
            if book is None:
                raise InvalidEventError("unknown CLOB asset")
            side = change.get("side")
            if side not in {"BUY", "SELL"}:
                raise InvalidEventError("unknown CLOB side")
            try:
                price = parse_decimal(str(change.get("price")), "price", strictly_positive=True)
                quantity = parse_decimal(str(change.get("size")), "size", allow_zero=True)
                best_bid = None if change.get("best_bid") is None else parse_decimal(
                    str(change["best_bid"]), "best_bid", allow_zero=True
                )
                best_ask = None if change.get("best_ask") is None else parse_decimal(
                    str(change["best_ask"]), "best_ask", allow_zero=True
                )
            except ValueError as exc:
                raise InvalidEventError(str(exc)) from exc
            levels = book.bids if side == "BUY" else book.asks
            if quantity == 0:
                levels.pop(price, None)
            else:
                levels[price] = quantity
            _refresh(book)
            book.last_event_timestamp_ns = timestamp_ns
            change_message = dict(message)
            change_message.update(change)
            payload = PolymarketPriceChangePayload(
                side, price, quantity, best_bid, best_ask,
                change.get("hash") if isinstance(change.get("hash"), str) else None,
            )
            events.append(_make_event(
                book, "price_change", EventStream.POLYMARKET_PRICE_CHANGE,
                payload, change_message, timestamp_ns,
                received_wall_timestamp_ns, received_monotonic_ns,
                ingest_sequence_start + offset,
            ))
        return tuple(events)

    asset_id = str(message.get("asset_id", ""))
    book = books.get(asset_id)
    if book is None:
        raise InvalidEventError("unknown CLOB asset")
    book.last_event_timestamp_ns = timestamp_ns

    if event_type == "book":
        bids = _book_levels(message.get("bids"), "bids")
        asks = _book_levels(message.get("asks"), "asks")
        book.bids, book.asks = bids, asks
        book.initialized = True
        book.coherent = True
        book.divergence_count = 0
        book.book_hash = message.get("hash") if isinstance(message.get("hash"), str) else None
        _refresh(book)
        if not book.coherent:
            raise InvalidEventError("CLOB book is crossed")
        payload = PolymarketBookPayload(
            tuple(PriceLevel(p, bids[p]) for p in sorted(bids, reverse=True)[:20]),
            tuple(PriceLevel(p, asks[p]) for p in sorted(asks)[:20]),
            book.book_hash,
        )
        stream = EventStream.POLYMARKET_BOOK
    elif event_type == "best_bid_ask":
        try:
            best_bid = parse_decimal(str(message["best_bid"]), "best_bid", allow_zero=True)
            best_ask = parse_decimal(str(message["best_ask"]), "best_ask", allow_zero=True)
            spread = parse_decimal(str(message["spread"]), "spread", allow_zero=True)
        except (KeyError, ValueError) as exc:
            raise InvalidEventError("invalid best bid/ask") from exc
        if best_bid != book.best_bid or best_ask != book.best_ask:
            book.divergence_count += 1
            if book.divergence_count >= 3:
                book.coherent = False
        else:
            book.divergence_count = 0
        payload = PolymarketBestBidAskPayload(best_bid, best_ask, spread)
        stream = EventStream.POLYMARKET_BEST_BID_ASK
    elif event_type == "last_trade_price":
        try:
            payload = PolymarketLastTradePayload(
                parse_decimal(str(message["price"]), "price", strictly_positive=True),
                parse_decimal(str(message["size"]), "size", strictly_positive=True),
                str(message["side"]),
                None if message.get("fee_rate_bps") is None else parse_decimal(
                    str(message["fee_rate_bps"]), "fee_rate_bps", allow_zero=True
                ),
            )
        except (KeyError, ValueError) as exc:
            raise InvalidEventError("invalid last trade") from exc
        book.last_trade = payload
        stream = EventStream.POLYMARKET_LAST_TRADE
    elif event_type == "tick_size_change":
        try:
            payload = PolymarketTickSizePayload(
                parse_decimal(str(message["old_tick_size"]), "old_tick_size", strictly_positive=True),
                parse_decimal(str(message["new_tick_size"]), "new_tick_size", strictly_positive=True),
            )
        except (KeyError, ValueError) as exc:
            raise InvalidEventError("invalid tick size") from exc
        book.tick_size = payload.new_tick_size
        stream = EventStream.POLYMARKET_TICK_SIZE_CHANGE
    elif event_type == "market_resolved":
        winning_asset = message.get("winning_asset_id")
        outcome_value = message.get("winning_outcome")
        outcome = None
        if isinstance(outcome_value, str) and outcome_value.lower() in {"up", "down"}:
            outcome = Outcome(outcome_value.lower())
        payload = PolymarketResolvedPayload(
            str(winning_asset) if winning_asset is not None else None,
            outcome,
        )
        book.resolved = True
        stream = EventStream.POLYMARKET_MARKET_RESOLVED
    else:
        raise InvalidEventError("unknown CLOB event type")

    return (_make_event(
        book, str(event_type), stream, payload, message, timestamp_ns,
        received_wall_timestamp_ns, received_monotonic_ns, ingest_sequence_start,
    ),)


class PolymarketClobSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
    ) -> None:
        self._config = config
        self._publish = publish
        self._next_sequence = next_sequence
        self._stop = asyncio.Event()
        self._assets_changed = asyncio.Event()
        self._desired: set[str] = set()
        self._subscribed: set[str] = set()
        self._retire_at: dict[str, float] = {}
        self.books: dict[str, ClobBookState] = {}
        self.reconnect_count = 0
        self.invalid_count = 0
        self.divergence_count = 0
        self.connected = False

    def on_market_snapshot(self, snapshot: object) -> None:
        from polymarket_btc.data_collection.market_discovery import TimeframeSnapshot
        if not isinstance(snapshot, TimeframeSnapshot):
            raise TypeError("expected TimeframeSnapshot")
        required = set()
        for market in (snapshot.current_market, snapshot.next_market):
            if market is None:
                continue
            for asset_id, outcome in (
                (market.up_token_id, Outcome.UP),
                (market.down_token_id, Outcome.DOWN),
            ):
                required.add(asset_id)
                book = self.books.setdefault(asset_id, ClobBookState(asset_id))
                book.timeframe = snapshot.timeframe
                book.market_id = market.market_id
                book.condition_id = market.condition_id
                book.outcome = outcome
        owned = {
            asset_id for asset_id, book in self.books.items()
            if book.timeframe is snapshot.timeframe
        }
        now = time.monotonic()
        for asset_id in owned - required:
            self._retire_at.setdefault(
                asset_id,
                now + self._config.clob.unsubscribe_grace_seconds,
            )
        for asset_id in required:
            self._retire_at.pop(asset_id, None)
        self._desired.update(required)
        self._assets_changed.set()

    def _expire_assets(self) -> None:
        now = time.monotonic()
        expired = [asset for asset, deadline in self._retire_at.items() if now >= deadline]
        for asset in expired:
            self._desired.discard(asset)
            del self._retire_at[asset]

    async def stop(self) -> None:
        self._stop.set()
        self._assets_changed.set()

    async def _heartbeat(self, websocket: object) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self._config.clob.heartbeat_seconds)
            await websocket.send("PING")  # type: ignore[attr-defined]

    async def _subscriptions(self, websocket: object) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._assets_changed.wait(), timeout=0.25)
            except TimeoutError:
                pass
            self._assets_changed.clear()
            self._expire_assets()
            added = sorted(self._desired - self._subscribed)
            removed = sorted(self._subscribed - self._desired)
            if added:
                await websocket.send(json.dumps(
                    subscription_update(added, "subscribe"), separators=(",", ":")
                ))  # type: ignore[attr-defined]
                self._subscribed.update(added)
            if removed:
                await websocket.send(json.dumps(
                    subscription_update(removed, "unsubscribe"), separators=(",", ":")
                ))  # type: ignore[attr-defined]
                self._subscribed.difference_update(removed)

    async def run(self) -> None:
        backoff = ExponentialBackoff(
            self._config.reconnect.initial_delay_seconds,
            self._config.reconnect.maximum_delay_seconds,
            self._config.reconnect.multiplier,
            self._config.reconnect.jitter_fraction,
        )
        while not self._stop.is_set():
            if not self._desired:
                self._assets_changed.clear()
                await self._assets_changed.wait()
                if self._stop.is_set():
                    return
            heartbeat: asyncio.Task[None] | None = None
            subscriptions: asyncio.Task[None] | None = None
            try:
                for book in self.books.values():
                    book.initialized = False
                initial = sorted(self._desired)
                async with connect(
                    self._config.clob.url,
                    max_size=4 * 1024 * 1024,
                    ping_interval=None,
                ) as websocket:
                    self.connected = True
                    await websocket.send(json.dumps(
                        initial_subscription(initial), separators=(",", ":")
                    ))
                    self._subscribed = set(initial)
                    heartbeat = asyncio.create_task(self._heartbeat(websocket))
                    subscriptions = asyncio.create_task(self._subscriptions(websocket))
                    async for raw in websocket:
                        wall_ns, monotonic_ns = time.time_ns(), time.monotonic_ns()
                        if self._stop.is_set():
                            break
                        try:
                            decoded = json.loads(raw)
                            messages = decoded if isinstance(decoded, list) else [decoded]
                            for message in messages:
                                events = apply_clob_message(
                                    message,
                                    self.books,
                                    wall_ns,
                                    monotonic_ns,
                                    self._next_sequence(),
                                )
                                for event in events:
                                    await self._publish(event)
                        except (json.JSONDecodeError, InvalidEventError, TypeError):
                            self.invalid_count += 1
                            continue
            except asyncio.CancelledError:
                raise
            except Exception:
                self.reconnect_count += 1
                self._subscribed.clear()
                if self._stop.is_set():
                    break
                await asyncio.sleep(backoff.next_delay())
            finally:
                self.connected = False
                for task in (heartbeat, subscriptions):
                    if task is not None:
                        task.cancel()
                await asyncio.gather(
                    *(task for task in (heartbeat, subscriptions) if task is not None),
                    return_exceptions=True,
                )
