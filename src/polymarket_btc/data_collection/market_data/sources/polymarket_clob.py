"""Polymarket CLOB connection, subscriptions, and pure message parsing."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import asyncio
import time
import uuid
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
    SourceStatusPayload,
    parse_decimal,
)
from ..config import MarketDataConfig
from .base import ExponentialBackoff


@dataclass(frozen=True, slots=True)
class ClobAssetMetadata:
    asset_id: str
    timeframe: Timeframe
    market_id: str
    condition_id: str
    outcome: Outcome


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


def _book_levels(value: object, name: str) -> tuple[PriceLevel, ...]:
    if not isinstance(value, list):
        raise InvalidEventError(f"{name} must be a list")
    result: dict[object, object] = {}
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
    prices = sorted(result, reverse=name == "bids")
    return tuple(PriceLevel(price, result[price]) for price in prices)  # type: ignore[arg-type]


def _make_event(
    metadata: ClobAssetMetadata,
    event_type: str,
    stream: EventStream,
    payload: object,
    message: dict[str, object],
    timestamp_ns: int,
    wall_ns: int,
    monotonic_ns: int,
    sequence: int,
    source_session_id: str | None,
) -> MarketDataEvent:
    return MarketDataEvent(
        schema_version=2,
        ingest_sequence=sequence,
        event_id=_event_id(message, metadata.asset_id, event_type, timestamp_ns),
        source=EventSource.POLYMARKET_CLOB,
        stream=stream,
        instrument=metadata.asset_id,
        source_timestamp_ns=timestamp_ns,
        server_timestamp_ns=None,
        received_wall_timestamp_ns=wall_ns,
        received_monotonic_ns=monotonic_ns,
        source_sequence=message.get("hash") if isinstance(message.get("hash"), str) else None,
        timeframe=metadata.timeframe,
        market_id=metadata.market_id,
        condition_id=metadata.condition_id,
        asset_id=metadata.asset_id,
        outcome=metadata.outcome,
        payload=payload,  # type: ignore[arg-type]
        source_session_id=source_session_id,
    )


def apply_clob_message(
    message: object,
    assets: dict[str, ClobAssetMetadata],
    received_wall_timestamp_ns: int,
    received_monotonic_ns: int,
    ingest_sequence_start: int,
    source_session_id: str | None = None,
) -> tuple[MarketDataEvent, ...]:
    if not isinstance(message, dict):
        raise InvalidEventError("CLOB message must be an object")
    event_type = message.get("event_type")
    timestamp_ns = _timestamp(message)
    if event_type == "market_resolved":
        raw_asset_ids = message.get("assets_ids")
        if raw_asset_ids is not None and (
            not isinstance(raw_asset_ids, list)
            or not all(isinstance(item, str) and item for item in raw_asset_ids)
        ):
            raise InvalidEventError("market_resolved assets_ids is invalid")
        market = message.get("market")
        condition = message.get("condition_id")
        affected = (
            tuple(raw_asset_ids)
            if isinstance(raw_asset_ids, list)
            else tuple(
                asset_id
                for asset_id, metadata in assets.items()
                if (
                    isinstance(market, str)
                    and market in {metadata.market_id, metadata.condition_id}
                )
                or (
                    isinstance(condition, str)
                    and metadata.condition_id == condition
                )
            )
        )
        if not affected or any(asset_id not in assets for asset_id in affected):
            raise InvalidEventError("unknown CLOB market resolution")
        winning_asset = (
            message.get("winning_asset_id")
            if isinstance(message.get("winning_asset_id"), str)
            else None
        )
        outcome_value = message.get("winning_outcome")
        winning_outcome = None
        if isinstance(outcome_value, str) and outcome_value.lower() in {"up", "down"}:
            winning_outcome = Outcome(outcome_value.lower())
        elif winning_asset in assets:
            winning_outcome = assets[winning_asset].outcome
        payload = PolymarketResolvedPayload(
            winning_asset,
            winning_outcome,
            affected,
            str(market) if isinstance(market, str) else assets[affected[0]].market_id,
            (
                str(condition)
                if isinstance(condition, str)
                else assets[affected[0]].condition_id
            ),
        )
        return tuple(
            _make_event(
                assets[asset_id],
                "market_resolved",
                EventStream.POLYMARKET_MARKET_RESOLVED,
                payload,
                message,
                timestamp_ns,
                received_wall_timestamp_ns,
                received_monotonic_ns,
                ingest_sequence_start + offset,
                source_session_id,
            )
            for offset, asset_id in enumerate(affected)
        )
    if event_type == "price_change":
        changes = message.get("price_changes")
        if not isinstance(changes, list) or not changes:
            raise InvalidEventError("price_changes must be non-empty")
        events: list[MarketDataEvent] = []
        for offset, change in enumerate(changes):
            if not isinstance(change, dict):
                raise InvalidEventError("price change must be an object")
            asset_id = str(change.get("asset_id", ""))
            metadata = assets.get(asset_id)
            if metadata is None:
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
            change_message = dict(message)
            change_message.update(change)
            payload = PolymarketPriceChangePayload(
                side, price, quantity, best_bid, best_ask,
                change.get("hash") if isinstance(change.get("hash"), str) else None,
            )
            events.append(_make_event(
                metadata, "price_change", EventStream.POLYMARKET_PRICE_CHANGE,
                payload, change_message, timestamp_ns,
                received_wall_timestamp_ns, received_monotonic_ns,
                ingest_sequence_start + offset,
                source_session_id,
            ))
        return tuple(events)

    asset_id = str(message.get("asset_id", ""))
    metadata = assets.get(asset_id)
    if metadata is None:
        raise InvalidEventError("unknown CLOB asset")

    if event_type == "book":
        bids = _book_levels(message.get("bids"), "bids")
        asks = _book_levels(message.get("asks"), "asks")
        if bids and asks and bids[0].price >= asks[0].price:
            raise InvalidEventError("CLOB book is crossed")
        payload = PolymarketBookPayload(
            bids,
            asks,
            message.get("hash") if isinstance(message.get("hash"), str) else None,
        )
        stream = EventStream.POLYMARKET_BOOK
    elif event_type == "best_bid_ask":
        try:
            best_bid = parse_decimal(str(message["best_bid"]), "best_bid", allow_zero=True)
            best_ask = parse_decimal(str(message["best_ask"]), "best_ask", allow_zero=True)
            spread = parse_decimal(str(message["spread"]), "spread", allow_zero=True)
        except (KeyError, ValueError) as exc:
            raise InvalidEventError("invalid best bid/ask") from exc
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
        stream = EventStream.POLYMARKET_LAST_TRADE
    elif event_type == "tick_size_change":
        try:
            payload = PolymarketTickSizePayload(
                parse_decimal(str(message["old_tick_size"]), "old_tick_size", strictly_positive=True),
                parse_decimal(str(message["new_tick_size"]), "new_tick_size", strictly_positive=True),
            )
        except (KeyError, ValueError) as exc:
            raise InvalidEventError("invalid tick size") from exc
        stream = EventStream.POLYMARKET_TICK_SIZE_CHANGE
    else:
        raise InvalidEventError("unknown CLOB event type")

    return (_make_event(
        metadata, str(event_type), stream, payload, message, timestamp_ns,
        received_wall_timestamp_ns, received_monotonic_ns, ingest_sequence_start,
        source_session_id,
    ),)


class PolymarketClobSource:
    def __init__(
        self,
        config: MarketDataConfig,
        publish: Callable[[MarketDataEvent], Awaitable[None]],
        next_sequence: Callable[[], int],
        on_connection: Callable[[bool], None] | None = None,
    ) -> None:
        self._config = config
        self._publish = publish
        self._next_sequence = next_sequence
        self._stop = asyncio.Event()
        self._assets_changed = asyncio.Event()
        self._desired: set[str] = set()
        self._subscribed: set[str] = set()
        self._retire_at: dict[str, float] = {}
        self.assets: dict[str, ClobAssetMetadata] = {}
        self.reconnect_count = 0
        self.invalid_count = 0
        self.divergence_count = 0
        self.connected = False
        self.current_session_id: str | None = None
        self._on_connection = on_connection or (lambda _connected: None)

    def _set_connected(self, connected: bool) -> None:
        self.connected = connected
        self._on_connection(connected)

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
                self.assets[asset_id] = ClobAssetMetadata(
                    asset_id,
                    snapshot.timeframe,
                    market.market_id,
                    market.condition_id,
                    outcome,
                )
        owned = {
            asset_id for asset_id, metadata in self.assets.items()
            if metadata.timeframe is snapshot.timeframe
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
            self.assets.pop(asset, None)
            del self._retire_at[asset]

    async def stop(self) -> None:
        self._stop.set()
        self._assets_changed.set()

    async def _publish_status(
        self,
        *,
        connected: bool,
        session_id: str,
        reason: str | None,
    ) -> None:
        wall_ns = time.time_ns()
        await self._publish(MarketDataEvent(
            schema_version=2,
            ingest_sequence=self._next_sequence(),
            event_id=f"clob:status:{session_id}:{connected}:{wall_ns}",
            source=EventSource.POLYMARKET_CLOB,
            stream=EventStream.SOURCE_STATUS,
            instrument="POLYMARKET_CLOB",
            source_timestamp_ns=wall_ns,
            server_timestamp_ns=None,
            received_wall_timestamp_ns=wall_ns,
            received_monotonic_ns=time.monotonic_ns(),
            source_sequence=None,
            timeframe=None,
            market_id=None,
            condition_id=None,
            asset_id=None,
            outcome=None,
            payload=SourceStatusPayload(
                EventSource.POLYMARKET_CLOB,
                connected,
                session_id,
                self.reconnect_count,
                reason,
            ),
            source_session_id=session_id,
        ))

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
            disconnect_reason: str | None = None
            session_id: str | None = None
            try:
                initial = sorted(self._desired)
                async with connect(
                    self._config.clob.url,
                    max_size=4 * 1024 * 1024,
                    ping_interval=None,
                ) as websocket:
                    session_id = uuid.uuid4().hex
                    self.current_session_id = session_id
                    self._set_connected(True)
                    await self._publish_status(
                        connected=True,
                        session_id=session_id,
                        reason=None,
                    )
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
                                    self.assets,
                                    wall_ns,
                                    monotonic_ns,
                                    self._next_sequence(),
                                    session_id,
                                )
                                for event in events:
                                    await self._publish(event)
                        except (json.JSONDecodeError, InvalidEventError, TypeError):
                            self.invalid_count += 1
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                disconnect_reason = str(exc)
                self.reconnect_count += 1
                self._subscribed.clear()
                if self._stop.is_set():
                    break
                await asyncio.sleep(backoff.next_delay())
            finally:
                self._set_connected(False)
                if session_id is not None:
                    await self._publish_status(
                        connected=False,
                        session_id=session_id,
                        reason=disconnect_reason or "connection_closed",
                    )
                for task in (heartbeat, subscriptions):
                    if task is not None:
                        task.cancel()
                await asyncio.gather(
                    *(task for task in (heartbeat, subscriptions) if task is not None),
                    return_exceptions=True,
                )
