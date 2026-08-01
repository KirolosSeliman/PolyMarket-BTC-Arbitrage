"""Async orchestration for Market Discovery, sources, state, and storage."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime
import itertools
import json
import time
from collections.abc import AsyncIterator

import pyarrow.parquet as pq

from polymarket_btc.data_collection.market_discovery import (
    GammaClient,
    MarketDiscoveryRunner,
    MarketResolver,
    TimeframeSnapshot,
    TransitionController,
    TransitionLogger,
)

from .binance_symbol_catalog import SymbolCatalog
from .config import MarketDataConfig
from .event_bus import EventBus
from .health import HealthRegistry, write_health_file
from .models import (
    EventSource,
    EventStream,
    BackpressureFatalError,
    GatewayInvariantError,
    MarketDataEvent,
    MarketDataSnapshot,
    MarketWindowPayload,
    MarketWindowStatePayload,
    SnapshotTickPayload,
    ShutdownTimeoutError,
)
from .snapshot import SnapshotPublisher
from .reducer import MarketDataReducer
from .sources.binance_spot import BinanceSpotSource
from .sources.binance_spot_full_depth import BinanceSpotFullDepthSource
from .sources.binance_futures_depth import BinanceFuturesDepthSource
from .sources.binance_futures_liquidations import BinanceFuturesLiquidationSource
from .sources.binance_futures_rest_poller import BinanceFuturesRestPoller
from .sources.binance_futures_rest_streams import (
    BinanceFuturesKlineRestSource,
    BinanceFuturesMarkPriceRestSource,
    BinanceFuturesTicker24hRestSource,
    BinanceFuturesTradeRestSource,
)
from .sources.polymarket_clob import PolymarketClobSource
from .sources.rtds_chainlink import ChainlinkRtdsSource
from .state import StateStore
from .storage import (
    PARQUET_SCHEMA,
    ParquetSnapshotWriter,
    RawEventStorage,
    recover_partial_files,
    stream_sha256,
)


@dataclass(frozen=True, slots=True)
class SourceInfo:
    label: str
    description: str
    # asset_kind is the top selection level (e.g. "crypto"); asset is the
    # specific asset within that kind (e.g. "BTC"). Both None means the
    # source isn't asset-scoped and belongs in the catch-all "other" bucket
    # (Polymarket, Chainlink). market is "spot" | "futures" | None, the axis
    # within an asset. Adding another kind or asset later (a non-crypto
    # kind, ETH, ...) is just more catalog entries -- the control panel
    # groups by whatever asset_kind/asset/market values it finds; nothing
    # here is BTC- or crypto-specific by construction.
    asset_kind: str | None
    asset: str | None
    market: str | None
    # tag is the second, cross-cutting browsing axis: "Par tag" groups every
    # asset-scoped source by what KIND of data it is (funding, order book,
    # liquidations...) instead of by asset -- so picking "Funding" surfaces
    # every asset's funding source together once more than one asset exists,
    # not just BTC's.
    tag: str | None = None
    # mode mirrors the plugin contract's PLUGIN_INFO["mode"]: "collect"
    # (default) means this source only exists as a live feed the gateway
    # subscribes to/polls. "access" means it also has a bulk historical
    # fetch implemented (see sources/binance_futures_historical.py), so it
    # can run under the control panel's "déjà collecté" mode (time range
    # instead of a live duration, no gateway involved).
    mode: str = "collect"
    # Only meaningful when mode == "access": Binance itself only retains
    # this many days of history server-side for some endpoints (open
    # interest, long/short ratio) -- None means no such limit (klines,
    # trades, mark price/funding are all unlimited, verified live). The
    # control panel's date picker uses this to clamp/hint the start date.
    historical_limit_days: int | None = None
    # Longer explanation shown in the control panel's (i) info bubble on
    # click -- distinct from `description`, which is always visible inline
    # and stays short. None means no bubble is offered for this source.
    detail: str | None = None


SOURCE_CATALOG: dict[str, SourceInfo] = {
    "chainlink": SourceInfo(
        "Chainlink RTDS", "Prix BTC/USD de référence, utilisé pour la résolution des marchés", None, None, None,
        detail=(
            "Prix de référence BTC/USD publié par Chainlink, utilisé pour déterminer "
            "l'issue des marchés Polymarket à l'échéance. Ce n'est pas un flux de "
            "trading -- juste le prix officiel de résolution."
        ),
    ),
    "polymarket": SourceInfo(
        "Polymarket", "Market discovery + carnets CLOB 5 min / 15 min", None, None, None,
        detail=(
            "Découverte des marchés Polymarket BTC actifs, plus leur carnet d'ordres "
            "CLOB à deux fréquences (5 min et 15 min). L'API publique de Polymarket ne "
            "renvoie que l'état en direct du carnet -- aucune archive historique "
            "officielle n'existe, donc ce flux n'est disponible qu'en collecte live."
        ),
    ),
    "binance_spot": SourceInfo(
        "Carnet + trades + bougies + stats 24h", "Un seul flux WebSocket combiné", "crypto", "BTC", "spot",
        tag="Flux combiné",
        detail=(
            "Un seul flux WebSocket Binance Spot combinant carnet, trades, bougies et "
            "statistiques 24h pour ce symbole. Collecte uniquement en direct -- pas "
            "d'archive groupée, chaque composant (bougies, trades...) a son propre "
            "historique séparé côté futures si besoin de données passées."
        ),
    ),
    "binance_spot_dom": SourceInfo(
        "DOM profondeur complète", "Carnet reconstruit, agrégé en paliers de prix", "crypto", "BTC", "spot",
        tag="Carnet",
        detail=(
            "Carnet d'ordres complet (profondeur intégrale) reconstruit à partir du "
            "flux de mises à jour, agrégé en paliers de prix. Binance n'archive pas le "
            "carnet historique -- ni gratuitement ni en payant -- donc disponible "
            "uniquement en collecte live, jamais en mode déjà collecté."
        ),
    ),
    "binance_futures_trade": SourceInfo(
        "Trades récents (prix)", "Polling REST incrémental", "crypto", "BTC", "futures",
        tag="Trades", mode="access",
        detail=(
            "Trades exécutés tick-by-tick (aggTrades Binance), historique illimité et "
            "gratuit depuis l'origine du symbole. C'est la donnée requise pour "
            "recréer le prix sous forme de bougies à une granularité personnalisée "
            "(1 seconde, 5 secondes...) : Binance Futures n'offre pas de bougies "
            "natives en dessous de la minute, contrairement au spot."
        ),
    ),
    "binance_futures_dom": SourceInfo(
        "DOM profondeur complète", "Carnet reconstruit, agrégé en paliers de prix", "crypto", "BTC", "futures",
        tag="Carnet",
        detail=(
            "Carnet d'ordres complet (profondeur intégrale) sur le contrat futures "
            "perpétuel, reconstruit en paliers de prix. Comme pour le spot, Binance ne "
            "conserve aucune archive du carnet -- disponible uniquement en collecte live."
        ),
    ),
    "binance_futures_mark_price": SourceInfo(
        "Mark price / funding", "Polling REST", "crypto", "BTC", "futures",
        tag="Funding", mode="access",
        detail=(
            "Prix mark (référence de liquidation) et taux de funding du contrat "
            "futures. Historique illimité et gratuit : bougies de mark price "
            "(markPriceKlines) + historique des paiements de funding aux 8h "
            "(fundingRate) -- c'est le taux effectivement payé, pas le taux continu "
            "affiché en direct."
        ),
    ),
    "binance_futures_liquidations": SourceInfo(
        "Liquidations forcées", "Flux WebSocket, filtré côté client sur ce symbole", "crypto", "BTC", "futures",
        tag="Liquidations",
        detail=(
            "Liquidations forcées exécutées sur Binance Futures, reçues en direct via "
            "WebSocket et filtrées côté client sur ce symbole. Aucune archive "
            "officielle chez Binance -- disponible uniquement en collecte live, jamais "
            "en mode déjà collecté."
        ),
    ),
    "binance_futures_open_interest_long_short": SourceInfo(
        "Open interest / long-short ratio", "Polling REST, 5 min", "crypto", "BTC", "futures",
        tag="Positionnement", mode="access", historical_limit_days=30,
        detail=(
            "Open interest total et ratio long/short des comptes top traders, toutes "
            "les 5 minutes. Historique disponible mais limité aux 30 derniers jours "
            "côté Binance, même en payant -- au-delà, seul un fournisseur tiers "
            "(ex. Tardis.dev) archive ces données."
        ),
    ),
    "binance_futures_kline": SourceInfo(
        "Bougies 1m", "Polling REST", "crypto", "BTC", "futures",
        tag="Bougies", mode="access",
        detail=(
            "Bougies OHLCV natives Binance à 1 minute, historique illimité et gratuit "
            "depuis l'origine du symbole. Pour une granularité plus fine (ex. 1 "
            "seconde), il faut reconstruire soi-même les bougies à partir des trades "
            "bruts -- voir 'Trades récents (prix)'."
        ),
    ),
    "binance_futures_ticker": SourceInfo(
        "Stats 24h", "Polling REST", "crypto", "BTC", "futures",
        tag="Statistiques",
        detail=(
            "Statistiques glissantes sur 24h (variation de prix, volume, plus haut/"
            "plus bas) rafraîchies en continu. Pas d'historique disponible -- "
            "seulement un instantané en direct."
        ),
    ),
}

# Flat key -> label view, kept for callers that only need the valid-key set
# or a plain description (enabled_sources filtering, existing tests).
SOURCE_KEYS: dict[str, str] = {key: info.label for key, info in SOURCE_CATALOG.items()}


def parse_source_key(key: str) -> tuple[str, str | None]:
    """Splits a catalog key into its base key and symbol override. Plain
    BTC keys ("binance_spot") have no colon and resolve to (key, None) --
    today's meaning, unchanged. A compound key ("binance_spot:ETH") is an
    extra-symbol request: (base_key, short_symbol)."""
    base_key, sep, symbol = key.partition(":")
    return (base_key, symbol) if sep else (key, None)


# Base keys that have a parameterized source class and can run for a symbol
# other than BTC (see parse_source_key/MarketDataGateway.extra_sources).
# binance_futures_liquidations is handled separately below: it's a single
# shared all-symbols connection, not one instance per symbol.
_EXTRA_SOURCE_CLASSES: dict[str, type] = {
    "binance_spot": BinanceSpotSource,
    "binance_spot_dom": BinanceSpotFullDepthSource,
    "binance_futures_dom": BinanceFuturesDepthSource,
    "binance_futures_mark_price": BinanceFuturesMarkPriceRestSource,
    "binance_futures_trade": BinanceFuturesTradeRestSource,
    "binance_futures_kline": BinanceFuturesKlineRestSource,
    "binance_futures_ticker": BinanceFuturesTicker24hRestSource,
}

# Which market (spot/futures) each extra-symbol-capable base key needs a
# real listing in, to decide which symbols can offer it. Includes
# binance_futures_liquidations even though it's built specially (one shared
# connection, see MarketDataGateway.__init__) -- it's still futures-scoped
# for catalog purposes. binance_futures_open_interest_long_short has no
# parameterized *live* source class (absent from _EXTRA_SOURCE_CLASSES
# above), but that's harmless here: its SOURCE_CATALOG template is
# mode="access", replace(template, asset=symbol) propagates that to every
# generated entry, and runs.py enforces mode server-side at run start (an
# access-only key is dropped from a collect run and vice versa) -- so a
# generated entry for it can only ever be run through the historical
# fetcher, never through MarketDataGateway's live construction path.
_EXTRA_BASE_KEY_MARKET: dict[str, str] = {
    "binance_spot": "spot",
    "binance_spot_dom": "spot",
    "binance_futures_trade": "futures",
    "binance_futures_dom": "futures",
    "binance_futures_mark_price": "futures",
    "binance_futures_liquidations": "futures",
    "binance_futures_kline": "futures",
    "binance_futures_ticker": "futures",
    "binance_futures_open_interest_long_short": "futures",
}


def build_extra_catalog(catalog: SymbolCatalog) -> dict[str, SourceInfo]:
    """Generates one SourceInfo per (base key, symbol) pair for every
    Binance symbol `catalog` lists as tradable in that base key's market --
    same label/description/tag as BTC's own entry for that base key (so
    "Par tag" groups them together), asset=symbol. BTC itself is skipped
    here since it already has its own static SOURCE_CATALOG entries."""
    entries: dict[str, SourceInfo] = {}
    for base_key, market in _EXTRA_BASE_KEY_MARKET.items():
        template = SOURCE_CATALOG[base_key]
        symbols = catalog.spot if market == "spot" else catalog.futures
        for symbol in sorted(symbols):
            if symbol == "BTC":
                continue
            entries[f"{base_key}:{symbol}"] = replace(template, asset=symbol)
    return entries


class MarketDataGateway:
    def __init__(
        self,
        config: MarketDataConfig,
        *,
        start_live_sources: bool = True,
        start_market_discovery: bool = True,
        enabled_sources: frozenset[str] | None = None,
    ) -> None:
        """enabled_sources restricts which built-in feeds actually connect.
        None (the default) means every feed -- existing callers (run/smoke/
        live) are unaffected. A non-None set is intersected against
        SOURCE_KEYS; unknown keys are silently ignored so a stale UI
        selection can't crash collection. Used by the control panel to run
        a user-picked subset for a data-collection export."""
        self.config = config
        self.start_live_sources = start_live_sources
        self.start_market_discovery = start_market_discovery
        self.enabled_sources = enabled_sources
        self.bus = EventBus(
            config.queues.state_capacity,
            config.queues.storage_capacity,
            config.queues.market_state_capacity,
            put_timeout_seconds=config.queues.put_timeout_seconds,
        )
        self.health_registry = HealthRegistry()
        self.state = StateStore(
            chainlink_stale_after_ms=config.rtds.stale_after_ms,
            binance_depth_stale_after_ms=config.binance.stale_depth_after_ms,
            binance_trade_stale_after_ms=config.binance.stale_trade_after_ms,
            health_registry=self.health_registry,
        )
        self.raw_storage = RawEventStorage(
            config.storage.data_dir,
            zstd_level=config.storage.zstd_level,
            rotate_seconds=config.storage.raw_rotate_seconds,
            rotate_bytes=config.storage.raw_rotate_bytes,
        )
        self.parquet_storage = ParquetSnapshotWriter(
            config.storage.data_dir,
            zstd_level=config.storage.zstd_level,
            rotate_seconds=config.storage.snapshot_rotate_seconds,
            rotate_rows=config.storage.snapshot_rotate_rows,
        )
        self.snapshot_storage_queue: asyncio.Queue[MarketDataSnapshot] = asyncio.Queue(
            config.queues.snapshot_storage_capacity
        )
        self._raw_written = 0
        self._snapshots_written = 0
        self.event_counts = {stream: 0 for stream in EventStream}
        self.publisher = SnapshotPublisher(
            subscriber_capacity=config.queues.subscriber_capacity,
        )
        self.reducer = MarketDataReducer(self.state)
        self._tick_sequence = 0
        self._sequence = itertools.count(1)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[object]] = []
        self._entered = False
        self._fatal: BaseException | None = None
        self._fatal_future: asyncio.Future[None] | None = None
        self.health: dict[str, object] = self.health_registry.to_health_file_payload(time.time_ns())
        self.chainlink = ChainlinkRtdsSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.binance = BinanceSpotSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.clob = PolymarketClobSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.spot_full_depth = BinanceSpotFullDepthSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.futures_depth = BinanceFuturesDepthSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.futures_mark_price = BinanceFuturesMarkPriceRestSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.futures_liquidations = BinanceFuturesLiquidationSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.futures_rest = BinanceFuturesRestPoller(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.futures_trade = BinanceFuturesTradeRestSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.futures_kline = BinanceFuturesKlineRestSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        self.futures_ticker = BinanceFuturesTicker24hRestSource(
            config,
            self._publish_source,
            lambda: 0,
            None,
            self.health_registry,
        )
        # Extra (non-BTC) symbols requested via compound enabled_sources keys
        # ("binance_spot:ETH"). Empty unless enabled_sources explicitly names
        # one -- enabled_sources=None (every CLI caller except the control
        # panel) means "all 9 BTC sources, exactly as before", never "and
        # every extra symbol too". Published through _publish_extra_source,
        # which bypasses state_queue entirely (see EventBus.publish_storage_only)
        # so these events never reach StateStore/the BTC composite snapshot.
        self.extra_sources: dict[str, object] = {}
        if enabled_sources:
            liquidation_symbols: set[str] = set()
            for key in enabled_sources:
                base_key, short_symbol = parse_source_key(key)
                if short_symbol is None:
                    continue
                pair = f"{short_symbol.upper()}USDT"
                if base_key == "binance_futures_liquidations":
                    liquidation_symbols.add(pair)
                    continue
                source_class = _EXTRA_SOURCE_CLASSES.get(base_key)
                if source_class is None:
                    continue
                self.extra_sources[key] = source_class(
                    config, self._publish_extra_source, lambda: 0, None, self.health_registry,
                    symbol=pair,
                )
            if liquidation_symbols:
                self.extra_sources["binance_futures_liquidations:*"] = BinanceFuturesLiquidationSource(
                    config, self._publish_extra_source, lambda: 0, None, self.health_registry,
                    symbol_filters=frozenset(liquidation_symbols),
                )
        resolver = MarketResolver(GammaClient())
        controller = TransitionController(resolver)
        transition_log = config.storage.data_dir / "market_discovery" / "transitions.jsonl"
        self.discovery = MarketDiscoveryRunner(
            resolver,
            controller,
            TransitionLogger(transition_log),
            self.market_discovery_callback,
        )

    def next_sequence(self) -> int:
        return next(self._sequence)

    def _source_enabled(self, key: str) -> bool:
        return self.enabled_sources is None or key in self.enabled_sources

    async def _publish_source(self, event: MarketDataEvent) -> None:
        """Allocate sequence numbers only for valid events submitted to the bus."""
        accepted = await self.bus.publish(event, self.next_sequence)
        if accepted:
            self.event_counts[event.stream] += 1

    async def _publish_extra_source(self, event: MarketDataEvent) -> None:
        """Same bookkeeping as _publish_source, but for extra (non-BTC)
        symbols: goes to storage_queue only (see publish_storage_only's
        docstring for why), and records connection health itself since
        StateStore.apply -- the only other place record_message() normally
        gets called -- never sees these events."""
        accepted = await self.bus.publish_storage_only(event, self.next_sequence)
        if accepted:
            self.event_counts[event.stream] += 1
            self.health_registry.record_message(
                event.source, event.received_wall_timestamp_ns, instrument=event.instrument,
            )

    def market_discovery_callback(self, snapshot: TimeframeSnapshot) -> None:
        self.bus.publish_market_state_nowait(snapshot)

    def _write_snapshot(self, snapshot: MarketDataSnapshot) -> None:
        self.parquet_storage.write(snapshot)
        self._snapshots_written += 1

    async def _reducer(self) -> None:
        while not self._stop.is_set() or not self.bus.state_queue.empty():
            try:
                event = await asyncio.wait_for(self.bus.state_queue.get(), timeout=0.1)
            except TimeoutError:
                continue
            try:
                snapshot = self.reducer.apply(event)
                if snapshot is not None:
                    self.publisher.publish(snapshot)
                    try:
                        await asyncio.wait_for(
                            self.snapshot_storage_queue.put(snapshot),
                            timeout=self.config.queues.put_timeout_seconds,
                        )
                    except TimeoutError as exc:
                        raise BackpressureFatalError(
                            "snapshot storage queue remained full past the deadline"
                        ) from exc
            finally:
                self.bus.state_queue.task_done()

    async def _raw_writer(self) -> None:
        last_flush = time.monotonic()
        while not self._stop.is_set() or not self.bus.storage_queue.empty():
            try:
                first = await asyncio.wait_for(
                    self.bus.storage_queue.get(), timeout=0.05
                )
            except TimeoutError:
                continue
            batch = [first]
            if self.bus.storage_queue.qsize() < self.config.storage.raw_flush_events:
                await asyncio.sleep(
                    self.config.storage.raw_flush_interval_ms / 1_000
                )
            batch_limit = (
                self.bus.storage_queue.qsize() + 1
                if self._stop.is_set()
                else self.config.storage.raw_flush_events
            )
            while (
                len(batch) < batch_limit
                and not self.bus.storage_queue.empty()
            ):
                batch.append(self.bus.storage_queue.get_nowait())
            now = time.monotonic()
            fsync = (
                now - last_flush
                >= self.config.storage.raw_fsync_interval_ms / 1_000
            )
            try:
                await asyncio.to_thread(self._persist_raw_batch, batch, fsync)
                self._raw_written += len(batch)
                if fsync:
                    last_flush = now
            finally:
                for _event in batch:
                    self.bus.storage_queue.task_done()

    async def _parquet_writer(self) -> None:
        while not self._stop.is_set() or not self.snapshot_storage_queue.empty():
            try:
                snapshot = await asyncio.wait_for(
                    self.snapshot_storage_queue.get(), timeout=0.1
                )
            except TimeoutError:
                continue
            try:
                await asyncio.to_thread(self._write_snapshot, snapshot)
            finally:
                self.snapshot_storage_queue.task_done()

    def _persist_raw_batch(
        self,
        events: list[MarketDataEvent],
        fsync: bool,
    ) -> None:
        for event in events:
            self.raw_storage.write(event)
        self.raw_storage.flush(fsync=fsync)

    async def _market_manager(self) -> None:
        while not self._stop.is_set() or not self.bus.market_state_queue.empty():
            try:
                value = await asyncio.wait_for(
                    self.bus.market_state_queue.get(), timeout=0.1
                )
            except TimeoutError:
                continue
            try:
                if not isinstance(value, TimeframeSnapshot):
                    raise GatewayInvariantError("market state queue contains an invalid value")
                self.clob.on_market_snapshot(value)
                now_ns = time.time_ns()
                event_time_ns = int(
                    value.updated_at_utc.timestamp() * 1_000_000_000
                )
                def market_payload(market: object) -> MarketWindowPayload | None:
                    if market is None:
                        return None
                    return MarketWindowPayload(
                        value.timeframe,
                        market.market_id,  # type: ignore[union-attr]
                        market.condition_id,  # type: ignore[union-attr]
                        market.slug,  # type: ignore[union-attr]
                        int(market.start_time_utc.timestamp() * 1_000_000_000),  # type: ignore[union-attr]
                        int(market.end_time_utc.timestamp() * 1_000_000_000),  # type: ignore[union-attr]
                        market.up_token_id,  # type: ignore[union-attr]
                        market.down_token_id,  # type: ignore[union-attr]
                        market.resolution_source,  # type: ignore[union-attr]
                    )
                payload = MarketWindowStatePayload(
                    value.state.value,
                    value.timeframe,
                    market_payload(value.current_market),
                    market_payload(value.next_market),
                    (
                        None
                        if value.expected_transition_utc is None
                        else int(value.expected_transition_utc.timestamp() * 1_000_000_000)
                    ),
                    event_time_ns,
                    value.attempt_count,
                    value.last_error,
                )
                event = MarketDataEvent(
                    2,
                    0,
                    f"market:{value.timeframe.value}:{value.state.value}:{event_time_ns}",
                    EventSource.MARKET_DISCOVERY,
                    EventStream.MARKET_WINDOW_STATE,
                    f"BTC-{value.timeframe.value}",
                    event_time_ns,
                    None,
                    now_ns,
                    time.monotonic_ns(),
                    None,
                    value.timeframe,
                    None,
                    None,
                    None,
                    None,
                    payload,
                )
                accepted = await self.bus.publish(event, self.next_sequence)
                if accepted:
                    self.event_counts[event.stream] += 1
            finally:
                self.bus.market_state_queue.task_done()

    async def _snapshot_scheduler(self) -> None:
        interval = self.config.service.snapshot_interval_ms / 1_000
        next_slot = time.monotonic()
        while not self._stop.is_set():
            next_slot += interval
            delay = next_slot - time.monotonic()
            if delay > 0:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                    return
                except TimeoutError:
                    pass
            else:
                next_slot = time.monotonic() + interval
            self._tick_sequence += 1
            now_ns = time.time_ns()
            event = MarketDataEvent(
                2,
                0,
                f"snapshot-tick:{self._tick_sequence}",
                EventSource.MARKET_DISCOVERY,
                EventStream.SNAPSHOT_TICK,
                "gateway",
                now_ns,
                None,
                now_ns,
                time.monotonic_ns(),
                str(self._tick_sequence),
                None,
                None,
                None,
                None,
                None,
                SnapshotTickPayload(
                    self._tick_sequence,
                    now_ns,
                    self.health_registry.all_source_snapshots(now_ns),
                ),
            )
            accepted = await self.bus.publish(event, self.next_sequence)
            if accepted:
                self.event_counts[event.stream] += 1

    async def _health_writer(self) -> None:
        while not self._stop.is_set():
            self._update_health("running")
            write_health_file(self.config.health.health_file, self.health)
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.config.health.update_interval_seconds,
                )
            except TimeoutError:
                pass

    def _update_health(self, state: str) -> None:
        snapshot = self.state.snapshot(time.time_ns(), 0)
        self.health.update({
            "gateway_state": state,
            "ready": snapshot.ready_for_strategy,
            "not_ready_reasons": list(snapshot.not_ready_reasons),
            "connections": {
                "rtds": self.chainlink.connected,
                "binance": self.binance.connected,
                "clob": self.clob.connected,
                "binance_spot_full_depth": self.spot_full_depth.connected,
                "binance_futures_depth": self.futures_depth.connected,
                "binance_futures_mark_price": self.futures_mark_price.connected,
                "binance_futures_liquidations": self.futures_liquidations.connected,
                "binance_futures_rest": self.futures_rest.connected,
                "binance_futures_trade": self.futures_trade.connected,
                "binance_futures_kline": self.futures_kline.connected,
                "binance_futures_ticker": self.futures_ticker.connected,
            },
            "reconnect_count": (
                self.chainlink.reconnect_count
                + self.binance.reconnect_count
                + self.clob.reconnect_count
                + self.spot_full_depth.reconnect_count
                + self.futures_depth.reconnect_count
                + self.futures_mark_price.reconnect_count
                + self.futures_liquidations.reconnect_count
                + self.futures_rest.reconnect_count
                + self.futures_trade.reconnect_count
                + self.futures_kline.reconnect_count
                + self.futures_ticker.reconnect_count
            ),
            "invalid_message_count": (
                self.chainlink.invalid_count
                + self.binance.invalid_count
                + self.clob.invalid_count
                + self.spot_full_depth.invalid_count
                + self.futures_depth.invalid_count
                + self.futures_mark_price.invalid_count
                + self.futures_liquidations.invalid_count
                + self.futures_rest.invalid_count
                + self.futures_trade.invalid_count
                + self.futures_kline.invalid_count
                + self.futures_ticker.invalid_count
            ),
            "duplicate_count": sum(
                row.duplicate_count
                for _source, row in self.health_registry.all_source_snapshots(time.time_ns())
            ),
            "divergence_count": self.clob.divergence_count,
            "queue_size": {
                "state": self.bus.state_queue.qsize(),
                "storage": self.bus.storage_queue.qsize(),
                "market_state": self.bus.market_state_queue.qsize(),
            },
            "queue_capacity": {
                "state": self.bus.state_queue.maxsize,
                "storage": self.bus.storage_queue.maxsize,
                "market_state": self.bus.market_state_queue.maxsize,
            },
            "queue_high_water": dict(self.bus.high_water),
            "raw_events_written": self._raw_written,
            "snapshots_written": self._snapshots_written,
            "active_token_ids": sorted(self.clob.assets),
        })
        self.health_registry.update_runtime(**self.health)
        self.health = self.health_registry.to_health_file_payload(time.time_ns())

    def _spawn(self, coroutine: object, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)  # type: ignore[arg-type]
        task.add_done_callback(self._task_done)
        self._tasks.append(task)

    def _task_done(self, task: asyncio.Task[object]) -> None:
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None and self._fatal is None:
            self._fatal = exception
            self.health_registry.record_fatal(exception, time.time_ns())
            if self._fatal_future is not None and not self._fatal_future.done():
                self._fatal_future.set_exception(exception)
            self._stop.set()

    async def wait_for_fatal(self) -> None:
        if self._fatal_future is None:
            self._fatal_future = asyncio.get_running_loop().create_future()
        await self._fatal_future

    def runtime_report(self) -> dict[str, object]:
        snapshot = self.state.snapshot(time.time_ns(), self._tick_sequence)
        raw_root = self.config.storage.data_dir / "raw"
        snapshots_root = self.config.storage.data_dir / "snapshots"
        raw_manifests = list(raw_root.rglob("*.manifest.json"))
        parquet_manifests = list(snapshots_root.rglob("*.manifest.json"))
        parquet_files = list(snapshots_root.rglob("*.parquet"))
        raw_valid = bool(raw_manifests)
        for manifest in raw_manifests:
            try:
                value = json.loads(manifest.read_text(encoding="utf-8"))
                data = manifest.with_name(str(value["relative_path"]))
                raw_valid = raw_valid and data.is_file() and stream_sha256(data) == value["sha256"]
            except (OSError, ValueError, KeyError):
                raw_valid = False
        parquet_valid = bool(parquet_manifests and parquet_files)
        for manifest in parquet_manifests:
            try:
                value = json.loads(manifest.read_text(encoding="utf-8"))
                data = manifest.with_name(manifest.name.replace(".manifest.json", ".parquet"))
                metadata = pq.read_metadata(data)
                schema_names = set(pq.read_table(data).schema.names)
                parquet_valid = parquet_valid and data.is_file()
                parquet_valid = parquet_valid and stream_sha256(data) == value["sha256"]
                parquet_valid = parquet_valid and metadata.num_rows == int(value["row_count"])
                parquet_valid = parquet_valid and set(PARQUET_SCHEMA.names).issubset(schema_names)
            except (OSError, ValueError, KeyError, TypeError):
                parquet_valid = False
        parquet_readable = True
        for path in parquet_files:
            try:
                table = pq.read_table(path)
                parquet_readable = parquet_readable and table.num_rows > 0
            except Exception:
                parquet_readable = False
        health_file_valid = False
        health_ready = False
        try:
            health_payload = json.loads(self.config.health.health_file.read_text(encoding="utf-8"))
            health_file_valid = (
                isinstance(health_payload, dict)
                and isinstance(health_payload.get("gateway_state"), str)
                and isinstance(health_payload.get("updated_at_ns"), int)
            )
            health_ready = health_file_valid and health_payload.get("ready") is True
        except (OSError, ValueError):
            pass
        return {
            "event_counts": {stream.value: count for stream, count in self.event_counts.items()},
            "raw_events_written": self._raw_written,
            "snapshots_written": self._snapshots_written,
            "active_token_ids": sorted(self.clob.assets),
            "initialized_active_books": sum(
                1 for asset in self.state._books_by_asset.values()
                if asset.initialized and asset.asset_id in self.clob.assets
            ),
            "final_snapshot": snapshot,
            "fatal_error": None if self._fatal is None else repr(self._fatal),
            "raw_manifest_valid": raw_valid,
            "parquet_manifest_valid": parquet_valid,
            "parquet_readable": parquet_readable and bool(parquet_files),
            "health_file_valid": health_file_valid,
            "health_ready": health_ready,
            "queues_drained": all(queue.empty() for queue in (
                self.bus.state_queue, self.bus.storage_queue,
                self.bus.market_state_queue, self.snapshot_storage_queue,
            )),
        }

    async def __aenter__(self) -> "MarketDataGateway":
        if self._entered:
            raise RuntimeError("gateway cannot be entered twice")
        self._entered = True
        self._fatal_future = asyncio.get_running_loop().create_future()
        recover_partial_files(
            self.config.storage.data_dir,
            zstd_level=self.config.storage.zstd_level,
        )
        self._spawn(self._reducer(), "market-data-reducer")
        self._spawn(self._raw_writer(), "market-data-raw-writer")
        self._spawn(self._parquet_writer(), "market-data-parquet-writer")
        self._spawn(self._market_manager(), "market-data-market-manager")
        self._spawn(self._snapshot_scheduler(), "market-data-snapshot-scheduler")
        self._spawn(self._health_writer(), "market-data-health")
        if self.start_live_sources:
            if self._source_enabled("chainlink"):
                self._spawn(self.chainlink.run(), "market-data-rtds")
            if self._source_enabled("binance_spot"):
                self._spawn(self.binance.run(), "market-data-binance")
            if self._source_enabled("polymarket"):
                self._spawn(self.clob.run(), "market-data-clob")
            if self._source_enabled("binance_spot_dom"):
                self._spawn(self.spot_full_depth.run(), "market-data-spot-full-depth")
            if self._source_enabled("binance_futures_dom"):
                self._spawn(self.futures_depth.run(), "market-data-futures-depth")
            if self._source_enabled("binance_futures_mark_price"):
                self._spawn(self.futures_mark_price.run(), "market-data-futures-mark-price")
            if self._source_enabled("binance_futures_liquidations"):
                self._spawn(self.futures_liquidations.run(), "market-data-futures-liquidations")
            if self._source_enabled("binance_futures_open_interest_long_short"):
                self._spawn(self.futures_rest.run(), "market-data-futures-rest")
            if self._source_enabled("binance_futures_trade"):
                self._spawn(self.futures_trade.run(), "market-data-futures-trade")
            if self._source_enabled("binance_futures_kline"):
                self._spawn(self.futures_kline.run(), "market-data-futures-kline")
            if self._source_enabled("binance_futures_ticker"):
                self._spawn(self.futures_ticker.run(), "market-data-futures-ticker")
            for key, source in self.extra_sources.items():
                self._spawn(source.run(), f"market-data-extra-{key}")
        if self.start_market_discovery and self._source_enabled("polymarket"):
            self._spawn(self.discovery.run_forever(), "market-discovery")
        return self

    async def snapshots(self) -> AsyncIterator[MarketDataSnapshot]:
        queue = self.publisher.subscribe()
        try:
            while True:
                if self._fatal is not None:
                    raise self._fatal
                try:
                    snapshot = await asyncio.wait_for(queue.get(), timeout=0.5)
                except TimeoutError:
                    if self._stop.is_set():
                        break
                    continue
                if snapshot is None:
                    break
                yield snapshot
        finally:
            self.publisher.unsubscribe(queue)

    async def shutdown(self) -> None:
        if not self._entered:
            return
        self._update_health("stopping")
        await self.publisher.stop()
        await asyncio.gather(
            self.chainlink.stop(),
            self.binance.stop(),
            self.clob.stop(),
            self.spot_full_depth.stop(),
            self.futures_depth.stop(),
            self.futures_mark_price.stop(),
            self.futures_liquidations.stop(),
            self.futures_rest.stop(),
            self.futures_trade.stop(),
            self.futures_kline.stop(),
            self.futures_ticker.stop(),
            *(source.stop() for source in self.extra_sources.values()),
        )
        for task in self._tasks:
            if task.get_name() in {
                "market-discovery",
                "market-data-rtds",
                "market-data-binance",
                "market-data-clob",
                "market-data-spot-full-depth",
                "market-data-futures-depth",
                "market-data-futures-mark-price",
                "market-data-futures-liquidations",
                "market-data-futures-rest",
                "market-data-futures-trade",
                "market-data-futures-kline",
                "market-data-futures-ticker",
                "market-data-snapshot-publisher",
                "market-data-snapshot-scheduler",
                "market-data-health",
            } or task.get_name().startswith("market-data-extra-"):
                task.cancel()
        self._stop.set()
        try:
            async with asyncio.timeout(self.config.service.shutdown_timeout_seconds):
                await self.bus.state_queue.join()
                await self.bus.storage_queue.join()
                await self.bus.market_state_queue.join()
                await self.snapshot_storage_queue.join()
                await asyncio.gather(*self._tasks, return_exceptions=True)
        except TimeoutError as exc:
            self.health["last_fatal_error"] = "shutdown_timeout"
            raise ShutdownTimeoutError("gateway shutdown timed out") from exc
        finally:
            self.raw_storage.flush(fsync=True)
            self.raw_storage.close()
            self.parquet_storage.close()
            self.bus.close()
            self._update_health("stopped")
            write_health_file(self.config.health.health_file, self.health)
            self._entered = False

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.shutdown()
