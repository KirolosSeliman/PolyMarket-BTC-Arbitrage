"""Data-collection run lifecycle: start a gateway with a user-picked subset
of sources plus optional plugins, track its progress, stop it on request or
after a duration, and export one consolidated dataset file a future
backtest analyzer can consume.

One run at a time by design -- this is a hands-on collection tool, not a
scheduler; running two overlapping collections against the same Binance/
Polymarket connections would just double-count events for no benefit.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
import itertools
import json
import logging
from pathlib import Path
import re
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from ..market_data.binance_symbol_catalog import (
    DEFAULT_MAX_AGE_SECONDS,
    SymbolCatalog,
    load_cached_or_fetch,
)
from ..market_data.config import MarketDataConfig, load_config
from ..market_data.gateway import (
    SOURCE_CATALOG,
    MarketDataGateway,
    SourceInfo,
    build_extra_catalog,
    parse_source_key,
)
from ..market_data.sources.binance_futures_historical import HISTORICAL_FETCHERS
from ..market_data.storage import RawEventStorage
from .plugins import PluginContext, discover_plugins, run_plugin

_LOGGER = logging.getLogger(__name__)

PLUGIN_LOG_LINES = 200

# Mirrors discover_plugins' own rules: a simple flat filename, no path
# separators or traversal, and not starting with "_" (that prefix means
# "skip me" to the scanner, so a leading underscore is rejected here too --
# an imported plugin that discovery would silently ignore is worse than one
# rejected up front with a clear reason).
PLUGIN_FILENAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*\.py$")


def _iso(now_ns: int) -> str:
    return datetime.fromtimestamp(now_ns / 1_000_000_000, tz=UTC).isoformat()


def new_run_id(now_ns: int) -> str:
    instant = datetime.fromtimestamp(now_ns / 1_000_000_000, tz=UTC)
    return f"{instant:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"


@dataclass(slots=True)
class RunState:
    run_id: str
    sources: list[str]
    plugins: list[str]
    duration_seconds: float | None
    data_dir: Path
    started_at_ns: int
    # "collect" (default): live gateway + polling plugins, bounded by
    # duration_seconds. "access": no gateway -- only mode="access" plugins
    # run, each once, reading whatever start/end range they were given
    # (both optional; a plugin is free to ignore them).
    mode: str = "collect"
    start_ts_ns: int | None = None
    end_ts_ns: int | None = None
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    gateway: MarketDataGateway | None = None
    plugin_tasks: dict[str, asyncio.Task] = field(default_factory=dict)
    plugin_logs: dict[str, deque[str]] = field(default_factory=dict)
    # Only populated for access-mode built-in historical sources (plugins
    # aren't asked to report this) -- key -> 0..1 fraction of its requested
    # time range fetched so far. Lets the UI show a real percentage and an
    # estimated time remaining instead of an indeterminate spinner.
    source_progress: dict[str, float] = field(default_factory=dict)
    ended_at_ns: int | None = None
    error: str | None = None
    export: dict | None = None
    task: asyncio.Task | None = None


def export_run(state: RunState) -> dict:
    """Consolidates every Parquet snapshot part the run produced into one
    dataset.parquet, and writes manifest.json describing the run. Runs
    even on a failed/aborted collection so partial data isn't stranded."""
    snapshot_root = state.data_dir / "snapshots"
    parquet_files = sorted(snapshot_root.rglob("*.parquet")) if snapshot_root.is_dir() else []
    dataset_file: str | None = None
    row_count = 0
    if parquet_files:
        try:
            tables = [pq.read_table(path) for path in parquet_files]
            combined = pa.concat_tables(tables)
            dataset_path = state.data_dir / "dataset.parquet"
            pq.write_table(combined, dataset_path, compression="zstd")
            dataset_file = dataset_path.name
            row_count = combined.num_rows
        except Exception as exc:  # a corrupt part must not lose the manifest
            _LOGGER.exception("dataset export failed for run %s", state.run_id)
            state.error = state.error or f"export failed: {exc!r}"
    plugin_files = sorted(
        str(path.relative_to(state.data_dir))
        for path in state.data_dir.iterdir()
        if path.is_file() and path.suffix in (".jsonl", ".csv", ".log")
    ) if state.data_dir.is_dir() else []
    raw_root = state.data_dir / "raw"
    raw_files = sorted(
        str(path.relative_to(state.data_dir)) for path in raw_root.rglob("*.jsonl.zst")
    ) if raw_root.is_dir() else []
    manifest = {
        "run_id": state.run_id,
        "mode": state.mode,
        "started_at_utc": _iso(state.started_at_ns),
        "ended_at_utc": _iso(state.ended_at_ns) if state.ended_at_ns is not None else None,
        "duration_seconds": state.duration_seconds,
        "start_ts_utc": _iso(state.start_ts_ns) if state.start_ts_ns is not None else None,
        "end_ts_utc": _iso(state.end_ts_ns) if state.end_ts_ns is not None else None,
        "sources": state.sources,
        "plugins": state.plugins,
        "snapshot_row_count": row_count,
        "dataset_file": dataset_file,
        "plugin_files": plugin_files,
        "raw_files": raw_files,
        "data_dir": str(state.data_dir),
        "error": state.error,
    }
    (state.data_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return manifest


class CollectionRunManager:
    def __init__(
        self,
        *,
        config_path: Path,
        collections_dir: Path,
        plugins_dir: Path,
        symbol_cache_path: Path | None = None,
    ) -> None:
        self.config_path = config_path
        self.collections_dir = collections_dir
        self.plugins_dir = plugins_dir
        # Shared/global cache, deliberately outside collections_dir -- it's
        # not per-run data, it's Binance's tradable-symbol list.
        self.symbol_cache_path = symbol_cache_path or (
            collections_dir.parent / "cache" / "binance_symbols.json"
        )
        self.current: RunState | None = None

    def _symbol_catalog(self) -> SymbolCatalog:
        return load_cached_or_fetch(self.symbol_cache_path, max_age_seconds=DEFAULT_MAX_AGE_SECONDS)

    def refresh_symbol_catalog(self) -> SymbolCatalog:
        """Forces a live re-fetch, bypassing the cache TTL -- for a manual
        "refresh" action in the UI rather than waiting out the 24h cache."""
        return load_cached_or_fetch(self.symbol_cache_path, force=True)

    def _merged_catalog(self) -> dict[str, SourceInfo]:
        """BTC's static catalog plus every other Binance symbol's generated
        entries. A symbol-catalog fetch failure (first run with no cache yet,
        offline, Binance outage) degrades to BTC-only rather than breaking
        the whole source list -- see load_cached_or_fetch's own fallback for
        the "stale cache still available" case; this is the last-resort one."""
        try:
            extra = build_extra_catalog(self._symbol_catalog())
        except Exception:
            _LOGGER.exception("binance symbol catalog unavailable, falling back to BTC-only sources")
            extra = {}
        return {**SOURCE_CATALOG, **extra}

    def available_sources(self) -> list[dict[str, str | None]]:
        return [
            {
                "key": key,
                "label": info.label,
                "description": info.description,
                "asset_kind": info.asset_kind,
                "asset": info.asset,
                "market": info.market,
                "tag": info.tag,
                "mode": info.mode,
                "historical_limit_days": info.historical_limit_days,
                "detail": info.detail,
            }
            for key, info in self._merged_catalog().items()
        ]

    def available_plugins(self) -> list[dict[str, str]]:
        return [
            {
                "id": info.id, "label": info.label,
                "description": info.description, "category": info.category,
                "mode": info.mode, "detail": info.detail,
            }
            for info in discover_plugins(self.plugins_dir)
        ]

    def import_plugin_file(
        self, filename: str, content: str, *, overwrite: bool = False,
    ) -> dict[str, object]:
        """Saves a plugin .py file dropped in through the browser's file
        picker. Same trust level as copying the file there by hand -- this
        just does the copying -- so validation is about safety (no path
        escape, no silent clobber) rather than sandboxing untrusted code."""
        if not PLUGIN_FILENAME_RE.match(filename):
            raise ValueError(
                "filename must be a simple name ending in .py (letters, digits, "
                "underscores, not starting with '_')"
            )
        self.plugins_dir.mkdir(parents=True, exist_ok=True)
        target = self.plugins_dir / filename
        if target.exists() and not overwrite:
            raise FileExistsError(f"{filename} already exists in {self.plugins_dir}")
        target.write_text(content, encoding="utf-8")
        recognized = any(info.id == target.stem for info in discover_plugins(self.plugins_dir))
        return {"filename": filename, "recognized": recognized}

    def start(
        self,
        *,
        sources: list[str],
        plugins: list[str],
        duration_seconds: float | None,
        mode: str = "collect",
        start_ts_ns: int | None = None,
        end_ts_ns: int | None = None,
    ) -> RunState:
        if self.current is not None and self.current.ended_at_ns is None:
            raise RuntimeError("a collection run is already in progress")
        if mode not in ("collect", "access"):
            raise ValueError("mode must be 'collect' or 'access'")
        catalog = self._merged_catalog()
        valid_sources = [key for key in sources if key in catalog]
        if mode == "access":
            unsupported_sources = [
                key for key in valid_sources if catalog[key].mode != "access"
            ]
            if unsupported_sources:
                raise ValueError(
                    "mode déjà collecté : pas encore disponible pour "
                    + ", ".join(unsupported_sources)
                )
            plugin_infos = {info.id: info for info in discover_plugins(self.plugins_dir)}
            incompatible_plugins = [
                key for key in plugins
                if key in plugin_infos and plugin_infos[key].mode != "access"
            ]
            if incompatible_plugins:
                raise ValueError(
                    "mode déjà collecté : plugin(s) en mode collecte, incompatible(s) : "
                    + ", ".join(incompatible_plugins)
                )
            if not valid_sources and not plugins:
                raise ValueError("select at least one accessible data source or plugin")
        elif not valid_sources:
            raise ValueError("select at least one data source")
        now_ns = time.time_ns()
        run_id = new_run_id(now_ns)
        data_dir = self.collections_dir / run_id
        data_dir.mkdir(parents=True, exist_ok=True)
        state = RunState(
            run_id=run_id,
            sources=valid_sources,
            plugins=list(plugins),
            duration_seconds=None if mode == "access" else duration_seconds,
            data_dir=data_dir,
            started_at_ns=now_ns,
            mode=mode,
            start_ts_ns=start_ts_ns,
            end_ts_ns=end_ts_ns,
        )
        base_config = load_config(self.config_path)
        state.task = asyncio.create_task(
            self._run(state, base_config), name=f"collection-run-{run_id}"
        )
        self.current = state
        return state

    def stop(self) -> None:
        if self.current is None or self.current.ended_at_ns is not None:
            raise RuntimeError("no collection run is in progress")
        self.current.stop_event.set()

    def status(self) -> dict | None:
        state = self.current
        if state is None:
            return None
        now_ns = time.time_ns()
        event_counts: dict[str, int] = {}
        health: list[dict[str, object]] = []
        if state.gateway is not None:
            event_counts = {
                stream.value: count for stream, count in state.gateway.event_counts.items() if count
            }
            health = [
                {
                    "source": source.value,
                    "connected": row.connected,
                    "age_ms": row.age_ms,
                    "invalid": row.invalid_count,
                    "reconnects": row.reconnect_count,
                }
                for source, row in state.gateway.health_registry.all_source_snapshots(now_ns)
            ]
        return {
            "run_id": state.run_id,
            "running": state.ended_at_ns is None,
            "mode": state.mode,
            "sources": state.sources,
            "plugins": state.plugins,
            "started_at_utc": _iso(state.started_at_ns),
            "ended_at_utc": _iso(state.ended_at_ns) if state.ended_at_ns is not None else None,
            "elapsed_seconds": (now_ns - state.started_at_ns) / 1_000_000_000,
            "duration_seconds": state.duration_seconds,
            "start_ts_utc": _iso(state.start_ts_ns) if state.start_ts_ns is not None else None,
            "end_ts_utc": _iso(state.end_ts_ns) if state.end_ts_ns is not None else None,
            "event_counts": event_counts,
            "health": health,
            "plugin_logs": {key: list(value)[-20:] for key, value in state.plugin_logs.items()},
            "source_progress": dict(state.source_progress),
            "export": state.export,
            "error": state.error,
        }

    def list_runs(self) -> list[dict]:
        if not self.collections_dir.is_dir():
            return []
        runs = []
        for manifest_path in sorted(self.collections_dir.glob("*/manifest.json"), reverse=True):
            try:
                runs.append(json.loads(manifest_path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                continue
        return runs

    async def _run(self, state: RunState, base_config: MarketDataConfig) -> None:
        if state.mode == "access":
            await self._run_access(state, base_config)
            return
        config = replace(
            base_config,
            storage=replace(base_config.storage, data_dir=state.data_dir),
            health=replace(base_config.health, health_file=state.data_dir / "runtime" / "health.json"),
        )
        gateway = MarketDataGateway(
            config,
            start_market_discovery="polymarket" in state.sources,
            enabled_sources=frozenset(state.sources),
        )
        state.gateway = gateway
        self._start_plugins(state)
        try:
            async with gateway:
                consumer = asyncio.create_task(self._drain(gateway))
                fatal_task = asyncio.create_task(gateway.wait_for_fatal())
                try:
                    await self._wait_for_stop_or_deadline(state)
                finally:
                    consumer.cancel()
                    fatal_task.cancel()
                    await asyncio.gather(consumer, fatal_task, return_exceptions=True)
        except Exception as exc:
            state.error = repr(exc)
        finally:
            state.stop_event.set()
            for task in state.plugin_tasks.values():
                task.cancel()
            if state.plugin_tasks:
                await asyncio.gather(*state.plugin_tasks.values(), return_exceptions=True)
            state.ended_at_ns = time.time_ns()
            state.export = export_run(state)

    async def _run_access(self, state: RunState, base_config: MarketDataConfig) -> None:
        """Access-mode runs: no gateway -- just the mode="access" plugins,
        each running once, plus (for the built-in sources that have a real
        bulk historical fetch -- start() already rejected any source that
        doesn't declare mode="access") one task per selected source writing
        straight into a RawEventStorage this method owns. Neither plugins
        nor historical fetchers are obliged to watch stop_event themselves
        (no loop to check it from) -- instead this method races the whole
        batch against stop_event and cancels every still-running task the
        moment it fires, the same best-effort guarantee "collect" mode's
        plugin_tasks already get in _run() above. A cancelled fetcher keeps
        whatever it already wrote (crash-isolation contract, unchanged)."""
        self._start_plugins(state)
        raw_storage: RawEventStorage | None = None
        source_tasks: dict[str, asyncio.Task] = {}
        if state.sources:
            raw_storage = RawEventStorage(state.data_dir, zstd_level=base_config.storage.zstd_level)
            sequence = itertools.count()
            for key in state.sources:
                log = state.plugin_logs[key] = deque(maxlen=PLUGIN_LOG_LINES)
                state.source_progress[key] = 0.0
                source_tasks[key] = asyncio.create_task(
                    self._run_historical_source(
                        key, raw_storage, sequence.__next__, log.append,
                        start_ts_ns=state.start_ts_ns, end_ts_ns=state.end_ts_ns,
                        on_progress=lambda fraction, key=key: state.source_progress.__setitem__(key, fraction),
                    ),
                    name=f"historical-source-{key}",
                )
        try:
            tasks = [*state.plugin_tasks.values(), *source_tasks.values()]
            if tasks:
                gather_task = asyncio.gather(*tasks, return_exceptions=True)
                stop_waiter = asyncio.create_task(state.stop_event.wait())
                done, _pending = await asyncio.wait(
                    [gather_task, stop_waiter], return_when=asyncio.FIRST_COMPLETED,
                )
                if gather_task not in done:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await gather_task
                if not stop_waiter.done():
                    stop_waiter.cancel()
        finally:
            if raw_storage is not None:
                raw_storage.close()
            state.stop_event.set()
            state.ended_at_ns = time.time_ns()
            state.export = export_run(state)

    @staticmethod
    async def _run_historical_source(
        key: str, raw_storage: RawEventStorage, next_sequence: Callable[[], int], log: Callable[[str], None],
        *, start_ts_ns: int | None, end_ts_ns: int | None, on_progress: Callable[[float], None],
    ) -> None:
        """One selected built-in access-mode source key -- resolves the
        symbol from a possibly-compound key ("binance_futures_kline:ETH"),
        defaulting the plain-key case to BTC, and dispatches to the matching
        fetcher. A failure here logs and stops just this source (matches the
        plugin crash-isolation contract), leaving whatever it already wrote
        in place."""
        base_key, short_symbol = parse_source_key(key)
        symbol = f"{short_symbol.upper()}USDT" if short_symbol else "BTCUSDT"
        fetcher = HISTORICAL_FETCHERS.get(base_key)
        if fetcher is None:
            log("aucune récupération historique disponible pour cette source, ignorée")
            return
        try:
            await fetcher(
                symbol=symbol,
                start_ts_ns=start_ts_ns,
                end_ts_ns=end_ts_ns,
                on_progress=on_progress,
                raw_storage=raw_storage,
                next_sequence=next_sequence,
                log=log,
            )
        except asyncio.CancelledError:
            log("annulé -- ce qui a déjà été récupéré est conservé")
            raise
        except Exception as exc:
            log(f"erreur : {exc!r}")

    def _start_plugins(self, state: RunState) -> None:
        plugin_infos = {info.id: info for info in discover_plugins(self.plugins_dir)}
        for plugin_id in state.plugins:
            info = plugin_infos.get(plugin_id)
            log = state.plugin_logs[plugin_id] = deque(maxlen=PLUGIN_LOG_LINES)
            if info is None:
                log.append("plugin introuvable au démarrage, ignoré")
                continue
            context = PluginContext(
                state.data_dir, state.stop_event, log.append,
                start_ts_ns=state.start_ts_ns, end_ts_ns=state.end_ts_ns,
            )
            state.plugin_tasks[plugin_id] = asyncio.create_task(
                run_plugin(info, context), name=f"plugin-{plugin_id}"
            )

    @staticmethod
    async def _drain(gateway: MarketDataGateway) -> None:
        async for _snapshot in gateway.snapshots():
            pass

    @staticmethod
    async def _wait_for_stop_or_deadline(state: RunState) -> None:
        loop = asyncio.get_running_loop()
        deadline = None if state.duration_seconds is None else loop.time() + state.duration_seconds
        while not state.stop_event.is_set():
            timeout = None if deadline is None else max(0.0, deadline - loop.time())
            if timeout == 0.0:
                return
            try:
                await asyncio.wait_for(state.stop_event.wait(), timeout=timeout)
            except TimeoutError:
                if deadline is not None:
                    return


__all__ = ["CollectionRunManager", "RunState", "export_run", "new_run_id"]
