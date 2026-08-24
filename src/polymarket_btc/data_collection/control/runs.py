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
import shutil
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
from .concepts import discover_concepts
from .config_schema import config_field_to_dict
from .execution import discover_execution_profiles
from .management import discover_management_profiles
from .microsystems import discover_microsystems
from .plugins import PluginContext, discover_plugins, run_plugin
from .strategy_filter import discover_filter_profiles

_LOGGER = logging.getLogger(__name__)

PLUGIN_LOG_LINES = 200

# Binance's own documented kline interval enum (identical set for spot and
# futures) -- the only values its REST endpoints accept, so validating
# against this up front turns a typo into an immediate 400 instead of a
# wasted round trip to Binance that would 400 anyway.
VALID_KLINE_INTERVALS = (
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
)

# Mirrors discover_plugins' own rules (and discover_concepts'/
# discover_microsystems'/discover_execution_profiles', which all copy the
# same scanner): a simple flat filename, no path separators or traversal,
# and not starting with "_" (that prefix means "skip me" to the scanner, so
# a leading underscore is rejected here too -- an imported script that
# discovery would silently ignore is worse than one rejected up front with
# a clear reason). Shared by every import_*_file method below, not just
# plugins, hence the generic name.
SCRIPT_FILENAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*\.py$")


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
    # Candle width for the two access-mode sources built from klines
    # (binance_futures_kline, binance_futures_mark_price) -- ignored by
    # every other source. A coarser interval means far fewer rows/requests
    # for the same date range (a 1h candle is 60x fewer rows than 1m over
    # the same span), the direct lever for "make this download faster"
    # when 1-minute precision isn't actually needed.
    kline_interval: str = "1m"
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


# Reverses _run_historical_source's own (base_key, symbol) -> EventSource
# mapping: a raw segment's own on-disk EventSource value matches its
# catalog base key directly for three of the four access-mode sources;
# open interest and long-short ratio share one EventSource
# (BINANCE_FUTURES_REST) for a catalog key that has no EventSource of its
# own at all.
_EVENT_SOURCE_TO_BASE_KEY = {
    "binance_futures_kline": "binance_futures_kline",
    "binance_futures_trade": "binance_futures_trade",
    "binance_futures_mark_price": "binance_futures_mark_price",
    "binance_futures_rest": "binance_futures_open_interest_long_short",
}

# The two access-mode sources whose fetcher accepts a candle-width
# ("interval") parameter -- fetch_and_store_historical_klines and
# fetch_and_store_historical_mark_price (which bundles markPriceKlines with
# fundingRate). aggTrades and open-interest/long-short have no such notion.
_KLINE_SHAPED_BASE_KEYS = frozenset({"binance_futures_kline", "binance_futures_mark_price"})


def _catalog_key_for_raw_path(path: Path) -> str | None:
    """A raw segment's directory already encodes `source=<EventSource
    value>` and `instrument=<SYMBOL>USDT` (RawEventStorage's own layout) --
    reconstructs the catalog key (base_key, or "base_key:ASSET" for a
    non-BTC instrument) so per-key coverage can be grouped correctly. None
    for anything outside the four access-mode base keys (nothing else needs
    per-key access-mode coverage)."""
    source_part = next((p for p in path.parts if p.startswith("source=")), None)
    instrument_part = next((p for p in path.parts if p.startswith("instrument=")), None)
    if source_part is None or instrument_part is None:
        return None
    base_key = _EVENT_SOURCE_TO_BASE_KEY.get(source_part.removeprefix("source="))
    if base_key is None:
        return None
    asset = instrument_part.removeprefix("instrument=").removesuffix("USDT")
    return base_key if asset == "BTC" else f"{base_key}:{asset}"


def _access_mode_source_coverage(raw_paths: list[Path]) -> dict[str, dict[str, str | None]]:
    """Per catalog key: the time range the data itself actually covers,
    read from each raw segment's own sidecar manifest -- never the run's
    requested range, which a fetch that came up short for one key (no
    history that far back, a transient failure, anything) would otherwise
    misrepresent for every other key collected in the same run.

    Prefers each segment's first/last_source_timestamp_ns (the events' own
    real-world timestamps -- a kline's close_time, a trade's trade_time)
    over first/last_received_timestamp_ns (wall-clock receipt time): for an
    access-mode bulk historical fetch those can differ by months (fetched
    today, dated a year ago) -- confirmed live, a day fetched from a year
    back showed as "covering" a few hundred milliseconds around the fetch's
    own wall-clock moment instead of the actual historical day. Only
    source_timestamp_ns answers "what period does this collection actually
    cover." Falls back to the received-time fields for a sidecar written
    before source timestamps were tracked here (storage.py) -- still
    correct for a live/near-real-time collection, where the two nearly
    coincide; wrong only for an old access-mode sidecar, which re-collecting
    fixes. A segment with a missing/corrupt sidecar is skipped, not fatal,
    same tolerance as raw_event_count above."""
    bounds: dict[str, tuple[int, int]] = {}
    for path in raw_paths:
        key = _catalog_key_for_raw_path(path)
        if key is None:
            continue
        try:
            sidecar_path = path.with_name(path.name.removesuffix(".jsonl.zst") + ".manifest.json")
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            first_ns = sidecar.get("first_source_timestamp_ns")
            last_ns = sidecar.get("last_source_timestamp_ns")
            if first_ns is None or last_ns is None:
                first_ns = sidecar["first_received_timestamp_ns"]
                last_ns = sidecar["last_received_timestamp_ns"]
            first_ns, last_ns = int(first_ns), int(last_ns)
        except (OSError, ValueError, TypeError, KeyError):
            continue
        if key in bounds:
            existing_first, existing_last = bounds[key]
            bounds[key] = (min(existing_first, first_ns), max(existing_last, last_ns))
        else:
            bounds[key] = (first_ns, last_ns)
    return {
        key: {"start_ts_utc": _iso(first_ns), "end_ts_utc": _iso(last_ns)}
        for key, (first_ns, last_ns) in bounds.items()
    }


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
    raw_paths = sorted(raw_root.rglob("*.jsonl.zst")) if raw_root.is_dir() else []
    raw_files = [str(path.relative_to(state.data_dir)) for path in raw_paths]
    # snapshot_row_count only ever reflects dataset.parquet, which access-mode
    # runs never produce (no gateway/reducer runs, so snapshots/ stays empty)
    # -- an access-mode run's real yield lives in each raw segment's own
    # sidecar manifest instead. A missing/corrupt sidecar contributes 0
    # rather than failing the whole export, same tolerance as everywhere
    # else raw segments are read.
    raw_event_count = 0
    for path in raw_paths:
        try:
            sidecar_path = path.with_name(path.name.removesuffix(".jsonl.zst") + ".manifest.json")
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            raw_event_count += int(sidecar.get("event_count") or 0)
        except (OSError, ValueError, TypeError):
            continue

    if state.mode == "access":
        source_coverage = _access_mode_source_coverage(raw_paths)
    else:
        # One live gateway, one dataset -- every requested source/plugin
        # shares the same actually-observed span. Derived from the real
        # snapshot timestamps when there are any (accurate even if the run
        # stopped earlier than requested); falls back to the requested
        # range only when there's no data to derive it from at all (in
        # which case there's nothing for a later reader to serve anyway).
        if dataset_file is not None:
            timestamps = combined.column("snapshot_timestamp_ns").to_pylist()
            run_start, run_end = _iso(min(timestamps)), _iso(max(timestamps))
        else:
            run_start = _iso(state.start_ts_ns) if state.start_ts_ns is not None else None
            run_end = _iso(state.end_ts_ns) if state.end_ts_ns is not None else None
        source_coverage = {
            key: {"start_ts_utc": run_start, "end_ts_utc": run_end}
            for key in [*state.sources, *state.plugins]
        }

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
        "kline_interval": state.kline_interval,
        "snapshot_row_count": row_count,
        "raw_event_count": raw_event_count,
        "source_coverage": source_coverage,
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
        concepts_dir: Path,
        microsystems_dir: Path,
        execution_dir: Path,
        management_dir: Path,
        filter_dir: Path,
        symbol_cache_path: Path | None = None,
    ) -> None:
        self.config_path = config_path
        self.collections_dir = collections_dir
        self.plugins_dir = plugins_dir
        self.concepts_dir = concepts_dir
        self.microsystems_dir = microsystems_dir
        self.execution_dir = execution_dir
        self.management_dir = management_dir
        self.filter_dir = filter_dir
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

    def _data_requirements_for(self, keys: list[str]) -> list[dict[str, object]]:
        """Groups a concept's data_sources (or a microsystem's data_inputs)
        by *type* rather than literal key, so the UI can offer "this concept
        needs candles" instead of "this concept needs BTC's candles"
        specifically. A catalog key's type is its `tag` (the same axis
        "Par tag" already groups the collector's own source browser by) when
        the entry is asset-scoped; otherwise (a plugin, or a non-asset-scoped
        built-in like chainlink/polymarket, or a stale/unknown key) the key
        itself is its own atomic type -- there's no cross-asset
        generalization mechanism for those, so nothing to swap to.

        A type with exactly one asset-scoped key is "swappable" -- the
        author happened to pick one asset, but any other asset offering the
        same tag works just as well, so a strategy instance is free to
        rebind it. A type with more than one key (whatever their assets) is
        "locked": the author deliberately named specific keys together (most
        often a cross-asset comparison), so those exact keys stay fixed."""
        catalog = self._merged_catalog()
        groups: dict[str, list[tuple[str, str | None]]] = {}
        order: list[str] = []
        for key in keys:
            info = catalog.get(key)
            if info is not None and info.asset is not None and info.tag is not None:
                type_label, asset = info.tag, info.asset
            else:
                type_label, asset = key, None
            if type_label not in groups:
                groups[type_label] = []
                order.append(type_label)
            groups[type_label].append((key, asset))
        requirements: list[dict[str, object]] = []
        for type_label in order:
            entries = groups[type_label]
            swappable = len(entries) == 1 and entries[0][1] is not None
            requirements.append({
                "type": type_label,
                "swappable": swappable,
                "keys": [key for key, _asset in entries],
                "default_asset": entries[0][1] if swappable else None,
            })
        return requirements

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
        if not SCRIPT_FILENAME_RE.match(filename):
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

    def available_concepts(self) -> list[dict[str, object]]:
        return [
            {
                "id": info.id, "label": info.label, "description": info.description,
                "category": info.category, "detail": info.detail,
                "data_sources": list(info.data_sources),
                "data_requirements": self._data_requirements_for(list(info.data_sources)),
                "config_schema": [config_field_to_dict(f) for f in info.config_schema],
            }
            for info in discover_concepts(self.concepts_dir)
        ]

    def import_concept_file(
        self, filename: str, content: str, *, overwrite: bool = False,
    ) -> dict[str, object]:
        """Same contract/trust level as import_plugin_file, targeting
        concepts_dir instead."""
        if not SCRIPT_FILENAME_RE.match(filename):
            raise ValueError(
                "filename must be a simple name ending in .py (letters, digits, "
                "underscores, not starting with '_')"
            )
        self.concepts_dir.mkdir(parents=True, exist_ok=True)
        target = self.concepts_dir / filename
        if target.exists() and not overwrite:
            raise FileExistsError(f"{filename} already exists in {self.concepts_dir}")
        target.write_text(content, encoding="utf-8")
        recognized = any(info.id == target.stem for info in discover_concepts(self.concepts_dir))
        return {"filename": filename, "recognized": recognized}

    def available_microsystems(self) -> list[dict[str, object]]:
        return [
            {
                "id": info.id, "label": info.label, "description": info.description,
                "category": info.category, "detail": info.detail,
                "concept_inputs": list(info.concept_inputs),
                "data_inputs": list(info.data_inputs),
                "data_requirements": self._data_requirements_for(list(info.data_inputs)),
                "config_schema": [config_field_to_dict(f) for f in info.config_schema],
            }
            for info in discover_microsystems(self.microsystems_dir)
        ]

    def import_microsystem_file(
        self, filename: str, content: str, *, overwrite: bool = False,
    ) -> dict[str, object]:
        """Same contract/trust level as import_plugin_file, targeting
        microsystems_dir instead."""
        if not SCRIPT_FILENAME_RE.match(filename):
            raise ValueError(
                "filename must be a simple name ending in .py (letters, digits, "
                "underscores, not starting with '_')"
            )
        self.microsystems_dir.mkdir(parents=True, exist_ok=True)
        target = self.microsystems_dir / filename
        if target.exists() and not overwrite:
            raise FileExistsError(f"{filename} already exists in {self.microsystems_dir}")
        target.write_text(content, encoding="utf-8")
        recognized = any(info.id == target.stem for info in discover_microsystems(self.microsystems_dir))
        return {"filename": filename, "recognized": recognized}

    def available_execution_profiles(self) -> list[dict[str, object]]:
        return [
            {
                "id": info.id, "label": info.label, "description": info.description,
                "category": info.category, "detail": info.detail,
                "config_schema": [config_field_to_dict(f) for f in info.config_schema],
            }
            for info in discover_execution_profiles(self.execution_dir)
        ]

    def import_execution_profile_file(
        self, filename: str, content: str, *, overwrite: bool = False,
    ) -> dict[str, object]:
        """Same contract/trust level as import_plugin_file, targeting
        execution_dir instead."""
        if not SCRIPT_FILENAME_RE.match(filename):
            raise ValueError(
                "filename must be a simple name ending in .py (letters, digits, "
                "underscores, not starting with '_')"
            )
        self.execution_dir.mkdir(parents=True, exist_ok=True)
        target = self.execution_dir / filename
        if target.exists() and not overwrite:
            raise FileExistsError(f"{filename} already exists in {self.execution_dir}")
        target.write_text(content, encoding="utf-8")
        recognized = any(
            info.id == target.stem for info in discover_execution_profiles(self.execution_dir)
        )
        return {"filename": filename, "recognized": recognized}

    def available_management_profiles(self) -> list[dict[str, object]]:
        return [
            {
                "id": info.id, "label": info.label, "description": info.description,
                "category": info.category, "detail": info.detail,
                "config_schema": [config_field_to_dict(f) for f in info.config_schema],
            }
            for info in discover_management_profiles(self.management_dir)
        ]

    def import_management_profile_file(
        self, filename: str, content: str, *, overwrite: bool = False,
    ) -> dict[str, object]:
        """Same contract/trust level as import_plugin_file, targeting
        management_dir instead."""
        if not SCRIPT_FILENAME_RE.match(filename):
            raise ValueError(
                "filename must be a simple name ending in .py (letters, digits, "
                "underscores, not starting with '_')"
            )
        self.management_dir.mkdir(parents=True, exist_ok=True)
        target = self.management_dir / filename
        if target.exists() and not overwrite:
            raise FileExistsError(f"{filename} already exists in {self.management_dir}")
        target.write_text(content, encoding="utf-8")
        recognized = any(
            info.id == target.stem for info in discover_management_profiles(self.management_dir)
        )
        return {"filename": filename, "recognized": recognized}

    def available_filter_profiles(self) -> list[dict[str, object]]:
        return [
            {
                "id": info.id, "label": info.label, "description": info.description,
                "category": info.category, "detail": info.detail,
                "config_schema": [config_field_to_dict(f) for f in info.config_schema],
            }
            for info in discover_filter_profiles(self.filter_dir)
        ]

    def import_filter_profile_file(
        self, filename: str, content: str, *, overwrite: bool = False,
    ) -> dict[str, object]:
        """Same contract/trust level as import_plugin_file, targeting
        filter_dir instead."""
        if not SCRIPT_FILENAME_RE.match(filename):
            raise ValueError(
                "filename must be a simple name ending in .py (letters, digits, "
                "underscores, not starting with '_')"
            )
        self.filter_dir.mkdir(parents=True, exist_ok=True)
        target = self.filter_dir / filename
        if target.exists() and not overwrite:
            raise FileExistsError(f"{filename} already exists in {self.filter_dir}")
        target.write_text(content, encoding="utf-8")
        recognized = any(
            info.id == target.stem for info in discover_filter_profiles(self.filter_dir)
        )
        return {"filename": filename, "recognized": recognized}

    def read_plugin_source(self, plugin_id: str) -> dict[str, str]:
        """{"id","filename","content"} for a discovered plugin's own .py
        file -- lets a concept/microsystem author see exactly what a
        plugin-backed data source looks like before writing code against
        it. Raises FileNotFoundError if plugin_id doesn't resolve."""
        for info in discover_plugins(self.plugins_dir):
            if info.id == plugin_id:
                return {"id": info.id, "filename": info.path.name, "content": info.path.read_text(encoding="utf-8")}
        raise FileNotFoundError(f"no plugin named {plugin_id!r}")

    def read_concept_source(self, concept_id: str) -> dict[str, str]:
        """Same shape as read_plugin_source, for a discovered concept --
        lets a microsystem author see exactly what a concept it depends on
        looks like."""
        for info in discover_concepts(self.concepts_dir):
            if info.id == concept_id:
                return {"id": info.id, "filename": info.path.name, "content": info.path.read_text(encoding="utf-8")}
        raise FileNotFoundError(f"no concept named {concept_id!r}")

    def read_microsystem_source(self, microsystem_id: str) -> dict[str, str]:
        """Same shape as read_concept_source, for a discovered microsystem --
        lets microsystem refinement embed the current script in its prompt."""
        for info in discover_microsystems(self.microsystems_dir):
            if info.id == microsystem_id:
                return {"id": info.id, "filename": info.path.name, "content": info.path.read_text(encoding="utf-8")}
        raise FileNotFoundError(f"no microsystem named {microsystem_id!r}")

    def read_strategy_filter_source(self, filter_id: str) -> dict[str, str]:
        """Same shape as read_concept_source, for a discovered strategy
        filter -- a filter's own refinement prompt must embed its current
        script, same reasoning as concept/microsystem refinement."""
        for info in discover_filter_profiles(self.filter_dir):
            if info.id == filter_id:
                return {"id": info.id, "filename": info.path.name, "content": info.path.read_text(encoding="utf-8")}
        raise FileNotFoundError(f"no filter named {filter_id!r}")

    def read_execution_source(self, execution_id: str) -> dict[str, str]:
        """Same shape as read_concept_source, for a discovered execution
        profile -- lets the Builder's duplicate-for-a-strategy flow read the
        current script before re-importing it under a new id."""
        for info in discover_execution_profiles(self.execution_dir):
            if info.id == execution_id:
                return {"id": info.id, "filename": info.path.name, "content": info.path.read_text(encoding="utf-8")}
        raise FileNotFoundError(f"no execution profile named {execution_id!r}")

    def read_management_source(self, management_id: str) -> dict[str, str]:
        """Same shape as read_concept_source, for a discovered management
        profile -- lets the Builder's duplicate-for-a-strategy flow read the
        current script before re-importing it under a new id."""
        for info in discover_management_profiles(self.management_dir):
            if info.id == management_id:
                return {"id": info.id, "filename": info.path.name, "content": info.path.read_text(encoding="utf-8")}
        raise FileNotFoundError(f"no management profile named {management_id!r}")

    def delete_concept_source(self, concept_id: str) -> None:
        """Permanently removes a discovered concept's .py file. Callers
        (StrategyManager.delete_source) are responsible for refusing this
        while any saved strategy still references concept_id -- this method
        itself does no such check, same division of responsibility as
        delete_strategy vs. the server route that confirms with the user."""
        for info in discover_concepts(self.concepts_dir):
            if info.id == concept_id:
                info.path.unlink()
                return
        raise FileNotFoundError(f"no concept named {concept_id!r}")

    def delete_microsystem_source(self, microsystem_id: str) -> None:
        """Same contract as delete_concept_source, for a microsystem."""
        for info in discover_microsystems(self.microsystems_dir):
            if info.id == microsystem_id:
                info.path.unlink()
                return
        raise FileNotFoundError(f"no microsystem named {microsystem_id!r}")

    def delete_execution_source(self, execution_id: str) -> None:
        """Same contract as delete_concept_source, for an execution profile."""
        for info in discover_execution_profiles(self.execution_dir):
            if info.id == execution_id:
                info.path.unlink()
                return
        raise FileNotFoundError(f"no execution profile named {execution_id!r}")

    def delete_management_source(self, management_id: str) -> None:
        """Same contract as delete_concept_source, for a management profile."""
        for info in discover_management_profiles(self.management_dir):
            if info.id == management_id:
                info.path.unlink()
                return
        raise FileNotFoundError(f"no management profile named {management_id!r}")

    def _data_context_blocks(self, *, sources: list[str], plugins: list[str]) -> list[str]:
        """One markdown block per selected data-catalog entry: built-in
        sources get their label/description/detail/mode (no code -- there
        is none); plugins get the same plus their full .py source, so an AI
        writing a concept/microsystem sees exactly what the data looks
        like. Unknown keys are silently dropped, matching start()'s own
        `key in catalog` filtering leniency -- this is best-effort
        scaffolding for a prompt, not something that must reject a stale
        selection."""
        blocks: list[str] = []
        catalog = self._merged_catalog()
        for key in sources:
            info = catalog.get(key)
            if info is None:
                continue
            blocks.append(
                f"### Source : `{key}`\n"
                f"- Label : {info.label}\n"
                f"- Description : {info.description}\n"
                f"- Détail : {info.detail or '(aucun)'}\n"
                f"- Mode : {info.mode}\n"
            )
        plugin_infos = {info.id: info for info in discover_plugins(self.plugins_dir)}
        for plugin_id in plugins:
            info = plugin_infos.get(plugin_id)
            if info is None:
                continue
            source = self.read_plugin_source(plugin_id)
            blocks.append(
                f"### Plugin : `{plugin_id}`\n"
                f"- Label : {info.label}\n"
                f"- Description : {info.description}\n"
                f"- Détail : {info.detail or '(aucun)'}\n"
                f"- Code source (`{source['filename']}`) :\n\n"
                f"```python\n{source['content']}\n```\n"
            )
        return blocks

    def build_concept_prompt(self, *, sources: list[str], plugins: list[str], template: str) -> str:
        """Appends a "Contexte : données sélectionnées" section to
        `template` (the raw docs/nouveau_concept_prompt.md text) so an AI
        writing a concept sees exactly what the selected data looks like.
        Raises ValueError if sources and plugins are both empty -- a
        concept prompt with no data context defeats the point."""
        if not sources and not plugins:
            raise ValueError("select at least one data source or plugin to build a concept prompt")
        blocks = self._data_context_blocks(sources=sources, plugins=plugins)
        return template + "\n\n---\n\n## Contexte : données sélectionnées\n\n" + "\n".join(blocks)

    def build_microsystem_prompt(
        self, *, concepts: list[str], sources: list[str], plugins: list[str], template: str,
    ) -> str:
        """Same mechanism as build_concept_prompt, also embedding each
        selected concept's full .py source so an AI writing a microsystem
        sees exactly what the concepts it wires to look like. Raises
        ValueError if concepts, sources, and plugins are all empty."""
        if not concepts and not sources and not plugins:
            raise ValueError("select at least one concept, data source, or plugin to build a microsystem prompt")
        blocks = self._data_context_blocks(sources=sources, plugins=plugins)
        concept_infos = {info.id: info for info in discover_concepts(self.concepts_dir)}
        for concept_id in concepts:
            info = concept_infos.get(concept_id)
            if info is None:
                continue
            source = self.read_concept_source(concept_id)
            blocks.append(
                f"### Concept : `{concept_id}`\n"
                f"- Label : {info.label}\n"
                f"- Description : {info.description}\n"
                f"- Détail : {info.detail or '(aucun)'}\n"
                f"- Code source (`{source['filename']}`) :\n\n"
                f"```python\n{source['content']}\n```\n"
            )
        return template + "\n\n---\n\n## Contexte : données et concepts sélectionnés\n\n" + "\n".join(blocks)

    def start(
        self,
        *,
        sources: list[str],
        plugins: list[str],
        duration_seconds: float | None,
        mode: str = "collect",
        start_ts_ns: int | None = None,
        end_ts_ns: int | None = None,
        kline_interval: str = "1m",
    ) -> RunState:
        if self.current is not None and self.current.ended_at_ns is None:
            raise RuntimeError("a collection run is already in progress")
        if mode not in ("collect", "access"):
            raise ValueError("mode must be 'collect' or 'access'")
        if kline_interval not in VALID_KLINE_INTERVALS:
            raise ValueError(
                f"kline_interval must be one of {', '.join(VALID_KLINE_INTERVALS)} (got {kline_interval!r})"
            )
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
            kline_interval=kline_interval,
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

    def delete_run(self, run_id: str) -> None:
        """Permanently removes a past collection run's entire directory --
        raw segments, exported dataset, manifest, everything under
        collections_dir/run_id. Irreversible; the control panel's own
        "supprimer" button is expected to confirm with the user before
        calling this. Refuses to delete the run currently in progress (stop
        it first -- same "one run at a time" reasoning start() already
        enforces) so a collection can never be pulled out from under itself
        mid-flight."""
        if self.current is not None and self.current.run_id == run_id and self.current.ended_at_ns is None:
            raise RuntimeError("cannot delete a collection run that is still in progress -- stop it first")
        target = (self.collections_dir / run_id).resolve()
        try:
            target.relative_to(self.collections_dir.resolve())
        except ValueError:
            raise ValueError(f"invalid run_id: {run_id!r}") from None
        if target == self.collections_dir.resolve() or not target.is_dir():
            raise FileNotFoundError(f"no collection run named {run_id!r}")
        shutil.rmtree(target)

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
                        kline_interval=state.kline_interval,
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
        *, start_ts_ns: int | None, end_ts_ns: int | None, kline_interval: str,
        on_progress: Callable[[float], None],
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
        # Only the two kline-shaped fetchers (candles, mark price -- which
        # bundles markPriceKlines with fundingRate) take a candle width;
        # aggTrades and open-interest/long-short have no such notion.
        extra_kwargs = {"interval": kline_interval} if base_key in _KLINE_SHAPED_BASE_KEYS else {}
        try:
            await fetcher(
                symbol=symbol,
                start_ts_ns=start_ts_ns,
                end_ts_ns=end_ts_ns,
                on_progress=on_progress,
                raw_storage=raw_storage,
                next_sequence=next_sequence,
                log=log,
                **extra_kwargs,
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


__all__ = ["VALID_KLINE_INTERVALS", "CollectionRunManager", "RunState", "export_run", "new_run_id"]
