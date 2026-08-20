"""Custom "concept" scripts: drop a `.py` file in `concepts/` that declares
CONCEPT_INFO and a sync `compute(context)`, and it becomes selectable when
building a strategy.

A concept turns already-collected data into "information" -- it never
fetches anything itself. Which data it needs is declared up front
(`data_sources`, using the exact same catalog keys the data-collection
console already uses: built-in source keys and custom plugin ids), and what's
adjustable about how it interacts with the market is declared alongside it
(`config_schema`, see config_schema.py) so the control panel can render a
form for it without the concept author writing any UI code.

Nothing in this module actually runs a concept against real data yet -- that
is the future backtest module's job. This module only defines and validates
the contract so that job has something stable to build on.

Security note: same trust level as plugins.py -- a concept is a plain Python
file this process imports and executes with full privileges. Only place
scripts you wrote or reviewed in `concepts/`.

Contract:

    CONCEPT_INFO = {
        "label": "...", "description": "...",   # required
        "category": "...",   # optional, defaults to "Général" -- same free-text
                              # grouping rule as PLUGIN_INFO["category"]
        "detail": "...",     # optional, str -- shown in the UI's (i) info bubble.
                              # None/absent -> no bubble offered.
        "data_sources": [...],  # required, non-empty list[str] of data-catalog
                                 # keys (built-in source keys or plugin ids) this
                                 # concept consumes.
        "config_schema": [...],  # optional, list of CONFIG_SCHEMA entries (see
                                  # config_schema.py) -- everything the concept
                                  # wants the user able to adjust. Defaults to [].
    }

    def compute(context: ConceptContext) -> object:
        ...

`compute` must be a plain (sync) function, not `async def` -- a concept
operates on data that already arrived, never on a live connection, so there
is nothing to await. `context.data` maps each declared data_sources key to
that data (the future backtest module decides the exact handle shape);
`context.config` maps each config_schema field's name to its resolved value;
`context.log` is a short-status callback, same spirit as PluginContext.log.

    def required_lookback_seconds(config: dict) -> float | None:
        ...

Optional. Declares the longest trailing window of history (in seconds,
counting back from "now" at any evaluation point) this concept could ever
need from *any* of its data_sources, given a resolved config -- e.g. a
concept that reconstructs `lookback_candles` candles of `candle_seconds`
width from raw trades needs nothing older than
`lookback_candles * candle_seconds` (times a safety margin for quiet
periods with fewer trades than expected per candle). Not defining this
function (the default for a concept that doesn't opt in) means "unbounded":
`context.data[key]` is every record from the backtest's own start, which is
always correct but can be slow for a concept that reprocesses all of it
from scratch on every evaluation step -- the backtest engine hands a
windowed instance only its declared trailing slice instead, which is
strictly faster and, as long as the declared window is genuinely long
enough, produces an identical result (anything older was already being
discarded by the concept's own lookback trimming). Get the margin wrong
(too short) and older-but-still-relevant data silently disappears instead
-- when unsure, don't define this function at all rather than guess low.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import importlib.util
import inspect
from pathlib import Path

from .config_schema import ConfigField, parse_config_schema

DEFAULT_CATEGORY = "Général"


@dataclass(slots=True)
class ConceptContext:
    data: Mapping[str, object]
    config: Mapping[str, object]
    log: Callable[[str], None]


@dataclass(slots=True)
class ConceptInfo:
    id: str
    label: str
    description: str
    category: str
    data_sources: tuple[str, ...]
    config_schema: tuple[ConfigField, ...]
    path: Path
    compute: Callable[[ConceptContext], object]
    detail: str | None = None
    required_lookback_seconds: Callable[[Mapping[str, object]], object] | None = None


def _load_concept(path: Path) -> ConceptInfo | None:
    spec = importlib.util.spec_from_file_location(f"polymarket_btc_concept_{path.stem}", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    info = getattr(module, "CONCEPT_INFO", None)
    compute = getattr(module, "compute", None)
    if not isinstance(info, dict):
        return None
    if not callable(compute) or inspect.iscoroutinefunction(compute):
        return None
    label = info.get("label")
    description = info.get("description")
    if not isinstance(label, str) or not label or not isinstance(description, str) or not description:
        return None
    category = str(info.get("category") or "").strip() or DEFAULT_CATEGORY
    data_sources_raw = info.get("data_sources")
    if not isinstance(data_sources_raw, list) or not data_sources_raw:
        return None
    if not all(isinstance(key, str) and key for key in data_sources_raw):
        return None
    config_schema = parse_config_schema(info.get("config_schema", []))
    if config_schema is None:
        return None
    detail_raw = info.get("detail")
    lookback_resolver = getattr(module, "required_lookback_seconds", None)
    if not callable(lookback_resolver) or inspect.iscoroutinefunction(lookback_resolver):
        lookback_resolver = None
    return ConceptInfo(
        id=path.stem,
        label=label,
        description=description,
        category=category,
        data_sources=tuple(data_sources_raw),
        config_schema=config_schema,
        path=path,
        compute=compute,
        detail=str(detail_raw) if detail_raw else None,
        required_lookback_seconds=lookback_resolver,
    )


def discover_concepts(concepts_dir: Path) -> list[ConceptInfo]:
    """Scan concepts_dir for valid concept files. A file that fails to
    import or doesn't match the contract is skipped, not fatal -- one
    broken script must never hide the working ones."""
    if not concepts_dir.is_dir():
        return []
    found: list[ConceptInfo] = []
    for path in sorted(concepts_dir.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        try:
            info = _load_concept(path)
        except Exception:
            continue
        if info is not None:
            found.append(info)
    return found


__all__ = ["ConceptContext", "ConceptInfo", "DEFAULT_CATEGORY", "discover_concepts"]
