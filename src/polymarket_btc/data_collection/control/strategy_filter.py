"""Custom "strategy filter" scripts: drop a `.py` file in `filter_profiles/`
that declares FILTER_INFO and a sync `filter(context)`, and it becomes
selectable as a strategy's own filter.

A strategy filter is the 5th and last stage of a strategy's own pipeline --
concept(s) -> microsystem(s) -> execution -> management -> filter. It runs
once management has already computed a fully-resolved proposed trade
(direction, entry price, stop-loss, take-profit) and can only ever **veto**
that trade -- it never edits direction/stop-loss/take-profit, and it never
touches the concept/microsystem/execution/management scripts that produced
them. This is what gives a filter "its own nuance": a strategy's concepts/
microsystems/execution/management stay exactly as authored, and a filter
adds an independent, separately-refinable layer of judgment on top (see
strategy_filter_refinement.py) without those other scripts ever needing to
change.

As with concepts.py/microsystems.py/execution.py/management.py, nothing
here actually runs a filter against real output yet -- this module only
defines and validates the contract for the future backtest module to build
on.

Contract:

    FILTER_INFO = {
        "label": "...", "description": "...",   # required
        "category": "...",   # optional, defaults to "Général"
        "detail": "...",     # optional, (i) info bubble content
        "config_schema": [...],  # optional, see config_schema.py. Defaults to [].
    }

    def filter(context: FilterContext) -> object:
        ...

`filter` must be sync, same rule and reasoning as concepts/microsystems/
execution/management. `context.execution`/`context.management` are whatever
the strategy's own execution/management stages returned; `context.direction`/
`context.entry_price`/`context.stop_loss`/`context.take_profit` are the
fully-resolved proposed trade management just computed; `context.microsystems`
maps every microsystem instance id in the strategy to that instance's own
compute() output, same full map execution/management themselves receive
(deliberately no `context.concepts` -- same convention execution/management
already follow: a filter reasons about the resolved trade and the
microsystem layer, not raw concept output); `context.config` maps each
config_schema field's name to its resolved value.

To veto the proposed trade, return a dict with `"veto": True` (an optional
`"reason"` string is picked up by context.log for the human-readable replay
readout, same as execution/management's own `log()` convention). Returning
anything else (including None, or a dict without `"veto": True`) allows the
trade through unchanged. An exception raised from `filter` is treated the
same as "allow" (fails open, not closed) -- see backtest_engine.py's own
simulate_combo for why: a buggy fail-closed filter would silently veto
every trade, which is harder for an author to notice/diagnose than an inert
one, and matches management's own existing error-isolation convention
(an exception there already means unbounded risk, accepted today).
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
class FilterContext:
    execution: object
    management: object
    direction: str
    entry_price: float
    stop_loss: float | None
    take_profit: float | None
    microsystems: Mapping[str, object]
    config: Mapping[str, object]
    log: Callable[[str], None]


@dataclass(slots=True)
class FilterInfo:
    id: str
    label: str
    description: str
    category: str
    config_schema: tuple[ConfigField, ...]
    path: Path
    filter: Callable[[FilterContext], object]
    detail: str | None = None


def _load_filter_profile(path: Path) -> FilterInfo | None:
    spec = importlib.util.spec_from_file_location(f"polymarket_btc_filter_{path.stem}", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    info = getattr(module, "FILTER_INFO", None)
    filter_fn = getattr(module, "filter", None)
    if not isinstance(info, dict):
        return None
    if not callable(filter_fn) or inspect.iscoroutinefunction(filter_fn):
        return None
    label = info.get("label")
    description = info.get("description")
    if not isinstance(label, str) or not label or not isinstance(description, str) or not description:
        return None
    category = str(info.get("category") or "").strip() or DEFAULT_CATEGORY
    config_schema = parse_config_schema(info.get("config_schema", []))
    if config_schema is None:
        return None
    detail_raw = info.get("detail")
    return FilterInfo(
        id=path.stem,
        label=label,
        description=description,
        category=category,
        config_schema=config_schema,
        path=path,
        filter=filter_fn,
        detail=str(detail_raw) if detail_raw else None,
    )


def discover_filter_profiles(filter_dir: Path) -> list[FilterInfo]:
    """Scan filter_dir for valid filter-profile files. A file that fails to
    import or doesn't match the contract is skipped, not fatal."""
    if not filter_dir.is_dir():
        return []
    found: list[FilterInfo] = []
    for path in sorted(filter_dir.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        try:
            info = _load_filter_profile(path)
        except Exception:
            continue
        if info is not None:
            found.append(info)
    return found


__all__ = ["DEFAULT_CATEGORY", "FilterContext", "FilterInfo", "discover_filter_profiles"]
