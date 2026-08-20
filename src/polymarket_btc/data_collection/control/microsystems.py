"""Custom "microsystem" scripts: drop a `.py` file in `microsystems/` that
declares MICROSYSTEM_INFO and a sync `compute(context)`, and it becomes
selectable when building a strategy.

Where a concept turns data into information, a microsystem turns concepts
(and optionally raw data) into the reasoning logic of an analysis -- the
same *kind* of relationship as data-to-concept, one level up: concept(s)
(and/or data) in, a combined signal out. A strategy is built from one or
more microsystem instances.

As with concepts.py, nothing here actually runs a microsystem against real
data yet -- this module only defines and validates the contract for the
future backtest module to build on.

Contract:

    MICROSYSTEM_INFO = {
        "label": "...", "description": "...",   # required
        "category": "...",   # optional, defaults to "Général"
        "detail": "...",     # optional, (i) info bubble content
        "concept_inputs": [...],  # optional list[str] of concept ids this
                                   # microsystem wires to. Defaults to [].
        "data_inputs": [...],     # optional list[str] of data-catalog keys
                                   # this microsystem reads directly, same key
                                   # space as a concept's data_sources.
                                   # Defaults to [].
        "config_schema": [...],   # optional, see config_schema.py. Defaults to [].
    }
    # concept_inputs and data_inputs may not both be empty -- a microsystem
    # with nothing feeding it can't reason about anything.

    def compute(context: MicrosystemContext) -> object:
        ...

`compute` must be sync, same rule and reasoning as concepts. `context.concepts`
maps each declared concept_inputs id to that concept instance's own compute()
output; `context.data` maps each declared data_inputs key to that data;
`context.config` maps each config_schema field's name to its resolved value.

    def required_lookback_seconds(config: dict) -> float | None:
        ...

Optional, same contract and reasoning as a concept's own
required_lookback_seconds (see concepts.py) -- bounds how far back a
microsystem's own direct data_inputs reach, independent of whichever
concepts it's wired to (each of those is bounded, or not, by its own
declaration).
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
class MicrosystemContext:
    concepts: Mapping[str, object]
    data: Mapping[str, object]
    config: Mapping[str, object]
    log: Callable[[str], None]


@dataclass(slots=True)
class MicrosystemInfo:
    id: str
    label: str
    description: str
    category: str
    concept_inputs: tuple[str, ...]
    data_inputs: tuple[str, ...]
    config_schema: tuple[ConfigField, ...]
    path: Path
    compute: Callable[[MicrosystemContext], object]
    detail: str | None = None
    required_lookback_seconds: Callable[[Mapping[str, object]], object] | None = None


def _string_list(raw: object) -> tuple[str, ...] | None:
    if not isinstance(raw, list) or not all(isinstance(item, str) and item for item in raw):
        return None
    return tuple(raw)


def _load_microsystem(path: Path) -> MicrosystemInfo | None:
    spec = importlib.util.spec_from_file_location(f"polymarket_btc_microsystem_{path.stem}", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    info = getattr(module, "MICROSYSTEM_INFO", None)
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
    concept_inputs = _string_list(info.get("concept_inputs", []))
    data_inputs = _string_list(info.get("data_inputs", []))
    if concept_inputs is None or data_inputs is None:
        return None
    if not concept_inputs and not data_inputs:
        return None
    config_schema = parse_config_schema(info.get("config_schema", []))
    if config_schema is None:
        return None
    detail_raw = info.get("detail")
    lookback_resolver = getattr(module, "required_lookback_seconds", None)
    if not callable(lookback_resolver) or inspect.iscoroutinefunction(lookback_resolver):
        lookback_resolver = None
    return MicrosystemInfo(
        id=path.stem,
        label=label,
        description=description,
        category=category,
        concept_inputs=concept_inputs,
        data_inputs=data_inputs,
        config_schema=config_schema,
        path=path,
        compute=compute,
        detail=str(detail_raw) if detail_raw else None,
        required_lookback_seconds=lookback_resolver,
    )


def discover_microsystems(microsystems_dir: Path) -> list[MicrosystemInfo]:
    """Scan microsystems_dir for valid microsystem files. A file that fails
    to import or doesn't match the contract is skipped, not fatal."""
    if not microsystems_dir.is_dir():
        return []
    found: list[MicrosystemInfo] = []
    for path in sorted(microsystems_dir.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        try:
            info = _load_microsystem(path)
        except Exception:
            continue
        if info is not None:
            found.append(info)
    return found


__all__ = ["DEFAULT_CATEGORY", "MicrosystemContext", "MicrosystemInfo", "discover_microsystems"]
