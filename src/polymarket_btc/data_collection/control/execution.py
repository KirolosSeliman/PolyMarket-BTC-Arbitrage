"""Custom "execution profile" scripts: drop a `.py` file in
`execution_profiles/` that declares EXECUTION_INFO and a sync
`execute(context)`, and it becomes selectable as a strategy's execution
profile.

An execution profile holds the parameters needed to actually run a
strategy -- what those parameters are is intentionally not fixed here
(position sizing, risk limits, timing are examples, not a required list);
only the mechanism (a CONFIG_SCHEMA the profile's own script declares) is
fixed. A strategy has exactly one execution profile instance, which
receives every microsystem instance's output.

As with concepts.py/microsystems.py, nothing here actually runs an
execution profile against real output yet -- this module only defines and
validates the contract for the future backtest module to build on.

Contract:

    EXECUTION_INFO = {
        "label": "...", "description": "...",   # required
        "category": "...",   # optional, defaults to "Général"
        "detail": "...",     # optional, (i) info bubble content
        "config_schema": [...],  # optional, see config_schema.py. Defaults to [].
    }

    def execute(context: ExecutionContext) -> object:
        ...

`execute` must be sync, same rule and reasoning as concepts/microsystems.
`context.microsystems` maps every microsystem instance id in the strategy to
that instance's own compute() output; `context.config` maps each
config_schema field's name to its resolved value.
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
class ExecutionContext:
    microsystems: Mapping[str, object]
    config: Mapping[str, object]
    log: Callable[[str], None]


@dataclass(slots=True)
class ExecutionInfo:
    id: str
    label: str
    description: str
    category: str
    config_schema: tuple[ConfigField, ...]
    path: Path
    execute: Callable[[ExecutionContext], object]
    detail: str | None = None


def _load_execution_profile(path: Path) -> ExecutionInfo | None:
    spec = importlib.util.spec_from_file_location(f"polymarket_btc_execution_{path.stem}", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    info = getattr(module, "EXECUTION_INFO", None)
    execute = getattr(module, "execute", None)
    if not isinstance(info, dict):
        return None
    if not callable(execute) or inspect.iscoroutinefunction(execute):
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
    return ExecutionInfo(
        id=path.stem,
        label=label,
        description=description,
        category=category,
        config_schema=config_schema,
        path=path,
        execute=execute,
        detail=str(detail_raw) if detail_raw else None,
    )


def discover_execution_profiles(execution_dir: Path) -> list[ExecutionInfo]:
    """Scan execution_dir for valid execution-profile files. A file that
    fails to import or doesn't match the contract is skipped, not fatal."""
    if not execution_dir.is_dir():
        return []
    found: list[ExecutionInfo] = []
    for path in sorted(execution_dir.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        try:
            info = _load_execution_profile(path)
        except Exception:
            continue
        if info is not None:
            found.append(info)
    return found


__all__ = ["DEFAULT_CATEGORY", "ExecutionContext", "ExecutionInfo", "discover_execution_profiles"]
