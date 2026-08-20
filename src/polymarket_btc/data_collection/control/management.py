"""Custom "management profile" scripts: drop a `.py` file in
`management_profiles/` that declares MANAGEMENT_INFO and a sync
`manage(context)`, and it becomes selectable as a strategy's management
profile.

A management profile runs *after* the execution profile's signal -- it
holds the parameters needed to manage a trade once execution has decided to
take one (stop-loss, take-profit, whether those stay fixed or adapt as new
information arrives, and anything else a given strategy needs). As with
execution profiles, there is **no fixed list** of what those parameters
are -- only the mechanism (a CONFIG_SCHEMA the profile's own script
declares) is fixed. A strategy has exactly one management profile instance,
which receives the execution profile's own output plus every microsystem
instance's output.

As with concepts.py/microsystems.py/execution.py, nothing here actually
runs a management profile against real output yet -- this module only
defines and validates the contract for the future backtest module to build
on.

Contract:

    MANAGEMENT_INFO = {
        "label": "...", "description": "...",   # required
        "category": "...",   # optional, defaults to "Général"
        "detail": "...",     # optional, (i) info bubble content
        "config_schema": [...],  # optional, see config_schema.py. Defaults to [].
    }

    def manage(context: ManagementContext) -> object:
        ...

`manage` must be sync, same rule and reasoning as concepts/microsystems/
execution. `context.execution` is whatever the strategy's execution
profile's own `execute()` returned; `context.microsystems` maps every
microsystem instance id in the strategy to that instance's own compute()
output, same full map the execution profile itself receives; `context.config`
maps each config_schema field's name to its resolved value.
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
class ManagementContext:
    execution: object
    microsystems: Mapping[str, object]
    config: Mapping[str, object]
    log: Callable[[str], None]


@dataclass(slots=True)
class ManagementInfo:
    id: str
    label: str
    description: str
    category: str
    config_schema: tuple[ConfigField, ...]
    path: Path
    manage: Callable[[ManagementContext], object]
    detail: str | None = None


def _load_management_profile(path: Path) -> ManagementInfo | None:
    spec = importlib.util.spec_from_file_location(f"polymarket_btc_management_{path.stem}", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    info = getattr(module, "MANAGEMENT_INFO", None)
    manage = getattr(module, "manage", None)
    if not isinstance(info, dict):
        return None
    if not callable(manage) or inspect.iscoroutinefunction(manage):
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
    return ManagementInfo(
        id=path.stem,
        label=label,
        description=description,
        category=category,
        config_schema=config_schema,
        path=path,
        manage=manage,
        detail=str(detail_raw) if detail_raw else None,
    )


def discover_management_profiles(management_dir: Path) -> list[ManagementInfo]:
    """Scan management_dir for valid management-profile files. A file that
    fails to import or doesn't match the contract is skipped, not fatal."""
    if not management_dir.is_dir():
        return []
    found: list[ManagementInfo] = []
    for path in sorted(management_dir.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        try:
            info = _load_management_profile(path)
        except Exception:
            continue
        if info is not None:
            found.append(info)
    return found


__all__ = ["DEFAULT_CATEGORY", "ManagementContext", "ManagementInfo", "discover_management_profiles"]
