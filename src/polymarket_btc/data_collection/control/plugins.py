"""Custom data-collection plugins: drop a `.py` file in `plugins/` that
declares `PLUGIN_INFO` and an async `run(context)`, and the Data Collection
page will list it as an extra checkbox alongside the built-in feeds.

Security note: a plugin is a plain Python file this process imports and
executes with full privileges -- the exact same trust level as any other
module in this repository. Only place scripts you wrote or reviewed in
`plugins/`; this is not a sandbox for untrusted code.

Contract (see plugins/example_funding_history.py for a working sample):

    PLUGIN_INFO = {
        "label": "...", "description": "...",
        "category": "...",  # optional, defaults to "Général". Free text --
                             # matching an existing plugin's category groups
                             # them together in the UI; anything new becomes
                             # its own category automatically, the same way
                             # a new asset just needs a new catalog entry.
        "mode": "collect",  # optional, "collect" (default) or "access".
        "detail": "...",    # optional, str -- longer explanation shown in the
                             # UI's (i) info bubble on click. Distinct from
                             # "description", which is always visible inline
                             # and should stay short. None/absent -> no bubble
                             # offered for this plugin.
    }

    async def run(context: PluginContext) -> None:
        ...

Two valid shapes for run(), matching PLUGIN_INFO["mode"]:

- **"collect"** (default): the data has to be gathered over time -- poll an
  API or hold a live connection. Loop until stop_event fires:

    async def run(context: PluginContext) -> None:
        while not context.stop_event.is_set():
            ... collect one unit of data, append it under context.data_dir ...
            try:
                await asyncio.wait_for(context.stop_event.wait(), timeout=POLL_SECONDS)
            except TimeoutError:
                pass

- **"access"**: the data already exists somewhere (your own database, a
  folder of files, a service already running) -- there's nothing to poll.
  Read/convert it once and return; no loop, no stop_event wait:

    async def run(context: PluginContext) -> None:
        ... read the existing source, write it converted into context.data_dir ...
        context.log("done: N records converted")
        # returns immediately -- the host doesn't expect this to keep running

A plugin never touches the gateway's EventBus/StateStore/HealthRegistry --
it just writes its own file(s) into the collection run's directory, which
ride along in that run's exported dataset folder. A crashing plugin is
logged and stopped; it never brings down the rest of the collection.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import importlib.util
from pathlib import Path


@dataclass(slots=True)
class PluginContext:
    data_dir: Path
    stop_event: asyncio.Event
    log: Callable[[str], None]
    # Only ever set for an "access" run (see PLUGIN_INFO["mode"] below): the
    # time range the user picked in the "déjà collecté" UI, as nanosecond
    # epoch timestamps. Both None in a "collect" run, and optional even in
    # an "access" run (the user can leave either bound blank) -- a plugin
    # that has no notion of a time range is free to ignore them entirely.
    start_ts_ns: int | None = None
    end_ts_ns: int | None = None


DEFAULT_CATEGORY = "Général"
VALID_MODES = ("collect", "access")
DEFAULT_MODE = "collect"


@dataclass(slots=True)
class PluginInfo:
    id: str
    label: str
    description: str
    category: str
    mode: str
    path: Path
    run: Callable[[PluginContext], Awaitable[None]]
    # Longer explanation shown in the control panel's (i) info bubble on
    # click -- see PLUGIN_INFO["detail"] above. None -> no bubble offered.
    detail: str | None = None


def _load_plugin(path: Path) -> PluginInfo | None:
    spec = importlib.util.spec_from_file_location(f"polymarket_btc_plugin_{path.stem}", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    info = getattr(module, "PLUGIN_INFO", None)
    run = getattr(module, "run", None)
    if not isinstance(info, dict) or not asyncio.iscoroutinefunction(run):
        return None
    category = str(info.get("category") or "").strip() or DEFAULT_CATEGORY
    mode = info.get("mode", DEFAULT_MODE)
    if mode not in VALID_MODES:
        mode = DEFAULT_MODE
    detail_raw = info.get("detail")
    return PluginInfo(
        id=path.stem,
        label=str(info.get("label", path.stem)),
        description=str(info.get("description", "")),
        category=category,
        mode=mode,
        path=path,
        run=run,
        detail=str(detail_raw) if detail_raw else None,
    )


def discover_plugins(plugins_dir: Path) -> list[PluginInfo]:
    """Scan plugins_dir for valid plugin files. A file that fails to import
    or doesn't match the contract is skipped, not fatal -- one broken
    script must never hide the working ones."""
    if not plugins_dir.is_dir():
        return []
    found: list[PluginInfo] = []
    for path in sorted(plugins_dir.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        try:
            info = _load_plugin(path)
        except Exception:
            continue
        if info is not None:
            found.append(info)
    return found


async def run_plugin(info: PluginInfo, context: PluginContext) -> None:
    """Runs one plugin to completion/cancellation, isolating its failures."""
    try:
        await info.run(context)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        context.log(f"plugin '{info.id}' crashed: {exc!r}")


__all__ = ["PluginContext", "PluginInfo", "discover_plugins", "run_plugin"]
