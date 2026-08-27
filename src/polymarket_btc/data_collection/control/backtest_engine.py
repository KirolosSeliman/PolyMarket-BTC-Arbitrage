"""Replays a strategy against historical data and computes win rate.

Concept/microsystem config is fixed at authoring time -- only execution and
management variables are ever swept. So concept/microsystem outputs don't
depend on the sweep at all: `build_timeline` computes them **once** (the
expensive part -- real historical data, real `compute()` calls per
evaluation step), and `simulate_combo` replays the **cheap** execution ->
management -> trade-outcome step against that shared timeline once per
parameter combination. `run_backtest` dispatches combinations across a
process pool -- this is the concrete fix for "long period x many variable
combinations" being slow: 1 expensive pass + N cheap passes, parallelized,
instead of N full passes.

Trade/management output convention (new, additive -- see
docs/nouveau_execution_prompt.md / docs/nouveau_management_prompt.md):
`execute()` is recognized as opening a trade when it returns a dict with a
`"direction"` key case-insensitively in {"long","buy","haussier","bullish"}
(-> long) or {"short","sell","baissier","bearish"} (-> short); anything else
means no trade this step. `manage()` is recognized for stop-loss/take-profit
via `"stop_loss_pct"`/`"take_profit_pct"` (percent distance from entry
price) in its own returned dict. A trade with no resolved SL/TP (no
management profile, or one that returns neither) closes when the execution
signal next flips or goes neutral, so nothing is ever left open forever.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from itertools import product
from pathlib import Path
import random

from .backtest_data import read_records
from .concepts import ConceptContext, ConceptInfo, discover_concepts
from .config_schema import resolve_config
from .execution import ExecutionContext, ExecutionInfo, discover_execution_profiles
from .management import ManagementContext, ManagementInfo, discover_management_profiles
from .microsystems import MicrosystemContext, MicrosystemInfo, discover_microsystems
from .strategy_filter import FilterContext, FilterInfo, discover_filter_profiles


def _noop_log(_message: str) -> None:
    pass


_LONG_WORDS = {"long", "buy", "haussier", "bullish"}
_SHORT_WORDS = {"short", "sell", "baissier", "bearish"}


def normalize_direction(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    lowered = value.strip().lower()
    if lowered in _LONG_WORDS:
        return "long"
    if lowered in _SHORT_WORDS:
        return "short"
    return None


@dataclass(slots=True)
class TimelineStep:
    timestamp: float
    concept_outputs: dict[str, object]
    microsystem_outputs: dict[str, object]


def _instance_key_bindings(
    requirements: list[dict[str, object]], data_bindings: Mapping[str, str],
) -> list[tuple[str, str]]:
    """(literal_key, concrete_key) pairs for a concept/microsystem instance:
    literal is what its script actually reads via `context.data[literal]`,
    concrete is the real data-catalog key it's really bound to -- swappable
    requirements resolve through data_bindings (default to their own
    authored key if unset), locked requirements are always their own
    literal key(s) (literal == concrete). Mirrors
    StrategyManager._resolve_data_bindings' own resolution rule so a
    backtest sees exactly what was saved. Depends only on the instance's own
    (static) requirements/bindings, never on accumulated data, so it's
    computed once per instance rather than per evaluation step."""
    pairs: list[tuple[str, str]] = []
    for requirement in requirements:
        if requirement["swappable"]:
            literal_key = requirement["keys"][0]
            concrete_key = data_bindings.get(requirement["type"], literal_key)
            pairs.append((literal_key, concrete_key))
        else:
            pairs.extend((literal_key, literal_key) for literal_key in requirement["keys"])
    return pairs


def _detect_candle_seconds(records: list[dict]) -> float | None:
    """A robust, cheap-up-front estimate of a key's real record spacing --
    the median gap over a sample of up to the first 500 consecutive pairs,
    not just the first two (immune to one irregular gap, e.g. a data hole
    right at the start). Collection intervals range from 1m to 1M (see
    VALID_KLINE_INTERVALS in runs.py) and aren't known statically, so a
    concept whose lookback is naturally candle-based (lookback_candles *
    candle_seconds) needs this detected, not assumed -- guessing wrong
    (e.g. always assuming 1m) would silently hand it too short a window
    for any coarser interval, a correctness bug wearing a performance fix's
    clothes. None if there's not enough data (fewer than 2 records) to
    estimate from -- callers treat that as "can't bound yet"."""
    if len(records) < 2:
        return None
    sample = records[: min(len(records), 501)]
    diffs = sorted(sample[i + 1]["timestamp"] - sample[i]["timestamp"] for i in range(len(sample) - 1))
    return diffs[len(diffs) // 2] if diffs else None


def _resolve_lookback_seconds(info: object, config: dict, candle_seconds: float | None) -> float | None:
    """The trailing-history window (seconds) a concept/microsystem instance
    actually needs, per its own optional required_lookback_seconds(config,
    candle_seconds) -- None (the default, for any script that doesn't
    define the function) means unbounded: today's behavior, context.data
    [key] is every record accumulated since the backtest's own start_ts. A
    script that opts in gets, per instance, only its declared trailing
    window no matter how far the walk has progressed -- the concrete fix
    for a concept like fvg.py that reconstructs candles from scratch on
    every evaluation step: without a bound, that rebuild re-scans the
    *entire* accumulated history so far (confirmed the dominant cost in a
    slow backtest by reading concepts/fvg.py's _normalize_candles), growing
    for the whole walk; with a bound, it only ever sees its own declared
    window -- identical final result, since anything older was always
    going to be discarded by the concept's own lookback trimming anyway.
    `candle_seconds` is this instance's own detected record spacing (see
    _detect_candle_seconds), passed through so a resolver expressed in
    candles doesn't have to guess an interval. An invalid/failing resolver
    degrades to unbounded (never a crash, never a silently wrong window)."""
    resolver = getattr(info, "required_lookback_seconds", None)
    if resolver is None:
        return None
    try:
        value = resolver(config, candle_seconds)
    except Exception:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        return None
    return float(value)


def _advance_window_start(records: list[dict], window_start: int, cutoff: float) -> int:
    """Moves a windowed instance's own per-key cursor forward past any
    record older than `cutoff` -- monotonic (only ever advances), so summed
    over a whole timeline walk this costs the same O(total records) a
    single pass would, never revisiting a record once it's fallen out of
    the window."""
    while window_start < len(records) and records[window_start]["timestamp"] < cutoff:
        window_start += 1
    return window_start


def _instance_step_data(
    bindings: list[tuple[str, str]],
    lookback: float | None,
    t: float,
    accumulated: dict[str, list[dict]],
    cursors: dict[str, int],
    records_by_key: Mapping[str, list[dict]],
    own_windows: dict[str, int] | None,
) -> tuple[dict[str, list[dict]], tuple]:
    """The `context.data` dict + a hashable signature for one concept/
    microsystem instance at the current evaluation step. Two steps with an
    equal signature are guaranteed to have produced identical `data` --
    signatures are built entirely from integer cursors, never object
    identity/id(), so equality can never be a false positive from an old
    accumulated-list slice being garbage-collected and a later, different
    one coincidentally reusing its address.

    Unbounded (lookback is None): reads the shared accumulated-since-
    start_ts prefix everyone else also reads -- unchanged from before
    windowing existed. Windowed: reads only this instance's own trailing
    slice, advancing its own per-key window_start (monotonic, see
    _advance_window_start) instead of sharing the global accumulated dict,
    since a different instance's own declared window can differ."""
    if lookback is None:
        data = {literal: accumulated.get(concrete, []) for literal, concrete in bindings}
        signature = tuple(cursors.get(concrete, 0) for _literal, concrete in bindings)
        return data, signature
    cutoff = t - lookback
    data = {}
    sig_parts = []
    for literal, concrete in bindings:
        records = records_by_key.get(concrete, [])
        window_start = _advance_window_start(records, own_windows[concrete], cutoff)
        own_windows[concrete] = window_start
        cursor = cursors.get(concrete, 0)
        data[literal] = records[window_start:cursor]
        sig_parts.append((window_start, cursor))
    return data, tuple(sig_parts)


def required_concrete_keys(
    strategy: dict, concept_infos: Mapping[str, ConceptInfo], microsystem_infos: Mapping[str, MicrosystemInfo],
    data_requirements_for: Callable[[list[str]], list[dict[str, object]]],
) -> set[str]:
    """Every concrete data-catalog key a strategy's resolved instances
    actually touch -- the union `backtest_data.read_records` needs to fetch,
    and what `relevant_runs`/eligibility checks against."""
    keys: set[str] = set()
    for entry in strategy.get("concepts", []):
        info = concept_infos.get(entry["concept_id"])
        if info is None:
            continue
        requirements = data_requirements_for(list(info.data_sources))
        bindings = entry.get("data_bindings") or {}
        for requirement in requirements:
            if requirement["swappable"]:
                keys.add(bindings.get(requirement["type"], requirement["keys"][0]))
            else:
                keys.update(requirement["keys"])
    for entry in strategy.get("microsystems", []):
        info = microsystem_infos.get(entry["microsystem_id"])
        if info is None:
            continue
        requirements = data_requirements_for(list(info.data_inputs))
        bindings = entry.get("data_bindings") or {}
        for requirement in requirements:
            if requirement["swappable"]:
                keys.add(bindings.get(requirement["type"], requirement["keys"][0]))
            else:
                keys.update(requirement["keys"])
    return keys


def estimate_warmup_seconds(
    strategy: dict, concept_infos: Mapping[str, ConceptInfo], microsystem_infos: Mapping[str, MicrosystemInfo],
    data_requirements_for: Callable[[list[str]], list[dict[str, object]]],
    records_by_key: Mapping[str, list[dict]],
) -> float | None:
    """The longest trailing warm-up window any of strategy's own concept/
    microsystem instances needs before a narrow replay window (e.g.
    strategy-filter-refine reviewing one specific real trade, rather than
    a full-coverage backtest) can reproduce the exact same outputs a
    full-coverage run would have produced at that same moment -- the bound
    a caller must extend a narrow window backward by by. None means at
    least one instance never declared required_lookback_seconds (needs
    everything since the strategy's own start), so there's no shortcut:
    the caller has to fall back to the strategy's full coverage start."""
    max_seconds: float = 0.0
    for entries, infos, data_key in (
        (strategy.get("concepts", []), concept_infos, "concept_id"),
        (strategy.get("microsystems", []), microsystem_infos, "microsystem_id"),
    ):
        for entry in entries:
            info = infos.get(entry[data_key])
            if info is None:
                continue
            data_sources = info.data_sources if data_key == "concept_id" else info.data_inputs
            requirements = data_requirements_for(list(data_sources))
            bindings = _instance_key_bindings(requirements, entry.get("data_bindings") or {})
            candle_seconds = _detect_candle_seconds(records_by_key.get(bindings[0][1], [])) if bindings else None
            lookback = _resolve_lookback_seconds(info, entry.get("config") or {}, candle_seconds)
            if lookback is None:
                return None
            max_seconds = max(max_seconds, lookback)
    return max_seconds


def build_timeline(
    strategy: dict,
    concept_infos: Mapping[str, ConceptInfo],
    microsystem_infos: Mapping[str, MicrosystemInfo],
    records_by_key: Mapping[str, list[dict]],
    concept_requirements: Mapping[str, list[dict[str, object]]],
    microsystem_requirements: Mapping[str, list[dict[str, object]]],
    start_ts: float,
    end_ts: float,
    cadence_seconds: float,
    on_progress: Callable[[float], None] | None = None,
) -> list[TimelineStep]:
    """Walks the evaluation cadence across [start_ts, end_ts]. At each step,
    every key's accumulated-so-far record list grows (append-only, each
    record visited exactly once across the whole walk), then every concept
    instance computes from that, then every microsystem instance computes
    from the concept outputs -- exactly the "full history up to now, the
    concept trims its own lookback" shape concepts/fvg.py already assumes,
    unless an instance opts into a bounded trailing window instead (see
    _resolve_lookback_seconds).

    Two performance passes beyond a literal per-step recompute, both purely
    memoizing (identical output, never an approximation):

    - `accumulated[key]` is only ever re-sliced when that key's cursor
      actually advanced since the previous step -- unchanged, it's the same
      list object as last time, at zero extra cost.
    - Each concept/microsystem instance's own compute() is skipped and its
      last output reused whenever its own dependency signature (its data
      cursors, plus -- for a microsystem -- the concept signatures it's
      wired to) is unchanged since its own last run. This matters most for
      an instance whose data updates slower than the evaluation cadence
      (e.g. hourly funding-rate data under a 5s cadence would otherwise
      recompute ~720x more than it needs to); it can't help an instance
      whose data changes essentially every step (continuous tick trades),
      which is exactly what a declared lookback window (above) targets
      instead."""
    if cadence_seconds <= 0:
        raise ValueError("cadence_seconds must be positive")

    total_steps = int((end_ts - start_ts) / cadence_seconds) + 1
    # Reported at most ~200 times over the whole walk regardless of how many
    # steps that actually is -- frequent enough for a UI polling once a
    # second to see smooth movement, cheap enough (a plain float callback,
    # not real work) to add no measurable overhead of its own.
    report_every = max(1, total_steps // 200)

    cursors = {key: 0 for key in records_by_key}
    accumulated: dict[str, list[dict]] = {key: [] for key in records_by_key}
    steps: list[TimelineStep] = []

    # MicrosystemContext.concepts is keyed by concept *id* (what a
    # microsystem's own script declares in concept_inputs and reads via
    # context.concepts.get(concept_id) -- see microsystems.py's contract and
    # e.g. fvg_sweep_reversal.py's context.concepts.get("fvg")), not by the
    # strategy's own per-instance instance_id.
    concept_id_by_instance = {
        entry["instance_id"]: entry["concept_id"] for entry in strategy.get("concepts", [])
    }

    concept_entries = [e for e in strategy.get("concepts", []) if e["concept_id"] in concept_infos]
    microsystem_entries = [e for e in strategy.get("microsystems", []) if e["microsystem_id"] in microsystem_infos]

    concept_bindings = {
        entry["instance_id"]: _instance_key_bindings(
            concept_requirements[entry["concept_id"]], entry.get("data_bindings") or {},
        )
        for entry in concept_entries
    }
    microsystem_bindings = {
        entry["instance_id"]: _instance_key_bindings(
            microsystem_requirements[entry["microsystem_id"]], entry.get("data_bindings") or {},
        )
        for entry in microsystem_entries
    }
    # candle_seconds is detected from an instance's own first binding --
    # every concept/microsystem here has exactly one data type per
    # requirement, so the first binding's spacing is the instance's own
    # spacing. A multi-binding instance mixing different-cadence keys would
    # need a more careful choice, but none exist today.
    concept_lookback = {
        entry["instance_id"]: _resolve_lookback_seconds(
            concept_infos[entry["concept_id"]], entry.get("config") or {},
            _detect_candle_seconds(records_by_key.get(concept_bindings[entry["instance_id"]][0][1], []))
            if concept_bindings[entry["instance_id"]] else None,
        )
        for entry in concept_entries
    }
    microsystem_lookback = {
        entry["instance_id"]: _resolve_lookback_seconds(
            microsystem_infos[entry["microsystem_id"]], entry.get("config") or {},
            _detect_candle_seconds(records_by_key.get(microsystem_bindings[entry["instance_id"]][0][1], []))
            if microsystem_bindings[entry["instance_id"]] else None,
        )
        for entry in microsystem_entries
    }
    concept_windows = {
        entry["instance_id"]: {concrete: 0 for _literal, concrete in concept_bindings[entry["instance_id"]]}
        for entry in concept_entries
        if concept_lookback[entry["instance_id"]] is not None
    }
    microsystem_windows = {
        entry["instance_id"]: {concrete: 0 for _literal, concrete in microsystem_bindings[entry["instance_id"]]}
        for entry in microsystem_entries
        if microsystem_lookback[entry["instance_id"]] is not None
    }

    concept_cache: dict[str, tuple[tuple, object]] = {}
    microsystem_cache: dict[str, tuple[tuple, object]] = {}
    concept_signature_by_instance: dict[str, tuple] = {}

    # accumulated[key] is only ever read by _instance_step_data's unbounded
    # (lookback is None) branch -- if every instance touching a key has a
    # bounded lookback (every real concept/microsystem today does, once
    # they declare required_lookback_seconds), nothing ever reads that
    # key's accumulated list, so rebuilding it every step would be pure
    # waste: still an O(records) reslice on every cursor advance, same cost
    # this whole windowing mechanism exists to avoid.
    unbounded_keys: set[str] = set()
    for entry in concept_entries:
        if concept_lookback[entry["instance_id"]] is None:
            unbounded_keys.update(concrete for _literal, concrete in concept_bindings[entry["instance_id"]])
    for entry in microsystem_entries:
        if microsystem_lookback[entry["instance_id"]] is None:
            unbounded_keys.update(concrete for _literal, concrete in microsystem_bindings[entry["instance_id"]])

    t = start_ts
    while t <= end_ts:
        for key, records in records_by_key.items():
            cursor = cursors[key]
            while cursor < len(records) and records[cursor]["timestamp"] <= t:
                cursor += 1
            if cursor != cursors[key]:
                if key in unbounded_keys:
                    accumulated[key] = records[:cursor]
                cursors[key] = cursor

        concept_outputs: dict[str, object] = {}
        for entry in concept_entries:
            instance_id = entry["instance_id"]
            data, signature = _instance_step_data(
                concept_bindings[instance_id], concept_lookback[instance_id], t,
                accumulated, cursors, records_by_key, concept_windows.get(instance_id),
            )
            cached = concept_cache.get(instance_id)
            if cached is not None and cached[0] == signature:
                concept_outputs[instance_id] = cached[1]
            else:
                info = concept_infos[entry["concept_id"]]
                context = ConceptContext(data=data, config=entry.get("config") or {}, log=_noop_log)
                try:
                    result = info.compute(context)
                except Exception:
                    result = None
                concept_outputs[instance_id] = result
                concept_cache[instance_id] = (signature, result)
            concept_signature_by_instance[instance_id] = signature

        microsystem_outputs: dict[str, object] = {}
        for entry in microsystem_entries:
            instance_id = entry["instance_id"]
            concept_instance_ids = entry.get("concept_instance_ids") or []
            data, own_signature = _instance_step_data(
                microsystem_bindings[instance_id], microsystem_lookback[instance_id], t,
                accumulated, cursors, records_by_key, microsystem_windows.get(instance_id),
            )
            signature = (own_signature, tuple(concept_signature_by_instance.get(cid) for cid in concept_instance_ids))
            cached = microsystem_cache.get(instance_id)
            if cached is not None and cached[0] == signature:
                microsystem_outputs[instance_id] = cached[1]
            else:
                info = microsystem_infos[entry["microsystem_id"]]
                concepts_in = {
                    concept_id_by_instance[cid]: concept_outputs.get(cid)
                    for cid in concept_instance_ids
                    if cid in concept_id_by_instance
                }
                context = MicrosystemContext(
                    concepts=concepts_in, data=data, config=entry.get("config") or {}, log=_noop_log,
                )
                try:
                    result = info.compute(context)
                except Exception:
                    result = None
                microsystem_outputs[instance_id] = result
                microsystem_cache[instance_id] = (signature, result)

        steps.append(TimelineStep(timestamp=t, concept_outputs=concept_outputs, microsystem_outputs=microsystem_outputs))
        if on_progress is not None and (len(steps) % report_every == 0 or len(steps) == total_steps):
            on_progress(min(1.0, len(steps) / total_steps))
        t += cadence_seconds
    return steps


def _price_at(price_path: list[dict], ts: float, cursor: int) -> tuple[float | None, int]:
    while cursor + 1 < len(price_path) and price_path[cursor + 1]["timestamp"] <= ts:
        cursor += 1
    if cursor < len(price_path) and price_path[cursor]["timestamp"] <= ts:
        return price_path[cursor]["price"], cursor
    return None, cursor


def simulate_combo(
    timeline: list[TimelineStep],
    price_path: list[dict],
    execution_info: ExecutionInfo,
    execution_config: dict,
    management_info: ManagementInfo | None,
    management_config: dict,
    filter_info: FilterInfo | None = None,
    filter_config: dict | None = None,
    detail: bool = False,
) -> dict[str, object]:
    """Replays execution -> management -> filter -> trade-outcome against a
    precomputed timeline for one parameter combination. Cheap by design --
    no concept/microsystem work happens here. `detail=True` additionally
    records each trade's own entry/exit for the "voir les trades pris"
    replay view -- falls straight out of the same loop that already tracks
    open_trade/win-loss, just keeping the record instead of only counting
    it, so it costs nothing extra to compute (only to carry in the return
    value) and is only requested for the one combo a user can actually
    inspect (see run_backtest), never for every combo in a sweep.

    `filter_info` (None when the strategy has no filter -- every trade
    management proposes opens unconditionally, exactly today's behavior)
    runs last, once management has already computed a fully-resolved
    proposed trade -- see strategy_filter.py's own module docstring for the
    veto contract and why an exception there fails open rather than
    closed."""
    price_cursor = 0
    open_trade: dict[str, object] | None = None
    trades = 0
    wins = 0
    filter_vetoes = 0
    last_price: float | None = None
    last_ts: float | None = None
    trade_log: list[dict[str, object]] = []

    def close(exit_time: float, exit_price: float, outcome: str) -> None:
        nonlocal trades, wins, open_trade
        trades += 1
        wins += 1 if outcome == "win" else 0
        if detail:
            trade_log.append({
                "entry_time": open_trade["entry_time"], "entry_price": open_trade["entry_price"],
                "exit_time": exit_time, "exit_price": exit_price,
                "direction": open_trade["direction"], "outcome": outcome,
                "stop_loss": open_trade["stop_loss"], "take_profit": open_trade["take_profit"],
                "execution_log": open_trade.get("execution_log"),
                "management_log": open_trade.get("management_log"),
                "filter_log": open_trade.get("filter_log"),
            })
        open_trade = None

    for step in timeline:
        price, price_cursor = _price_at(price_path, step.timestamp, price_cursor)
        if price is None:
            continue
        last_price, last_ts = price, step.timestamp

        if open_trade is not None:
            hit: str | None = None
            if open_trade["direction"] == "long":
                if open_trade["stop_loss"] is not None and price <= open_trade["stop_loss"]:
                    hit = "loss"
                elif open_trade["take_profit"] is not None and price >= open_trade["take_profit"]:
                    hit = "win"
            else:
                if open_trade["stop_loss"] is not None and price >= open_trade["stop_loss"]:
                    hit = "loss"
                elif open_trade["take_profit"] is not None and price <= open_trade["take_profit"]:
                    hit = "win"
            if hit is not None:
                close(step.timestamp, price, hit)

        # Captured (rather than sent to _noop_log like every other context in
        # this module) only when detail=True -- the one-combo replay a user
        # can actually inspect, never a sweep -- so the script's own
        # human-readable reasoning ("rebond confirmé sur un FVG ...", "SL=...
        # (mèche), TP=...") reaches the preview's own readout instead of
        # being thrown away, without adding any overhead to a real sweep.
        exec_log_lines: list[str] = []
        exec_context = ExecutionContext(
            microsystems=step.microsystem_outputs, config=execution_config,
            log=(exec_log_lines.append if detail else _noop_log),
        )
        try:
            execution_output = execution_info.execute(exec_context)
        except Exception:
            execution_output = None
        direction = normalize_direction(
            execution_output.get("direction") if isinstance(execution_output, dict) else None
        )

        if open_trade is not None and direction != open_trade["direction"]:
            won = (
                price > open_trade["entry_price"] if open_trade["direction"] == "long"
                else price < open_trade["entry_price"]
            )
            close(step.timestamp, price, "win" if won else "loss")

        if open_trade is None and direction is not None:
            stop_loss = take_profit = None
            management_output = None
            management_log_lines: list[str] = []
            if management_info is not None:
                mgmt_context = ManagementContext(
                    execution=execution_output, microsystems=step.microsystem_outputs,
                    config=management_config,
                    log=(management_log_lines.append if detail else _noop_log),
                )
                try:
                    management_output = management_info.manage(mgmt_context)
                except Exception:
                    management_output = None
                if isinstance(management_output, dict):
                    sl_pct = management_output.get("stop_loss_pct")
                    tp_pct = management_output.get("take_profit_pct")
                    if isinstance(sl_pct, (int, float)) and not isinstance(sl_pct, bool):
                        stop_loss = price * (1 - sl_pct / 100) if direction == "long" else price * (1 + sl_pct / 100)
                    if isinstance(tp_pct, (int, float)) and not isinstance(tp_pct, bool):
                        take_profit = price * (1 + tp_pct / 100) if direction == "long" else price * (1 - tp_pct / 100)

            vetoed = False
            filter_log_lines: list[str] = []
            if filter_info is not None:
                filter_context = FilterContext(
                    execution=execution_output, management=management_output,
                    direction=direction, entry_price=price, stop_loss=stop_loss, take_profit=take_profit,
                    microsystems=step.microsystem_outputs, config=filter_config or {},
                    log=(filter_log_lines.append if detail else _noop_log),
                )
                try:
                    filter_output = filter_info.filter(filter_context)
                    vetoed = isinstance(filter_output, dict) and bool(filter_output.get("veto"))
                except Exception:
                    vetoed = False  # fails open, see this function's own docstring

            if vetoed:
                filter_vetoes += 1
            else:
                open_trade = {
                    "direction": direction, "entry_time": step.timestamp, "entry_price": price,
                    "stop_loss": stop_loss, "take_profit": take_profit,
                    "execution_log": exec_log_lines[-1] if exec_log_lines else None,
                    "management_log": management_log_lines[-1] if management_log_lines else None,
                    "filter_log": filter_log_lines[-1] if filter_log_lines else None,
                }

    if open_trade is not None and last_price is not None:
        won = (
            last_price > open_trade["entry_price"] if open_trade["direction"] == "long"
            else last_price < open_trade["entry_price"]
        )
        close(last_ts, last_price, "win" if won else "loss")

    result: dict[str, object] = {
        "trades": trades, "wins": wins, "win_rate": (wins / trades) if trades else None,
        "filter_vetoes": filter_vetoes,
    }
    if detail:
        result["trade_log"] = trade_log
    return result


def expand_numeric_range(start: float, stop: float, step: float) -> list[float]:
    if step <= 0:
        raise ValueError("sweep step must be positive")
    values = []
    value = start
    # A tolerance guards against float accumulation excluding the intended
    # endpoint (e.g. 3 + 5*1 should include 8, not stop at 7.999999...).
    while value <= stop + step * 1e-9:
        values.append(round(value, 10))
        value += step
    return values


def expand_sweep(
    config_schema, sweep_spec: Mapping[str, object], fixed_values: Mapping[str, object] = {},
) -> list[dict[str, object]]:
    """`sweep_spec`: {field_name: {"min":, "max":, "step":}} for a numeric
    field being swept, or {field_name: [values...]} for a select field being
    swept over a subset of its options. A field absent from `sweep_spec`
    uses `fixed_values[field.name]` if present (e.g. the value edited on the
    backtest page), else the script's own declared default. Returns the
    cartesian product as a list of {field_name: value} dicts -- a single
    dict if nothing is swept."""
    axes: list[list[tuple[str, object]]] = []
    for field in config_schema:
        spec = sweep_spec.get(field.name)
        if spec is None:
            axes.append([(field.name, fixed_values.get(field.name, field.default))])
        elif isinstance(spec, dict):
            values = expand_numeric_range(float(spec["min"]), float(spec["max"]), float(spec["step"]))
            axes.append([(field.name, value) for value in values])
        else:
            values = list(spec) or [field.default]
            axes.append([(field.name, value) for value in values])
    if not axes:
        return [{}]
    return [dict(combo) for combo in product(*axes)]


# --- Process-pool sweep dispatch. Loaded profile callables (from
# importlib.util.spec_from_file_location) aren't picklable, so a worker
# can't receive a live ExecutionInfo/ManagementInfo as a task argument --
# each worker re-discovers them from disk once (cheap, a handful of files),
# and the (potentially large) timeline is set up once per worker via
# `initializer` rather than re-pickled per task, relying on fork's
# copy-on-write to share it cheaply (this project only targets Linux). ---

_worker_state: dict[str, object] = {}


def _init_worker(
    timeline: list[TimelineStep], price_path: list[dict], execution_dir: Path, management_dir: Path,
    filter_dir: Path,
) -> None:
    _worker_state["timeline"] = timeline
    _worker_state["price_path"] = price_path
    _worker_state["execution_dir"] = execution_dir
    _worker_state["management_dir"] = management_dir
    _worker_state["filter_dir"] = filter_dir


def _run_combo_in_worker(
    execution_id: str, execution_config: dict, management_id: str | None, management_config: dict,
    filter_id: str | None, filter_config: dict,
) -> dict[str, object]:
    execution_info = next(
        info for info in discover_execution_profiles(_worker_state["execution_dir"]) if info.id == execution_id
    )
    management_info = None
    if management_id is not None:
        management_info = next(
            info for info in discover_management_profiles(_worker_state["management_dir"]) if info.id == management_id
        )
    filter_info = None
    if filter_id is not None:
        filter_info = next(
            info for info in discover_filter_profiles(_worker_state["filter_dir"]) if info.id == filter_id
        )
    result = simulate_combo(
        _worker_state["timeline"], _worker_state["price_path"],
        execution_info, execution_config, management_info, management_config,
        filter_info, filter_config,
    )
    return {"execution_config": execution_config, "management_config": management_config, **result}


def run_backtest(
    *,
    strategy: dict,
    concepts_dir: Path,
    microsystems_dir: Path,
    execution_dir: Path,
    management_dir: Path,
    filter_dir: Path,
    data_requirements_for: Callable[[list[str]], list[dict[str, object]]],
    manifests: list[dict],
    instrument: str,
    start_ts: float,
    end_ts: float,
    cadence_seconds: float,
    execution_sweep: Mapping[str, object],
    management_sweep: Mapping[str, object],
    execution_config: Mapping[str, object] = {},
    management_config: Mapping[str, object] = {},
    filter_config: Mapping[str, object] = {},
    max_workers: int | None = None,
    on_progress: Callable[[float], None] | None = None,
    records_by_key: Mapping[str, list[dict]] | None = None,
) -> dict[str, object]:
    """`on_progress`, when given, is called with a 0..1 fraction as the
    backtest advances -- timeline building (build_timeline's own walk,
    almost always the dominant cost, see its docstring) covers 0..0.9;
    the sweep's combos completing (or, for a single combo, one immediate
    jump) covers the remaining 0.9..1.0. A rough split, not a promise --
    good enough for a UI progress bar/ETA, not a scheduling guarantee.

    filter_config is not part of the sweep (unlike execution_config/
    management_config, whose sweep-expanded values `combos` iterates) --
    the strategy's own saved filter config applies as one fixed value
    across every combo, matching how execution/management's own non-swept
    fields already work, and avoiding a third partner in the sweep's
    cartesian merge (see the `combos` comprehension below, already fragile
    with just two).

    `records_by_key`, when given, is used as-is instead of fetching --
    read_records' underlying raw storage has no seek/index, so every call
    re-decompresses and re-parses a manifest's *entire* segment file
    regardless of how narrow [start_ts, end_ts] is (confirmed the
    dominant cost of a bounded reasoning-replay call by profiling
    StrategyFilterRefinementManager._instance_window). A caller that
    already has the records it needs (e.g. that same method, which reads
    once over a wide range and slices in memory for both the display
    candles and this call) passes them through here rather than paying a
    second full-file scan for data it already has."""
    concept_infos = {info.id: info for info in discover_concepts(concepts_dir)}
    microsystem_infos = {info.id: info for info in discover_microsystems(microsystems_dir)}
    execution_infos = {info.id: info for info in discover_execution_profiles(execution_dir)}
    management_infos = {info.id: info for info in discover_management_profiles(management_dir)}
    filter_infos = {info.id: info for info in discover_filter_profiles(filter_dir)}

    execution_entry = strategy.get("execution")
    if execution_entry is None:
        raise ValueError("strategy has no execution profile -- nothing to backtest")
    execution_info = execution_infos.get(execution_entry["execution_id"])
    if execution_info is None:
        raise ValueError(f"unknown execution_id: {execution_entry['execution_id']!r}")

    management_entry = strategy.get("management")
    management_info = management_infos.get(management_entry["management_id"]) if management_entry else None

    filter_entry = strategy.get("filter")
    filter_info = filter_infos.get(filter_entry["filter_id"]) if filter_entry else None

    concept_requirements = {
        cid: data_requirements_for(list(info.data_sources)) for cid, info in concept_infos.items()
    }
    microsystem_requirements = {
        mid: data_requirements_for(list(info.data_inputs)) for mid, info in microsystem_infos.items()
    }

    keys = required_concrete_keys(strategy, concept_infos, microsystem_infos, data_requirements_for)
    start_ts_ns, end_ts_ns = int(start_ts * 1e9), int(end_ts * 1e9)
    # `instrument` here is a bare asset name ("BTC"), matching SourceInfo.asset
    # and what the eligibility endpoint's default_instrument returns -- raw
    # storage partitions by the concrete USDT-margined pair symbol instead
    # (see sources/binance_futures_historical.py), so the conversion happens
    # once here rather than pushing a Binance-naming detail onto every caller.
    instrument_symbol = f"{instrument.upper()}USDT"
    if records_by_key is None:
        records_by_key = {
            key: read_records(key, start_ts_ns, end_ts_ns, manifests, instrument=instrument_symbol) for key in keys
        }
    # The canonical price path prices fills/SL/TP for the simulated
    # instrument -- independent of whatever data keys the strategy's own
    # concepts happen to declare (a strategy built only on klines is common,
    # and its klines are still perfectly usable for pricing). Tried in
    # order of fidelity: real trades first, then kline close price, then
    # mark price as a last resort. A key already in records_by_key (the
    # strategy needed it anyway) is reused rather than fetched twice.
    price_path: list[dict[str, object]] = []
    # Real OHLC (klines only -- aggTrades/mark price have no candle
    # structure to give) lets the replay chart draw actual candlesticks
    # when zoomed in close, instead of only ever a price line. None when
    # the winning price source doesn't carry it.
    candles: list[dict[str, object]] | None = None
    for candidate_key, price_field in (
        ("binance_futures_trade", "price"),
        ("binance_futures_kline", "close"),
        ("binance_futures_mark_price", "mark_price"),
    ):
        records = records_by_key.get(candidate_key)
        if records is None:
            records = read_records(candidate_key, start_ts_ns, end_ts_ns, manifests, instrument=instrument_symbol)
        price_path = [
            {"timestamp": record["timestamp"], "price": record[price_field]}
            for record in records if price_field in record
        ]
        if price_path:
            candles = [
                {
                    "timestamp": record["timestamp"], "open": record["open"], "high": record["high"],
                    "low": record["low"], "close": record["close"],
                }
                for record in records
                if all(field in record for field in ("open", "high", "low", "close"))
            ] or None
            break
    if not price_path:
        raise ValueError(
            "aucune donnée de prix disponible pour l'actif simulé sur cette période -- "
            "aucune collecte ne couvre les trades, les bougies ou le mark price de cet instrument"
        )

    timeline_progress = None if on_progress is None else (lambda f: on_progress(f * 0.9))
    timeline = build_timeline(
        strategy, concept_infos, microsystem_infos, records_by_key,
        concept_requirements, microsystem_requirements, start_ts, end_ts, cadence_seconds,
        on_progress=timeline_progress,
    )

    # Fallback chain for a field that isn't being swept: an explicit
    # override from the request, else the strategy's own saved config value,
    # else the script's declared default -- resolve_config already merges
    # the first two; expand_sweep falls through to field.default for the third.
    execution_fixed = resolve_config(execution_info.config_schema, {
        **(execution_entry.get("config") or {}), **execution_config,
    })
    management_fixed = (
        resolve_config(management_info.config_schema, {
            **((management_entry or {}).get("config") or {}), **management_config,
        })
        if management_info is not None else {}
    )
    filter_fixed = (
        resolve_config(filter_info.config_schema, {
            **((filter_entry or {}).get("config") or {}), **filter_config,
        })
        if filter_info is not None else {}
    )

    combos = [
        {
            "execution_config": {k: v for k, v in combo.items() if k in {f.name for f in execution_info.config_schema}},
            "management_config": {k: v for k, v in combo.items() if management_info and k in {f.name for f in management_info.config_schema}},
        }
        for combo in [
            dict(exec_combo, **mgmt_combo)
            for exec_combo in expand_sweep(execution_info.config_schema, execution_sweep, execution_fixed)
            for mgmt_combo in (
                expand_sweep(management_info.config_schema, management_sweep, management_fixed) if management_info else [{}]
            )
        ]
    ]

    results: list[dict[str, object]] = []
    if len(combos) == 1:
        combo = combos[0]
        result = simulate_combo(
            timeline, price_path, execution_info, combo["execution_config"], management_info, combo["management_config"],
            filter_info, filter_fixed,
        )
        results.append({**combo, **result})
        if on_progress is not None:
            on_progress(0.9)
    else:
        with ProcessPoolExecutor(
            max_workers=max_workers, initializer=_init_worker,
            initargs=(timeline, price_path, execution_dir, management_dir, filter_dir),
        ) as pool:
            futures = {
                pool.submit(
                    _run_combo_in_worker, execution_info.id, combo["execution_config"],
                    management_info.id if management_info else None, combo["management_config"],
                    filter_info.id if filter_info else None, filter_fixed,
                ): combo
                for combo in combos
            }
            completed = 0
            for future in as_completed(futures):
                results.append(future.result())
                completed += 1
                if on_progress is not None:
                    on_progress(0.9 + 0.1 * (completed / len(combos)))

    def sort_key(result: dict[str, object]) -> tuple[bool, float]:
        rate = result["win_rate"]
        return (rate is not None, rate if rate is not None else -1.0)

    results.sort(key=sort_key, reverse=True)
    best = results[0] if results else None

    # The "voir les trades pris" replay view needs each trade's own entry/
    # exit plus every concept/microsystem's output over time -- expensive
    # to carry for every sweep combination, so it's only ever computed for
    # the one combo a user can actually open (the best, or the single
    # fixed-mode result): one more pass over the already-shared timeline,
    # not a new sweep.
    replay: dict[str, object] | None = None
    if best is not None:
        detailed = simulate_combo(
            timeline, price_path, execution_info, best["execution_config"],
            management_info, best["management_config"], filter_info, filter_fixed, detail=True,
        )
        replay = {
            "price_path": price_path,
            "candles": candles,
            "timeline": [
                {"timestamp": step.timestamp, "concepts": step.concept_outputs, "microsystems": step.microsystem_outputs}
                for step in timeline
            ],
            "trades": detailed["trade_log"],
        }

    if on_progress is not None:
        on_progress(1.0)
    return {"results": results, "best": best, "evaluation_steps": len(timeline), "replay": replay}


# --- Example scenarios: "did I wire this correctly," not a real backtest --
# runs a strategy (or any subset of it, e.g. one concept in isolation, for
# the builder's own per-item preview) against invented data instead of a
# real collection, so a strategy under construction can be sanity-checked
# before any data has even been collected for it. Reuses build_timeline/
# simulate_combo exactly as run_backtest does; only *where the data comes
# from* differs, so the result is genuinely what the strategy's own scripts
# actually do, not a mocked-up approximation of it. ---

_SYNTHETIC_BASE_KEYS = ("binance_futures_kline", "binance_futures_trade")


def _synthetic_kline_records(
    *, count: int, candle_seconds: float, seed: int, start_price: float,
) -> list[dict[str, object]]:
    """A reproducible, plausible-looking OHLC series -- not hand-fitted to
    any one concept's pattern (this has to work for whatever a strategy
    author writes, not just the ICT concepts shipped in this repo), just
    volatile enough that gap- and stop-hunt-like structures have a real
    chance to occur on their own. Occasional larger "impulse" candles
    (volatility clustering, the way real markets actually move) make that
    meaningfully more likely than a flat-variance random walk would."""
    rng = random.Random(seed)
    candles: list[dict[str, object]] = []
    price = start_price
    ts = 0.0
    for _ in range(count):
        is_impulse = rng.random() < 0.08
        pct = rng.gauss(0, 0.006 if is_impulse else 0.0015)
        open_price = price
        close_price = max(0.01, open_price * (1 + pct))
        wick_reach = abs(rng.gauss(0, 0.0015))
        high = max(open_price, close_price) * (1 + wick_reach)
        low = min(open_price, close_price) * (1 - wick_reach)
        close_time = ts + candle_seconds - 0.001
        candles.append({
            "open_time": ts, "close_time": close_time, "timestamp": close_time,
            "open": round(open_price, 2), "high": round(high, 2),
            "low": round(low, 2), "close": round(close_price, 2),
            "volume": round(abs(rng.gauss(10, 3)) + 0.1, 3),
        })
        price = close_price
        ts += candle_seconds
    return candles


def _synthetic_trade_records(
    candles: list[dict[str, object]], *, seed: int, trades_per_candle: int = 4,
) -> list[dict[str, object]]:
    """A handful of trades per synthetic candle, interpolated within its
    own [low, high] range and time span -- for a concept that reads
    binance_futures_trade directly instead of klines. Derived from the
    same candles (not an independent random series) so both keys describe
    one consistent invented market, exactly like real trades and real
    klines for the same instrument always agree."""
    rng = random.Random(seed + 1)
    trades: list[dict[str, object]] = []
    for candle in candles:
        span = candle["close_time"] - candle["open_time"]
        for i in range(trades_per_candle):
            frac = (i + rng.random()) / trades_per_candle
            price = rng.uniform(candle["low"], candle["high"])
            trades.append({
                "price": round(price, 2), "quantity": round(abs(rng.gauss(0.5, 0.2)) + 0.01, 3),
                "timestamp": candle["open_time"] + frac * span,
                "taker_side": rng.choice(["buy", "sell"]),
            })
    return trades


def run_example_scenario(
    *,
    strategy: dict,
    concepts_dir: Path,
    microsystems_dir: Path,
    execution_dir: Path,
    management_dir: Path,
    filter_dir: Path,
    data_requirements_for: Callable[[list[str]], list[dict[str, object]]],
    cadence_seconds: float = 60.0,
    candle_count: int = 400,
    seed: int = 42,
) -> dict[str, object]:
    """Same replay shape run_backtest's own `"replay"` field returns
    (price_path, candles, timeline, trades) -- built from synthetic data
    instead of a real collection, so the exact chart the backtest page
    already draws can render this too, unmodified. `strategy` doesn't have
    to be a complete, saved strategy: the builder's own per-concept/
    per-microsystem "i" preview passes just the one instance being
    inspected (plus, for a microsystem, whichever concept instances feed
    it) with execution/management left out -- trades stay empty, but the
    concept/microsystem outputs on the chart are exactly what that one
    script actually does against a real-shaped input."""
    concept_infos = {info.id: info for info in discover_concepts(concepts_dir)}
    microsystem_infos = {info.id: info for info in discover_microsystems(microsystems_dir)}
    execution_infos = {info.id: info for info in discover_execution_profiles(execution_dir)}
    management_infos = {info.id: info for info in discover_management_profiles(management_dir)}
    filter_infos = {info.id: info for info in discover_filter_profiles(filter_dir)}

    concept_requirements = {
        cid: data_requirements_for(list(info.data_sources)) for cid, info in concept_infos.items()
    }
    microsystem_requirements = {
        mid: data_requirements_for(list(info.data_inputs)) for mid, info in microsystem_infos.items()
    }

    keys = required_concrete_keys(strategy, concept_infos, microsystem_infos, data_requirements_for)
    needed_base_keys = {key.split(":", 1)[0] for key in keys}

    # seed_klines is the one series every synthetic key derives from, so a
    # strategy needing both klines and trades still sees one internally
    # consistent invented market rather than two unrelated random walks.
    # Always computed (cheap, and start_ts/end_ts need it regardless) but
    # only *exposed* under a base key the strategy's own resolved instances
    # actually asked for -- mirrors how a real collection only ever has
    # data for what was actually collected. Concretely: a strategy built
    # purely on klines never gets trades fabricated for it, so the
    # price-path fallback below naturally lands on kline close price and
    # the chart gets real OHLC candles -- exactly what a genuinely
    # kline-only real backtest would show too.
    seed_klines = _synthetic_kline_records(count=candle_count, candle_seconds=cadence_seconds, seed=seed, start_price=100.0)
    synthetic_by_base_key: dict[str, list[dict[str, object]]] = {}
    if "binance_futures_kline" in needed_base_keys:
        synthetic_by_base_key["binance_futures_kline"] = seed_klines
    if "binance_futures_trade" in needed_base_keys:
        synthetic_by_base_key["binance_futures_trade"] = _synthetic_trade_records(seed_klines, seed=seed)

    # A compound key ("binance_futures_kline:ETH") describes a different
    # asset than the bare key, but there's no "real" ETH here either --
    # every asset gets the same invented series, keyed by its own base type
    # only, same tolerance discover_concepts/read_records already have for
    # keys outside their own known set (silently empty, never a crash).
    records_by_key: dict[str, list[dict[str, object]]] = {
        key: list(synthetic_by_base_key.get(key.split(":", 1)[0], [])) for key in keys
    }

    start_ts = seed_klines[0]["open_time"] if seed_klines else 0.0
    end_ts = seed_klines[-1]["timestamp"] if seed_klines else 0.0

    # Mirrors run_backtest's own price-path fallback chain (trade fidelity
    # first, then kline close) minus mark_price, which has no synthetic
    # series here -- nothing in this scenario is ever priced from it.
    price_path: list[dict[str, object]] = []
    candles_out: list[dict[str, object]] | None = None
    for candidate_key, price_field in (("binance_futures_trade", "price"), ("binance_futures_kline", "close")):
        records = synthetic_by_base_key.get(candidate_key, [])
        candidates = [{"timestamp": r["timestamp"], "price": r[price_field]} for r in records if price_field in r]
        if candidates:
            price_path = candidates
            if candidate_key == "binance_futures_kline":
                candles_out = [
                    {"timestamp": r["timestamp"], "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"]}
                    for r in records
                ]
            break

    timeline = build_timeline(
        strategy, concept_infos, microsystem_infos, records_by_key,
        concept_requirements, microsystem_requirements, start_ts, end_ts, cadence_seconds,
    )

    trade_log: list[dict[str, object]] = []
    execution_entry = strategy.get("execution")
    if execution_entry is not None and price_path:
        execution_info = execution_infos.get(execution_entry["execution_id"])
        if execution_info is not None:
            management_entry = strategy.get("management")
            management_info = management_infos.get(management_entry["management_id"]) if management_entry else None
            filter_entry = strategy.get("filter")
            filter_info = filter_infos.get(filter_entry["filter_id"]) if filter_entry else None
            execution_fixed = resolve_config(execution_info.config_schema, execution_entry.get("config") or {})
            management_fixed = (
                resolve_config(management_info.config_schema, (management_entry or {}).get("config") or {})
                if management_info is not None else {}
            )
            filter_fixed = (
                resolve_config(filter_info.config_schema, (filter_entry or {}).get("config") or {})
                if filter_info is not None else {}
            )
            detailed = simulate_combo(
                timeline, price_path, execution_info, execution_fixed,
                management_info, management_fixed, filter_info, filter_fixed, detail=True,
            )
            trade_log = detailed["trade_log"]

    return {
        "price_path": price_path,
        "candles": candles_out,
        "timeline": [
            {"timestamp": step.timestamp, "concepts": step.concept_outputs, "microsystems": step.microsystem_outputs}
            for step in timeline
        ],
        "trades": trade_log,
        "evaluation_steps": len(timeline),
    }


__all__ = [
    "build_timeline", "estimate_warmup_seconds", "expand_numeric_range", "expand_sweep", "normalize_direction",
    "required_concrete_keys", "run_backtest", "run_example_scenario", "simulate_combo", "TimelineStep",
]
