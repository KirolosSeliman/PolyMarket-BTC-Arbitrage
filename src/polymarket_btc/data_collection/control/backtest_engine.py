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

from .backtest_data import read_records
from .concepts import ConceptContext, ConceptInfo, discover_concepts
from .config_schema import resolve_config
from .execution import ExecutionContext, ExecutionInfo, discover_execution_profiles
from .management import ManagementContext, ManagementInfo, discover_management_profiles
from .microsystems import MicrosystemContext, MicrosystemInfo, discover_microsystems


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


def _resolve_lookback_seconds(info: object, config: dict) -> float | None:
    """The trailing-history window (seconds) a concept/microsystem instance
    actually needs, per its own optional required_lookback_seconds(config)
    -- None (the default, for any script that doesn't define the function)
    means unbounded: today's behavior, context.data[key] is every record
    accumulated since the backtest's own start_ts. A script that opts in
    gets, per instance, only its declared trailing window no matter how far
    the walk has progressed -- the concrete fix for a concept like fvg.py
    that reconstructs candles from scratch on every evaluation step: without
    a bound, that rebuild re-scans the *entire* accumulated history so far
    (confirmed the dominant cost in a slow backtest by reading
    concepts/fvg.py's _build_candles), growing for the whole walk; with a
    bound, it only ever sees its own declared window -- identical final
    result, since anything older was always going to be discarded by the
    concept's own lookback trimming anyway. An invalid/failing resolver
    degrades to unbounded (never a crash, never a silently wrong window)."""
    resolver = getattr(info, "required_lookback_seconds", None)
    if resolver is None:
        return None
    try:
        value = resolver(config)
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
    concept_lookback = {
        entry["instance_id"]: _resolve_lookback_seconds(concept_infos[entry["concept_id"]], entry.get("config") or {})
        for entry in concept_entries
    }
    microsystem_lookback = {
        entry["instance_id"]: _resolve_lookback_seconds(
            microsystem_infos[entry["microsystem_id"]], entry.get("config") or {},
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

    t = start_ts
    while t <= end_ts:
        for key, records in records_by_key.items():
            cursor = cursors[key]
            while cursor < len(records) and records[cursor]["timestamp"] <= t:
                cursor += 1
            if cursor != cursors[key]:
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
    detail: bool = False,
) -> dict[str, object]:
    """Replays execution -> management -> trade-outcome against a
    precomputed timeline for one parameter combination. Cheap by design --
    no concept/microsystem work happens here. `detail=True` additionally
    records each trade's own entry/exit for the "voir les trades pris"
    replay view -- falls straight out of the same loop that already tracks
    open_trade/win-loss, just keeping the record instead of only counting
    it, so it costs nothing extra to compute (only to carry in the return
    value) and is only requested for the one combo a user can actually
    inspect (see run_backtest), never for every combo in a sweep."""
    price_cursor = 0
    open_trade: dict[str, object] | None = None
    trades = 0
    wins = 0
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

        exec_context = ExecutionContext(microsystems=step.microsystem_outputs, config=execution_config, log=_noop_log)
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
            if management_info is not None:
                mgmt_context = ManagementContext(
                    execution=execution_output, microsystems=step.microsystem_outputs,
                    config=management_config, log=_noop_log,
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
            open_trade = {
                "direction": direction, "entry_time": step.timestamp, "entry_price": price,
                "stop_loss": stop_loss, "take_profit": take_profit,
            }

    if open_trade is not None and last_price is not None:
        won = (
            last_price > open_trade["entry_price"] if open_trade["direction"] == "long"
            else last_price < open_trade["entry_price"]
        )
        close(last_ts, last_price, "win" if won else "loss")

    result: dict[str, object] = {"trades": trades, "wins": wins, "win_rate": (wins / trades) if trades else None}
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
) -> None:
    _worker_state["timeline"] = timeline
    _worker_state["price_path"] = price_path
    _worker_state["execution_dir"] = execution_dir
    _worker_state["management_dir"] = management_dir


def _run_combo_in_worker(
    execution_id: str, execution_config: dict, management_id: str | None, management_config: dict,
) -> dict[str, object]:
    execution_info = next(
        info for info in discover_execution_profiles(_worker_state["execution_dir"]) if info.id == execution_id
    )
    management_info = None
    if management_id is not None:
        management_info = next(
            info for info in discover_management_profiles(_worker_state["management_dir"]) if info.id == management_id
        )
    result = simulate_combo(
        _worker_state["timeline"], _worker_state["price_path"],
        execution_info, execution_config, management_info, management_config,
    )
    return {"execution_config": execution_config, "management_config": management_config, **result}


def run_backtest(
    *,
    strategy: dict,
    concepts_dir: Path,
    microsystems_dir: Path,
    execution_dir: Path,
    management_dir: Path,
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
    max_workers: int | None = None,
) -> dict[str, object]:
    concept_infos = {info.id: info for info in discover_concepts(concepts_dir)}
    microsystem_infos = {info.id: info for info in discover_microsystems(microsystems_dir)}
    execution_infos = {info.id: info for info in discover_execution_profiles(execution_dir)}
    management_infos = {info.id: info for info in discover_management_profiles(management_dir)}

    execution_entry = strategy.get("execution")
    if execution_entry is None:
        raise ValueError("strategy has no execution profile -- nothing to backtest")
    execution_info = execution_infos.get(execution_entry["execution_id"])
    if execution_info is None:
        raise ValueError(f"unknown execution_id: {execution_entry['execution_id']!r}")

    management_entry = strategy.get("management")
    management_info = management_infos.get(management_entry["management_id"]) if management_entry else None

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
            break
    if not price_path:
        raise ValueError(
            "aucune donnée de prix disponible pour l'actif simulé sur cette période -- "
            "aucune collecte ne couvre les trades, les bougies ou le mark price de cet instrument"
        )

    timeline = build_timeline(
        strategy, concept_infos, microsystem_infos, records_by_key,
        concept_requirements, microsystem_requirements, start_ts, end_ts, cadence_seconds,
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
        )
        results.append({**combo, **result})
    else:
        with ProcessPoolExecutor(
            max_workers=max_workers, initializer=_init_worker,
            initargs=(timeline, price_path, execution_dir, management_dir),
        ) as pool:
            futures = {
                pool.submit(
                    _run_combo_in_worker, execution_info.id, combo["execution_config"],
                    management_info.id if management_info else None, combo["management_config"],
                ): combo
                for combo in combos
            }
            for future in as_completed(futures):
                results.append(future.result())

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
            management_info, best["management_config"], detail=True,
        )
        replay = {
            "price_path": price_path,
            "timeline": [
                {"timestamp": step.timestamp, "concepts": step.concept_outputs, "microsystems": step.microsystem_outputs}
                for step in timeline
            ],
            "trades": detailed["trade_log"],
        }

    return {"results": results, "best": best, "evaluation_steps": len(timeline), "replay": replay}


__all__ = [
    "build_timeline", "expand_numeric_range", "expand_sweep", "normalize_direction",
    "required_concrete_keys", "run_backtest", "simulate_combo", "TimelineStep",
]
