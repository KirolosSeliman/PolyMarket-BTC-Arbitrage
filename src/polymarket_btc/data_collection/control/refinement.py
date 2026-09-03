"""Shared machinery for every "Perfectionner" (human-labeling refinement)
feature in this app -- concepts, microsystems, and (later) strategy
filters all follow the identical shape: scan real collected data/replays
for moments something was detected, show one at a time for a human
Oui/Non judgment (Non requires an explanation), and once enough judgment
has accumulated (MIN_TOTAL_FOR_PROMPT/MIN_NO_FOR_PROMPT), build a prompt
embedding the current script plus every "Non" explanation for an external
AI to revise -- never a live API call (see concept_refinement.py's own
module docstring for why: a deliberate, considered choice, not a
fallback).

This module owns everything that's genuinely identical across those
consumers: storage I/O (labels.jsonl append-only, pool_cache.json
derived/disposable -- see concept_refinement.py's docstring for the exact
storage contract, unchanged by this extraction), the background-job/
polling wrapper (mirrors backtest_jobs.BacktestJobManager), the
progress/gating/prompt-building logic, and the shape-based duck-typing
helpers a candidate is found and identified through (ported from
strategy_builder.html's previewWalkForAnnotations/previewLooksLikeZone/
previewLooksLikeLevel). Each consumer (ConceptRefinementManager,
MicrosystemRefinementManager, ...) owns only what genuinely differs: what
compute() needs to run, and what counts as one candidate.

Four candidate shapes are recognized:
  "zone"  -- direction+high+low+formed_at (e.g. an FVG).
  "level" -- level+(formed_at or swept_at) (e.g. a liquidity pool/sweep).
  "setup" -- a compound node containing 2+ zone/level sub-nodes (e.g. a
             microsystem's own detected pattern, tying several zones/
             levels together into one coherent thing to judge). Judging a
             whole setup is the point for a microsystem -- its value is
             the *relationship* between its inputs, not any one zone
             alone, which concept-level refinement already covers.
  "trade" -- a resolved trade record (entry_time+direction+...) from a
             strategy's own real-data replay (see
             strategy_filter_refinement.py). Unlike the other three,
             never found via walk_for_annotations/find_setup_candidates --
             a trade comes directly from run_backtest's own trade_log, so
             only instance_key's own identity (entry_time+direction,
             already unique within one strategy's deterministic replay)
             needs to know this shape exists.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import time
import uuid

MIN_TOTAL_FOR_PROMPT = 10
MIN_NO_FOR_PROMPT = 1
# Trailing record count fed to compute() at each scan step -- deliberately
# NOT a concept/microsystem's own required_lookback_seconds: measured
# against the one real usable manifest today (28,800 BTCUSDT 1-minute
# klines), an unbounded scan (mirroring real-backtest semantics) extrapolates
# to ~19 minutes; this bounded window keeps a full scan to ~90-115s -- still
# background-job territory, but not "may as well be unbounded."
SCAN_WINDOW_RECORDS = 1500
# How much real context around a candidate's own timestamp next_instance's
# display window covers -- same reasoning already applied this session to
# strategy_builder.html's PREVIEW_MAX_FORMATION_PULLBACK_CANDLES: enough to
# see the setup and its resolution, not so much the candidate itself is a
# couple of invisible pixels. Expressed in seconds (not "N candles") to
# avoid needing to generically detect candle granularity up front.
NEXT_WINDOW_BEFORE_SECONDS = 40 * 60
NEXT_WINDOW_AFTER_SECONDS = 20 * 60
NEXT_WINDOW_MAX_RECORDS = 300


def noop_log(_message: str) -> None:
    pass


def is_num(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def looks_like_zone(node: object) -> bool:
    """Python port of strategy_builder.html's previewLooksLikeZone -- same
    shape-based duck typing, no concept-specific field names."""
    return (
        isinstance(node, dict) and node.get("direction") in ("bullish", "bearish")
        and is_num(node.get("high")) and is_num(node.get("low")) and node["high"] > node["low"]
        and is_num(node.get("formed_at"))
    )


def looks_like_level(node: object) -> bool:
    """Python port of previewLooksLikeLevel."""
    return (
        isinstance(node, dict) and is_num(node.get("level"))
        and (is_num(node.get("formed_at")) or is_num(node.get("swept_at")))
    )


def looks_like_setup_entry(node: object) -> bool:
    """A dict that is not itself zone/level-shaped, but directly contains
    2+ zone/level-shaped sub-fields -- the generic (not hardcoded to any
    one microsystem's field names) signature of "this is a compound
    setup," e.g. {"initial_fvg": {...zone...}, "sweep": {...level...},
    "reversal_fvg": {...zone...}, "detected_at": ...}. Requiring 2+ rules
    out a dict that merely happens to *contain* one zone/level among
    unrelated fields."""
    if not isinstance(node, dict) or looks_like_zone(node) or looks_like_level(node):
        return False
    sub_hits = sum(1 for value in node.values() if looks_like_zone(value) or looks_like_level(value))
    return sub_hits >= 2


def walk_for_annotations(value: object, depth: int = 0, found: list[tuple[str, dict]] | None = None) -> list[tuple[str, dict]]:
    """Python port of previewWalkForAnnotations -- recursively walks any
    nested dict/list (depth-capped, same limit as the JS original) and
    classifies each object node as a "zone" or "level" purely by shape,
    regardless of which concept/microsystem produced it or what its
    output's top-level keys are named."""
    if found is None:
        found = []
    if depth > 6 or not isinstance(value, (dict, list)):
        return found
    if isinstance(value, list):
        for item in value:
            walk_for_annotations(item, depth + 1, found)
    else:
        if looks_like_zone(value):
            found.append(("zone", value))
        elif looks_like_level(value):
            found.append(("level", value))
        for item in value.values():
            walk_for_annotations(item, depth + 1, found)
    return found


def find_setup_candidates(value: object, depth: int = 0, found: list[dict] | None = None) -> list[dict]:
    """Depth-capped walk collecting whole dicts matching
    looks_like_setup_entry. Unlike walk_for_annotations, deliberately does
    NOT recurse into a matched setup's own sub-fields once found (those
    are the setup's own zone/level parts, not further setups) -- but does
    keep walking sibling branches for more setup entries elsewhere in the
    tree (e.g. every entry in a "setups": [...] array)."""
    if found is None:
        found = []
    if depth > 6 or not isinstance(value, (dict, list)):
        return found
    if isinstance(value, list):
        for item in value:
            find_setup_candidates(item, depth + 1, found)
    else:
        if looks_like_setup_entry(value):
            found.append(value)
            return found
        for item in value.values():
            find_setup_candidates(item, depth + 1, found)
    return found


def trigger_timestamp(shape: str, node: dict) -> float | None:
    """The timestamp at which `node` first became "the thing to show."
    For a "level," when both are present, swept_at (when it actually
    resolved) is preferred over formed_at (when the underlying pool merely
    formed, which can be hundreds of candles earlier and, on its own,
    would mean a sweep is never surfaced as a candidate at all, only the
    pool that eventually got swept). A "zone" only ever carries formed_at.
    A "setup" (a compound node) triggers on the LATEST of its own
    sub-nodes' own trigger timestamps -- the setup as a whole only
    becomes "the thing to show" once every part of it has actually
    happened, not as soon as its earliest piece (e.g. the initial FVG)
    forms."""
    if shape == "setup":
        sub_timestamps = [
            ts for sub_shape, sub_node in walk_for_annotations(node)
            for ts in (trigger_timestamp(sub_shape, sub_node),) if ts is not None
        ]
        return max(sub_timestamps) if sub_timestamps else None
    if shape == "level" and is_num(node.get("swept_at")):
        return node["swept_at"]
    formed_at = node.get("formed_at")
    return formed_at if is_num(formed_at) else None


def round_value(value: object, ndigits: int = 8) -> object:
    if is_num(value):
        return round(float(value), ndigits)
    return value


def _node_identity(shape: str, node: dict) -> dict[str, object]:
    if shape == "zone":
        return {
            "direction": node.get("direction"), "formed_at": round_value(node.get("formed_at")),
            "high": round_value(node.get("high")), "low": round_value(node.get("low")),
        }
    if shape == "trade":
        return {"entry_time": round_value(node.get("entry_time")), "direction": node.get("direction")}
    return {
        "direction_or_side": node.get("direction", node.get("side")),
        "level": round_value(node.get("level")),
        "formed_at": round_value(node.get("formed_at")), "swept_at": round_value(node.get("swept_at")),
    }


def instance_key(owner_id: str, shape: str, node: dict) -> str:
    """Stable identity for "this specific real chart moment," used to
    never show (or re-count) the same instance twice. Keyed only on the
    fields the duck-typing predicates already require -- deliberately
    excludes volatile fields (fill_pct/status/...) that legitimately
    change as later candles arrive after formation, and would otherwise
    make the same real instance look new on every rescan. A "setup"'s
    identity is the sorted set of its own sub-nodes' identities (sorted so
    the key is invariant to which order the walk happened to find them
    in, not just which order they appear as dict keys)."""
    if shape == "setup":
        sub_identities = sorted(
            json.dumps({"shape": sub_shape, **_node_identity(sub_shape, sub_node)}, sort_keys=True, default=str)
            for sub_shape, sub_node in walk_for_annotations(node)
        )
        identity: dict[str, object] = {"sub_nodes": sub_identities}
    else:
        identity = _node_identity(shape, node)
    payload = {"owner_id": owner_id, "shape": shape, **identity}
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def key_instrument(key: str) -> str:
    """"binance_futures_kline" -> "BTCUSDT", "binance_futures_kline:ETH" ->
    "ETHUSDT" -- the compound-key convention backtest_data.py's
    price_path_coverage already documents, inverted. `key` itself (with any
    ":ASSET" suffix intact) is what read_records/key_coverage need, this
    only extracts the instrument to pass alongside it."""
    _base, _, asset = key.partition(":")
    return f"{(asset or 'BTC').upper()}USDT"


def iso_to_ts(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


# ============================================================
# Storage -- labels.jsonl (precious, append-only) + pool_cache.json
# (derived, disposable). See module docstring for the storage contract.
# ============================================================

def labels_path(feedback_dir: Path, owner_id: str) -> Path:
    return feedback_dir / owner_id / "labels.jsonl"


def pool_cache_path(feedback_dir: Path, owner_id: str) -> Path:
    return feedback_dir / owner_id / "pool_cache.json"


def auto_refine_marker_path(feedback_dir: Path, owner_id: str) -> Path:
    return feedback_dir / owner_id / "auto_refine_triggered.marker"


def try_claim_auto_refine(feedback_dir: Path, owner_id: str) -> bool:
    """True exactly once per owner_id -- creates the marker file (O_EXCL)
    and returns True the first time; a later call while it still exists
    returns False. The single asyncio event loop with no `await` inside a
    label handler already makes this race-free in practice; O_EXCL is
    cheap insurance, not a fix for a real race."""
    path = auto_refine_marker_path(feedback_dir, owner_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.open("x", encoding="utf-8").close()
        return True
    except FileExistsError:
        return False


def release_auto_refine_claim(feedback_dir: Path, owner_id: str) -> None:
    """Called when a claimed auto-refine job fails (Claude Code error,
    timeout, unparseable response) -- undoes try_claim_auto_refine so the
    next label (the eligibility threshold is already met, so this is
    immediate) retries once more, instead of a transient failure silently
    disabling the one-shot trigger forever."""
    auto_refine_marker_path(feedback_dir, owner_id).unlink(missing_ok=True)


# One fixed, generic scenario shared by concept/microsystem (and later
# filter) refinement's own "generate a synthetic instance" feature -- a
# tight oscillation (a "range") for _RANGE_STEPS steps, then a single
# sharp move with much higher volume (a "breakout"), the same textbook
# shape validated by hand against concepts/range_breakout.py. Shaped per
# source per backtest_data.py's own _ACCESS_EXTRACTORS/_COLLECT_
# EXTRACTORS (the real field names concepts/microsystems actually
# receive) -- a source this app doesn't know how to read for real is
# simply not included in the result, matching how a concept declaring an
# unsupported source already gets nothing for it in real scans.
_BASE_PRICE = 100.0
_OSCILLATION = 0.02
_BREAKOUT_PRICE = 103.0
_RANGE_STEPS = 60
_STEP_SECONDS = 60.0


def _synthetic_kline_series() -> list[dict[str, object]]:
    candles = []
    t = 0.0
    for i in range(_RANGE_STEPS):
        o = _BASE_PRICE + (_OSCILLATION if i % 2 == 0 else -_OSCILLATION)
        c = _BASE_PRICE + (-_OSCILLATION if i % 2 == 0 else _OSCILLATION)
        candles.append({
            "open": o, "high": max(o, c) + 0.01, "low": min(o, c) - 0.01, "close": c,
            "volume": 1.0, "timestamp": t + _STEP_SECONDS, "open_time": t,
            "close_time": t + _STEP_SECONDS, "is_closed": True,
        })
        t += _STEP_SECONDS
    candles.append({
        "open": _BASE_PRICE, "high": _BREAKOUT_PRICE + 0.5, "low": _BASE_PRICE - 0.1, "close": _BREAKOUT_PRICE,
        "volume": 50.0, "timestamp": t + _STEP_SECONDS, "open_time": t,
        "close_time": t + _STEP_SECONDS, "is_closed": True,
    })
    return candles


def _synthetic_trade_series() -> list[dict[str, object]]:
    trades = []
    t = 0.0
    for i in range(_RANGE_STEPS):
        price = _BASE_PRICE + (_OSCILLATION if i % 2 == 0 else -_OSCILLATION)
        trades.append({"price": price, "quantity": 1.0, "timestamp": t, "taker_side": "buy"})
        t += _STEP_SECONDS
    trades.append({"price": _BREAKOUT_PRICE, "quantity": 50.0, "timestamp": t, "taker_side": "buy"})
    return trades


def _synthetic_mark_price_series() -> list[dict[str, object]]:
    records = []
    t = 0.0
    for i in range(_RANGE_STEPS):
        price = _BASE_PRICE + (_OSCILLATION if i % 2 == 0 else -_OSCILLATION)
        records.append({"mark_price": price, "index_price": price, "funding_rate": 0.0001, "timestamp": t})
        t += _STEP_SECONDS
    records.append({
        "mark_price": _BREAKOUT_PRICE, "index_price": _BREAKOUT_PRICE, "funding_rate": 0.0005, "timestamp": t,
    })
    return records


def _synthetic_price_series() -> list[dict[str, object]]:
    records = []
    t = 0.0
    for i in range(_RANGE_STEPS):
        price = _BASE_PRICE + (_OSCILLATION if i % 2 == 0 else -_OSCILLATION)
        records.append({"price": price, "timestamp": t})
        t += _STEP_SECONDS
    records.append({"price": _BREAKOUT_PRICE, "timestamp": t})
    return records


_SYNTHETIC_SERIES_BUILDERS: dict[str, object] = {
    "binance_futures_kline": _synthetic_kline_series,
    "binance_futures_trade": _synthetic_trade_series,
    "binance_futures_mark_price": _synthetic_mark_price_series,
    "chainlink": _synthetic_price_series,
    "binance_spot": _synthetic_price_series,
}


def build_synthetic_candle_set(data_sources: list[str]) -> dict[str, list[dict[str, object]]]:
    result: dict[str, list[dict[str, object]]] = {}
    for key in data_sources:
        builder = _SYNTHETIC_SERIES_BUILDERS.get(key.partition(":")[0])
        if builder is not None:
            result[key] = builder()
    return result


def read_labels(feedback_dir: Path, owner_id: str) -> list[dict[str, object]]:
    path = labels_path(feedback_dir, owner_id)
    if not path.is_file():
        return []
    labels: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            continue  # tolerate a torn last line, same philosophy as StrategyManager.list_strategies
        if isinstance(entry, dict):
            labels.append(entry)
    return labels


def empty_pool_cache() -> dict[str, object]:
    return {"script_sha256": None, "scanned_through": {}, "candidates": []}


def load_pool_cache(feedback_dir: Path, owner_id: str) -> dict[str, object]:
    path = pool_cache_path(feedback_dir, owner_id)
    if not path.is_file():
        return empty_pool_cache()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return empty_pool_cache()
    if not isinstance(payload, dict):
        return empty_pool_cache()
    payload.setdefault("scanned_through", {})
    payload.setdefault("candidates", [])
    return payload


def write_pool_cache(feedback_dir: Path, owner_id: str, cache: dict[str, object]) -> None:
    path = pool_cache_path(feedback_dir, owner_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def append_label(
    feedback_dir: Path, owner_id: str, *, shape: str, node: dict, label: str, note: str = "",
    trigger_ts: float | None = None,
) -> dict[str, object]:
    """Appends one Oui/Non judgment -- append-only, see module docstring
    for why. A "non" always requires a non-empty note (the UI enforces
    this too, via a required textarea, but a script/API caller must not
    be able to bypass it -- a "non" with no reason is useless for
    build_prompt, which exists specifically to surface those reasons)."""
    if shape not in ("zone", "level", "setup", "trade"):
        raise ValueError(f"shape must be 'zone', 'level', 'setup', or 'trade', got {shape!r}")
    if not isinstance(node, dict):
        raise ValueError("node must be an object")
    if label not in ("oui", "non"):
        raise ValueError(f"label must be 'oui' or 'non', got {label!r}")
    note = (note or "").strip()
    if label == "non" and not note:
        raise ValueError("a 'non' label requires a non-empty note explaining the missed nuance")
    entry: dict[str, object] = {
        "labeled_at_utc": datetime.now(UTC).isoformat(),
        "instance_key": instance_key(owner_id, shape, node),
        "shape": shape, "node": node, "trigger_ts": trigger_ts, "label": label,
    }
    if label == "non":
        entry["note"] = note
    path = labels_path(feedback_dir, owner_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    return progress(feedback_dir, owner_id)


def progress(feedback_dir: Path, owner_id: str) -> dict[str, object]:
    labels = read_labels(feedback_dir, owner_id)
    no_count = sum(1 for entry in labels if entry.get("label") == "non")
    total = len(labels)
    return {
        "total": total, "no_count": no_count,
        "min_total": MIN_TOTAL_FOR_PROMPT, "min_no": MIN_NO_FOR_PROMPT,
        "eligible_for_prompt": total >= MIN_TOTAL_FOR_PROMPT and no_count >= MIN_NO_FOR_PROMPT,
    }


def build_prompt(feedback_dir: Path, owner_id: str, *, template: str, source: str) -> str:
    """Raises ValueError below the gate (see MIN_TOTAL_FOR_PROMPT/
    MIN_NO_FOR_PROMPT) -- an example with no disagreement has nothing for
    an AI to fix. Once eligible, embeds `source` (the current script
    content -- supplied by the caller, since concept/microsystem/filter
    each read their own source differently, e.g. CollectionRunManager.
    read_concept_source vs. read_microsystem_source) plus EVERY "non"
    example's note verbatim (never a sample/subset -- the whole point is
    the AI seeing every nuance that's been flagged so far, whether there's
    1 or 20+)."""
    labels = read_labels(feedback_dir, owner_id)
    no_entries = [entry for entry in labels if entry.get("label") == "non"]
    total = len(labels)
    if total < MIN_TOTAL_FOR_PROMPT or len(no_entries) < MIN_NO_FOR_PROMPT:
        raise ValueError(
            f"il faut au moins {MIN_TOTAL_FOR_PROMPT} exemples annotés dont au moins "
            f"{MIN_NO_FOR_PROMPT} désaccord ('non') pour générer un prompt de perfectionnement -- "
            f"actuellement : {total} exemple(s) annoté(s), {len(no_entries)} désaccord(s)"
        )
    sections = [
        template,
        "\n\n---\n\n## Contexte : script actuel à perfectionner\n\n",
        f"```python\n{source}\n```\n",
        "\n---\n\n## Retours de l'utilisateur sur des exemples réels\n\n",
        (
            f"Sur {total} exemples réels annotés, l'utilisateur a validé "
            f"{total - len(no_entries)} comme corrects et rejeté {len(no_entries)} comme incorrects. "
            "Voici CHAQUE cas rejeté avec l'explication de ce qui n'allait pas ou quelle nuance a été "
            "manquée :\n"
        ),
    ]
    for i, entry in enumerate(no_entries, start=1):
        node = entry.get("node") or {}
        sections.append(
            f"\n### Exemple rejeté {i}\n"
            f"- Ce que le script avait détecté : `{json.dumps(node, ensure_ascii=False, default=str)}`\n"
            f"- Pourquoi ce n'est pas correct : {entry.get('note', '')}\n"
        )
    sections.append(
        "\nAméliore le script ci-dessus pour qu'il tienne compte de ces nuances, sans casser les "
        "cas déjà validés comme corrects.\n"
    )
    return "".join(sections)


# ============================================================
# Background job / polling -- mirrors backtest_jobs.BacktestJobManager.
# ============================================================

@dataclass(slots=True)
class ScanJob:
    job_id: str
    started_at_ns: int
    task: asyncio.Task | None = None
    ended_at_ns: int | None = None
    result: dict[str, object] | None = None
    error: str | None = None
    progress: float = 0.0


def run_scan_job(
    jobs: dict[str, ScanJob], scan_fn: Callable[[Callable[[float], None] | None], dict[str, object]],
    *, name_prefix: str = "scan-job",
) -> ScanJob:
    """Backgrounds scan_fn(on_progress) via loop.run_in_executor, polled
    via scan_job_status -- mirrors backtest_jobs.BacktestJobManager
    exactly, for the same reason: a real-data scan is far slower than the
    server's own request timeout (see concept_refinement.py's module
    docstring). Generic over WHAT scan_fn actually scans (a concept, a
    microsystem, a strategy's real trades, ...) -- callers close over
    their own id via a lambda."""
    job_id = uuid.uuid4().hex
    job = ScanJob(job_id=job_id, started_at_ns=time.time_ns())
    loop = asyncio.get_event_loop()

    def on_progress(fraction: float) -> None:
        job.progress = fraction

    async def runner() -> None:
        try:
            job.result = await loop.run_in_executor(None, lambda: scan_fn(on_progress))
        except Exception as exc:
            job.error = str(exc)
        finally:
            job.ended_at_ns = time.time_ns()
            job.progress = 1.0

    job.task = asyncio.create_task(runner(), name=f"{name_prefix}-{job_id}")
    jobs[job_id] = job
    return job


def scan_job_status(jobs: dict[str, ScanJob], job_id: str) -> dict[str, object] | None:
    job = jobs.get(job_id)
    if job is None:
        return None
    end_ns = job.ended_at_ns if job.ended_at_ns is not None else time.time_ns()
    return {
        "job_id": job.job_id,
        "done": job.ended_at_ns is not None,
        "elapsed_seconds": (end_ns - job.started_at_ns) / 1_000_000_000,
        "progress": job.progress,
        "error": job.error,
        "result": job.result,
    }


__all__ = [
    "MIN_NO_FOR_PROMPT", "MIN_TOTAL_FOR_PROMPT", "NEXT_WINDOW_AFTER_SECONDS",
    "NEXT_WINDOW_BEFORE_SECONDS", "NEXT_WINDOW_MAX_RECORDS", "SCAN_WINDOW_RECORDS",
    "ScanJob", "append_label", "auto_refine_marker_path", "build_prompt", "build_synthetic_candle_set",
    "empty_pool_cache", "find_setup_candidates", "instance_key", "iso_to_ts", "key_instrument", "labels_path",
    "load_pool_cache", "looks_like_level", "looks_like_setup_entry", "looks_like_zone", "noop_log",
    "pool_cache_path", "progress", "read_labels", "release_auto_refine_claim", "round_value",
    "run_scan_job", "scan_job_status", "trigger_timestamp", "try_claim_auto_refine",
    "walk_for_annotations", "write_pool_cache",
]
