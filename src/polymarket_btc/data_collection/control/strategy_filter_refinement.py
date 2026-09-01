"""Strategy-filter refinement ("Perfectionner un filtre de stratégie"):
human labeling of real trades a strategy's own concept/microsystem/
execution/management pipeline would take, accumulated into a prompt an
external AI can act on to build or improve the strategy's own filter --
never a live API call, same mechanism and reasoning as concept/microsystem
refinement (see refinement.py's own module docstring).

Unlike concept/microsystem refinement (which scan for zone/level/setup
detections inside one script's own output), a filter judges *trades*:
"would this trade have been taken" is inherently a full-strategy question
(concepts -> microsystems -> execution -> management all wired), so this
reuses run_backtest directly instead of a bespoke per-step scan loop --
run_backtest's existing single-combo fast path (no sweep, no process pool)
already returns exactly the needed shape (result["replay"]["trades"], full
trade_log with human-readable execution_log/management_log attached).

The one load-bearing detail: scan() always forces the strategy's own
filter field to None before replaying, regardless of what's currently
saved -- otherwise a trade the filter already vetoes would never appear as
a candidate, and a human could never confirm what an existing filter is
already correctly doing, or build one from nothing.

Keyed by strategy *name*, not filter_id -- the entry point lives on that
strategy's own Step 6 in the builder, matching the "one filter per
strategy in practice" usage pattern, and avoids inventing cross-strategy
pool merging nobody asked for. A candidate's identity is its own
entry_time+direction (refinement.py's "trade" shape) -- already unique
within one strategy's own deterministic real-data replay, no zone/level
hashing needed. No incremental *partial* rescan (unlike concept/
microsystem refinement's own scanned_through tracking) -- re-running the
full-strategy backtest in full each scan is simple and correct (candidates
already seen are deduplicated by instance_key regardless), and this still
runs as a background job like every other real-data scan in this app. It
*does* skip the backtest entirely, though, when the strategy definition
and data coverage are both byte-identical to the last scan (see
_scan_is_stale below) -- a full-strategy replay is deterministic, so two
scans of the exact same inputs would always produce the exact same
candidates; without this, re-clicking "Perfectionner" during a review
session (nothing about the strategy or the data changed) paid the full
replay cost again for zero new information.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import random

from . import refinement
from .backtest_data import read_records
from .backtest_engine import estimate_warmup_seconds, required_concrete_keys, run_backtest
from .concept_generation import auto_suffixed_filename, generate_concept_via_claude_code
from .concepts import discover_concepts
from .microsystems import discover_microsystems
from .runs import CollectionRunManager
from .strategies import StrategyManager

DEFAULT_CADENCE_SECONDS = 60.0


@dataclass(slots=True)
class StrategyFilterRefinementManager:
    feedback_dir: Path
    runs: CollectionRunManager
    strategies: StrategyManager
    jobs: dict[str, refinement.ScanJob] = field(default_factory=dict)
    claude_code_command: list[str] | None = None
    claude_code_timeout_seconds: float = 600.0

    def scan(
        self, strategy_name: str, *, on_progress: Callable[[float], None] | None = None,
    ) -> dict[str, object]:
        """Replays the strategy (filter forced off) over its own full real
        data coverage, in one combo (no sweep), and records every trade not
        already seen (by refinement.instance_key) as a new "trade"-shaped
        candidate. Raises ValueError if the strategy is unknown, has no
        execution/management wired yet (nothing to replay), or has no real
        data coverage at all."""
        strategy = self.strategies.load_strategy(strategy_name)
        if strategy is None:
            raise ValueError(f"unknown strategy: {strategy_name!r}")
        eligibility = self.strategies.backtest_eligibility(strategy_name)
        if eligibility is None:
            raise ValueError(f"unknown strategy: {strategy_name!r}")
        if eligibility["missing_execution"] or eligibility["missing_management"]:
            raise ValueError(
                f"la stratégie {strategy_name!r} doit avoir un profil d'exécution et de gestion "
                "avant de pouvoir perfectionner son filtre"
            )
        coverage = eligibility["coverage"]
        if not coverage:
            raise ValueError(
                "aucune donnée réelle disponible pour cette stratégie -- limité par : "
                + (eligibility["narrowest_key"] or ", ".join(eligibility["required_data_keys"]))
            )

        # Forced None regardless of what's saved -- see module docstring.
        # created_at_utc/updated_at_utc are excluded from the hash too --
        # save_strategy stamps a fresh updated_at_utc on every save
        # regardless of whether concepts/microsystems/execution/management
        # actually changed, which would otherwise invalidate this cache
        # (forcing a full real-data replay again) on a no-op resave.
        unfiltered_strategy = {
            k: v for k, v in strategy.items() if k not in ("created_at_utc", "updated_at_utc")
        }
        unfiltered_strategy["filter"] = None

        script_hash = hashlib.sha256(
            json.dumps(unfiltered_strategy, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        # Canonical string, not the raw list/tuple value: coverage round-
        # trips through JSON in the cache file, which turns its tuples into
        # lists -- comparing the freshly computed value (still tuples)
        # against that with `==` would spuriously see a change every time.
        coverage_key = json.dumps(coverage, sort_keys=True, default=str)
        cache = refinement.load_pool_cache(self.feedback_dir, strategy_name)
        if cache.get("script_sha256") == script_hash and cache.get("coverage_key") == coverage_key:
            # Same strategy definition, same available data as last scan --
            # a full-strategy replay is deterministic, so re-running it
            # would find exactly the same trades already in the pool.
            return {"candidate_count": len(cache["candidates"]), "added": 0, "coverage": coverage}
        if cache.get("script_sha256") != script_hash:
            cache = refinement.empty_pool_cache()
        cache["script_sha256"] = script_hash

        start_ts = refinement.iso_to_ts(coverage[0][0])
        end_ts = refinement.iso_to_ts(coverage[-1][1])

        result = run_backtest(
            strategy=unfiltered_strategy,
            concepts_dir=self.runs.concepts_dir, microsystems_dir=self.runs.microsystems_dir,
            execution_dir=self.runs.execution_dir, management_dir=self.runs.management_dir,
            filter_dir=self.runs.filter_dir,
            data_requirements_for=self.runs._data_requirements_for,
            manifests=self.runs.list_runs(), instrument=eligibility["default_instrument"],
            start_ts=start_ts, end_ts=end_ts, cadence_seconds=DEFAULT_CADENCE_SECONDS,
            execution_sweep={}, management_sweep={},
            on_progress=on_progress,
        )
        trades = ((result.get("replay") or {}).get("trades")) or []

        seen = {c["instance_key"] for c in cache["candidates"]}
        new_candidates: list[dict[str, object]] = []
        for trade in trades:
            ikey = refinement.instance_key(strategy_name, "trade", trade)
            if ikey in seen:
                continue
            seen.add(ikey)
            new_candidates.append({
                "instance_key": ikey, "shape": "trade", "node": trade,
                "trigger_ts": trade.get("entry_time"),
            })

        cache["candidates"] = cache["candidates"] + new_candidates
        cache["coverage_key"] = coverage_key
        cache["scanned_at_utc"] = datetime.now(UTC).isoformat()
        refinement.write_pool_cache(self.feedback_dir, strategy_name, cache)
        if on_progress is not None:
            on_progress(1.0)
        return {"candidate_count": len(cache["candidates"]), "added": len(new_candidates), "coverage": coverage}

    def start_scan_job(self, *, strategy_name: str) -> refinement.ScanJob:
        return refinement.run_scan_job(
            self.jobs, lambda on_progress: self.scan(strategy_name, on_progress=on_progress),
            name_prefix="filter-scan-job",
        )

    def scan_job_status(self, job_id: str) -> dict[str, object] | None:
        return refinement.scan_job_status(self.jobs, job_id)

    def _instance_window(
        self, trade: dict, instrument: str, strategy: dict, coverage_start_ts: float,
    ) -> dict[str, object]:
        """A bounded window around one trade's own entry/exit: the real
        candles next_instance's chart already drew, plus (new) a bounded
        replay -- each concept/microsystem's own output leading up to the
        trade -- so a human labeling it can see *why* the strategy took it,
        not just the price action around it. Same intent as concept/
        microsystem refinement's own _instance_window, sized around
        [entry_time, exit_time] instead of a single node's own formed_at.

        The replay window starts far enough back for every instance's own
        required_lookback_seconds to have "warmed up" to what the original
        full scan (which is what actually found this trade) would have
        seen at that same moment -- coverage_start_ts is the fallback for
        any instance that never declared one (needs everything, same as
        the original scan; see estimate_warmup_seconds). The replay is
        best-effort: if it fails for any reason, the trade is still shown
        with its candles (via a plain narrow fallback read), just without
        the reasoning panel.

        Reads every required concrete key exactly once, over the widest
        range this review could ever need (coverage_start_ts through the
        trade's own exit+buffer), then slices in memory for both the
        bounded replay and the narrow display candles -- read_records'
        underlying raw storage has no seek/index, so a call's cost is
        dominated by decompressing/parsing a manifest's *entire* segment
        file regardless of how narrow [start_ts, end_ts] is (confirmed by
        profiling: the previous two-read version -- one narrow read here,
        one wider one inside run_backtest -- paid that full-file cost
        twice for the same key). A wider requested range costs the same
        as a narrower one under that constraint, so there's no penalty to
        reading generously up front and trimming afterward."""
        manifests = self.runs.list_runs()
        entry_time = trade.get("entry_time")
        exit_time = trade.get("exit_time", entry_time)
        earliest = min(entry_time, exit_time)
        latest = max(entry_time, exit_time)
        display_start_ts = earliest - refinement.NEXT_WINDOW_BEFORE_SECONDS
        display_end_ts = latest + refinement.NEXT_WINDOW_AFTER_SECONDS
        display_key = "binance_futures_kline"
        instrument_symbol = f"{instrument.upper()}USDT"

        replay = None
        display_records: list[dict] = []
        try:
            concept_infos = {info.id: info for info in discover_concepts(self.runs.concepts_dir)}
            microsystem_infos = {info.id: info for info in discover_microsystems(self.runs.microsystems_dir)}
            keys_to_fetch = required_concrete_keys(
                strategy, concept_infos, microsystem_infos, self.runs._data_requirements_for,
            ) | {display_key}

            fetch_start_ns, fetch_end_ns = int(coverage_start_ts * 1e9), int(display_end_ts * 1e9)
            all_records_by_key = {
                key: read_records(key, fetch_start_ns, fetch_end_ns, manifests, instrument=instrument_symbol)
                for key in keys_to_fetch
            }

            warmup_seconds = estimate_warmup_seconds(
                strategy, concept_infos, microsystem_infos, self.runs._data_requirements_for, all_records_by_key,
            )
            replay_start_ts = (
                coverage_start_ts if warmup_seconds is None
                else max(coverage_start_ts, earliest - warmup_seconds)
            )
            replay_records_by_key = {
                key: [r for r in records if r["timestamp"] >= replay_start_ts]
                for key, records in all_records_by_key.items()
            }

            unfiltered_strategy = {k: v for k, v in strategy.items() if k not in ("created_at_utc", "updated_at_utc")}
            unfiltered_strategy["filter"] = None
            result = run_backtest(
                strategy=unfiltered_strategy,
                concepts_dir=self.runs.concepts_dir, microsystems_dir=self.runs.microsystems_dir,
                execution_dir=self.runs.execution_dir, management_dir=self.runs.management_dir,
                filter_dir=self.runs.filter_dir,
                data_requirements_for=self.runs._data_requirements_for,
                manifests=manifests, instrument=instrument,
                start_ts=replay_start_ts, end_ts=display_end_ts,
                cadence_seconds=DEFAULT_CADENCE_SECONDS, execution_sweep={}, management_sweep={},
                records_by_key=replay_records_by_key,
            )
            replay = result.get("replay")

            display_records = [
                r for r in all_records_by_key.get(display_key, [])
                if display_start_ts <= r["timestamp"] <= display_end_ts
            ]
        except Exception:
            replay = None
            start_ns, end_ns = int(display_start_ts * 1e9), int(display_end_ts * 1e9)
            try:
                display_records = read_records(display_key, start_ns, end_ns, manifests, instrument=instrument_symbol)
            except Exception:
                display_records = []
        if len(display_records) > refinement.NEXT_WINDOW_MAX_RECORDS:
            display_records = display_records[-refinement.NEXT_WINDOW_MAX_RECORDS:]
        return {"key": display_key, "candles": display_records, "replay": replay}

    def next_instance(self, *, strategy_name: str) -> dict[str, object]:
        strategy = self.strategies.load_strategy(strategy_name)
        if strategy is None:
            raise ValueError(f"unknown strategy: {strategy_name!r}")
        cache = refinement.load_pool_cache(self.feedback_dir, strategy_name)
        candidates = cache.get("candidates") or []
        labeled_keys = {
            entry["instance_key"] for entry in refinement.read_labels(self.feedback_dir, strategy_name)
            if "instance_key" in entry
        }
        remaining = [c for c in candidates if c["instance_key"] not in labeled_keys]
        progress = self.progress(strategy_name=strategy_name)
        if not candidates:
            return {"instance": None, "exhausted": False, "no_candidates": True, "progress": progress}
        if not remaining:
            return {"instance": None, "exhausted": True, "no_candidates": False, "progress": progress}
        chosen = random.choice(remaining)
        eligibility = self.strategies.backtest_eligibility(strategy_name)
        coverage_start_ts = refinement.iso_to_ts(eligibility["coverage"][0][0])
        window = self._instance_window(chosen["node"], eligibility["default_instrument"], strategy, coverage_start_ts)
        return {
            "instance": {
                "shape": chosen["shape"], "node": chosen["node"], "trigger_ts": chosen["trigger_ts"],
                "window": window,
            },
            "exhausted": False, "no_candidates": False, "progress": progress,
        }

    def label(
        self, *, strategy_name: str, shape: str, node: dict, label: str, note: str = "",
        trigger_ts: float | None = None,
    ) -> dict[str, object]:
        return refinement.append_label(
            self.feedback_dir, strategy_name, shape=shape, node=node, label=label, note=note, trigger_ts=trigger_ts,
        )

    def progress(self, *, strategy_name: str) -> dict[str, object]:
        return refinement.progress(self.feedback_dir, strategy_name)

    def build_prompt(self, *, strategy_name: str, template: str) -> str:
        """Embeds the strategy's own current filter source when one is
        already configured; a strategy with no filter yet (the common
        first-time case -- see module docstring) gets a placeholder
        explaining there's nothing to perfect yet, this prompt is meant to
        create the first one."""
        strategy = self.strategies.load_strategy(strategy_name)
        if strategy is None:
            raise ValueError(f"unknown strategy: {strategy_name!r}")
        filter_entry = strategy.get("filter")
        if filter_entry is not None:
            source = self.runs.read_strategy_filter_source(filter_entry["filter_id"])["content"]
        else:
            source = (
                "# Aucun filtre n'est actuellement configuré pour cette stratégie.\n"
                "# Ce prompt sert à en créer un nouveau à partir des exemples réels ci-dessous.\n"
            )
        return refinement.build_prompt(self.feedback_dir, strategy_name, template=template, source=source)

    def start_auto_refine_job(self, *, strategy_name: str, template: str) -> refinement.ScanJob | None:
        """See ConceptRefinementManager.start_auto_refine_job -- identical
        mechanism, fires exactly once per strategy_name. If the strategy
        has no filter configured yet, build_prompt's own placeholder
        source means this effectively authors a first one, same as the
        manual flow already does in that case."""
        if self.claude_code_command is None:
            return None
        if not self.progress(strategy_name=strategy_name)["eligible_for_prompt"]:
            return None
        if not refinement.try_claim_auto_refine(self.feedback_dir, strategy_name):
            return None
        prompt = self.build_prompt(strategy_name=strategy_name, template=template)

        def _run(_on_progress) -> dict[str, object]:
            try:
                generated = generate_concept_via_claude_code(
                    prompt, command=self.claude_code_command, timeout_seconds=self.claude_code_timeout_seconds,
                )
                filename = auto_suffixed_filename(generated["filename"])
                return self.runs.import_filter_profile_file(filename, generated["content"], overwrite=False)
            except Exception:
                refinement.release_auto_refine_claim(self.feedback_dir, strategy_name)
                raise

        return refinement.run_scan_job(self.jobs, _run, name_prefix="filter-auto-refine-job")


__all__ = ["StrategyFilterRefinementManager"]
