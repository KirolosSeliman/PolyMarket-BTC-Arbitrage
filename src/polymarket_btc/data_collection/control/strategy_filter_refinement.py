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
hashing needed. No incremental rescan (unlike concept/microsystem
refinement's own scanned_through tracking) -- a full-strategy backtest is
already the more expensive operation of the two, but re-running it in full
each scan is simple and correct (candidates already seen are deduplicated
by instance_key regardless), and this still runs as a background job like
every other real-data scan in this app.
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
from .backtest_engine import run_backtest
from .runs import CollectionRunManager
from .strategies import StrategyManager

DEFAULT_CADENCE_SECONDS = 60.0


@dataclass(slots=True)
class StrategyFilterRefinementManager:
    feedback_dir: Path
    runs: CollectionRunManager
    strategies: StrategyManager
    jobs: dict[str, refinement.ScanJob] = field(default_factory=dict)

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
        cache = refinement.load_pool_cache(self.feedback_dir, strategy_name)
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

    def _instance_window(self, trade: dict, instrument: str) -> dict[str, object]:
        """A narrow, bounded real-candle window around one trade's own
        entry/exit -- same intent as concept/microsystem refinement's own
        _instance_window, sized around [entry_time, exit_time] instead of
        a single node's own formed_at."""
        manifests = self.runs.list_runs()
        entry_time = trade.get("entry_time")
        exit_time = trade.get("exit_time", entry_time)
        earliest = min(entry_time, exit_time)
        latest = max(entry_time, exit_time)
        start_ns = int((earliest - refinement.NEXT_WINDOW_BEFORE_SECONDS) * 1e9)
        end_ns = int((latest + refinement.NEXT_WINDOW_AFTER_SECONDS) * 1e9)
        display_key = "binance_futures_kline"
        records = read_records(
            display_key, start_ns, end_ns, manifests, instrument=f"{instrument.upper()}USDT",
        )
        if len(records) > refinement.NEXT_WINDOW_MAX_RECORDS:
            records = records[-refinement.NEXT_WINDOW_MAX_RECORDS:]
        return {"key": display_key, "candles": records}

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
        window = self._instance_window(chosen["node"], eligibility["default_instrument"])
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


__all__ = ["StrategyFilterRefinementManager"]
