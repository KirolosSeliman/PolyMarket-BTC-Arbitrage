"""Concept refinement ("Perfectionner un concept"): human labeling of real detected instances,
accumulated into a prompt an external AI can act on to revise the script.

A concept is a rule-based pattern detector (concepts/fvg.py etc.) authored
via the same "copy a prompt into an AI, paste the script back" workflow used
for microsystems/execution/management. This module adds a feedback loop on
top of that: scan real collected data for moments a concept detected
something (a zone/level -- see refinement.py for the shared shape-based
duck typing this is built on), show one at a time for a human Oui/Non
judgment (Non requires an explanation), and once enough judgment has
accumulated, build a prompt embedding the current script plus every "Non"
explanation for an external AI to revise -- never a live API call, matching
every other script in this app being user-authored, human-reviewed code.

(An earlier version of this module called the Anthropic API directly,
automatically, on every "Non" -- reverted at the user's explicit request:
that requires a paid API key, and this feature must stay free with zero
external dependency. The local hardware available (a 4GB-VRAM GPU, no
Ollama/Docker installed) also isn't viable for a free local model at
comparable quality, so the copy-paste-to-a-free-AI-chat workflow below is
the deliberate, considered choice, not a fallback.)

Deliberately does not expose a config editor: every scan evaluates the
concept under its own schema defaults (resolve_config(schema, {})). The
point of this feature is surfacing nuance for an AI to fix in the script's
logic, not hand-tuning numeric thresholds -- the normal strategy-builder
config form already covers that, elsewhere.

Storage, background-job polling, and the progress/gating/prompt-building
mechanics are all shared with microsystem_refinement.py (and later
strategy filter refinement) via refinement.py -- see that module's own
docstring for the full storage contract (labels.jsonl append-only,
pool_cache.json derived/disposable) and performance note (why scanning
always runs as a background job). This module owns only what's genuinely
concept-specific: scan()'s single-stage compute() loop and
_instance_window()'s single-node display window.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
from pathlib import Path
import random

from . import refinement
from .backtest_data import combined_coverage, narrowest_key, read_records
from .concept_generation import auto_suffixed_filename, generate_concept_via_claude_code
from .concepts import ConceptContext, ConceptInfo, discover_concepts
from .config_schema import resolve_config
from .runs import CollectionRunManager


@dataclass(slots=True)
class ConceptRefinementManager:
    feedback_dir: Path
    runs: CollectionRunManager
    jobs: dict[str, refinement.ScanJob] = field(default_factory=dict)
    # None (the default) means auto-perfectionnement is disabled -- kept
    # backward compatible with every test/caller that constructs this
    # manager without these two, same as ConceptGenerationManager's own
    # command/timeout_seconds.
    claude_code_command: list[str] | None = None
    claude_code_timeout_seconds: float = 600.0

    def _concept_info(self, concept_id: str) -> ConceptInfo:
        for info in discover_concepts(self.runs.concepts_dir):
            if info.id == concept_id:
                return info
        raise ValueError(f"unknown concept_id: {concept_id!r}")

    def scan(
        self, concept_id: str, *, on_progress: Callable[[float], None] | None = None,
    ) -> dict[str, object]:
        """Steps through every real record available for `concept_id`'s own
        data_sources (no data_bindings override -- see module docstring),
        feeding compute() a bounded trailing window at each step, and
        records every zone/level not already seen (by refinement.
        instance_key) as a new candidate the first time it appears -- see
        the loop's own comment for why capture is identity-based rather
        than gated on a node's own timestamp lining up with the current
        step. Incremental: a concept whose script hasn't changed since the
        last scan only walks forward from pool_cache.json's own
        scanned_through, not from the beginning. Raises ValueError if the
        concept is unknown or has no real data coverage at all."""
        info = self._concept_info(concept_id)
        resolved_config = resolve_config(info.config_schema, {})
        keys = list(dict.fromkeys(info.data_sources))  # de-duplicated, order-preserving

        manifests = self.runs.list_runs()
        coverage = combined_coverage(set(keys), manifests)
        if not coverage:
            missing = narrowest_key(set(keys), manifests)
            raise ValueError(
                "aucune donnée réelle disponible pour ce concept -- limité par : "
                + (missing or ", ".join(sorted(keys)))
            )

        script_hash = hashlib.sha256(
            self.runs.read_concept_source(concept_id)["content"].encode("utf-8")
        ).hexdigest()
        cache = refinement.load_pool_cache(self.feedback_dir, concept_id)
        if cache.get("script_sha256") != script_hash:
            cache = refinement.empty_pool_cache()
        cache["script_sha256"] = script_hash

        start_ns = int(refinement.iso_to_ts(coverage[0][0]) * 1e9)
        end_ns = int(refinement.iso_to_ts(coverage[-1][1]) * 1e9)
        records_by_key = {
            key: read_records(key, start_ns, end_ns, manifests, instrument=refinement.key_instrument(key))
            for key in keys
        }

        step_timestamps = sorted({r["timestamp"] for records in records_by_key.values() for r in records})
        scanned_through: dict[str, float] = cache["scanned_through"]  # type: ignore[assignment]
        resume_after = min((scanned_through.get(key, -1.0) for key in keys), default=-1.0)
        seen = {c["instance_key"] for c in cache["candidates"]}  # type: ignore[union-attr]
        cursors = {key: 0 for key in keys}
        new_candidates: list[dict[str, object]] = []

        total_steps = len(step_timestamps)
        for i, t in enumerate(step_timestamps):
            for key, records in records_by_key.items():
                cursor = cursors[key]
                while cursor < len(records) and records[cursor]["timestamp"] <= t:
                    cursor += 1
                cursors[key] = cursor
            if t <= resume_after:
                if on_progress is not None and total_steps and i % max(1, total_steps // 200) == 0:
                    on_progress(i / total_steps)
                continue
            data = {
                key: records[max(0, cursors[key] - refinement.SCAN_WINDOW_RECORDS):cursors[key]]
                for key, records in records_by_key.items()
            }
            try:
                result = info.compute(ConceptContext(data=data, config=resolved_config, log=refinement.noop_log))
            except Exception:
                result = None  # a concept that raises on a real window loses only this one step, matches build_timeline
            for shape, node in refinement.walk_for_annotations(result):
                # Capture is purely identity-based (has this exact node been
                # seen before), not "does its own trigger timestamp equal
                # the current step" -- a concept is free to stamp formed_at
                # using whatever convention it likes (fvg.py uses each
                # candle's open_time; read_records' own kline timestamp
                # field is close_time, 60s later for 1-minute candles), so
                # those two will often never exactly coincide. Real data
                # confirmed this: fvg.py finds real FVGs (12 in a single
                # day's worth of klines, checked directly), but an
                # equality-gated scan against that same data found zero,
                # every candidate silently discarded by a timestamp that
                # was always one candle width off. The dedup set already
                # does the actual job an equality gate was meant to do
                # (never recount a still-active, still-returned node) --
                # trigger_timestamp's result is used below only as
                # descriptive metadata (which moment to center the display
                # window on), never as a gate.
                ikey = refinement.instance_key(concept_id, shape, node)
                if ikey in seen:
                    continue
                seen.add(ikey)
                new_candidates.append({
                    "instance_key": ikey, "shape": shape, "node": node,
                    "trigger_ts": refinement.trigger_timestamp(shape, node),
                })
            if on_progress is not None and total_steps and i % max(1, total_steps // 200) == 0:
                on_progress(i / total_steps)

        cache["candidates"] = cache["candidates"] + new_candidates  # type: ignore[operator]
        cache["scanned_through"] = {
            key: (records[-1]["timestamp"] if records else scanned_through.get(key, resume_after))
            for key, records in records_by_key.items()
        }
        cache["scanned_at_utc"] = datetime.now(UTC).isoformat()
        refinement.write_pool_cache(self.feedback_dir, concept_id, cache)
        if on_progress is not None:
            on_progress(1.0)
        return {"candidate_count": len(cache["candidates"]), "added": len(new_candidates), "coverage": coverage}

    def start_scan_job(self, *, concept_id: str) -> refinement.ScanJob:
        return refinement.run_scan_job(
            self.jobs, lambda on_progress: self.scan(concept_id, on_progress=on_progress),
            name_prefix="concept-scan-job",
        )

    def scan_job_status(self, job_id: str) -> dict[str, object] | None:
        return refinement.scan_job_status(self.jobs, job_id)

    def _instance_window(self, info: ConceptInfo, candidate: dict[str, object]) -> dict[str, object]:
        """A narrow, bounded real-candle window around one candidate's own
        timestamp -- unlike scan()'s own full-range read, this stays
        O(window) regardless of how much history has accumulated, so
        next_instance doesn't slow back down as months of data pile up."""
        keys = list(dict.fromkeys(info.data_sources))
        display_key = keys[0]
        manifests = self.runs.list_runs()
        trigger_ts = candidate["trigger_ts"]
        node = candidate["node"]
        formed_at = node.get("formed_at") if refinement.is_num(node.get("formed_at")) else trigger_ts
        earliest = min(formed_at, trigger_ts)
        start_ns = int((earliest - refinement.NEXT_WINDOW_BEFORE_SECONDS) * 1e9)
        end_ns = int((trigger_ts + refinement.NEXT_WINDOW_AFTER_SECONDS) * 1e9)
        records = read_records(display_key, start_ns, end_ns, manifests, instrument=refinement.key_instrument(display_key))
        if len(records) > refinement.NEXT_WINDOW_MAX_RECORDS:
            records = records[-refinement.NEXT_WINDOW_MAX_RECORDS:]
        return {"key": display_key, "candles": records}

    def next_instance(self, *, concept_id: str) -> dict[str, object]:
        """A random not-yet-labeled candidate from the last scan's pool --
        random (not sequential) so repeated calls don't show things in a
        predictable order, and never-yet-labeled (persisted across
        sessions, not just this one) so a real example is never shown
        twice. `exhausted` means every known candidate has already been
        judged -- more real data collected later, or a rescan after the
        script changes, produces a fresh pool. `no_candidates` means the
        concept has never fired anywhere in the scanned real history at
        all -- a legitimate, clean state, not an error."""
        info = self._concept_info(concept_id)
        cache = refinement.load_pool_cache(self.feedback_dir, concept_id)
        candidates = cache.get("candidates") or []
        labeled_keys = {
            entry["instance_key"] for entry in refinement.read_labels(self.feedback_dir, concept_id)
            if "instance_key" in entry
        }
        remaining = [c for c in candidates if c["instance_key"] not in labeled_keys]
        progress = self.progress(concept_id=concept_id)
        if not candidates:
            return {"instance": None, "exhausted": False, "no_candidates": True, "progress": progress}
        if not remaining:
            return {"instance": None, "exhausted": True, "no_candidates": False, "progress": progress}
        chosen = random.choice(remaining)
        window = self._instance_window(info, chosen)
        return {
            "instance": {
                "shape": chosen["shape"], "node": chosen["node"], "trigger_ts": chosen["trigger_ts"],
                "window": window,
            },
            "exhausted": False, "no_candidates": False, "progress": progress,
        }

    def label(
        self, *, concept_id: str, shape: str, node: dict, label: str, note: str = "",
        trigger_ts: float | None = None,
    ) -> dict[str, object]:
        return refinement.append_label(
            self.feedback_dir, concept_id, shape=shape, node=node, label=label, note=note, trigger_ts=trigger_ts,
        )

    def progress(self, *, concept_id: str) -> dict[str, object]:
        return refinement.progress(self.feedback_dir, concept_id)

    def build_prompt(self, *, concept_id: str, template: str) -> str:
        source = self.runs.read_concept_source(concept_id)["content"]
        return refinement.build_prompt(self.feedback_dir, concept_id, template=template, source=source)

    def start_auto_refine_job(self, *, concept_id: str, template: str) -> refinement.ScanJob | None:
        """Fires exactly once per concept_id, the first time labeling
        crosses refinement.py's own eligibility gate -- see
        refinement.try_claim_auto_refine. Improves the concept via Claude
        Code (same subprocess call the creation pilot uses) and imports
        the result as a NEW, `_auto`-suffixed file (never overwrites the
        original -- a real strategy may already depend on it), so it just
        shows up in Builder like anything else, adopted manually."""
        if self.claude_code_command is None:
            return None
        if not self.progress(concept_id=concept_id)["eligible_for_prompt"]:
            return None
        if not refinement.try_claim_auto_refine(self.feedback_dir, concept_id):
            return None
        prompt = self.build_prompt(concept_id=concept_id, template=template)

        def _run(_on_progress) -> dict[str, object]:
            try:
                generated = generate_concept_via_claude_code(
                    prompt, command=self.claude_code_command, timeout_seconds=self.claude_code_timeout_seconds,
                )
                filename = auto_suffixed_filename(generated["filename"])
                return self.runs.import_concept_file(filename, generated["content"], overwrite=False)
            except Exception:
                refinement.release_auto_refine_claim(self.feedback_dir, concept_id)
                raise

        return refinement.run_scan_job(self.jobs, _run, name_prefix="concept-auto-refine-job")

    def generate_synthetic_instance(self, *, concept_id: str) -> dict[str, object]:
        """For when there's too little (or no) real collected data to find
        a real instance to review -- builds ONE fixed, generic synthetic
        scenario (see refinement.build_synthetic_candle_set: oscillate tightly,
        then break out sharply with much higher volume) and runs this
        concept's own real compute() against it, so the Python code
        decides what's detected, not any external claim. No AI call, no
        network, no subprocess -- pure and instant, unlike the Claude Code
        calls elsewhere in this app. Generic by design (not tailored to
        what THIS concept specifically looks for), so some concepts won't
        react to it at all -- that's an accepted, disclosed trade-off for
        speed and zero dependency, not a bug. Returns the exact same shape
        next_instance() does, so the frontend renders/labels it through
        the identical existing path."""
        info = self._concept_info(concept_id)
        synthetic_data = refinement.build_synthetic_candle_set(list(info.data_sources))
        resolved_config = resolve_config(info.config_schema, {})
        try:
            result = info.compute(ConceptContext(data=synthetic_data, config=resolved_config, log=refinement.noop_log))
        except Exception as exc:
            raise ValueError(f"le concept a levé une erreur sur cet exemple : {exc}") from None
        annotations = refinement.walk_for_annotations(result)
        if not annotations:
            raise ValueError(
                "ce scénario synthétique générique (range puis cassure) n'a rien déclenché pour ce concept "
                "-- certains concepts ont besoin d'un motif différent, essaie avec de vraies données"
            )
        shape, node = annotations[0]
        display_key = list(dict.fromkeys(info.data_sources))[0]
        return {
            "shape": shape, "node": node, "trigger_ts": refinement.trigger_timestamp(shape, node),
            "window": {"key": display_key, "candles": synthetic_data.get(display_key, [])},
            "synthetic": True,
        }


__all__ = ["ConceptRefinementManager"]
