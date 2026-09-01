"""Microsystem refinement ("Perfectionner un microsystème"): mirrors
concept_refinement.py exactly (see refinement.py's own docstring for the
shared storage/job/gating machinery both delegate to -- labels.jsonl
append-only, pool_cache.json derived/disposable, background-job polling,
progress/gating/prompt-building), with two differences that come from what
a microsystem actually is (microsystems.py's own docstring: "concept(s)
(and/or data) in, a combined signal out"):

  - scan() is two-stage per step: first every concept this microsystem is
    wired to (concept_inputs) computes under its own schema defaults over
    its own data_sources -- mirrors build_timeline's own two-stage step in
    backtest_engine.py, without that function's memoization/instance-id/
    data-binding machinery (all built for a whole strategy's cartesian mix
    of concept/microsystem *instances*; a bounded scan of one microsystem
    *definition* needs none of it) -- then the microsystem's own
    compute(MicrosystemContext(concepts=...)) runs against those concept
    outputs plus its own direct data_inputs.
  - a judged candidate is a whole "setup" (refinement.find_setup_candidates
    / instance_key/trigger_timestamp's "setup" branch), not a fragmented
    zone/level -- a microsystem's value is the *relationship* between its
    wired concepts' outputs, not any one zone alone (already covered by
    concept-level refinement).
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
from .microsystems import MicrosystemContext, MicrosystemInfo, discover_microsystems
from .runs import CollectionRunManager


@dataclass(slots=True)
class MicrosystemRefinementManager:
    feedback_dir: Path
    runs: CollectionRunManager
    jobs: dict[str, refinement.ScanJob] = field(default_factory=dict)
    claude_code_command: list[str] | None = None
    claude_code_timeout_seconds: float = 600.0

    def _microsystem_info(self, microsystem_id: str) -> MicrosystemInfo:
        for info in discover_microsystems(self.runs.microsystems_dir):
            if info.id == microsystem_id:
                return info
        raise ValueError(f"unknown microsystem_id: {microsystem_id!r}")

    def _wired_concepts(self, info: MicrosystemInfo) -> dict[str, ConceptInfo]:
        """Each of info.concept_inputs resolved to its own ConceptInfo, via
        the same discover_concepts catalog concept_refinement.py itself
        uses. A concept_input naming an id that no longer resolves
        (deleted/renamed concept file) is silently dropped, matching
        build_timeline's own `if info is None: continue` leniency."""
        all_concepts = {c.id: c for c in discover_concepts(self.runs.concepts_dir)}
        return {cid: all_concepts[cid] for cid in info.concept_inputs if cid in all_concepts}

    def _scan_keys(self, info: MicrosystemInfo, wired_concepts: dict[str, ConceptInfo]) -> list[str]:
        """De-duplicated, order-preserving: every wired concept's own
        data_sources (in concept_inputs' declared order) followed by the
        microsystem's own direct data_inputs -- so keys[0], used as the
        chart's display key, is the first wired concept's own
        data_sources[0] whenever at least one concept is wired (matches
        the plan's own display-key resolution), falling back to the
        microsystem's own first data_input otherwise."""
        return list(dict.fromkeys(
            [key for c in wired_concepts.values() for key in c.data_sources] + list(info.data_inputs)
        ))

    def scan(
        self, microsystem_id: str, *, on_progress: Callable[[float], None] | None = None,
    ) -> dict[str, object]:
        """Two-stage version of ConceptRefinementManager.scan() -- see this
        module's own docstring. Raises ValueError if the microsystem is
        unknown, has no data sources resolvable at all (every concept_input
        stale and no data_inputs of its own), or has no real data coverage."""
        info = self._microsystem_info(microsystem_id)
        wired_concepts = self._wired_concepts(info)
        resolved_config = resolve_config(info.config_schema, {})
        concept_configs = {cid: resolve_config(c.config_schema, {}) for cid, c in wired_concepts.items()}

        keys = self._scan_keys(info, wired_concepts)
        if not keys:
            raise ValueError(f"le microsystème {microsystem_id!r} n'a plus aucune source de données câblée")

        manifests = self.runs.list_runs()
        coverage = combined_coverage(set(keys), manifests)
        if not coverage:
            missing = narrowest_key(set(keys), manifests)
            raise ValueError(
                "aucune donnée réelle disponible pour ce microsystème -- limité par : "
                + (missing or ", ".join(sorted(keys)))
            )

        script_hash = hashlib.sha256(
            self.runs.read_microsystem_source(microsystem_id)["content"].encode("utf-8")
        ).hexdigest()
        cache = refinement.load_pool_cache(self.feedback_dir, microsystem_id)
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
            concept_outputs: dict[str, object] = {}
            for cid, cinfo in wired_concepts.items():
                try:
                    concept_outputs[cid] = cinfo.compute(
                        ConceptContext(data=data, config=concept_configs[cid], log=refinement.noop_log)
                    )
                except Exception:
                    concept_outputs[cid] = None  # a wired concept that raises loses only this one step
            try:
                result = info.compute(MicrosystemContext(
                    concepts=concept_outputs, data=data, config=resolved_config, log=refinement.noop_log,
                ))
            except Exception:
                result = None
            for setup in refinement.find_setup_candidates(result):
                ikey = refinement.instance_key(microsystem_id, "setup", setup)
                if ikey in seen:
                    continue
                seen.add(ikey)
                new_candidates.append({
                    "instance_key": ikey, "shape": "setup", "node": setup,
                    "trigger_ts": refinement.trigger_timestamp("setup", setup),
                })
            if on_progress is not None and total_steps and i % max(1, total_steps // 200) == 0:
                on_progress(i / total_steps)

        cache["candidates"] = cache["candidates"] + new_candidates  # type: ignore[operator]
        cache["scanned_through"] = {
            key: (records[-1]["timestamp"] if records else scanned_through.get(key, resume_after))
            for key, records in records_by_key.items()
        }
        cache["scanned_at_utc"] = datetime.now(UTC).isoformat()
        refinement.write_pool_cache(self.feedback_dir, microsystem_id, cache)
        if on_progress is not None:
            on_progress(1.0)
        return {"candidate_count": len(cache["candidates"]), "added": len(new_candidates), "coverage": coverage}

    def start_scan_job(self, *, microsystem_id: str) -> refinement.ScanJob:
        return refinement.run_scan_job(
            self.jobs, lambda on_progress: self.scan(microsystem_id, on_progress=on_progress),
            name_prefix="microsystem-scan-job",
        )

    def scan_job_status(self, job_id: str) -> dict[str, object] | None:
        return refinement.scan_job_status(self.jobs, job_id)

    def _instance_window(
        self, info: MicrosystemInfo, wired_concepts: dict[str, ConceptInfo], candidate: dict[str, object],
    ) -> dict[str, object]:
        """Same bounded-window intent as ConceptRefinementManager's own
        _instance_window, generalized to a compound setup: the window
        starts before the *earliest* of the setup's own sub-nodes (an
        initial FVG can sit dozens of candles before its sweep/reversal),
        each sub-node's own earliest moment being its formed_at (a pool's
        own origin) or, lacking that, its swept_at."""
        keys = self._scan_keys(info, wired_concepts)
        display_key = keys[0]
        manifests = self.runs.list_runs()
        trigger_ts = candidate["trigger_ts"]
        node = candidate["node"]
        sub_earliest = [
            sub_node.get("formed_at") if refinement.is_num(sub_node.get("formed_at")) else sub_node.get("swept_at")
            for _shape, sub_node in refinement.walk_for_annotations(node)
        ]
        sub_earliest = [ts for ts in sub_earliest if refinement.is_num(ts)]
        earliest = min([*sub_earliest, trigger_ts]) if sub_earliest else trigger_ts
        start_ns = int((earliest - refinement.NEXT_WINDOW_BEFORE_SECONDS) * 1e9)
        end_ns = int((trigger_ts + refinement.NEXT_WINDOW_AFTER_SECONDS) * 1e9)
        records = read_records(display_key, start_ns, end_ns, manifests, instrument=refinement.key_instrument(display_key))
        if len(records) > refinement.NEXT_WINDOW_MAX_RECORDS:
            records = records[-refinement.NEXT_WINDOW_MAX_RECORDS:]
        return {"key": display_key, "candles": records}

    def next_instance(self, *, microsystem_id: str) -> dict[str, object]:
        info = self._microsystem_info(microsystem_id)
        wired_concepts = self._wired_concepts(info)
        cache = refinement.load_pool_cache(self.feedback_dir, microsystem_id)
        candidates = cache.get("candidates") or []
        labeled_keys = {
            entry["instance_key"] for entry in refinement.read_labels(self.feedback_dir, microsystem_id)
            if "instance_key" in entry
        }
        remaining = [c for c in candidates if c["instance_key"] not in labeled_keys]
        progress = self.progress(microsystem_id=microsystem_id)
        if not candidates:
            return {"instance": None, "exhausted": False, "no_candidates": True, "progress": progress}
        if not remaining:
            return {"instance": None, "exhausted": True, "no_candidates": False, "progress": progress}
        chosen = random.choice(remaining)
        window = self._instance_window(info, wired_concepts, chosen)
        return {
            "instance": {
                "shape": chosen["shape"], "node": chosen["node"], "trigger_ts": chosen["trigger_ts"],
                "window": window,
            },
            "exhausted": False, "no_candidates": False, "progress": progress,
        }

    def label(
        self, *, microsystem_id: str, shape: str, node: dict, label: str, note: str = "",
        trigger_ts: float | None = None,
    ) -> dict[str, object]:
        return refinement.append_label(
            self.feedback_dir, microsystem_id, shape=shape, node=node, label=label, note=note, trigger_ts=trigger_ts,
        )

    def progress(self, *, microsystem_id: str) -> dict[str, object]:
        return refinement.progress(self.feedback_dir, microsystem_id)

    def build_prompt(self, *, microsystem_id: str, template: str) -> str:
        source = self.runs.read_microsystem_source(microsystem_id)["content"]
        return refinement.build_prompt(self.feedback_dir, microsystem_id, template=template, source=source)

    def start_auto_refine_job(self, *, microsystem_id: str, template: str) -> refinement.ScanJob | None:
        """See ConceptRefinementManager.start_auto_refine_job -- identical
        mechanism, fires exactly once per microsystem_id."""
        if self.claude_code_command is None:
            return None
        if not self.progress(microsystem_id=microsystem_id)["eligible_for_prompt"]:
            return None
        if not refinement.try_claim_auto_refine(self.feedback_dir, microsystem_id):
            return None
        prompt = self.build_prompt(microsystem_id=microsystem_id, template=template)

        def _run(_on_progress) -> dict[str, object]:
            try:
                generated = generate_concept_via_claude_code(
                    prompt, command=self.claude_code_command, timeout_seconds=self.claude_code_timeout_seconds,
                )
                filename = auto_suffixed_filename(generated["filename"])
                return self.runs.import_microsystem_file(filename, generated["content"], overwrite=False)
            except Exception:
                refinement.release_auto_refine_claim(self.feedback_dir, microsystem_id)
                raise

        return refinement.run_scan_job(self.jobs, _run, name_prefix="microsystem-auto-refine-job")


__all__ = ["MicrosystemRefinementManager"]
