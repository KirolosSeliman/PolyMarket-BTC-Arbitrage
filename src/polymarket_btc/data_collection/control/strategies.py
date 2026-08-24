"""Strategy assembly and persistence.

A strategy is *assembled configuration*, not code -- unlike plugins/
concepts/microsystems/execution profiles (each a `.py` file this process
imports), a strategy is a JSON file naming which concept/microsystem/
execution instances it uses, how they're wired together, and what each
instance's config values are. It's validated once, at save time, against
the concept/microsystem/execution/data catalogs `CollectionRunManager`
already owns -- not imported/executed the way the other four are.

Nothing in this module actually runs a strategy against real data -- that's
the future backtest module's job. This module only assembles and validates
the definition so that job has something correct to load.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
from pathlib import Path
import re

from .backtest_data import backtest_coverage, key_coverage
from .backtest_engine import required_concrete_keys, run_example_scenario
from .concepts import discover_concepts
from .config_schema import resolve_config
from .execution import discover_execution_profiles
from .management import discover_management_profiles
from .microsystems import discover_microsystems
from .runs import CollectionRunManager
from .strategy_filter import discover_filter_profiles

_LOGGER = logging.getLogger(__name__)

STRATEGY_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
INSTANCE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_EXAMPLE_WIN_SEARCH_ATTEMPTS = 30

# category -> (read_source method name, import method name, the strategy
# JSON field it lives under, the id field within that, whether that field
# holds a *list* of instances (concept/microsystem, a strategy can wire the
# same id multiple times) or a single nullable object (execution/
# management, at most one per strategy) -- see duplicate_and_rebind, the
# one place this drives real branching.
_DUPLICATE_CATEGORIES: dict[str, tuple[str, str, str, str, bool]] = {
    "concept": ("read_concept_source", "import_concept_file", "concepts", "concept_id", True),
    "microsystem": ("read_microsystem_source", "import_microsystem_file", "microsystems", "microsystem_id", True),
    "execution": ("read_execution_source", "import_execution_profile_file", "execution", "execution_id", False),
    "management": ("read_management_source", "import_management_profile_file", "management", "management_id", False),
}

# category -> (delete method name on self.runs, the list_strategies() field
# that names which ids of this category a strategy references, whether
# that field is a *list* of ids (concept/microsystem) or a single nullable
# id (execution/management)) -- see delete_source, which uses this to
# refuse deleting anything still wired into a saved strategy.
_DELETE_CATEGORIES: dict[str, tuple[str, str, bool]] = {
    "concept": ("delete_concept_source", "concept_ids", True),
    "microsystem": ("delete_microsystem_source", "microsystem_ids", True),
    "execution": ("delete_execution_source", "execution_id", False),
    "management": ("delete_management_source", "management_id", False),
}


@dataclass(slots=True)
class StrategyManager:
    strategies_dir: Path
    runs: CollectionRunManager

    def list_strategies(self) -> list[dict[str, object]]:
        """Scans strategies_dir/*.json. A corrupt or malformed file is
        skipped, not fatal -- same philosophy as discover_plugins: one bad
        file must never hide the working ones."""
        if not self.strategies_dir.is_dir():
            return []
        found: list[dict[str, object]] = []
        for path in sorted(self.strategies_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if not isinstance(payload, dict) or not isinstance(payload.get("name"), str):
                continue
            concepts = payload.get("concepts")
            microsystems = payload.get("microsystems")
            if not isinstance(concepts, list) or not isinstance(microsystems, list):
                continue
            execution = payload.get("execution")
            management = payload.get("management")
            found.append({
                "name": payload["name"],
                "concept_count": len(concepts),
                "microsystem_count": len(microsystems),
                "has_execution": execution is not None,
                "has_management": management is not None,
                "has_filter": payload.get("filter") is not None,
                "updated_at_utc": payload.get("updated_at_utc"),
                # Which global concept/microsystem/execution/management ids
                # this strategy actually references -- powers the Builder's
                # duplicate-for-a-strategy flow, which only ever needs to
                # offer strategies that genuinely use the id being
                # duplicated (see duplicate_and_rebind below).
                "concept_ids": sorted({
                    c["concept_id"] for c in concepts
                    if isinstance(c, dict) and isinstance(c.get("concept_id"), str)
                }),
                "microsystem_ids": sorted({
                    m["microsystem_id"] for m in microsystems
                    if isinstance(m, dict) and isinstance(m.get("microsystem_id"), str)
                }),
                "execution_id": execution.get("execution_id") if isinstance(execution, dict) else None,
                "management_id": management.get("management_id") if isinstance(management, dict) else None,
            })
        return found

    def load_strategy(self, name: str) -> dict[str, object] | None:
        """Reads one saved strategy by name -- None if it doesn't exist or
        is corrupt, same tolerance as list_strategies."""
        if not STRATEGY_NAME_RE.match(name):
            return None
        path = self.strategies_dir / f"{name}.json"
        if not path.is_file():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return payload if isinstance(payload, dict) else None

    def delete_strategy(self, name: str) -> None:
        """Permanently removes a saved strategy's .json file. Irreversible;
        the control panel's own "supprimer" button is expected to confirm
        with the user before calling this. STRATEGY_NAME_RE (a plain
        identifier, no path separators or traversal) already makes the
        target path safe, same guarantee load_strategy already relies on --
        no extra path resolution needed here unlike delete_run, which
        accepts a less constrained run_id."""
        if not STRATEGY_NAME_RE.match(name):
            raise ValueError(f"invalid strategy name: {name!r}")
        path = self.strategies_dir / f"{name}.json"
        if not path.is_file():
            raise FileNotFoundError(f"no strategy named {name!r}")
        path.unlink()

    def backtest_eligibility(self, name: str) -> dict[str, object] | None:
        """Whether `name` is ready to backtest, per the rule the backtest
        page enforces: an execution profile AND a management profile must
        both be set, and `coverage` only ever names a date range where
        *every* concrete data key the strategy's resolved concept/
        microsystem instances actually touch has genuinely been collected,
        AND at least one price-capable key (trades/klines/mark price) is
        available for the resolved instrument -- run_backtest's own price
        path needs that regardless of what the strategy's concepts declare
        (see backtest_coverage; without this, a strategy whose concepts
        never directly touch trades/klines/mark price -- e.g. one built
        only on open interest -- could show a range as fully backtestable
        purely because its own declared keys were covered, letting the
        user start a backtest that immediately failed with "no price data
        available"). Returns None if the strategy itself doesn't exist (a
        different failure mode than "not eligible yet")."""
        strategy = self.load_strategy(name)
        if strategy is None:
            return None
        concept_infos = {info.id: info for info in discover_concepts(self.runs.concepts_dir)}
        microsystem_infos = {info.id: info for info in discover_microsystems(self.runs.microsystems_dir)}
        keys = required_concrete_keys(strategy, concept_infos, microsystem_infos, self.runs._data_requirements_for)

        manifests = self.runs.list_runs()
        missing_data_keys = sorted(key for key in keys if not key_coverage(key, manifests))

        catalog = self.runs._merged_catalog()
        asset_counts: dict[str, int] = {}
        for key in keys:
            info = catalog.get(key)
            if info is not None and info.asset:
                asset_counts[info.asset] = asset_counts.get(info.asset, 0) + 1
        default_instrument = max(asset_counts, key=asset_counts.get) if asset_counts else "BTC"

        coverage, narrowest = backtest_coverage(keys, default_instrument, manifests)
        return {
            "missing_execution": strategy.get("execution") is None,
            "missing_management": strategy.get("management") is None,
            "required_data_keys": sorted(keys),
            "missing_data_keys": missing_data_keys,
            "coverage": coverage,
            "narrowest_key": narrowest,
            "default_instrument": default_instrument,
        }

    def _resolve_data_bindings(
        self,
        requirements: list[dict[str, object]],
        provided: object,
        *,
        known_data_keys: set[str],
    ) -> dict[str, str]:
        """Every *swappable* requirement (see
        CollectionRunManager._data_requirements_for) resolves to a concrete
        key: `provided`'s binding for that type if given, else the
        requirement's own authored default. A provided binding must be a
        real catalog key whose own derived type matches the requirement
        it's filling in for -- otherwise a "Bougies" requirement could
        silently get bound to a "Funding" key. Locked requirements need no
        binding at all -- they're fixed by the script itself."""
        if not isinstance(provided, dict):
            raise ValueError("data_bindings must be an object")
        swappable_types = {r["type"] for r in requirements if r["swappable"]}
        unknown_types = set(provided) - swappable_types
        if unknown_types:
            raise ValueError(f"unknown data binding type(s): {', '.join(sorted(unknown_types))}")
        bindings: dict[str, str] = {}
        for requirement in requirements:
            if not requirement["swappable"]:
                continue
            req_type = requirement["type"]
            key = provided.get(req_type, requirement["keys"][0])
            if not isinstance(key, str):
                raise ValueError(f"data binding for {req_type!r} must be a string key")
            if key not in known_data_keys:
                raise ValueError(f"data binding for {req_type!r} references unknown key: {key!r}")
            bound_type = self.runs._data_requirements_for([key])[0]["type"]
            if bound_type != req_type:
                raise ValueError(
                    f"data binding for {req_type!r} points to a key of type {bound_type!r}: {key!r}"
                )
            bindings[req_type] = key
        return bindings

    def _resolve_definition(
        self,
        *,
        concepts: list[dict],
        microsystems: list[dict],
        execution: dict | None,
        management: dict | None,
        filter: dict | None = None,
    ) -> dict[str, object]:
        """The validate-and-fill-in-defaults core of save_strategy, minus
        the naming/persistence around it -- shared with preview_example so
        a builder-in-progress preview is checked against *exactly* the same
        rules a real save would enforce (an unknown concept_id or a bad
        data binding fails the preview too, catching it before the user
        even tries to save), instead of a second, drifting copy of this
        logic living in the backtest engine."""
        concept_infos = {info.id: info for info in discover_concepts(self.runs.concepts_dir)}
        microsystem_infos = {info.id: info for info in discover_microsystems(self.runs.microsystems_dir)}
        execution_infos = {info.id: info for info in discover_execution_profiles(self.runs.execution_dir)}
        management_infos = {info.id: info for info in discover_management_profiles(self.runs.management_dir)}
        filter_infos = {info.id: info for info in discover_filter_profiles(self.runs.filter_dir)}
        known_data_keys = {row["key"] for row in self.runs.available_sources()}
        known_data_keys.update(row["id"] for row in self.runs.available_plugins())

        instance_ids: set[str] = set()
        resolved_concepts: list[dict[str, object]] = []
        for entry in concepts:
            if not isinstance(entry, dict):
                raise ValueError("each concept entry must be an object")
            instance_id = entry.get("instance_id")
            concept_id = entry.get("concept_id")
            if not isinstance(instance_id, str) or not INSTANCE_ID_RE.match(instance_id):
                raise ValueError(f"invalid concept instance_id: {instance_id!r}")
            if instance_id in instance_ids:
                raise ValueError(f"duplicate instance_id: {instance_id!r}")
            info = concept_infos.get(concept_id)
            if info is None:
                raise ValueError(f"unknown concept_id: {concept_id!r}")
            instance_ids.add(instance_id)
            resolved = resolve_config(info.config_schema, entry.get("config") or {})
            requirements = self.runs._data_requirements_for(list(info.data_sources))
            bindings = self._resolve_data_bindings(
                requirements, entry.get("data_bindings") or {}, known_data_keys=known_data_keys,
            )
            resolved_concepts.append({
                "instance_id": instance_id, "concept_id": concept_id, "config": resolved,
                "data_bindings": bindings,
            })

        resolved_microsystems: list[dict[str, object]] = []
        for entry in microsystems:
            if not isinstance(entry, dict):
                raise ValueError("each microsystem entry must be an object")
            instance_id = entry.get("instance_id")
            microsystem_id = entry.get("microsystem_id")
            if not isinstance(instance_id, str) or not INSTANCE_ID_RE.match(instance_id):
                raise ValueError(f"invalid microsystem instance_id: {instance_id!r}")
            if instance_id in instance_ids:
                raise ValueError(f"duplicate instance_id: {instance_id!r}")
            info = microsystem_infos.get(microsystem_id)
            if info is None:
                raise ValueError(f"unknown microsystem_id: {microsystem_id!r}")
            concept_instance_ids = entry.get("concept_instance_ids") or []
            if not isinstance(concept_instance_ids, list) or not all(
                isinstance(cid, str) for cid in concept_instance_ids
            ):
                raise ValueError("concept_instance_ids must be a list of strings")
            unknown_refs = [cid for cid in concept_instance_ids if cid not in instance_ids]
            if unknown_refs:
                raise ValueError(
                    f"microsystem {instance_id!r} references unknown concept instance(s): "
                    + ", ".join(unknown_refs)
                )
            instance_ids.add(instance_id)
            resolved = resolve_config(info.config_schema, entry.get("config") or {})
            requirements = self.runs._data_requirements_for(list(info.data_inputs))
            bindings = self._resolve_data_bindings(
                requirements, entry.get("data_bindings") or {}, known_data_keys=known_data_keys,
            )
            resolved_microsystems.append({
                "instance_id": instance_id, "microsystem_id": microsystem_id,
                "concept_instance_ids": concept_instance_ids, "data_bindings": bindings,
                "config": resolved,
            })

        resolved_execution: dict[str, object] | None = None
        if execution is not None:
            if not isinstance(execution, dict):
                raise ValueError("execution must be an object or null")
            execution_id = execution.get("execution_id")
            info = execution_infos.get(execution_id)
            if info is None:
                raise ValueError(f"unknown execution_id: {execution_id!r}")
            resolved = resolve_config(info.config_schema, execution.get("config") or {})
            resolved_execution = {"execution_id": execution_id, "config": resolved}

        resolved_management: dict[str, object] | None = None
        if management is not None:
            if not isinstance(management, dict):
                raise ValueError("management must be an object or null")
            management_id = management.get("management_id")
            info = management_infos.get(management_id)
            if info is None:
                raise ValueError(f"unknown management_id: {management_id!r}")
            resolved = resolve_config(info.config_schema, management.get("config") or {})
            resolved_management = {"management_id": management_id, "config": resolved}

        resolved_filter: dict[str, object] | None = None
        if filter is not None:
            if not isinstance(filter, dict):
                raise ValueError("filter must be an object or null")
            filter_id = filter.get("filter_id")
            info = filter_infos.get(filter_id)
            if info is None:
                raise ValueError(f"unknown filter_id: {filter_id!r}")
            resolved = resolve_config(info.config_schema, filter.get("config") or {})
            resolved_filter = {"filter_id": filter_id, "config": resolved}

        return {
            "concepts": resolved_concepts,
            "microsystems": resolved_microsystems,
            "execution": resolved_execution,
            "management": resolved_management,
            "filter": resolved_filter,
        }

    def save_strategy(
        self,
        *,
        name: str,
        concepts: list[dict],
        microsystems: list[dict],
        execution: dict | None,
        management: dict | None,
        filter: dict | None = None,
        overwrite: bool = False,
    ) -> dict[str, object]:
        if not STRATEGY_NAME_RE.match(name):
            raise ValueError(
                "name must be a simple identifier (letters, digits, underscores, "
                "starting with a letter)"
            )
        self.strategies_dir.mkdir(parents=True, exist_ok=True)
        target = self.strategies_dir / f"{name}.json"
        if target.exists() and not overwrite:
            raise FileExistsError(f"a strategy named {name!r} already exists")

        definition = self._resolve_definition(
            concepts=concepts, microsystems=microsystems, execution=execution, management=management,
            filter=filter,
        )

        now = datetime.now(UTC).isoformat()
        existing_created_at = now
        if target.exists():
            try:
                existing_created_at = json.loads(target.read_text(encoding="utf-8")).get("created_at_utc", now)
            except (OSError, ValueError):
                pass
        payload = {
            "name": name,
            **definition,
            "created_at_utc": existing_created_at,
            "updated_at_utc": now,
        }
        target.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return payload

    def duplicate_and_rebind(
        self, *, category: str, source_id: str, new_filename: str, strategy_name: str,
    ) -> dict[str, object]:
        """Forks category's source_id under new_filename -- a brand-new,
        independent script; the global original and every *other* strategy
        using it are left untouched -- and rebinds every one of
        strategy_name's own references to source_id so it points at the
        fork instead. Lets an edit meant for one strategy stop silently
        affecting every other strategy that happens to share the same
        concept/microsystem/execution/management script.

        Whole-strategy rebind, not per-instance: a strategy that wires the
        same concept twice (two distinct instance_ids) gets both rebound
        together, matching "this concept, for this strategy" rather than
        requiring instance-by-instance surgery.

        Raises ValueError for an unknown category or a strategy that
        doesn't actually reference source_id (nothing to rebind);
        FileNotFoundError for an unknown source_id or strategy_name;
        FileExistsError if new_filename collides with an existing script
        (propagated from the underlying import, never silently
        overwritten). If the strategy fails to re-validate on save for an
        unrelated reason, the already-imported duplicate file is left on
        disk -- no write path in this codebase does transactional
        rollback (save_strategy itself doesn't either), so this is
        consistent with the existing risk posture, not a regression."""
        try:
            read_attr, import_attr, list_key, id_field, is_list = _DUPLICATE_CATEGORIES[category]
        except KeyError:
            raise ValueError(f"category must be one of {sorted(_DUPLICATE_CATEGORIES)}, got {category!r}") from None
        read_fn = getattr(self.runs, read_attr)
        import_fn = getattr(self.runs, import_attr)

        content = read_fn(source_id)["content"]  # FileNotFoundError propagates for an unknown source_id
        import_result = import_fn(new_filename, content, overwrite=False)  # ValueError/FileExistsError propagate
        new_id = Path(new_filename).stem

        payload = self.load_strategy(strategy_name)
        if payload is None:
            raise FileNotFoundError(f"unknown strategy: {strategy_name!r}")

        rebound_count = 0
        if is_list:
            for entry in payload.get(list_key) or []:
                if isinstance(entry, dict) and entry.get(id_field) == source_id:
                    entry[id_field] = new_id
                    rebound_count += 1
        else:
            entry = payload.get(list_key)
            if isinstance(entry, dict) and entry.get(id_field) == source_id:
                entry[id_field] = new_id
                rebound_count = 1

        if rebound_count == 0:
            raise ValueError(
                f"la stratégie {strategy_name!r} ne référence pas {category} {source_id!r} -- rien à rebrancher"
            )

        saved = self.save_strategy(
            name=strategy_name,
            concepts=payload.get("concepts") or [],
            microsystems=payload.get("microsystems") or [],
            execution=payload.get("execution"),
            management=payload.get("management"),
            filter=payload.get("filter"),
            overwrite=True,
        )
        return {
            "filename": import_result["filename"], "new_id": new_id, "recognized": import_result["recognized"],
            "rebound_count": rebound_count, "strategy": saved,
        }

    def delete_source(self, *, category: str, source_id: str) -> dict[str, object]:
        """Permanently deletes a concept/microsystem/execution/management
        script, refusing if any saved strategy still references source_id --
        deleting out from under a strategy would leave it pointing at a
        script that no longer exists, the same failure mode duplicate_and_
        rebind exists to avoid (except here there's no fork to fall back
        to, so this has to block outright rather than offer a rebind).

        Raises ValueError for an unknown category, or when one or more
        strategies still reference source_id (message names them, so the
        caller can point the user at Dupliquer or at removing the
        reference first); FileNotFoundError for an unknown source_id."""
        try:
            delete_attr, list_field, is_list = _DELETE_CATEGORIES[category]
        except KeyError:
            raise ValueError(f"category must be one of {sorted(_DELETE_CATEGORIES)}, got {category!r}") from None

        blocking = []
        for entry in self.list_strategies():
            referenced = entry[list_field] if is_list else ([entry[list_field]] if entry[list_field] else [])
            if source_id in referenced:
                blocking.append(entry["name"])
        if blocking:
            raise ValueError(
                f"{source_id!r} est encore utilisé par : {', '.join(sorted(blocking))} -- "
                "retire-le de ces stratégies (ou duplique-le pour t'en détacher) avant de le supprimer."
            )

        getattr(self.runs, delete_attr)(source_id)  # FileNotFoundError propagates for an unknown source_id
        return {"category": category, "id": source_id}

    def preview_example(
        self,
        *,
        concepts: list[dict],
        microsystems: list[dict],
        execution: dict | None,
        management: dict | None,
        filter: dict | None = None,
        cadence_seconds: float = 60.0,
        seed: int = 42,
    ) -> dict[str, object]:
        """Runs a strategy under construction -- whole, or any subset of it
        (e.g. a single concept instance, for the builder's own per-item "i"
        preview) -- against an invented scenario instead of real collected
        data, so the user can see what their concept/microsystem/strategy
        actually does before any data exists for it, or before it's even
        saved. Validated through the same _resolve_definition save_strategy
        itself uses, so a broken wiring (unknown concept_id, a dangling
        concept_instance_ids reference, an invalid data binding) surfaces
        here exactly as it would on save, not as some other, less legible
        failure deeper in the engine."""
        definition = self._resolve_definition(
            concepts=concepts, microsystems=microsystems, execution=execution, management=management,
            filter=filter,
        )
        kwargs = dict(
            strategy=definition,
            concepts_dir=self.runs.concepts_dir,
            microsystems_dir=self.runs.microsystems_dir,
            execution_dir=self.runs.execution_dir,
            management_dir=self.runs.management_dir,
            filter_dir=self.runs.filter_dir,
            data_requirements_for=self.runs._data_requirements_for,
            cadence_seconds=cadence_seconds,
        )
        if execution is None:
            return run_example_scenario(seed=seed, **kwargs)

        # An "example" is meant to show the strategy working, not illustrate
        # a coin flip -- whichever synthetic seed happens to produce a loss
        # isn't representative of what's being demonstrated. Try a bounded
        # run of seeds starting from the requested one, deterministically
        # (same starting seed always searches the same sequence, so this
        # stays reproducible), and take the first one with a winning trade;
        # fall back to the last attempt if none of them found one (a real
        # scenario, just not a flattering one -- still better than an error).
        result = None
        for attempt in range(_EXAMPLE_WIN_SEARCH_ATTEMPTS):
            result = run_example_scenario(seed=seed + attempt, **kwargs)
            if any(trade["outcome"] == "win" for trade in result["trades"]):
                return result
        return result


__all__ = ["INSTANCE_ID_RE", "STRATEGY_NAME_RE", "StrategyManager"]
