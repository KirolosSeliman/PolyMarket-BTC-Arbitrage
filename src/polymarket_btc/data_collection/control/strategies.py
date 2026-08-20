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

from .backtest_data import combined_coverage, key_coverage, narrowest_key
from .backtest_engine import required_concrete_keys
from .concepts import discover_concepts
from .config_schema import resolve_config
from .execution import discover_execution_profiles
from .management import discover_management_profiles
from .microsystems import discover_microsystems
from .runs import CollectionRunManager

_LOGGER = logging.getLogger(__name__)

STRATEGY_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
INSTANCE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


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
            found.append({
                "name": payload["name"],
                "concept_count": len(concepts),
                "microsystem_count": len(microsystems),
                "has_execution": payload.get("execution") is not None,
                "has_management": payload.get("management") is not None,
                "updated_at_utc": payload.get("updated_at_utc"),
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

    def backtest_eligibility(self, name: str) -> dict[str, object] | None:
        """Whether `name` is ready to backtest, per the rule the backtest
        page enforces: an execution profile AND a management profile must
        both be set, and `coverage` only ever names a date range where
        *every* concrete data key the strategy's resolved concept/
        microsystem instances actually touch has genuinely been collected
        (combined_coverage intersects each key's own real coverage -- a key
        present but only for a narrower window still correctly shrinks the
        overall range, it doesn't get silently ignored). Returns None if the
        strategy itself doesn't exist (a different failure mode than "not
        eligible yet")."""
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

        return {
            "missing_execution": strategy.get("execution") is None,
            "missing_management": strategy.get("management") is None,
            "required_data_keys": sorted(keys),
            "missing_data_keys": missing_data_keys,
            "coverage": combined_coverage(keys, manifests),
            "narrowest_key": narrowest_key(keys, manifests),
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

    def save_strategy(
        self,
        *,
        name: str,
        concepts: list[dict],
        microsystems: list[dict],
        execution: dict | None,
        management: dict | None,
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

        concept_infos = {info.id: info for info in discover_concepts(self.runs.concepts_dir)}
        microsystem_infos = {info.id: info for info in discover_microsystems(self.runs.microsystems_dir)}
        execution_infos = {info.id: info for info in discover_execution_profiles(self.runs.execution_dir)}
        management_infos = {info.id: info for info in discover_management_profiles(self.runs.management_dir)}
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

        now = datetime.now(UTC).isoformat()
        existing_created_at = now
        if target.exists():
            try:
                existing_created_at = json.loads(target.read_text(encoding="utf-8")).get("created_at_utc", now)
            except (OSError, ValueError):
                pass
        payload = {
            "name": name,
            "concepts": resolved_concepts,
            "microsystems": resolved_microsystems,
            "execution": resolved_execution,
            "management": resolved_management,
            "created_at_utc": existing_created_at,
            "updated_at_utc": now,
        }
        target.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return payload


__all__ = ["INSTANCE_ID_RE", "STRATEGY_NAME_RE", "StrategyManager"]
