"""Shared "adjustable parameters" mechanism for concepts, microsystems, and
execution profiles (see concepts.py/microsystems.py/execution.py): each of
those declares an optional CONFIG_SCHEMA -- a list of named, typed fields
with defaults -- that the control panel renders as a form and resolves
against whatever values the user actually provides.

One implementation, reused by all three layers, rather than three near-
identical copies.
"""

from __future__ import annotations

from dataclasses import dataclass

VALID_CONFIG_TYPES = ("number", "text", "select")


@dataclass(frozen=True, slots=True)
class ConfigField:
    name: str
    type: str
    label: str
    default: object
    description: str | None = None
    # Required non-empty when type == "select"; the field's value (and
    # default) must be one of these. Unused/empty for "number" and "text".
    options: tuple[str, ...] = ()


def _valid_default(field_type: str, default: object, options: tuple[str, ...]) -> bool:
    if field_type == "number":
        return isinstance(default, (int, float)) and not isinstance(default, bool)
    if field_type == "text":
        return isinstance(default, str)
    if field_type == "select":
        return isinstance(default, str) and default in options
    return False


def parse_config_schema(raw: object) -> tuple[ConfigField, ...] | None:
    """None means `raw` isn't a valid CONFIG_SCHEMA at all -- the caller
    treats that as a whole-file contract violation, the same severity as a
    missing *_INFO dict. An empty list is valid (nothing adjustable)."""
    if not isinstance(raw, list):
        return None
    fields: list[ConfigField] = []
    seen_names: set[str] = set()
    for entry in raw:
        if not isinstance(entry, dict):
            return None
        name = entry.get("name")
        field_type = entry.get("type")
        label = entry.get("label")
        if not isinstance(name, str) or not name or name in seen_names:
            return None
        if field_type not in VALID_CONFIG_TYPES:
            return None
        if not isinstance(label, str) or not label:
            return None
        options_raw = entry.get("options", [])
        if not isinstance(options_raw, list) or not all(isinstance(o, str) for o in options_raw):
            return None
        options = tuple(options_raw)
        if field_type == "select" and not options:
            return None
        if "default" not in entry:
            return None
        default = entry["default"]
        if not _valid_default(field_type, default, options):
            return None
        description = entry.get("description")
        if description is not None and not isinstance(description, str):
            return None
        seen_names.add(name)
        fields.append(ConfigField(
            name=name, type=field_type, label=label, default=default,
            description=description, options=options,
        ))
    return tuple(fields)


def resolve_config(schema: tuple[ConfigField, ...], provided: dict[str, object]) -> dict[str, object]:
    """Defaults overridden by `provided`. Raises ValueError on an unknown
    key in `provided`, or a value that doesn't match its field's type --
    called at strategy-save time, where catching a typo immediately beats
    discovering it silently later."""
    by_name = {field.name: field for field in schema}
    unknown = set(provided) - set(by_name)
    if unknown:
        raise ValueError(f"unknown config field(s): {', '.join(sorted(unknown))}")
    resolved: dict[str, object] = {field.name: field.default for field in schema}
    for name, value in provided.items():
        field = by_name[name]
        if not _valid_default(field.type, value, field.options):
            raise ValueError(f"config field '{name}' has an invalid value for type '{field.type}'")
        resolved[name] = value
    return resolved


def config_field_to_dict(field: ConfigField) -> dict[str, object]:
    return {
        "name": field.name,
        "type": field.type,
        "label": field.label,
        "default": field.default,
        "description": field.description,
        "options": list(field.options),
    }


__all__ = [
    "VALID_CONFIG_TYPES",
    "ConfigField",
    "config_field_to_dict",
    "parse_config_schema",
    "resolve_config",
]
