import unittest

from polymarket_btc.data_collection.control.config_schema import (
    ConfigField,
    config_field_to_dict,
    parse_config_schema,
    resolve_config,
)


class ParseConfigSchemaTests(unittest.TestCase):
    def test_empty_list_is_valid(self) -> None:
        self.assertEqual(parse_config_schema([]), ())

    def test_not_a_list_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema({"name": "x"}))
        self.assertIsNone(parse_config_schema(None))
        self.assertIsNone(parse_config_schema("nope"))

    def test_valid_number_field(self) -> None:
        schema = parse_config_schema([{"name": "window", "type": "number", "label": "Window", "default": 30}])
        self.assertEqual(schema, (ConfigField(name="window", type="number", label="Window", default=30),))

    def test_valid_text_field(self) -> None:
        schema = parse_config_schema([{"name": "key", "type": "text", "label": "Key", "default": "x"}])
        self.assertEqual(schema[0].default, "x")

    def test_valid_select_field(self) -> None:
        schema = parse_config_schema([
            {"name": "mode", "type": "select", "label": "Mode", "default": "a", "options": ["a", "b"]},
        ])
        self.assertEqual(schema[0].options, ("a", "b"))

    def test_missing_name_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema([{"type": "number", "label": "x", "default": 1}]))

    def test_missing_label_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema([{"name": "x", "type": "number", "default": 1}]))

    def test_missing_default_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema([{"name": "x", "type": "number", "label": "x"}]))

    def test_invalid_type_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema([{"name": "x", "type": "boolean", "label": "x", "default": True}]))

    def test_duplicate_names_are_invalid(self) -> None:
        raw = [
            {"name": "x", "type": "number", "label": "x", "default": 1},
            {"name": "x", "type": "text", "label": "x2", "default": "y"},
        ]
        self.assertIsNone(parse_config_schema(raw))

    def test_select_without_options_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema([{"name": "x", "type": "select", "label": "x", "default": "a"}]))

    def test_select_default_not_in_options_is_invalid(self) -> None:
        raw = [{"name": "x", "type": "select", "label": "x", "default": "z", "options": ["a", "b"]}]
        self.assertIsNone(parse_config_schema(raw))

    def test_number_default_wrong_type_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema([{"name": "x", "type": "number", "label": "x", "default": "1"}]))

    def test_number_default_bool_is_invalid(self) -> None:
        # bool is technically an int subclass in Python -- must not sneak through.
        self.assertIsNone(parse_config_schema([{"name": "x", "type": "number", "label": "x", "default": True}]))

    def test_text_default_wrong_type_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema([{"name": "x", "type": "text", "label": "x", "default": 1}]))

    def test_entry_not_a_dict_is_invalid(self) -> None:
        self.assertIsNone(parse_config_schema(["not a dict"]))


class ResolveConfigTests(unittest.TestCase):
    def _schema(self):
        return parse_config_schema([
            {"name": "window", "type": "number", "label": "Window", "default": 30},
            {"name": "key", "type": "text", "label": "Key", "default": "x"},
        ])

    def test_defaults_used_when_nothing_provided(self) -> None:
        self.assertEqual(resolve_config(self._schema(), {}), {"window": 30, "key": "x"})

    def test_provided_values_override_defaults(self) -> None:
        resolved = resolve_config(self._schema(), {"window": 60})
        self.assertEqual(resolved, {"window": 60, "key": "x"})

    def test_unknown_key_raises(self) -> None:
        with self.assertRaises(ValueError):
            resolve_config(self._schema(), {"nope": 1})

    def test_wrong_type_value_raises(self) -> None:
        with self.assertRaises(ValueError):
            resolve_config(self._schema(), {"window": "not a number"})

    def test_select_value_outside_options_raises(self) -> None:
        schema = parse_config_schema([
            {"name": "mode", "type": "select", "label": "Mode", "default": "a", "options": ["a", "b"]},
        ])
        with self.assertRaises(ValueError):
            resolve_config(schema, {"mode": "z"})


class ConfigFieldToDictTests(unittest.TestCase):
    def test_shape(self) -> None:
        field = ConfigField(name="x", type="number", label="X", default=1, description="d", options=())
        self.assertEqual(config_field_to_dict(field), {
            "name": "x", "type": "number", "label": "X", "default": 1, "description": "d", "options": [],
        })


if __name__ == "__main__":
    unittest.main()
