"""
M7: substitution registry — replace 3rd-party Data Flow components with
specific ADF Mapping Data Flow transformations on conversion.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.converters.data_flow.transformation_converter import (
    convert_transformation,
)
from ssis_adf_agent.converters.substitution_registry import (
    EMPTY_REGISTRY,
    DataFlowSubstitution,
    SubstitutionRegistry,
    load_registry,
)
from ssis_adf_agent.parsers.models import DataFlowComponent


def _comp(component_type: str, name: str = "MyComp") -> DataFlowComponent:
    return DataFlowComponent(
        id="comp-1",
        name=name,
        component_class_id="{FAKE-CLASS}",
        component_type=component_type,
    )


class TestLoadRegistry:
    def test_loads_data_flow_and_control_flow_entries(self, tmp_path: Path):
        p = tmp_path / "reg.json"
        p.write_text(json.dumps({
            "version": "1",
            "data_flow_components": {
                "Cozyroc.SuperLookup": {
                    "adf_type": "Lookup",
                    "notes": "Use DS_SuperLookupReplacement",
                    "type_properties": {"broadcast": "Auto"},
                },
            },
            "control_flow_tasks": {
                "{ABC-123}": {
                    "adf_activity_type": "WebActivity",
                },
            },
        }), encoding="utf-8")

        reg = load_registry(p)
        assert isinstance(reg, SubstitutionRegistry)
        df_sub = reg.lookup_data_flow("Cozyroc.SuperLookup")
        assert df_sub is not None
        assert df_sub.adf_type == "Lookup"
        assert df_sub.notes == "Use DS_SuperLookupReplacement"
        assert df_sub.type_properties == {"broadcast": "Auto"}

        cf_sub = reg.lookup_control_flow("{ABC-123}")
        assert cf_sub is not None
        assert cf_sub.adf_activity_type == "WebActivity"

    def test_empty_registry_lookups_return_none(self):
        assert EMPTY_REGISTRY.lookup_data_flow("Anything") is None
        assert EMPTY_REGISTRY.lookup_control_flow("Anything") is None

    def test_missing_file_raises_value_error(self, tmp_path: Path):
        with pytest.raises(ValueError, match="not found"):
            load_registry(tmp_path / "does_not_exist.json")

    def test_invalid_json_raises_value_error(self, tmp_path: Path):
        p = tmp_path / "bad.json"
        p.write_text("not json", encoding="utf-8")
        with pytest.raises(ValueError, match="not valid JSON"):
            load_registry(p)

    def test_data_flow_entry_missing_adf_type_rejected(self, tmp_path: Path):
        p = tmp_path / "reg.json"
        p.write_text(json.dumps({
            "data_flow_components": {"Foo": {"notes": "no adf_type"}},
        }), encoding="utf-8")
        with pytest.raises(ValueError, match="adf_type"):
            load_registry(p)


class TestConvertTransformationWithRegistry:
    def test_known_ssis_type_unaffected_by_empty_registry(self):
        comp = _comp("DerivedColumn", name="Calc")
        result = convert_transformation(comp, registry=EMPTY_REGISTRY)
        # DerivedColumn produces either a node or None (pure pass-through);
        # what matters is we did NOT see a registry substitution description.
        if result is not None:
            assert "REGISTRY SUBSTITUTION" not in result.get("description", "")

    def test_unknown_type_falls_through_to_generic_when_no_registry(self):
        comp = _comp("Cozyroc.MysteryComponent", name="Mystery")
        result = convert_transformation(comp)
        # _generic emits a DerivedColumn placeholder.
        assert result is not None
        assert result["type"] == "DerivedColumn"
        assert "REGISTRY SUBSTITUTION" not in result.get("description", "")

    def test_registry_substitution_takes_precedence(self):
        comp = _comp("Cozyroc.SuperLookup", name="SuperLook")
        reg = SubstitutionRegistry(data_flow={
            "Cozyroc.SuperLookup": DataFlowSubstitution(
                adf_type="Lookup",
                notes="Use prebuilt lookup dataset.",
                type_properties={"broadcast": "Auto"},
            ),
        })
        result = convert_transformation(comp, registry=reg)
        assert result is not None
        assert result["type"] == "Lookup"
        assert "REGISTRY SUBSTITUTION" in result["description"]
        assert "Use prebuilt lookup dataset." in result["description"]
        assert result["typeProperties"] == {"broadcast": "Auto"}

    def test_registry_substitution_overrides_unsupported_default(self):
        # FuzzyLookup normally maps to _unsupported (Wait placeholder);
        # the registry entry should win.
        comp = _comp("FuzzyLookup", name="Fuzz")
        reg = SubstitutionRegistry(data_flow={
            "FuzzyLookup": DataFlowSubstitution(
                adf_type="Filter",
                notes="Approximate-match handled in downstream notebook.",
            ),
        })
        result = convert_transformation(comp, registry=reg)
        assert result is not None
        assert result["type"] == "Filter"  # NOT Wait
