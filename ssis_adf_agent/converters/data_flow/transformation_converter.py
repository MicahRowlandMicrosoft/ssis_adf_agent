"""
Data Flow transformation converter — maps SSIS Data Flow transformation component
types to ADF Mapping Data Flow transformation JSON snippets.

Each function returns a dict that goes into the ``transformations`` array of the
Mapping Data Flow JSON.
"""
from __future__ import annotations

import re
from typing import Any

from ...parsers.models import DataFlowComponent


def convert_transformation(component: DataFlowComponent) -> dict[str, Any] | None:
    """
    Dispatch to the right transformation builder based on component_type.
    Returns None for component types that should be silently skipped.
    """
    dispatch: dict[str, Any] = {
        "DerivedColumn": _derived_column,
        "Lookup": _lookup,
        "ConditionalSplit": _conditional_split,
        "Aggregate": _aggregate,
        "Sort": _sort,
        "UnionAll": _union_all,
        "Merge": _merge,
        "MergeJoin": _merge_join,
        "DataConversion": _data_conversion,
        "RowCount": _row_count,
        "Multicast": _multicast,
        "CharacterMap": _character_map,
        "ScriptComponent": _script_component,
        "FuzzyLookup": _unsupported,
        "FuzzyGrouping": _unsupported,
        "TermExtraction": _unsupported,
        "TermLookup": _unsupported,
        "ExportColumn": _unsupported,
        "ImportColumn": _unsupported,
        "Cache": _unsupported,
        "RecordsetDestination": _unsupported,
    }

    builder = dispatch.get(component.component_type)
    if builder is None:
        return _generic(component)
    return builder(component)


def _base(component: DataFlowComponent, transform_type: str) -> dict[str, Any]:
    return {
        "name": component.name.replace(" ", "_"),
        "description": f"SSIS {component.component_type}: {component.name}",
        "type": transform_type,
        "typeProperties": {},
    }


def _derived_column(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "DerivedColumn")
    # Try to build column expressions from properties
    columns: list[dict] = []
    for col in component.output_columns:
        # Expression may be stored in properties keyed by column name
        expr = component.properties.get(col.name) or f"/* TODO: expression for {col.name} */"
        columns.append({"name": col.name, "expression": expr})
    t["typeProperties"]["columns"] = columns
    return t


def _lookup(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Lookup")
    ref_conn = component.connection_id or "unknown"
    t["typeProperties"] = {
        "lookupTable": {
            "referenceName": f"DS_{component.name.replace(' ', '_')}_lookup",
            "type": "DatasetReference",
        },
        "existsOrNotExists": "exists",
        "matchMultipleRows": False,
        "conditions": [],  # TODO: populate join conditions from component.properties
    }
    return t


def _conditional_split(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "ConditionalSplit")
    conditions: list[dict] = []
    # Each output beyond the first is a named condition
    for i, out_col in enumerate(component.output_columns):
        conditions.append({
            "name": out_col.name,
            "expression": f"/* TODO: split condition {i} */",
        })
    t["typeProperties"]["conditions"] = conditions
    return t


def _aggregate(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Aggregate")
    t["typeProperties"] = {
        "groupBy": [],     # TODO: map from component.properties
        "aggregations": [],
    }
    return t


def _sort(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Sort")
    t["typeProperties"] = {
        "sortConditions": [],  # TODO: map from component.properties
        "caseSensitive": False,
    }
    return t


def _union_all(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Union")
    t["typeProperties"] = {}
    return t


def _merge(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Union")
    t["description"] += " [SSIS Merge — inputs must be pre-sorted]"
    return t


def _merge_join(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Join")
    join_type = component.properties.get("JoinType") or "inner"
    t["typeProperties"] = {
        "joinType": str(join_type).lower(),
        "conditions": [],   # TODO: extract from component.properties
    }
    return t


def _data_conversion(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Cast")
    columns: list[dict] = []
    for col in component.output_columns:
        columns.append({
            "name": col.name,
            "type": col.data_type.value,
            "length": col.length or None,
            "scale": col.scale or None,
        })
    t["typeProperties"]["columns"] = columns
    return t


def _row_count(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "SetVariable")
    var_name = component.properties.get("VariableName") or "RowCount"
    var_name_short = var_name.split("::")[-1]
    t["typeProperties"] = {
        "variableName": var_name_short,
        "value": "/* populated by Mapping Data Flow rowCount() */",
    }
    return t


def _multicast(component: DataFlowComponent) -> dict[str, Any]:
    # Multicast fans out the stream; in ADF you simply reference the same stream
    # in multiple downstream branches — no explicit transformation needed.
    return None  # type: ignore[return-value]


def _character_map(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "DerivedColumn")
    cols = []
    for col in component.output_columns:
        op = component.properties.get("MapFlags") or "upper"
        cols.append({"name": col.name, "expression": f"{op}({col.name})"})
    t["typeProperties"]["columns"] = cols
    return t


def _script_component(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "ExternalCall")
    t["description"] = (
        "[MANUAL REVIEW REQUIRED] Script Component has been mapped to an ExternalCall "
        "transformation. Implement logic in Azure Function / Databricks."
    )
    t["typeProperties"] = {"functionName": f"TODO_{component.name.replace(' ', '_')}"}
    return t


def _unsupported(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Wait")
    t["description"] = (
        f"[UNSUPPORTED — {component.component_type}] Manual implementation required. "
        "This component has no ADF Mapping Data Flow equivalent."
    )
    return t


def _generic(component: DataFlowComponent) -> dict[str, Any]:
    return {
        "name": component.name.replace(" ", "_"),
        "description": f"[Unknown component type: {component.component_type}] — manual review needed.",
        "type": "DerivedColumn",
        "typeProperties": {"columns": []},
    }
