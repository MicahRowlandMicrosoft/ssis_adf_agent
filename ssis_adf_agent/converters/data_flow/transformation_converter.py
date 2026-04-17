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
from ...translators.ssis_expression_translator import translate_expression
from ...warnings_collector import warn
from ._naming import safe_node_name


# ---------------------------------------------------------------------------
# Aggregation type enum used in SSIS Aggregate component
# ---------------------------------------------------------------------------

_AGG_TYPE_MAP: dict[str, str] = {
    "0": "groupBy",
    "1": "min",
    "2": "max",
    "4": "sum",
    "5": "avg",
    "6": "count",
    "7": "countDistinct",
}


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
        "name": safe_node_name(component.name, fallback="Transform"),
        "description": f"SSIS {component.component_type}: {component.name}",
        "type": transform_type,
        "typeProperties": {},
    }


# ---------------------------------------------------------------------------
# DerivedColumn — reads Expression from each output column's properties
# ---------------------------------------------------------------------------

def _derived_column(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "DerivedColumn")
    columns: list[dict] = []
    for col in component.output_columns:
        ssis_expr = col.properties.get("Expression") or col.properties.get("FriendlyExpression")
        if ssis_expr:
            adf_expr = translate_expression(ssis_expr)
        else:
            # Fallback: check component-level properties keyed by column name
            ssis_expr = component.properties.get(col.name)
            adf_expr = translate_expression(ssis_expr) if ssis_expr else f"/* TODO: expression for {col.name} */"
        columns.append({"name": col.name, "expression": adf_expr})
    t["typeProperties"]["columns"] = columns
    return t


# ---------------------------------------------------------------------------
# Lookup — reads JoinToReferenceColumn from input column properties
# ---------------------------------------------------------------------------

def _lookup(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Lookup")

    # Build join conditions from input column properties
    conditions: list[dict] = []
    for col in component.input_columns:
        ref_col = col.properties.get("JoinToReferenceColumn")
        if ref_col:
            conditions.append({
                "leftColumn": col.name,
                "rightColumn": ref_col,
            })

    # Determine lookup type from component properties
    no_match_behavior = component.properties.get("NoMatchBehavior") or "0"
    # 0 = fail on no match, 1 = redirect to no-match output
    match_multiple = (component.properties.get("DefaultCodePage") or "") != ""  # heuristic

    t["typeProperties"] = {
        "lookupTable": {
            "referenceName": f"DS_{component.name.replace(' ', '_')}_lookup",
            "type": "DatasetReference",
        },
        "existsOrNotExists": "exists",
        "matchMultipleRows": False,
        "conditions": conditions if conditions else [{"leftColumn": "/* TODO */", "rightColumn": "/* TODO */"}],
    }
    return t


# ---------------------------------------------------------------------------
# ConditionalSplit — reads conditions from output-level properties
# ---------------------------------------------------------------------------

def _conditional_split(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "ConditionalSplit")
    conditions: list[dict] = []

    # Output-level conditions stored by the parser as _output_conditions
    output_conditions = component.properties.get("_output_conditions", [])
    if output_conditions:
        # Sort by EvaluationOrder if available
        sorted_conds = sorted(
            output_conditions,
            key=lambda c: int(c.get("EvaluationOrder", "999") or "999"),
        )
        for cond in sorted_conds:
            output_name = cond.get("output_name", "Branch")
            ssis_expr = cond.get("Expression") or cond.get("FriendlyExpression") or ""
            adf_expr = translate_expression(ssis_expr) if ssis_expr else f"/* TODO: condition for {output_name} */"
            # Skip default output (no expression)
            if adf_expr:
                conditions.append({
                    "name": safe_node_name(output_name, fallback="Branch"),
                    "expression": adf_expr,
                })
    else:
        # Fallback: use output columns as branch names
        for i, out_col in enumerate(component.output_columns):
            conditions.append({
                "name": out_col.name,
                "expression": f"/* TODO: split condition {i} */",
            })

    t["typeProperties"]["conditions"] = conditions
    return t


# ---------------------------------------------------------------------------
# Aggregate — reads AggregationType and AggregationColumnId from column props
# ---------------------------------------------------------------------------

def _aggregate(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Aggregate")
    group_by: list[str] = []
    aggregations: list[dict] = []

    for col in component.output_columns:
        agg_type_str = col.properties.get("AggregationType") or ""
        agg_type = _AGG_TYPE_MAP.get(agg_type_str, "")

        if agg_type == "groupBy":
            group_by.append(col.name)
        elif agg_type:
            aggregations.append({
                "column": col.name,
                "function": agg_type,
            })
        else:
            # No aggregation info — treat as pass-through / group-by
            group_by.append(col.name)

    t["typeProperties"] = {
        "groupBy": group_by,
        "aggregations": aggregations,
    }
    return t


# ---------------------------------------------------------------------------
# Sort — reads SortKeyPosition from output column properties
# ---------------------------------------------------------------------------

def _sort(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Sort")
    sort_conditions: list[dict] = []

    sort_cols = []
    # SortKeyPosition can appear on either input or output columns depending on
    # SSIS version. Modern packages use NewSortKeyPosition (set by the Sort
    # component on its input columns) or cachedSortKeyPosition (set by
    # downstream components).
    for col in list(component.input_columns) + list(component.output_columns):
        pos_str = (
            col.properties.get("NewSortKeyPosition")
            or col.properties.get("SortKeyPosition")
            or "0"
        )
        try:
            pos = int(pos_str)
        except (ValueError, TypeError):
            pos = 0
        if pos != 0:
            sort_cols.append((abs(pos), col.name, "asc" if pos > 0 else "desc"))

    # Sort by position
    sort_cols.sort(key=lambda x: x[0])
    for _, name, order in sort_cols:
        sort_conditions.append({"column": name, "order": order})

    t["typeProperties"] = {
        "sortConditions": sort_conditions if sort_conditions else [{"column": "/* TODO */", "order": "asc"}],
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


# ---------------------------------------------------------------------------
# MergeJoin — reads join type + SortKeyPosition from input columns
# ---------------------------------------------------------------------------

def _merge_join(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Join")
    join_type = component.properties.get("JoinType") or "inner"

    # MergeJoin uses (New)SortKeyPosition on input columns to identify join keys
    # Group by lineageId or position
    conditions: list[dict] = []
    join_keys = []
    for col in component.input_columns:
        pos_str = (
            col.properties.get("NewSortKeyPosition")
            or col.properties.get("SortKeyPosition")
            or "0"
        )
        try:
            pos = int(pos_str)
        except (ValueError, TypeError):
            pos = 0
        if pos != 0:
            join_keys.append((abs(pos), col.name))

    # Pair join keys: SSIS pairs them by position (1st left with 1st right)
    # We only have a flat list, so pair by sort key position
    if join_keys:
        join_keys.sort(key=lambda x: x[0])
        # Heuristic: first half are left keys, second half are right keys
        mid = len(join_keys) // 2
        left_keys = join_keys[:mid] if mid > 0 else join_keys
        right_keys = join_keys[mid:] if mid > 0 else []
        for i in range(max(len(left_keys), len(right_keys))):
            left = left_keys[i][1] if i < len(left_keys) else "/* TODO */"
            right = right_keys[i][1] if i < len(right_keys) else "/* TODO */"
            conditions.append({"leftColumn": left, "rightColumn": right})

    t["typeProperties"] = {
        "joinType": str(join_type).lower(),
        "conditions": conditions if conditions else [{"leftColumn": "/* TODO */", "rightColumn": "/* TODO */"}],
    }
    return t


# ---------------------------------------------------------------------------
# DataConversion — maps SSIS data type conversions to ADF Cast
# ---------------------------------------------------------------------------

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
    warn(
        phase="convert", severity="warning",
        source="transformation_converter",
        message=f"Script Component '{component.name}' requires manual implementation",
        detail="Mapped to ExternalCall placeholder — implement in Azure Function or Databricks",
    )
    return t


def _unsupported(component: DataFlowComponent) -> dict[str, Any]:
    t = _base(component, "Wait")
    t["description"] = (
        f"[UNSUPPORTED — {component.component_type}] Manual implementation required. "
        "This component has no ADF Mapping Data Flow equivalent."
    )
    warn(
        phase="convert", severity="warning",
        source="transformation_converter",
        message=f"Unsupported component type '{component.component_type}' in '{component.name}'",
        detail="No ADF Mapping Data Flow equivalent — emitting placeholder Wait transformation",
    )
    return t


def _generic(component: DataFlowComponent) -> dict[str, Any]:
    warn(
        phase="convert", severity="warning",
        source="transformation_converter",
        message=f"Unknown component type '{component.component_type}' in '{component.name}'",
        detail="Emitting empty DerivedColumn placeholder — manual review needed",
    )
    return {
        "name": safe_node_name(component.name, fallback="Transform"),
        "description": f"[Unknown component type: {component.component_type}] — manual review needed.",
        "type": "DerivedColumn",
        "typeProperties": {"columns": []},
    }
