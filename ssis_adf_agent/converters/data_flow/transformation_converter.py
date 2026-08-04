"""
Data Flow transformation converter — maps SSIS Data Flow transformation component
types to ADF Mapping Data Flow transformation JSON snippets.

Each function returns a dict that goes into the ``transformations`` array of the
Mapping Data Flow JSON.
"""
from __future__ import annotations

import re
from typing import Any, Literal

from ...parsers.models import DataFlowComponent
from ...translators.ssis_expression_translator import translate_expression
from ...warnings_collector import warn
from ..substitution_registry import (
    EMPTY_REGISTRY,
    DataFlowSubstitution,
    SubstitutionRegistry,
)
from ._naming import safe_node_name

# DerivedColumn handling mode (see _derived_column).
DerivedColumnMode = Literal["preserve", "drop_passthrough", "rename_to_expression"]

# Matches the SSDT default name pattern that authors leave in place when they
# add a derived column and don't bother renaming it ("Derived Column 1",
# "Derived Column 2", ...).  These names are useless in ADF and almost always
# indicate the author meant to "Replace existing column" rather than "Add as
# new column".
_DEFAULT_DERIVED_NAME_RE = re.compile(r"^Derived Column \d+$")

# Matches a single bare identifier (the translated form of a no-op pass-through
# expression like FriendlyExpression="Biennium").  Used to detect cases where
# we can safely rename or drop the column.
_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

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


def convert_transformation(
    component: DataFlowComponent,
    *,
    registry: SubstitutionRegistry = EMPTY_REGISTRY,
    derived_column_mode: DerivedColumnMode = "preserve",
) -> dict[str, Any] | None:
    """
    Dispatch to the right transformation builder based on component_type.
    Returns None for component types that should be silently skipped.

    M7 — if ``registry`` declares a substitution for this component_type
    (typically a 3rd-party Cozyroc / KingswaySoft / in-house component), use
    it instead of falling through to the generic placeholder. The substitution
    short-circuits everything below — it is the customer's responsibility to
    ensure the chosen ADF transformation type is valid.

    ``derived_column_mode`` controls how DerivedColumn output columns left at
    the SSDT default name ("Derived Column 1", ...) are emitted.  See
    ``_derived_column`` for behaviour.
    """
    sub = registry.lookup_data_flow(component.component_type)
    if sub is not None:
        return _from_substitution(component, sub)

    dispatch: dict[str, Any] = {
        "DerivedColumn": lambda c: _derived_column(c, mode=derived_column_mode),
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

def _derived_column(
    component: DataFlowComponent,
    *,
    mode: DerivedColumnMode = "preserve",
) -> dict[str, Any] | None:
    """Convert an SSIS DerivedColumn to an ADF DerivedColumn transformation.

    SSDT auto-names new derived columns "Derived Column 1", "Derived Column 2",
    ... and many authors never rename them.  When that happens the ADF data
    flow ends up unreadable and — worse — usually contains pure pass-through
    columns that just duplicate a source column under a meaningless name.

    ``mode`` controls how those default-named columns are handled:

    * ``"preserve"`` (default) — keep the original names.  Emits an info-level
      warning when default names are detected so the user knows the option
      to re-convert with a different mode exists.
    * ``"drop_passthrough"`` — when the expression is a single bare column
      reference, omit the output column entirely; ``allowSchemaDrift`` lets
      the underlying source column flow through.
    * ``"rename_to_expression"`` — when the expression is a single bare column
      reference and that name doesn't collide with an existing input/output
      column, rename the output column to the referenced source column.
      Falls back to preserving the original name on collision.
    """
    t = _base(component, "DerivedColumn")

    # Build a set of names already used elsewhere in this component, for
    # rename-collision detection.  Includes input columns and any non-default
    # output column names.
    reserved_names: set[str] = {c.name for c in component.input_columns if c.name}
    reserved_names.update(
        c.name for c in component.output_columns
        if c.name and not _DEFAULT_DERIVED_NAME_RE.match(c.name)
    )

    columns: list[dict] = []
    used_renames: set[str] = set()
    default_named_count = 0
    default_named_bare_ref_count = 0
    default_named_complex_count = 0
    default_named_examples: list[dict[str, str]] = []
    renamed_count = 0
    dropped_count = 0
    collision_count = 0

    for col in component.output_columns:
        # Prefer FriendlyExpression (uses column names) over Expression (uses
        # lineage IDs like #{Package\...\Columns[X]} that ADF cannot parse).
        ssis_expr = col.properties.get("FriendlyExpression") or col.properties.get("Expression")
        if ssis_expr:
            adf_expr = translate_expression(ssis_expr)
        else:
            # Fallback: check component-level properties keyed by column name
            ssis_expr = component.properties.get(col.name)
            adf_expr = translate_expression(ssis_expr) if ssis_expr else f"/* TODO: expression for {col.name} */"

        # Skip pure pass-through columns where the expression is just the
        # column's own name (no transformation, no rename).  These flow
        # automatically via allowSchemaDrift.
        stripped = adf_expr.strip()
        if stripped == col.name or stripped == (col.name or "").strip("{}"):
            continue

        is_default = bool(col.name and _DEFAULT_DERIVED_NAME_RE.match(col.name))
        is_bare_ref = bool(_BARE_IDENT_RE.match(stripped))

        if is_default:
            default_named_count += 1
            if is_bare_ref:
                default_named_bare_ref_count += 1
            else:
                default_named_complex_count += 1
            # Capture up to 5 examples for the warning payload.
            if len(default_named_examples) < 5:
                default_named_examples.append({
                    "output_name": col.name,
                    "expression": adf_expr,
                })

            # Mode-specific handling for default-named columns whose
            # expression is a bare source-column reference.
            if is_bare_ref:
                if mode == "drop_passthrough":
                    dropped_count += 1
                    continue
                if mode == "rename_to_expression":
                    if stripped in reserved_names or stripped in used_renames:
                        collision_count += 1
                        # Fall through to preserve the original name below.
                    else:
                        used_renames.add(stripped)
                        renamed_count += 1
                        columns.append({"name": stripped, "expression": adf_expr})
                        continue

        columns.append({"name": col.name, "expression": adf_expr})

    # Surface a warning explaining the situation and the user's options.
    if default_named_count:
        # Recommend the cleanest mode based on the actual expression mix.
        # If every default-named column is a bare reference, drop_passthrough
        # is the safest aggressive cleanup.  If they're a mix, rename is
        # safer because it preserves the column.  If none are bare refs,
        # there's nothing the modes can do and preserve is correct.
        if default_named_complex_count == 0 and default_named_bare_ref_count > 0:
            recommended_mode = "drop_passthrough"
        elif default_named_bare_ref_count > 0:
            recommended_mode = "rename_to_expression"
        else:
            recommended_mode = "preserve"

        metadata: dict[str, Any] = {
            "component_name": component.name,
            "default_named_count": default_named_count,
            "bare_ref_count": default_named_bare_ref_count,
            "complex_expr_count": default_named_complex_count,
            "examples": default_named_examples,
            "recommended_mode": recommended_mode,
            "current_mode": mode,
        }

        if mode == "preserve":
            warn(
                phase="convert",
                severity="warning",
                source="data_flow.derived_column",
                message=(
                    f"DerivedColumn '{component.name}' has {default_named_count} output "
                    f"column(s) using the SSDT default name pattern ('Derived Column N'). "
                    f"The original SSIS author never renamed them in the designer."
                ),
                detail=(
                    "These columns will appear in the generated ADF data flow with the "
                    "same placeholder names. To clean them up, re-run convert_ssis_package "
                    "with one of:\n"
                    "  derived_column_mode='rename_to_expression' — when the expression is "
                    "a bare source-column reference (e.g. FriendlyExpression='Biennium'), "
                    "rename the output column to that source column. Skipped on collision.\n"
                    "  derived_column_mode='drop_passthrough' — drop pure pass-through "
                    "columns entirely; ADF allowSchemaDrift carries the underlying source "
                    "column through unchanged. Most aggressive, cleanest output.\n"
                    f"Default is 'preserve' (current behaviour) which keeps names as-is. "
                    f"Recommended for this component: '{recommended_mode}'."
                ),
                metadata=metadata,
            )
        elif mode == "rename_to_expression":
            parts: list[str] = []
            if renamed_count:
                parts.append(f"renamed {renamed_count}")
            if collision_count:
                parts.append(f"kept {collision_count} as-is (name collision with source/sibling column)")
            kept = default_named_count - renamed_count - collision_count
            if kept:
                parts.append(f"kept {kept} as-is (expression is not a bare column reference)")
            metadata["renamed_count"] = renamed_count
            metadata["collision_count"] = collision_count
            metadata["kept_count"] = kept
            warn(
                phase="convert",
                severity="info",
                source="data_flow.derived_column",
                message=(
                    f"DerivedColumn '{component.name}' default-named columns: "
                    + (", ".join(parts) if parts else "no changes applied") + "."
                ),
                metadata=metadata,
            )
        elif mode == "drop_passthrough":
            kept = default_named_count - dropped_count
            metadata["dropped_count"] = dropped_count
            metadata["kept_count"] = kept
            warn(
                phase="convert",
                severity="info",
                source="data_flow.derived_column",
                message=(
                    f"DerivedColumn '{component.name}': dropped {dropped_count} pass-through "
                    f"column(s) with default names" + (
                        f"; kept {kept} (expression is not a bare column reference)" if kept else ""
                    ) + "."
                ),
                detail=(
                    "Source columns flow through unchanged via allowSchemaDrift."
                ),
                metadata=metadata,
            )

    # If no meaningful columns remain, this DerivedColumn is a no-op — skip it
    # so the generator can fall back to a Copy Activity if no other transforms exist.
    if not columns:
        return None

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

    # TODO: surface NoMatchBehavior / match-multiple semantics from
    # component.properties when emitting the Lookup transformation.

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


def _from_substitution(
    component: DataFlowComponent,
    sub: DataFlowSubstitution,
) -> dict[str, Any]:
    """Build a transformation node from a substitution-registry entry (M7)."""
    t = _base(component, sub.adf_type)
    t["description"] = (
        f"[REGISTRY SUBSTITUTION] {component.component_type} -> {sub.adf_type}"
        + (f" — {sub.notes}" if sub.notes else "")
    )
    if sub.type_properties:
        t["typeProperties"] = dict(sub.type_properties)
    warn(
        phase="convert", severity="info",
        source="transformation_converter",
        message=(
            f"Component '{component.name}' ({component.component_type}) "
            f"replaced by registry substitution -> {sub.adf_type}"
        ),
        detail=sub.notes or "Substitution registry entry applied verbatim.",
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
