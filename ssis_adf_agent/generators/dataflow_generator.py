"""
Mapping Data Flow generator — emits ADF dataflow.json for each complex Data Flow Task.

Simple (single source → single destination, no transforms) data flows are handled
as Copy Activities; this generator only fires for multi-component flows.

Best practices applied:
  - allowSchemaDrift: true, validateSchema: false (Microsoft default for flexibility)
  - errorHandlingOption: stopOnFirstError (explicit)
  - Isolation level: READ_UNCOMMITTED for sources (Microsoft default for data flows)
  - Configurable compute settings (coreCount, computeType)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import re as _re

from ..parsers.models import DataFlowComponent, DataFlowPath, DataFlowTask, DataType, SSISPackage, TaskType
from ..converters.data_flow.source_converter import convert_source
from ..converters.data_flow.destination_converter import convert_destination
from ..converters.data_flow.transformation_converter import convert_transformation
from ..translators.ssis_expression_translator import translate_expression
from ..warnings_collector import warn

# ---------------------------------------------------------------------------
# SSIS DataType → ADF Mapping Data Flow DSL type
# ---------------------------------------------------------------------------

_DATATYPE_TO_DSL: dict[DataType, str] = {
    DataType.INT8: "short",
    DataType.INT16: "short",
    DataType.INT32: "integer",
    DataType.INT64: "long",
    DataType.UINT8: "short",
    DataType.UINT16: "integer",
    DataType.UINT32: "long",
    DataType.UINT64: "long",
    DataType.FLOAT: "float",
    DataType.DOUBLE: "double",
    DataType.CURRENCY: "decimal",
    DataType.DECIMAL: "decimal",
    DataType.BOOLEAN: "boolean",
    DataType.STRING: "string",
    DataType.WSTRING: "string",
    DataType.BYTES: "binary",
    DataType.DATE: "date",
    DataType.DBDATE: "date",
    DataType.DBTIME: "string",
    DataType.DBTIMESTAMP: "timestamp",
    DataType.GUID: "string",
    DataType.EMPTY: "string",
}

# ---------------------------------------------------------------------------
# Mapping Data Flow DSL identifier quoting
# ---------------------------------------------------------------------------

# Bare DSL identifiers must match [A-Za-z_][A-Za-z0-9_]*. Anything else
# (spaces, hyphens, special chars, leading digits) needs to be wrapped in
# curly braces, e.g. {Posting Fiscal Month}. Embedded '}' must be escaped.
_BARE_IDENT_RE = _re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _q(name: str) -> str:
    """Quote a column / stream name for ADF Mapping Data Flow DSL.

    Returns the bare name when it's a valid identifier, otherwise wraps it
    in curly braces with embedded '}' escaped.
    """
    if not name:
        return "{}"
    if _BARE_IDENT_RE.match(name):
        return name
    return "{" + name.replace("}", "\\}") + "}"


_SOURCE_TYPES = frozenset({
    "OleDbSource", "FlatFileSource", "ExcelSource", "OdbcSource",
    "ADONetSource", "SqlServerSource",
})
_DEST_TYPES = frozenset({
    "OleDbDestination", "FlatFileDestination", "ExcelDestination",
    "OdbcDestination", "ADONetDestination", "SqlServerDestination",
    "RecordsetDestination",
})


def generate_data_flows(
    package: SSISPackage,
    output_dir: Path,
) -> list[dict[str, Any]]:
    """
    For every complex Data Flow Task in the package, generate a Mapping Data Flow JSON.
    Files are written to *output_dir*/dataflow/.

    Returns the list of data flow dicts.
    """
    df_dir = output_dir / "dataflow"
    df_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []

    for task in package.tasks:
        if task.task_type != TaskType.DATA_FLOW:
            continue
        assert isinstance(task, DataFlowTask)

        sources_comps = [c for c in task.components if c.component_type in _SOURCE_TYPES]
        dest_comps = [c for c in task.components if c.component_type in _DEST_TYPES]
        transform_comps = [
            c for c in task.components
            if c.component_type not in _SOURCE_TYPES
            and c.component_type not in _DEST_TYPES
        ]

        # Only generate a Mapping Data Flow for tasks with transforms (or multiple sources/sinks)
        if not transform_comps and len(sources_comps) <= 1 and len(dest_comps) <= 1:
            continue  # handled as Copy Activity

        df_name = f"DF_{task.name.replace(' ', '_')}"

        sources = [convert_source(c) for c in sources_comps]
        sinks = [convert_destination(c) for c in dest_comps]
        transformations: list[dict[str, Any]] = []
        for comp in transform_comps:
            t = convert_transformation(comp)
            if t is not None:
                transformations.append(t)

        # Collect key columns from destination components for upsert config
        key_cols: list[str] = []
        for comp in dest_comps:
            if comp.key_columns:
                key_cols.extend(comp.key_columns)

        # Build data flow script using topology from parsed paths
        script = _build_dsl_script(sources, transformations, sinks, key_cols, task)

        # Strip private metadata fields (Pydantic models) before JSON serialization
        for _d in (*sources, *sinks):
            for _k in ("_output_columns", "_input_columns", "_key_columns"):
                _d.pop(_k, None)

        df: dict[str, Any] = {
            "name": df_name,
            "properties": {
                "description": f"Mapping Data Flow for SSIS Data Flow Task: {task.name}",
                "type": "MappingDataFlow",
                "typeProperties": {
                    "sources": sources,
                    "sinks": sinks,
                    "transformations": transformations,
                    "script": script,
                    "scriptLines": script.splitlines(),
                },
                "annotations": ["ssis-adf-agent"],
            },
        }

        (df_dir / f"{df_name}.json").write_text(
            json.dumps(df, indent=4, ensure_ascii=False),
            encoding="utf-8",
        )
        results.append(df)

    return results


def _build_dsl_script(
    sources: list[dict],
    transformations: list[dict],
    sinks: list[dict],
    key_columns: list[str] | None = None,
    task: DataFlowTask | None = None,
) -> str:
    """
    Build an ADF Data Flow DSL script that follows the actual component topology.

    Uses ``DataFlowPath`` from the parsed task to determine how components
    chain together.  Falls back to linear chaining if no paths are available.
    """
    lines: list[str] = []

    # Build a topology map: component_output_id → next component name
    # DataFlowPath.start_id is the output ID of the upstream component
    # DataFlowPath.end_id is the input ID of the downstream component
    # We map by name for the DSL script
    all_names = {s["name"] for s in sources} | {t["name"] for t in transformations} | {s["name"] for s in sinks}

    # Build predecessor map from DataFlowPaths if available
    predecessors: dict[str, list[str]] = {name: [] for name in all_names}
    if task and task.paths:
        _build_topology(task, predecessors)

    # If no topology was resolved, fall back to linear chaining
    if not any(predecessors.values()):
        if task and not task.paths:
            warn(
                phase="convert",
                severity="warning",
                source="dataflow_generator",
                message=f"Data flow '{task.name}' has no parsed paths — using linear chain fallback",
                detail="Topology may not match actual SSIS execution order; review the generated data flow",
            )
        _linear_chain(sources, transformations, sinks, predecessors)

    # Emit sources
    for s in sources:
        _emit_source(lines, s)

    # Emit transformations with correct upstream references
    for t in transformations:
        preds = predecessors.get(t["name"], [])
        upstream = preds[0] if preds else (sources[-1]["name"] if sources else "source1")
        _emit_transformation(lines, t, upstream)

    # Emit sinks
    for sk in sinks:
        preds = predecessors.get(sk["name"], [])
        upstream = preds[0] if preds else (
            transformations[-1]["name"] if transformations
            else (sources[-1]["name"] if sources else "source1")
        )
        _emit_sink(lines, sk, upstream, key_columns)

    return "\n".join(lines)


def _build_topology(
    task: DataFlowTask,
    predecessors: dict[str, list[str]],
) -> None:
    """Resolve DataFlowPath start/end IDs to component names."""
    # Build maps: output_id → component name, input_id → component name
    output_to_comp: dict[str, str] = {}
    input_to_comp: dict[str, str] = {}

    for comp in task.components:
        safe = comp.name.replace(" ", "_")
        # A component's ID often appears in path start/end refs
        output_to_comp[comp.id] = safe
        input_to_comp[comp.id] = safe
        # SSIS uses output/input IDs — the path start_id references
        # the output ID which is typically encoded as "{compId}\Output{N}"
        # Compute dynamic range from actual output columns + generous margin
        max_io = max(len(comp.output_columns), len(comp.input_columns), 5)
        for i in range(max_io):
            output_to_comp[f"{comp.id}\\Output {i}"] = safe
            output_to_comp[f"{comp.id}\\output {i}"] = safe
            input_to_comp[f"{comp.id}\\Input {i}"] = safe
            input_to_comp[f"{comp.id}\\input {i}"] = safe

    for path in task.paths:
        src_name = output_to_comp.get(path.start_id)
        dst_name = input_to_comp.get(path.end_id)
        # Fallback: try prefix-matching for IDs not in the pre-built map
        if not src_name:
            for comp in task.components:
                if path.start_id.startswith(comp.id):
                    src_name = comp.name.replace(" ", "_")
                    break
        if not dst_name:
            for comp in task.components:
                if path.end_id.startswith(comp.id):
                    dst_name = comp.name.replace(" ", "_")
                    break
        if src_name and dst_name and dst_name in predecessors:
            predecessors[dst_name].append(src_name)


def _linear_chain(
    sources: list[dict],
    transformations: list[dict],
    sinks: list[dict],
    predecessors: dict[str, list[str]],
) -> None:
    """Fallback: chain all components linearly."""
    prev: str | None = None
    for s in sources:
        if prev:
            predecessors[s["name"]].append(prev)
        prev = s["name"]
    for t in transformations:
        if prev:
            predecessors[t["name"]].append(prev)
        prev = t["name"]
    for sk in sinks:
        if prev:
            predecessors[sk["name"]].append(prev)
        prev = sk["name"]


def _emit_source(lines: list[str], s: dict) -> None:
    cols = s.get("_output_columns", [])
    if cols:
        col_defs = ",\n        ".join(
            f"{_q(c.name)} as {_DATATYPE_TO_DSL.get(c.data_type, 'string')}" for c in cols
        )
        lines.append(f"source(output(")
        lines.append(f"        {col_defs}")
        lines.append(f"    ),")
    else:
        lines.append(f"source(output(/* TODO: declare output schema */),")
    lines.append(f"    allowSchemaDrift: true,")
    lines.append(f"    validateSchema: false,")
    lines.append(f"    isolationLevel: 'READ_UNCOMMITTED',")
    lines.append(f"    errorHandlingOption: 'stopOnFirstError') ~> {s['name']}")


def _emit_transformation(lines: list[str], t: dict, upstream: str) -> None:
    ttype = t.get("type", "DerivedColumn")
    type_props = t.get("typeProperties", {})

    if ttype == "DerivedColumn":
        cols = type_props.get("columns", [])
        if cols:
            col_exprs = ", ".join(f"{_q(c['name'])} = {c['expression']}" for c in cols)
            lines.append(f"{upstream} derive({col_exprs}) ~> {t['name']}")
        else:
            lines.append(f"{upstream} derive(/* TODO: add expressions */) ~> {t['name']}")

    elif ttype == "Lookup":
        conds = type_props.get("conditions", [])
        if conds and conds[0].get("leftColumn", "").startswith("/*") is False:
            cond_str = " && ".join(
                f"{_q(c['leftColumn'])} == {_q(c['rightColumn'])}" for c in conds
            )
            lines.append(f"{upstream}, lookup({cond_str}) ~> {t['name']}")
        else:
            lines.append(f"{upstream}, lookup(/* TODO: join conditions */) ~> {t['name']}")

    elif ttype == "ConditionalSplit":
        conds = type_props.get("conditions", [])
        if conds:
            cond_str = ", ".join(f"{c['name']}: ({c['expression']})" for c in conds)
            lines.append(f"{upstream} split({cond_str},")
            lines.append(f"    disjoint: true) ~> {t['name']}")
        else:
            lines.append(f"{upstream} split(/* TODO: conditions */) ~> {t['name']}")

    elif ttype == "Aggregate":
        group_by = type_props.get("groupBy", [])
        aggs = type_props.get("aggregations", [])
        gb_str = ", ".join(_q(g) for g in group_by) if group_by else "/* TODO */"
        agg_strs = []
        for a in aggs:
            agg_strs.append(f"{_q(a['column'])} = {a['function']}({_q(a['column'])})")
        agg_str = ", ".join(agg_strs) if agg_strs else "/* TODO */"
        lines.append(f"{upstream} aggregate(groupBy({gb_str}),")
        lines.append(f"    {agg_str}) ~> {t['name']}")

    elif ttype == "Sort":
        conds = type_props.get("sortConditions", [])
        if conds and not conds[0].get("column", "").startswith("/*"):
            sort_str = ", ".join(
                f"{c['order']}({_q(c['column'])})" for c in conds
            )
            lines.append(f"{upstream} sort({sort_str}) ~> {t['name']}")
        else:
            lines.append(f"{upstream} sort(/* TODO: sort columns */) ~> {t['name']}")

    elif ttype == "Union":
        lines.append(f"{upstream} union(byName: true) ~> {t['name']}")

    elif ttype == "Join":
        join_type = type_props.get("joinType", "inner")
        conds = type_props.get("conditions", [])
        if conds and not conds[0].get("leftColumn", "").startswith("/*"):
            cond_str = " && ".join(
                f"{_q(c['leftColumn'])} == {_q(c['rightColumn'])}" for c in conds
            )
            lines.append(f"{upstream} join({cond_str},")
            lines.append(f"    joinType: '{join_type}') ~> {t['name']}")
        else:
            lines.append(f"{upstream} join(/* TODO: join conditions */,")
            lines.append(f"    joinType: '{join_type}') ~> {t['name']}")

    elif ttype == "Cast":
        cols = type_props.get("columns", [])
        if cols:
            cast_str = ", ".join(f"{_q(c['name'])} as {c.get('type', 'string')}" for c in cols)
            lines.append(f"{upstream} cast({cast_str}) ~> {t['name']}")
        else:
            lines.append(f"{upstream} cast(/* TODO */) ~> {t['name']}")

    else:
        lines.append(f"{upstream} derive(/* TODO: {ttype} */) ~> {t['name']}")


def _emit_sink(
    lines: list[str],
    sk: dict,
    upstream: str,
    key_columns: list[str] | None,
) -> None:
    has_keys = bool(key_columns)
    # NOTE: We intentionally do NOT emit an implicit `select(mapColumn(...)) ~> <sink>_mapped`
    # step here. Any node referenced in the script DSL must also be declared in
    # typeProperties.transformations, otherwise ADF rejects the data flow with
    # "Unable to parse". With allowSchemaDrift: true the sink passes columns through
    # automatically; if explicit column mapping is needed, add a proper named
    # Select transformation to the transformations array.

    lines.append(f"{upstream} sink(allowSchemaDrift: true,")
    lines.append(f"    validateSchema: false,")
    lines.append(f"    errorHandlingOption: 'stopOnFirstError',")
    if has_keys:
        keys_str = ", ".join(f"'{k}'" for k in key_columns)
        lines.append(f"    keys: [{keys_str}],")
        lines.append(f"    deletable: false,")
        lines.append(f"    insertable: true,")
        lines.append(f"    updateable: true,")
        lines.append(f"    upsertable: true) ~> {sk['name']}")
    else:
        lines.append(f"    deletable: false,")
        lines.append(f"    insertable: true,")
        lines.append(f"    updateable: false,")
        lines.append(f"    upsertable: false) ~> {sk['name']}")
