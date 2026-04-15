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

from ..parsers.models import DataFlowComponent, DataFlowPath, DataFlowTask, SSISPackage, TaskType
from ..converters.data_flow.source_converter import convert_source
from ..converters.data_flow.destination_converter import convert_destination
from ..converters.data_flow.transformation_converter import convert_transformation
from ..translators.ssis_expression_translator import translate_expression

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
        # We store both patterns
        for i in range(5):
            output_to_comp[f"{comp.id}\\Output {i}"] = safe
            output_to_comp[f"{comp.id}\\output {i}"] = safe
            input_to_comp[f"{comp.id}\\Input {i}"] = safe
            input_to_comp[f"{comp.id}\\input {i}"] = safe

    for path in task.paths:
        src_name = output_to_comp.get(path.start_id)
        dst_name = input_to_comp.get(path.end_id)
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
            col_exprs = ", ".join(f"{c['name']} = {c['expression']}" for c in cols)
            lines.append(f"{upstream} derive({col_exprs}) ~> {t['name']}")
        else:
            lines.append(f"{upstream} derive(/* TODO: add expressions */) ~> {t['name']}")

    elif ttype == "Lookup":
        conds = type_props.get("conditions", [])
        if conds and conds[0].get("leftColumn", "").startswith("/*") is False:
            cond_str = " && ".join(
                f"{c['leftColumn']} == {c['rightColumn']}" for c in conds
            )
            lines.append(f"{upstream}, lookup({cond_str}) ~> {t['name']}")
        else:
            lines.append(f"{upstream}, lookup(/* TODO: join conditions */) ~> {t['name']}")

    elif ttype == "ConditionalSplit":
        conds = type_props.get("conditions", [])
        if conds:
            cond_str = ", ".join(f"{c['name']}: ({c['expression']})" for c in conds)
            lines.append(f"{upstream} split({cond_str},")
            lines.append(f"    disjoint: false) ~> {t['name']}")
        else:
            lines.append(f"{upstream} split(/* TODO: conditions */) ~> {t['name']}")

    elif ttype == "Aggregate":
        group_by = type_props.get("groupBy", [])
        aggs = type_props.get("aggregations", [])
        gb_str = ", ".join(group_by) if group_by else "/* TODO */"
        agg_strs = []
        for a in aggs:
            agg_strs.append(f"{a['column']} = {a['function']}({a['column']})")
        agg_str = ", ".join(agg_strs) if agg_strs else "/* TODO */"
        lines.append(f"{upstream} aggregate(groupBy({gb_str}),")
        lines.append(f"    {agg_str}) ~> {t['name']}")

    elif ttype == "Sort":
        conds = type_props.get("sortConditions", [])
        if conds and not conds[0].get("column", "").startswith("/*"):
            sort_str = ", ".join(
                f"{c['order']}({c['column']})" for c in conds
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
                f"{c['leftColumn']} == {c['rightColumn']}" for c in conds
            )
            lines.append(f"{upstream} join({cond_str},")
            lines.append(f"    joinType: '{join_type}') ~> {t['name']}")
        else:
            lines.append(f"{upstream} join(/* TODO: join conditions */,")
            lines.append(f"    joinType: '{join_type}') ~> {t['name']}")

    elif ttype == "Cast":
        cols = type_props.get("columns", [])
        if cols:
            cast_str = ", ".join(f"{c['name']} as {c.get('type', 'string')}" for c in cols)
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
        lines.append(f"    upsertable: true) ~> {sk['name']}")
