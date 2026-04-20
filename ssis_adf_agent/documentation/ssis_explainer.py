"""
SSIS package explainer.

Produces a structured outline (JSON-serializable dict) describing what an
SSIS package does, the systems it touches, the step-by-step execution
sequence, and Mermaid diagrams for the control flow and each Data Flow Task.

The outline is designed to be consumed by an LLM (chat client) which can
then write rich prose. The Markdown renderer here produces a competent
deterministic baseline if no LLM elaboration is desired.
"""
from __future__ import annotations

from typing import Any

from ..analyzers.dependency_graph import topological_sort
from ..parsers.models import (
    DataFlowComponent,
    DataFlowTask,
    ExecutePackageTask,
    ExecuteSQLTask,
    FileSystemTask,
    ForEachLoopContainer,
    ForLoopContainer,
    ScriptTask,
    SequenceContainer,
    SSISConnectionManager,
    SSISPackage,
    SSISTask,
)
from .mermaid import control_flow_diagram, data_flow_diagram

# ---------------------------------------------------------------------------
# Component type → role classifier (sources vs destinations vs transforms)
# ---------------------------------------------------------------------------
_SOURCE_HINTS = ("source", "reader")
_DEST_HINTS = ("destination", "writer", "sink")


def _classify_component(c: DataFlowComponent) -> str:
    name = (c.component_type or "").lower()
    if any(h in name for h in _SOURCE_HINTS):
        return "source"
    if any(h in name for h in _DEST_HINTS):
        return "destination"
    return "transform"


# ---------------------------------------------------------------------------
# System catalog (databases, files, services touched by the package)
# ---------------------------------------------------------------------------

def _system_for_connection(cm: SSISConnectionManager) -> dict[str, Any]:
    kind = cm.type.value if hasattr(cm.type, "value") else str(cm.type)
    return {
        "name": cm.name,
        "kind": kind,
        "server": cm.server,
        "database": cm.database,
        "file_path": cm.file_path,
        "provider": cm.provider,
    }


def _walk(task: SSISTask) -> list[SSISTask]:
    """Yield this task and any nested children from containers."""
    items: list[SSISTask] = [task]
    if isinstance(task, SequenceContainer | ForEachLoopContainer | ForLoopContainer):
        for child in task.tasks:
            items.extend(_walk(child))
    return items


def _all_tasks(package: SSISPackage) -> list[SSISTask]:
    out: list[SSISTask] = []
    for t in package.tasks:
        out.extend(_walk(t))
    return out


# ---------------------------------------------------------------------------
# Step descriptions (deterministic prose per task type)
# ---------------------------------------------------------------------------

def _describe_task(task: SSISTask, conn_by_id: dict[str, SSISConnectionManager]) -> str:
    if isinstance(task, ExecuteSQLTask):
        cm = conn_by_id.get(task.connection_id or "")
        target = ""
        if cm:
            target = f" against `{cm.server or '?'}`/`{cm.database or '?'}`"
        sql = (task.sql_statement or "").strip().splitlines()
        first = sql[0] if sql else ""
        return f"Run SQL{target}: `{first[:120]}{'…' if len(first) > 120 else ''}`"
    if isinstance(task, DataFlowTask):
        srcs = [c.name for c in task.components if _classify_component(c) == "source"]
        dsts = [c.name for c in task.components if _classify_component(c) == "destination"]
        n_xform = sum(1 for c in task.components if _classify_component(c) == "transform")
        bits = []
        if srcs:
            bits.append(f"reads from {', '.join(srcs)}")
        if n_xform:
            bits.append(f"applies {n_xform} transformation(s)")
        if dsts:
            bits.append(f"writes to {', '.join(dsts)}")
        return "Data Flow — " + (", ".join(bits) if bits else "no components detected")
    if isinstance(task, ExecutePackageTask):
        ref = task.project_package_name or task.package_path or "?"
        return f"Execute child package `{ref}`"
    if isinstance(task, FileSystemTask):
        return f"File system: {task.operation} `{task.source_path}` → `{task.destination_path}`"
    if isinstance(task, ScriptTask):
        return (
            f"Run Script Task ({task.script_language}, entry `{task.entry_point}`); "
            "must be ported to an Azure Function"
        )
    if isinstance(task, ForEachLoopContainer):
        enum = task.enumerator_type.value if hasattr(task.enumerator_type, "value") else str(task.enumerator_type)
        return f"For-each loop ({enum}) executing {len(task.tasks)} child task(s)"
    if isinstance(task, ForLoopContainer):
        return f"For loop while `{task.eval_expression or '?'}`"
    if isinstance(task, SequenceContainer):
        return f"Sequence container with {len(task.tasks)} child task(s)"
    return f"{task.task_type.value if hasattr(task.task_type, 'value') else task.task_type}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_ssis_outline(package: SSISPackage) -> dict[str, Any]:
    """Build a JSON-serializable outline of the SSIS package."""
    conn_by_id = {cm.id: cm for cm in package.connection_managers}
    all_tasks = _all_tasks(package)

    # Classify systems by direction. We assume Execute SQL with INSERT/UPDATE
    # writes, otherwise reads. Data flows give explicit source/dest hints.
    used_as_source: set[str] = set()
    used_as_sink: set[str] = set()
    for t in all_tasks:
        if isinstance(t, ExecuteSQLTask) and t.connection_id:
            stmt = (t.sql_statement or "").lstrip().lower()
            if stmt.startswith(("insert", "update", "delete", "merge", "truncate")):
                used_as_sink.add(t.connection_id)
            else:
                used_as_source.add(t.connection_id)
        if isinstance(t, DataFlowTask):
            for c in t.components:
                if not c.connection_id:
                    continue
                role = _classify_component(c)
                if role == "source":
                    used_as_source.add(c.connection_id)
                elif role == "destination":
                    used_as_sink.add(c.connection_id)
        if isinstance(t, FileSystemTask):
            if t.source_path:
                used_as_source.add(f"file:{t.source_path}")
            if t.destination_path:
                used_as_sink.add(f"file:{t.destination_path}")

    systems: list[dict[str, Any]] = []
    for cm in package.connection_managers:
        sys = _system_for_connection(cm)
        roles: list[str] = []
        if cm.id in used_as_source:
            roles.append("source")
        if cm.id in used_as_sink:
            roles.append("sink")
        sys["roles"] = roles or ["unused"]
        systems.append(sys)

    # Step-by-step execution order (top-level only)
    task_by_id = {t.id: t for t in package.tasks}
    ordered_ids = topological_sort(package.tasks, package.constraints)
    steps: list[dict[str, Any]] = []
    for i, tid in enumerate(ordered_ids, start=1):
        t = task_by_id.get(tid)
        if t is None:
            continue
        steps.append(
            {
                "step": i,
                "task_id": t.id,
                "task_name": t.name,
                "task_type": t.task_type.value if hasattr(t.task_type, "value") else str(t.task_type),
                "disabled": t.disabled,
                "description": _describe_task(t, conn_by_id),
            }
        )

    # Control-flow Mermaid diagram
    nodes: list[tuple[str, str, str]] = [("__start__", "Start", "start")]
    edges: list[tuple[str, str, str]] = []
    for t in package.tasks:
        kind = _node_kind(t)
        nodes.append((t.id, t.name, kind))
    for c in package.constraints:
        label = _constraint_label(c)
        edges.append((c.from_task_id, c.to_task_id, label))
    # connect Start to roots (tasks with no inbound constraint)
    inbound = {c.to_task_id for c in package.constraints}
    for t in package.tasks:
        if t.id not in inbound:
            edges.append(("__start__", t.id, ""))
    nodes.append(("__end__", "End", "end"))
    leaves = {t.id for t in package.tasks} - {c.from_task_id for c in package.constraints}
    for tid in leaves:
        edges.append((tid, "__end__", ""))

    control_flow_md = control_flow_diagram(nodes, edges, title=f"Control flow — {package.name}")

    # Per-Data-Flow Task diagrams
    data_flow_diagrams: list[dict[str, Any]] = []
    for t in all_tasks:
        if isinstance(t, DataFlowTask):
            comps = [(c.id, c.name, _classify_component(c)) for c in t.components]
            paths = [(p.start_id, p.end_id) for p in t.paths]
            data_flow_diagrams.append(
                {
                    "task_id": t.id,
                    "task_name": t.name,
                    "component_count": len(t.components),
                    "mermaid": data_flow_diagram(comps, paths, title=f"Data flow — {t.name}"),
                }
            )

    # Event handlers as separate side-graph
    handlers: list[dict[str, Any]] = []
    for eh in package.event_handlers:
        handlers.append(
            {
                "event": eh.event_name,
                "parent": eh.parent_task_name or "(package)",
                "task_count": len(eh.tasks),
            }
        )

    return {
        "package_name": package.name,
        "source_file": package.source_file,
        "description": package.description,
        "totals": {
            "tasks_top_level": len(package.tasks),
            "tasks_total_with_nested": len(all_tasks),
            "data_flow_tasks": sum(1 for t in all_tasks if isinstance(t, DataFlowTask)),
            "script_tasks": sum(1 for t in all_tasks if isinstance(t, ScriptTask)),
            "execute_sql_tasks": sum(1 for t in all_tasks if isinstance(t, ExecuteSQLTask)),
            "containers": sum(
                1 for t in all_tasks
                if isinstance(t, SequenceContainer | ForEachLoopContainer | ForLoopContainer)
            ),
            "event_handlers": len(package.event_handlers),
            "connection_managers": len(package.connection_managers),
            "parameters": len(package.parameters),
            "variables": len(package.variables),
        },
        "systems": systems,
        "parameters": [
            {"name": p.name, "type": p.data_type, "required": p.required, "sensitive": p.sensitive}
            for p in package.parameters
        ],
        "variables_user": [
            {"name": v.name, "type": v.data_type, "value": v.value}
            for v in package.variables if v.namespace.lower() == "user"
        ],
        "steps": steps,
        "event_handlers": handlers,
        "diagrams": {
            "control_flow_mermaid": control_flow_md,
            "data_flow_mermaid": data_flow_diagrams,
        },
    }


def _node_kind(t: SSISTask) -> str:
    if isinstance(t, ForEachLoopContainer | ForLoopContainer):
        return "loop"
    if isinstance(t, SequenceContainer):
        return "container"
    return "task"


def _constraint_label(c: Any) -> str:
    val = c.value.value if hasattr(c.value, "value") else str(c.value)
    if val == "Success":
        return ""
    return val


# ---------------------------------------------------------------------------
# Markdown renderer (deterministic baseline; LLM may elaborate)
# ---------------------------------------------------------------------------

def render_ssis_markdown(outline: dict[str, Any]) -> str:
    parts: list[str] = []
    parts.append(f"# SSIS package: `{outline['package_name']}`")
    parts.append(f"_Source: `{outline['source_file']}`_\n")
    if outline.get("description"):
        parts.append(outline["description"] + "\n")

    t = outline["totals"]
    parts.append("## Inventory")
    parts.append(
        f"- **{t['tasks_total_with_nested']} task(s)** "
        f"({t['tasks_top_level']} top-level), "
        f"{t['data_flow_tasks']} data flow, "
        f"{t['execute_sql_tasks']} Execute SQL, "
        f"{t['script_tasks']} Script Task, "
        f"{t['containers']} container(s)"
    )
    parts.append(
        f"- {t['event_handlers']} event handler(s), "
        f"{t['connection_managers']} connection manager(s), "
        f"{t['parameters']} parameter(s), "
        f"{t['variables']} variable(s)"
    )

    parts.append("\n## Systems involved")
    if outline["systems"]:
        parts.append("| Connection | Kind | Server / File | Database | Roles |")
        parts.append("|---|---|---|---|---|")
        for s in outline["systems"]:
            parts.append(
                f"| {s['name']} | {s['kind']} | "
                f"{s.get('server') or s.get('file_path') or ''} | "
                f"{s.get('database') or ''} | {', '.join(s['roles'])} |"
            )
    else:
        parts.append("_No connection managers defined._")

    parts.append("\n## Step-by-step execution")
    for s in outline["steps"]:
        flag = " *(disabled)*" if s["disabled"] else ""
        parts.append(f"{s['step']}. **{s['task_name']}** ({s['task_type']}){flag} — {s['description']}")

    parts.append("\n## Control-flow diagram")
    parts.append("```mermaid")
    parts.append(outline["diagrams"]["control_flow_mermaid"])
    parts.append("```")

    if outline["diagrams"]["data_flow_mermaid"]:
        parts.append("\n## Data-flow diagrams")
        for d in outline["diagrams"]["data_flow_mermaid"]:
            parts.append(f"\n### {d['task_name']} ({d['component_count']} components)")
            parts.append("```mermaid")
            parts.append(d["mermaid"])
            parts.append("```")

    if outline["event_handlers"]:
        parts.append("\n## Event handlers")
        for eh in outline["event_handlers"]:
            parts.append(f"- `{eh['event']}` on `{eh['parent']}` — {eh['task_count']} task(s)")

    return "\n".join(parts)
