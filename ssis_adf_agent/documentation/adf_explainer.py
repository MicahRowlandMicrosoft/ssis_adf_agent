"""
ADF artifact explainer.

Reads a directory of generated ADF JSON artifacts (the output of
``convert_ssis_package``) and produces a structured outline plus Mermaid
diagrams of the resulting pipeline.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .mermaid import control_flow_diagram

_SUBDIRS = ("pipeline", "linkedService", "dataset", "dataflow", "trigger", "stubs")


def _load_jsons(d: Path) -> list[tuple[Path, dict[str, Any]]]:
    out: list[tuple[Path, dict[str, Any]]] = []
    if not d.exists():
        return out
    for f in sorted(d.glob("*.json")):
        try:
            with f.open(encoding="utf-8") as fh:
                out.append((f, json.load(fh)))
        except (OSError, json.JSONDecodeError):
            continue
    return out


def _activity_kind(activity_type: str) -> str:
    t = (activity_type or "").lower()
    if "until" in t or "foreach" in t or "filter" in t:
        return "loop"
    if "ifcondition" in t or "switch" in t:
        return "container"
    return "task"


def build_adf_outline(output_dir: str | Path) -> dict[str, Any]:
    """Read generated ADF artifacts and build a structured outline."""
    root = Path(output_dir)

    pipelines = _load_jsons(root / "pipeline")
    linked_services = _load_jsons(root / "linkedService")
    datasets = _load_jsons(root / "dataset")
    dataflows = _load_jsons(root / "dataflow")
    triggers = _load_jsons(root / "trigger")

    stubs_dir = root / "stubs"
    stub_funcs: list[dict[str, str]] = []
    if stubs_dir.exists():
        for sub in sorted(p for p in stubs_dir.iterdir() if p.is_dir()):
            stub_funcs.append({"function_name": sub.name, "path": str(sub)})

    pipeline_outlines: list[dict[str, Any]] = []
    for path, payload in pipelines:
        props = payload.get("properties", {})
        activities = props.get("activities", [])
        nodes: list[tuple[str, str, str]] = []
        edges: list[tuple[str, str, str]] = []
        for act in activities:
            aname = act.get("name", "?")
            atype = act.get("type", "?")
            nodes.append((aname, f"{aname}\\n[{atype}]", _activity_kind(atype)))
            for dep in act.get("dependsOn", []) or []:
                upstream = dep.get("activity")
                conditions = dep.get("dependencyConditions", []) or []
                label = ",".join(c for c in conditions if c and c != "Succeeded")
                if upstream:
                    edges.append((upstream, aname, label))
        # roots / sinks
        all_names = {a.get("name") for a in activities}
        downstream = {dep.get("activity") for a in activities for dep in (a.get("dependsOn") or [])}
        with_inbound = {
            a.get("name")
            for a in activities
            if a.get("dependsOn")
        }
        roots = [n for n in all_names if n not in with_inbound]
        leaves = [n for n in all_names if n not in downstream]
        nodes.insert(0, ("__start__", "Start", "start"))
        for r in roots:
            edges.append(("__start__", r, ""))
        nodes.append(("__end__", "End", "end"))
        for L in leaves:
            edges.append((L, "__end__", ""))

        diagram = control_flow_diagram(nodes, edges, title=f"Pipeline — {payload.get('name','')}")

        pipeline_outlines.append(
            {
                "name": payload.get("name", path.stem),
                "file": str(path),
                "description": props.get("description", ""),
                "annotations": props.get("annotations", []),
                "activity_count": len(activities),
                "activities_by_type": _count_by(activities, "type"),
                "parameters": list((props.get("parameters") or {}).keys()),
                "variables": list((props.get("variables") or {}).keys()),
                "activities": [
                    {
                        "name": a.get("name"),
                        "type": a.get("type"),
                        "depends_on": [
                            d.get("activity") for d in (a.get("dependsOn") or [])
                        ],
                    }
                    for a in activities
                ],
                "mermaid": diagram,
            }
        )

    ls_outlines = [
        {
            "name": p.get("name", path.stem),
            "type": p.get("properties", {}).get("type"),
            "file": str(path),
        }
        for path, p in linked_services
    ]
    ds_outlines = [
        {
            "name": p.get("name", path.stem),
            "type": p.get("properties", {}).get("type"),
            "linked_service": (
                p.get("properties", {})
                .get("linkedServiceName", {})
                .get("referenceName")
            ),
            "file": str(path),
        }
        for path, p in datasets
    ]
    df_outlines = [
        {
            "name": p.get("name", path.stem),
            "type": p.get("properties", {}).get("type"),
            "transformations": _df_transformations(p),
            "file": str(path),
        }
        for path, p in dataflows
    ]
    trig_outlines = [
        {
            "name": p.get("name", path.stem),
            "type": p.get("properties", {}).get("type"),
            "runtime_state": p.get("properties", {}).get("runtimeState", "Stopped"),
            "file": str(path),
        }
        for path, p in triggers
    ]

    return {
        "output_dir": str(root),
        "totals": {
            "pipelines": len(pipelines),
            "linked_services": len(linked_services),
            "datasets": len(datasets),
            "data_flows": len(dataflows),
            "triggers": len(triggers),
            "function_stubs": len(stub_funcs),
        },
        "pipelines": pipeline_outlines,
        "linked_services": ls_outlines,
        "datasets": ds_outlines,
        "data_flows": df_outlines,
        "triggers": trig_outlines,
        "function_stubs": stub_funcs,
    }


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for it in items:
        k = it.get(key) or "?"
        out[k] = out.get(k, 0) + 1
    return out


def _df_transformations(payload: dict[str, Any]) -> list[str]:
    """Extract mapping data flow transformation names."""
    props = payload.get("properties", {})
    type_props = props.get("typeProperties", {})
    out: list[str] = []
    for src in type_props.get("sources", []) or []:
        out.append(f"source:{src.get('name')}")
    for tx in type_props.get("transformations", []) or []:
        out.append(f"transform:{tx.get('name')}")
    for sink in type_props.get("sinks", []) or []:
        out.append(f"sink:{sink.get('name')}")
    return out


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------

def render_adf_markdown(outline: dict[str, Any]) -> str:
    parts: list[str] = []
    parts.append(f"# ADF artifacts in `{outline['output_dir']}`")

    t = outline["totals"]
    parts.append("## Inventory")
    parts.append(
        f"- {t['pipelines']} pipeline(s), {t['data_flows']} data flow(s), "
        f"{t['linked_services']} linked service(s), {t['datasets']} dataset(s), "
        f"{t['triggers']} trigger(s), {t['function_stubs']} Azure Function stub(s)"
    )

    for pipe in outline["pipelines"]:
        parts.append(f"\n## Pipeline: `{pipe['name']}`")
        if pipe["description"]:
            parts.append(pipe["description"])
        parts.append(f"_{pipe['activity_count']} activities_")
        if pipe["activities_by_type"]:
            by_type = ", ".join(f"`{k}`={v}" for k, v in pipe["activities_by_type"].items())
            parts.append(f"Activity types: {by_type}")
        if pipe["parameters"]:
            parts.append(f"Parameters: {', '.join(pipe['parameters'])}")
        parts.append("\n### Activity graph")
        parts.append("```mermaid")
        parts.append(pipe["mermaid"])
        parts.append("```")

    if outline["linked_services"]:
        parts.append("\n## Linked services")
        for ls in outline["linked_services"]:
            parts.append(f"- `{ls['name']}` — type `{ls['type']}`")

    if outline["data_flows"]:
        parts.append("\n## Mapping Data Flows")
        for df in outline["data_flows"]:
            xforms = ", ".join(df["transformations"]) or "_no body_"
            parts.append(f"- `{df['name']}`: {xforms}")

    if outline["function_stubs"]:
        parts.append("\n## Azure Function stubs (Script Tasks)")
        for f in outline["function_stubs"]:
            parts.append(f"- `{f['function_name']}` — `{f['path']}` _(requires manual port)_")

    return "\n".join(parts)
