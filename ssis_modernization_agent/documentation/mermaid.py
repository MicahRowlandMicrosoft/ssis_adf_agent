"""
Mermaid diagram builders.

Produces flowchart markup for SSIS control flow, SSIS data flow, and ADF
pipeline activity graphs. Output is plain-text Mermaid markup that renders
inline in VS Code chat, GitHub, and most Markdown viewers.
"""
from __future__ import annotations

import re

_SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_]")


def safe_id(value: str) -> str:
    """Mermaid node IDs must be alphanumeric/underscore only."""
    cleaned = _SAFE_ID_RE.sub("_", value or "node")
    if not cleaned or not cleaned[0].isalpha():
        cleaned = "n_" + cleaned
    return cleaned[:60]


def escape_label(label: str) -> str:
    """Escape characters that confuse Mermaid label parsers."""
    if not label:
        return ""
    out = label.replace('"', "'").replace("\n", " ")
    return out[:60]


def control_flow_diagram(
    nodes: list[tuple[str, str, str]],
    edges: list[tuple[str, str, str]],
    *,
    direction: str = "TD",
    title: str | None = None,
) -> str:
    """Build a Mermaid flowchart.

    Args:
        nodes:  list of (id, label, kind). kind in {"task","container","start","end","loop","handler"}
        edges:  list of (from_id, to_id, label)  (label may be "")
        direction: TD | LR
        title:  optional diagram caption

    Returns the Mermaid markup as a single string (no surrounding fences).
    """
    lines: list[str] = []
    if title:
        lines.append(f"%% {title}")
    lines.append(f"flowchart {direction}")

    # Node shapes per kind
    shapes = {
        "start":      lambda lbl: f'(["{lbl}"])',
        "end":        lambda lbl: f'(["{lbl}"])',
        "task":       lambda lbl: f'["{lbl}"]',
        "container":  lambda lbl: f'[["{lbl}"]]',
        "loop":       lambda lbl: f'{{{{"{lbl}"}}}}',
        "handler":    lambda lbl: f'(("{lbl}"))',
    }
    seen: set[str] = set()
    for nid, label, kind in nodes:
        sid = safe_id(nid)
        if sid in seen:
            continue
        seen.add(sid)
        shape_fn = shapes.get(kind, shapes["task"])
        lines.append(f"    {sid}{shape_fn(escape_label(label))}")

    for src, dst, lbl in edges:
        sid = safe_id(src)
        did = safe_id(dst)
        if lbl:
            lines.append(f"    {sid} -->|{escape_label(lbl)}| {did}")
        else:
            lines.append(f"    {sid} --> {did}")

    # CSS classes for visual differentiation
    lines.append("    classDef container fill:#eef,stroke:#558;")
    lines.append("    classDef loop fill:#fee,stroke:#855;")
    lines.append("    classDef handler fill:#efe,stroke:#585,stroke-dasharray:3 3;")
    container_ids = [safe_id(n[0]) for n in nodes if n[2] == "container"]
    loop_ids = [safe_id(n[0]) for n in nodes if n[2] == "loop"]
    handler_ids = [safe_id(n[0]) for n in nodes if n[2] == "handler"]
    if container_ids:
        lines.append(f"    class {','.join(container_ids)} container;")
    if loop_ids:
        lines.append(f"    class {','.join(loop_ids)} loop;")
    if handler_ids:
        lines.append(f"    class {','.join(handler_ids)} handler;")

    return "\n".join(lines)


def data_flow_diagram(
    components: list[tuple[str, str, str]],
    paths: list[tuple[str, str]],
    *,
    title: str | None = None,
) -> str:
    """Mermaid LR diagram for an SSIS Data Flow Task.

    Args:
        components: list of (id, label, kind). kind in {"source","transform","destination"}
        paths:      list of (from_id, to_id)
    """
    lines: list[str] = []
    if title:
        lines.append(f"%% {title}")
    lines.append("flowchart LR")
    shapes = {
        "source":      lambda lbl: f'[("{lbl}")]',
        "destination": lambda lbl: f'[("{lbl}")]',
        "transform":   lambda lbl: f'["{lbl}"]',
    }
    for cid, label, kind in components:
        sid = safe_id(cid)
        shape_fn = shapes.get(kind, shapes["transform"])
        lines.append(f"    {sid}{shape_fn(escape_label(label))}")
    for src, dst in paths:
        lines.append(f"    {safe_id(src)} --> {safe_id(dst)}")
    lines.append("    classDef src fill:#cfe,stroke:#283;")
    lines.append("    classDef dst fill:#fcd,stroke:#823;")
    src_ids = [safe_id(c[0]) for c in components if c[2] == "source"]
    dst_ids = [safe_id(c[0]) for c in components if c[2] == "destination"]
    if src_ids:
        lines.append(f"    class {','.join(src_ids)} src;")
    if dst_ids:
        lines.append(f"    class {','.join(dst_ids)} dst;")
    return "\n".join(lines)
