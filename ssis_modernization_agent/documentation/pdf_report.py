"""
Pre-migration PDF report generator (ReportLab).

Builds a single PDF combining:
  1. Cover page (package name + factory target + date)
  2. SSIS package summary (from ssis_explainer outline)
  3. ADF artifact summary (from adf_explainer outline)
  4. Parity validation results (from parity_validator)

Mermaid diagrams are included as fenced code blocks (text-only — they
won't render visually inside the PDF without an external renderer; for
visual diagrams use the Markdown view).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        PageBreak,
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )
    _REPORTLAB_AVAILABLE = True
except ImportError:
    _REPORTLAB_AVAILABLE = False


def _require_reportlab() -> None:
    if not _REPORTLAB_AVAILABLE:
        raise RuntimeError(
            "reportlab is required for PDF generation. Install it with:\n"
            "    pip install reportlab"
        )


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle("Title", parent=base["Title"], fontSize=22, leading=26, spaceAfter=12),
        "h1": ParagraphStyle("H1", parent=base["Heading1"], fontSize=16, spaceBefore=18, spaceAfter=6),
        "h2": ParagraphStyle("H2", parent=base["Heading2"], fontSize=13, spaceBefore=12, spaceAfter=4),
        "body": ParagraphStyle("Body", parent=base["BodyText"], fontSize=10, leading=13),
        "mono": ParagraphStyle("Mono", parent=base["Code"], fontSize=8, leading=10),
        "small": ParagraphStyle("Small", parent=base["BodyText"], fontSize=9, leading=11, textColor=colors.grey),
    }


def _kv_table(rows: list[tuple[str, str]], width: float = 6.5 * 72) -> Table:
    data = [[k, v] for k, v in rows]
    tbl = Table(data, colWidths=[2.0 * 72, width - 2.0 * 72])
    tbl.setStyle(
        TableStyle(
            [
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#444")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f4f4f8")),
                ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return tbl


def _esc(text: Any) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def build_pre_migration_pdf(
    *,
    output_pdf: str | Path,
    ssis_outline: dict[str, Any],
    adf_outline: dict[str, Any],
    parity: dict[str, Any],
    factory_target: dict[str, str] | None = None,
) -> str:
    """Build the pre-migration PDF and return the absolute path."""
    _require_reportlab()
    out_path = Path(output_pdf).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    styles = _styles()
    doc = SimpleDocTemplate(
        str(out_path), pagesize=LETTER,
        leftMargin=0.6 * inch, rightMargin=0.6 * inch,
        topMargin=0.7 * inch, bottomMargin=0.6 * inch,
        title=f"Pre-migration report — {ssis_outline.get('package_name','')}",
    )

    story: list[Any] = []

    # ---- Cover ----
    story.append(Paragraph("SSIS → ADF pre-migration report", styles["title"]))
    story.append(Paragraph(f"Package: <b>{_esc(ssis_outline.get('package_name',''))}</b>", styles["body"]))
    story.append(Paragraph(f"Generated: {datetime.now():%Y-%m-%d %H:%M}", styles["small"]))
    if factory_target:
        story.append(Spacer(1, 0.2 * inch))
        story.append(_kv_table([(k, _esc(v)) for k, v in factory_target.items()]))

    story.append(Spacer(1, 0.2 * inch))
    ok = parity.get("ok", False)
    verdict = "READY TO DEPLOY" if ok else "REVIEW BEFORE DEPLOYING"
    color = colors.HexColor("#1f7a1f") if ok else colors.HexColor("#a61212")
    story.append(
        Paragraph(
            f'<font color="{color.hexval()}"><b>Verdict: {verdict}</b></font>',
            styles["h1"],
        )
    )

    # ---- SSIS summary ----
    story.append(PageBreak())
    story.append(Paragraph("1. SSIS package", styles["h1"]))
    story.append(
        Paragraph(f"<b>Source file:</b> {_esc(ssis_outline.get('source_file',''))}", styles["body"])
    )
    if ssis_outline.get("description"):
        story.append(Paragraph(_esc(ssis_outline["description"]), styles["body"]))

    t = ssis_outline.get("totals", {})
    rows = [
        ("Total tasks (incl. nested)", str(t.get("tasks_total_with_nested", 0))),
        ("Top-level tasks", str(t.get("tasks_top_level", 0))),
        ("Data Flow Tasks", str(t.get("data_flow_tasks", 0))),
        ("Execute SQL Tasks", str(t.get("execute_sql_tasks", 0))),
        ("Script Tasks", str(t.get("script_tasks", 0))),
        ("Containers", str(t.get("containers", 0))),
        ("Event handlers", str(t.get("event_handlers", 0))),
        ("Connection managers", str(t.get("connection_managers", 0))),
        ("Parameters", str(t.get("parameters", 0))),
        ("Variables", str(t.get("variables", 0))),
    ]
    story.append(Spacer(1, 0.1 * inch))
    story.append(_kv_table(rows))

    if ssis_outline.get("systems"):
        story.append(Spacer(1, 0.15 * inch))
        story.append(Paragraph("Systems involved", styles["h2"]))
        sys_rows = [["Connection", "Kind", "Server / file", "Database", "Roles"]]
        for s in ssis_outline["systems"]:
            sys_rows.append(
                [
                    _esc(s["name"]),
                    _esc(s["kind"]),
                    _esc(s.get("server") or s.get("file_path") or ""),
                    _esc(s.get("database") or ""),
                    _esc(", ".join(s.get("roles", []))),
                ]
            )
        sys_tbl = Table(sys_rows, colWidths=[1.2*72, 1.0*72, 1.8*72, 1.3*72, 1.0*72])
        sys_tbl.setStyle(
            TableStyle(
                [
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dde")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        story.append(sys_tbl)

    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("Step-by-step execution", styles["h2"]))
    for step in ssis_outline.get("steps", []):
        flag = " <i>(disabled)</i>" if step.get("disabled") else ""
        story.append(
            Paragraph(
                f"{step['step']}. <b>{_esc(step['task_name'])}</b> "
                f"<font color='#888'>({_esc(step['task_type'])})</font>{flag} — "
                f"{_esc(step['description'])}",
                styles["body"],
            )
        )

    # ---- ADF summary ----
    story.append(PageBreak())
    story.append(Paragraph("2. Generated ADF artifacts", styles["h1"]))
    at = adf_outline.get("totals", {})
    story.append(
        _kv_table(
            [
                ("Output directory", _esc(adf_outline.get("output_dir", ""))),
                ("Pipelines", str(at.get("pipelines", 0))),
                ("Linked services", str(at.get("linked_services", 0))),
                ("Datasets", str(at.get("datasets", 0))),
                ("Mapping data flows", str(at.get("data_flows", 0))),
                ("Triggers", str(at.get("triggers", 0))),
                ("Function stubs", str(at.get("function_stubs", 0))),
            ]
        )
    )

    for pipe in adf_outline.get("pipelines", []):
        story.append(Spacer(1, 0.15 * inch))
        story.append(Paragraph(f"Pipeline: <b>{_esc(pipe['name'])}</b>", styles["h2"]))
        if pipe.get("description"):
            story.append(Paragraph(_esc(pipe["description"]), styles["body"]))
        story.append(
            Paragraph(
                f"{pipe['activity_count']} activities. Types: "
                + ", ".join(f"{_esc(k)}={v}" for k, v in pipe.get("activities_by_type", {}).items()),
                styles["body"],
            )
        )

    # ---- Parity ----
    story.append(PageBreak())
    story.append(Paragraph("3. Parity validation", styles["h1"]))
    s = parity.get("summary", {})
    story.append(
        _kv_table(
            [
                ("SSIS tasks total", str(s.get("ssis_total_tasks", 0))),
                ("ADF activities total", str(s.get("adf_total_activities", 0))),
                ("Connection managers → linked services",
                 f"{s.get('ssis_connection_managers', 0)} → {s.get('adf_linked_services', 0)}"),
                ("Data Flow Tasks → mapping data flows",
                 f"{s.get('ssis_data_flow_tasks', 0)} → {s.get('adf_mapping_data_flows', 0)}"),
                ("Script Tasks → function stubs",
                 f"{s.get('ssis_script_tasks', 0)} → {s.get('adf_function_stubs', 0)}"),
            ]
        )
    )

    matches = parity.get("matches", [])
    if matches:
        story.append(Spacer(1, 0.1 * inch))
        story.append(Paragraph("Matches", styles["h2"]))
        for m in matches:
            story.append(Paragraph(f"✓ {_esc(m)}", styles["body"]))

    issues = parity.get("issues", [])
    if issues:
        # Group by severity
        grouped: dict[str, list[dict[str, str]]] = {"error": [], "warning": [], "info": []}
        for i in issues:
            grouped.setdefault(i.get("severity", "info"), []).append(i)
        for sev, label, color_hex in [
            ("error", "Errors", "#a61212"),
            ("warning", "Warnings", "#a67212"),
            ("info", "Info", "#125ea6"),
        ]:
            items = grouped.get(sev, [])
            if not items:
                continue
            story.append(Spacer(1, 0.1 * inch))
            story.append(
                Paragraph(
                    f'<font color="{color_hex}"><b>{label} ({len(items)})</b></font>',
                    styles["h2"],
                )
            )
            for i in items:
                story.append(
                    Paragraph(
                        f"<b>[{_esc(i.get('category',''))}]</b> {_esc(i.get('message',''))}",
                        styles["body"],
                    )
                )
                if i.get("detail"):
                    story.append(Paragraph(f"<i>{_esc(i['detail'])}</i>", styles["small"]))

    if parity.get("artifact_dryrun"):
        story.append(Spacer(1, 0.15 * inch))
        story.append(Paragraph("SDK dry-run", styles["h2"]))
        d = parity["artifact_dryrun"].get("deserialized", {})
        story.append(
            Paragraph(
                "Deserialized: " + ", ".join(f"{_esc(k)}={v}" for k, v in d.items()),
                styles["body"],
            )
        )
        for err in parity["artifact_dryrun"].get("errors", []):
            story.append(
                Paragraph(
                    f"<font color='#a61212'>{_esc(err.get('kind'))}: {_esc(err.get('file'))}</font>",
                    styles["small"],
                )
            )
            story.append(Paragraph(_esc(err.get("error", "")), styles["mono"]))

    if parity.get("factory_check"):
        story.append(Spacer(1, 0.15 * inch))
        story.append(Paragraph("Target factory", styles["h2"]))
        story.append(_kv_table([(k, _esc(v)) for k, v in parity["factory_check"].items()]))

    doc.build(story)
    return str(out_path)
