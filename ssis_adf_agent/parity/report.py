"""Markdown rendering for behavioral parity comparisons."""
from __future__ import annotations

from typing import Any


def render_diff_markdown(comparison: dict[str, Any]) -> str:
    """Render a :class:`ParityComparison` (as dict) to a Markdown report."""
    lines: list[str] = []
    pkg = comparison.get("package_path", "?")
    dft = comparison.get("dataflow_task_name", "?")
    adf = comparison.get("adf_dataflow_path", "?")
    inp = comparison.get("input_dataset_path", "?")
    diff = comparison.get("diff", {}) or {}
    ssis_run = comparison.get("ssis_run", {}) or {}
    adf_run = comparison.get("adf_run", {}) or {}

    lines.append(f"# Behavioral Parity Report — `{dft}`")
    lines.append("")
    verdict = "✅ PASS" if diff.get("ok") else "❌ FAIL"
    lines.append(f"**Verdict:** {verdict}")
    lines.append("")

    lines.append("## Inputs")
    lines.append("")
    lines.append(f"- SSIS package: `{pkg}`")
    lines.append(f"- Data Flow Task: `{dft}`")
    lines.append(f"- ADF dataflow: `{adf}`")
    lines.append(f"- Input dataset: `{inp}`")
    lines.append(
        f"- Runners: SSIS=`{comparison.get('ssis_runner_name', '?')}`, "
        f"ADF=`{comparison.get('adf_runner_name', '?')}`"
    )
    lines.append("")

    lines.append("## Row counts")
    lines.append("")
    lines.append("| Side | Rows |")
    lines.append("|---|---:|")
    lines.append(f"| SSIS ({ssis_run.get('runner_name', '?')}) | {diff.get('ssis_row_count', 0)} |")
    lines.append(f"| ADF ({adf_run.get('runner_name', '?')}) | {diff.get('adf_row_count', 0)} |")
    lines.append(f"| Matched | {diff.get('matched_row_count', 0)} |")
    lines.append("")

    only_ssis = diff.get("columns_only_in_ssis") or []
    only_adf = diff.get("columns_only_in_adf") or []
    if only_ssis or only_adf:
        lines.append("## Schema drift")
        lines.append("")
        if only_ssis:
            lines.append(f"- Columns only in SSIS: {', '.join(f'`{c}`' for c in only_ssis)}")
        if only_adf:
            lines.append(f"- Columns only in ADF: {', '.join(f'`{c}`' for c in only_adf)}")
        lines.append("")

    summary = diff.get("summary") or {}
    if summary:
        lines.append("## Diff summary")
        lines.append("")
        lines.append("| Kind | Count |")
        lines.append("|---|---:|")
        for kind in (
            "value_mismatch",
            "missing_in_adf",
            "extra_in_adf",
            "duplicate_count",
        ):
            if kind in summary:
                lines.append(f"| {kind} | {summary[kind]} |")
        lines.append("")

    diffs = diff.get("diffs") or []
    if diffs:
        lines.append("## First diffs")
        lines.append("")
        lines.append("| Kind | Key | Column | SSIS value | ADF value |")
        lines.append("|---|---|---|---|---|")
        for d in diffs[:50]:
            key = ",".join(str(v) for v in (d.get("key") or []))
            lines.append(
                f"| {d.get('kind', '?')} | `{key}` | `{d.get('column') or ''}` | "
                f"`{d.get('ssis_value')!r}` | `{d.get('adf_value')!r}` |"
            )
        if len(diffs) > 50:
            lines.append("")
            lines.append(f"_…and {len(diffs) - 50} more diffs (truncated)._")
        lines.append("")

    lines.append("## What this proves")
    lines.append("")
    lines.append(
        "- ✅ matches mean *for the supplied input* the converted Mapping Data "
        "Flow produced the same output as the source SSIS Data Flow."
    )
    lines.append(
        "- It does **not** prove the conversion is correct on inputs you did "
        "not test.  Coverage is a function of the input dataset you provided."
    )
    lines.append(
        "- It does **not** prove performance, error-handling, or "
        "side-effect parity (e.g. file moves, audit-table writes)."
    )
    lines.append("")
    return "\n".join(lines)
