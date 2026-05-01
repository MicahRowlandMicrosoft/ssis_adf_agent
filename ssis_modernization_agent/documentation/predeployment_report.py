"""
Pre-deployment report generator.

Produces a comprehensive Markdown report for each SSIS package and its
converted ADF solution.  Targeted at the **engineer / admin** persona who
needs to understand:

  * What the SSIS package does (summary, components, diagrams)
  * What the ADF solution does (activities, linked services, diagrams)
  * Exactly which manual tasks must be completed **before** deployment
  * Exactly which manual tasks must be completed **after** deployment

The report is estate-aware: when given multiple package/output pairs it
emits one consolidated document with a per-package section and a shared
infrastructure checklist.

Inputs:
  * SSIS package paths (parsed via LocalReader)
  * ADF output directories (read from disk)
  * Migration plans (loaded from saved JSON)
  * Parity validation results (optional)
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ..parsers.models import (
    DataFlowTask,
    ExecutePackageTask,
    ExecuteSQLTask,
    FileSystemTask,
    ForEachLoopContainer,
    ForLoopContainer,
    ScriptTask,
    SendMailTask,
    SequenceContainer,
    SSISPackage,
)
from .adf_explainer import build_adf_outline
from .mermaid import control_flow_diagram, data_flow_diagram
from .ssis_explainer import build_ssis_outline


# ---------------------------------------------------------------------------
# Task enumeration helpers
# ---------------------------------------------------------------------------

def _walk_tasks(task: Any) -> list[Any]:
    items = [task]
    if isinstance(task, (SequenceContainer, ForEachLoopContainer, ForLoopContainer)):
        for child in task.tasks:
            items.extend(_walk_tasks(child))
    return items


def _all_tasks(package: SSISPackage) -> list[Any]:
    out = []
    for t in package.tasks:
        out.extend(_walk_tasks(t))
    return out


# ---------------------------------------------------------------------------
# Checklist rendering helpers
# ---------------------------------------------------------------------------


def _checklist_row(index: int, task: dict[str, str]) -> str:
    """Render one checklist table row.  Rows with an MCP tool are italicised."""
    tool = task.get("tool") or ""
    tool_cell = f"`{tool}`" if tool else ""
    cat = task["category"]
    name = task["task"]
    detail = task["detail"][:120]
    status = task["status"]
    if tool:
        # Wrap every cell value in italics
        return (
            f"| {index} | *{cat}* | *{name}* | "
            f"*{detail}* | *{status}* | {tool_cell} |"
        )
    return (
        f"| {index} | {cat} | {name} | "
        f"{detail} | {status} | {tool_cell} |"
    )


# ---------------------------------------------------------------------------
# Pre-deployment task extraction
# ---------------------------------------------------------------------------

def _extract_pre_deployment_tasks(
    package: SSISPackage,
    adf_outline: dict[str, Any],
    plan: dict[str, Any] | None,
) -> list[dict[str, str]]:
    """Return a list of tasks that MUST be completed before deployment."""
    tasks: list[dict[str, str]] = []

    # 1. Infrastructure provisioning
    if plan:
        for infra in plan.get("infrastructure_needed", []):
            tasks.append({
                "category": "Infrastructure",
                "task": f"Provision {infra['type']} (`{infra.get('name_hint', '?')}`)",
                "detail": infra.get("purpose", ""),
                "status": "required",
                "tool": "provision_adf_environment",
            })

    # 2. RBAC / permissions
    if plan:
        for rbac in plan.get("rbac_needed", []):
            scope = rbac.get("scope", "")
            role = rbac.get("role", "")
            tasks.append({
                "category": "RBAC / Permissions",
                "task": f"Grant `{role}` to ADF managed identity on `{scope}`",
                "detail": rbac.get("purpose", ""),
                "status": "required",
                "tool": "provision_adf_environment",
            })

    # 3. Self-Hosted IR for on-prem connections
    all_tasks_list = _all_tasks(package)
    on_prem_servers = set()
    for cm in package.connection_managers:
        server = (cm.server or "").lower()
        if server and "database.windows.net" not in server and server not in (".", "localhost"):
            on_prem_servers.add(cm.server)
    if on_prem_servers:
        servers = ", ".join(sorted(on_prem_servers))
        tasks.append({
            "category": "Integration Runtime",
            "task": "Install and register a Self-Hosted Integration Runtime",
            "detail": f"Required for on-prem connectivity to: {servers}",
            "status": "required",
            "tool": None,
        })

    # 4. Linked service configuration
    for ls in adf_outline.get("linked_services", []):
        ls_type = ls.get("type", "")
        ls_name = ls.get("name", "")
        if ls_type == "Custom" or "smtp" in ls_name.lower():
            tasks.append({
                "category": "Linked Service",
                "task": f"Configure linked service `{ls_name}` (type: {ls_type})",
                "detail": "No direct ADF equivalent — manual configuration required.",
                "status": "required",
                "tool": None,
            })

    # 5. Script Task stubs → Azure Function deployment
    stubs = adf_outline.get("function_stubs", [])
    if stubs:
        tasks.append({
            "category": "Azure Functions",
            "task": "Provision an Azure Function App for Script Task stubs",
            "detail": f"{len(stubs)} function stub(s) need hosting.",
            "status": "required",
            "tool": "provision_function_app",
        })
        for stub in stubs:
            tasks.append({
                "category": "Azure Functions",
                "task": f"Implement business logic in `{stub['function_name']}`",
                "detail": f"Review stub at `{stub['path']}` and port the original C#/VB logic.",
                "status": "required",
                "tool": None,
            })

    # 6. Send Mail → Logic App / Azure Communication Services
    for t in all_tasks_list:
        if isinstance(t, SendMailTask):
            tasks.append({
                "category": "Send Mail",
                "task": f"Create a Logic App or ACS endpoint for `{t.name}`",
                "detail": "ADF has no native Send Mail activity. The pipeline uses a Web Activity that must POST to a Logic App or Azure Function.",
                "status": "required",
                "tool": None,
            })

    # 7. File path remapping
    file_tasks = [t for t in all_tasks_list if isinstance(t, FileSystemTask)]
    if file_tasks:
        local_paths = set()
        for t in file_tasks:
            if t.source_path:
                local_paths.add(t.source_path)
            if t.destination_path:
                local_paths.add(t.destination_path)
        if local_paths:
            paths_str = ", ".join(f"`{p}`" for p in sorted(local_paths)[:5])
            tasks.append({
                "category": "File Paths",
                "task": "Update local/UNC file paths to Azure Storage URLs",
                "detail": f"Source paths detected: {paths_str}. Map to ADLS Gen2 / Blob containers.",
                "status": "required",
                "tool": "edit_migration_plan",
            })

    # 8. Key Vault secrets (if plan specifies)
    if plan:
        for ls in plan.get("linked_services", []):
            if ls.get("secret_name"):
                tasks.append({
                    "category": "Key Vault",
                    "task": f"Store secret `{ls['secret_name']}` in Key Vault",
                    "detail": f"For linked service `{ls['name']}`.",
                    "status": "required",
                    "tool": None,
                })

    # 9. Cross-database / linked server references
    if plan:
        for risk in plan.get("risks", []):
            msg = risk.get("message", "")
            if "cross-database" in msg.lower() or "linked server" in msg.lower():
                tasks.append({
                    "category": "Database Connectivity",
                    "task": risk.get("message", "Resolve cross-database references"),
                    "detail": risk.get("mitigation", ""),
                    "status": "required",
                    "tool": None,
                })

    return tasks


def _extract_post_deployment_tasks(
    package: SSISPackage,
    adf_outline: dict[str, Any],
    plan: dict[str, Any] | None,
) -> list[dict[str, str]]:
    """Return tasks that must be completed AFTER deploying ADF artifacts."""
    tasks: list[dict[str, str]] = []

    # 1. Activate triggers
    for trig in adf_outline.get("triggers", []):
        tasks.append({
            "category": "Triggers",
            "task": f"Start trigger `{trig['name']}`",
            "detail": "Triggers are deployed in Stopped state. Activate after validating pipelines.",
            "status": "required",
            "tool": None,
        })

    # 2. SQL-side permissions (db_datareader etc.)
    if plan:
        for rbac in plan.get("rbac_needed", []):
            scope = rbac.get("scope", "")
            role = rbac.get("role", "")
            if role.startswith("db_"):
                tasks.append({
                    "category": "SQL Permissions",
                    "task": f"Grant `{role}` via T-SQL on `{scope}`",
                    "detail": "ARM/Bicep cannot grant SQL-level roles. Run: "
                              f"CREATE USER [<adf-mi-name>] FROM EXTERNAL PROVIDER; "
                              f"ALTER ROLE {role} ADD MEMBER [<adf-mi-name>];",
                    "status": "required",
                    "tool": None,
                })

    # 3. Smoke-test each pipeline
    for pipe in adf_outline.get("pipelines", []):
        tasks.append({
            "category": "Validation",
            "task": f"Smoke-test pipeline `{pipe['name']}`",
            "detail": "Run a manual trigger and verify output matches SSIS baseline.",
            "status": "recommended",
            "tool": "smoke_test_pipeline",
        })

    # 4. Deferred simplifications
    if plan:
        applied = plan.get("migration_plan_applied", {})
        for simp in (applied or {}).get("deferred_simplifications", plan.get("simplifications", [])):
            action = simp.get("action", "")
            items = simp.get("items", [])
            if action in ("fold_to_copy_activity", "fold_to_stored_proc", "replace_with_function"):
                tasks.append({
                    "category": "Simplification",
                    "task": f"Apply `{action}` to: {', '.join(items)}",
                    "detail": simp.get("reason", ""),
                    "status": "recommended",
                    "tool": "convert_ssis_package",
                })

    # 5. Deploy artifacts
    if adf_outline.get("pipelines"):
        tasks.append({
            "category": "Deployment",
            "task": "Deploy ADF artifacts to the factory",
            "detail": "Push pipelines, linked services, datasets, and data flows to Azure Data Factory.",
            "status": "required",
            "tool": "deploy_to_adf",
        })

    # 6. Deploy function stubs
    if adf_outline.get("function_stubs"):
        tasks.append({
            "category": "Azure Functions",
            "task": "Deploy function stubs to Function App",
            "detail": "Zip-deploy generated stub code to the provisioned Function App.",
            "status": "required",
            "tool": "deploy_function_stubs",
        })

    # 7. Monitoring & alerting
    tasks.append({
        "category": "Observability",
        "task": "Configure Azure Monitor alerts for pipeline failures",
        "detail": "Set up alerts on the ADF factory for failed pipeline runs and high-latency activities.",
        "status": "recommended",
        "tool": None,
    })

    return tasks


# ---------------------------------------------------------------------------
# Per-package report section
# ---------------------------------------------------------------------------

def _render_package_section(
    package: SSISPackage,
    adf_dir: Path,
    plan: dict[str, Any] | None,
    section_num: int,
) -> str:
    """Render the full Markdown section for one package."""
    parts: list[str] = []
    ssis_outline = build_ssis_outline(package)
    adf_outline = build_adf_outline(adf_dir)

    # ── Header ──────────────────────────────────────────────────────────
    parts.append(f"## {section_num}. Package: `{package.name}`")
    parts.append("")

    # ── Summary ─────────────────────────────────────────────────────────
    parts.append("### Summary")
    parts.append("")
    parts.append(_build_prose_summary(package, ssis_outline, plan))
    parts.append("")

    # ── SSIS: Systems involved ──────────────────────────────────────────
    parts.append("### SSIS: Systems Involved")
    parts.append("")
    if ssis_outline["systems"]:
        parts.append("| Connection | Kind | Server / File | Database | Role |")
        parts.append("|---|---|---|---|---|")
        for s in ssis_outline["systems"]:
            parts.append(
                f"| {s['name']} | {s['kind']} | "
                f"{s.get('server') or s.get('file_path') or '—'} | "
                f"{s.get('database') or '—'} | {', '.join(s.get('roles', ['—']))} |"
            )
    parts.append("")

    # ── SSIS: Step-by-step execution ────────────────────────────────────
    parts.append("### SSIS: Execution Steps")
    parts.append("")
    for step in ssis_outline.get("steps", []):
        flag = " *(disabled)*" if step.get("disabled") else ""
        parts.append(
            f"{step['step']}. **{step['task_name']}** "
            f"(`{step['task_type']}`){flag} — {step['description']}"
        )
    parts.append("")

    # ── SSIS: Control flow diagram ──────────────────────────────────────
    parts.append("### SSIS: Control Flow Diagram")
    parts.append("")
    parts.append("```mermaid")
    parts.append(ssis_outline["diagrams"]["control_flow_mermaid"])
    parts.append("```")
    parts.append("")

    # ── SSIS: Data flow diagrams ────────────────────────────────────────
    if ssis_outline["diagrams"]["data_flow_mermaid"]:
        parts.append("### SSIS: Data Flow Diagrams")
        parts.append("")
        for d in ssis_outline["diagrams"]["data_flow_mermaid"]:
            parts.append(f"#### {d['task_name']} ({d['component_count']} components)")
            parts.append("")
            parts.append("```mermaid")
            parts.append(d["mermaid"])
            parts.append("```")
            parts.append("")

    # ── SSIS: Component descriptions ────────────────────────────────────
    parts.append("### SSIS: Component Details")
    parts.append("")
    all_t = _all_tasks(package)
    conn_by_id = {cm.id: cm for cm in package.connection_managers}
    for t in all_t:
        parts.append(f"- **{t.name}** (`{t.task_type.value if hasattr(t.task_type, 'value') else t.task_type}`)")
        desc = _describe_component(t, conn_by_id)
        if desc:
            parts.append(f"  {desc}")
    parts.append("")

    # ── ADF: Solution overview ──────────────────────────────────────────
    parts.append("### ADF: Solution Overview")
    parts.append("")
    at = adf_outline.get("totals", {})
    parts.append(
        f"- **{at.get('pipelines', 0)}** pipeline(s), "
        f"**{at.get('linked_services', 0)}** linked service(s), "
        f"**{at.get('datasets', 0)}** dataset(s), "
        f"**{at.get('data_flows', 0)}** data flow(s), "
        f"**{at.get('triggers', 0)}** trigger(s), "
        f"**{at.get('function_stubs', 0)}** Azure Function stub(s)"
    )
    parts.append("")

    # ── ADF: Pipeline activity graph ────────────────────────────────────
    for pipe in adf_outline.get("pipelines", []):
        parts.append(f"### ADF: Pipeline `{pipe['name']}`")
        parts.append("")
        if pipe.get("description"):
            parts.append(pipe["description"])
            parts.append("")
        parts.append(f"**{pipe['activity_count']} activities** — "
                      + ", ".join(f"`{k}` ×{v}" for k, v in pipe.get("activities_by_type", {}).items()))
        parts.append("")
        parts.append("```mermaid")
        parts.append(pipe["mermaid"])
        parts.append("```")
        parts.append("")

        # Activity-level detail
        parts.append("#### Activities")
        parts.append("")
        for act in pipe.get("activities", []):
            deps = ", ".join(act.get("depends_on", [])) or "none"
            parts.append(f"- **{act['name']}** (`{act['type']}`) — depends on: {deps}")
        parts.append("")

    # ── ADF: Linked services ────────────────────────────────────────────
    if adf_outline.get("linked_services"):
        parts.append("### ADF: Linked Services")
        parts.append("")
        parts.append("| Name | Type |")
        parts.append("|---|---|")
        for ls in adf_outline["linked_services"]:
            parts.append(f"| `{ls['name']}` | `{ls['type']}` |")
        parts.append("")

    # ── ADF: Datasets ───────────────────────────────────────────────────
    if adf_outline.get("datasets"):
        parts.append("### ADF: Datasets")
        parts.append("")
        parts.append("| Name | Type | Linked Service |")
        parts.append("|---|---|---|")
        for ds in adf_outline["datasets"]:
            parts.append(f"| `{ds['name']}` | `{ds['type']}` | `{ds.get('linked_service', '—')}` |")
        parts.append("")

    # ── ADF: Function stubs ─────────────────────────────────────────────
    if adf_outline.get("function_stubs"):
        parts.append("### ADF: Azure Function Stubs")
        parts.append("")
        for stub in adf_outline["function_stubs"]:
            parts.append(f"- **{stub['function_name']}** — `{stub['path']}`")
        parts.append("")

    # ── Plan: Simplifications applied ───────────────────────────────────
    if plan and plan.get("simplifications"):
        parts.append("### Migration Plan: Simplifications")
        parts.append("")
        for simp in plan["simplifications"]:
            items = ", ".join(simp.get("items", []))
            parts.append(f"- **{simp['action']}** → {items}")
            parts.append(f"  _{simp.get('reason', '')}_")
        parts.append("")

    # ── Plan: Risks ─────────────────────────────────────────────────────
    if plan and plan.get("risks"):
        parts.append("### Migration Plan: Risks")
        parts.append("")
        for risk in plan["risks"]:
            sev = risk.get("severity", "medium")
            parts.append(f"- **[{sev}]** {risk['message']}")
            if risk.get("mitigation"):
                parts.append(f"  _Mitigation:_ {risk['mitigation']}")
        parts.append("")

    # ── Pre-deployment tasks ────────────────────────────────────────────
    pre_tasks = _extract_pre_deployment_tasks(package, adf_outline, plan)
    parts.append("### Pre-Deployment Checklist")
    parts.append("")
    if pre_tasks:
        parts.append("| # | Category | Task | Detail | Status | Tool |")
        parts.append("|---|---|---|---|---|---|")
        for i, task in enumerate(pre_tasks, 1):
            parts.append(_checklist_row(i, task))
    else:
        parts.append("_No pre-deployment tasks identified._")
    parts.append("")

    # ── Post-deployment tasks ───────────────────────────────────────────
    post_tasks = _extract_post_deployment_tasks(package, adf_outline, plan)
    parts.append("### Post-Deployment Checklist")
    parts.append("")
    if post_tasks:
        parts.append("| # | Category | Task | Detail | Status | Tool |")
        parts.append("|---|---|---|---|---|---|")
        for i, task in enumerate(post_tasks, 1):
            parts.append(_checklist_row(i, task))
    else:
        parts.append("_No post-deployment tasks identified._")
    parts.append("")

    # ── Effort estimate ─────────────────────────────────────────────────
    if plan and plan.get("effort"):
        eff = plan["effort"]
        parts.append("### Effort Estimate")
        parts.append("")
        parts.append(f"- Architecture: {eff.get('architecture_hours', 0):.1f}h")
        parts.append(f"- Development: {eff.get('development_hours', 0):.1f}h "
                      f"(script porting: {eff.get('script_porting_hours', 0):.1f}h, "
                      f"data flows: {eff.get('dataflow_hours', 0):.1f}h)")
        parts.append(f"- Testing: {eff.get('testing_hours', 0):.1f}h")
        parts.append(f"- **Total: {eff.get('total_hours', 0):.1f}h** ({eff.get('bucket', '?')})")
        parts.append(f"- Range: {eff.get('low_hours', 0):.1f}h — {eff.get('high_hours', 0):.1f}h")
        parts.append("")

    return "\n".join(parts)


def _build_prose_summary(
    package: SSISPackage,
    ssis_outline: dict[str, Any],
    plan: dict[str, Any] | None,
) -> str:
    """Build a human-readable prose summary of what the package does and how."""
    systems = ssis_outline.get("systems", [])
    steps = ssis_outline.get("steps", [])

    # ── What: sources and destinations ──────────────────────────────────
    sources = [s for s in systems if "source" in (s.get("roles") or [])]
    sinks = [s for s in systems if "sink" in (s.get("roles") or [])]

    def _system_label(s: dict[str, Any]) -> str:
        kind = s.get("kind", "").upper()
        if s.get("database") and s.get("server"):
            return f"{kind} database `{s['database']}` on `{s['server']}`"
        if s.get("database"):
            return f"{kind} database `{s['database']}`"
        if s.get("file_path"):
            return f"{kind} file (`{s['file_path']}`)"
        if s.get("server"):
            return f"{kind} server `{s['server']}`"
        return f"{kind} connection `{s['name']}`"

    if sources:
        source_text = ", ".join(_system_label(s) for s in sources)
    else:
        source_text = "an unidentified source"

    if sinks:
        sink_text = ", ".join(_system_label(s) for s in sinks)
    else:
        sink_text = "an unidentified destination"

    summary = f"This package extracts data from {source_text} and loads it to {sink_text}."

    # ── How: high-level workflow description ────────────────────────────
    # Group steps by type for a concise narrative
    step_types: dict[str, list[str]] = {}
    for step in steps:
        ttype = step.get("task_type", "Unknown")
        step_types.setdefault(ttype, []).append(step.get("task_name", "?"))

    how_parts: list[str] = []
    if "ScriptTask" in step_types:
        names = step_types["ScriptTask"]
        how_parts.append(
            f"runs {len(names)} Script Task(s) ({', '.join(names)}) for custom logic"
        )
    if "ExecuteSQLTask" in step_types:
        names = step_types["ExecuteSQLTask"]
        how_parts.append(
            f"executes {len(names)} SQL statement(s) ({', '.join(names)})"
        )
    if "DataFlowTask" in step_types:
        names = step_types["DataFlowTask"]
        how_parts.append(
            f"moves data through {len(names)} data flow pipeline(s) ({', '.join(names)})"
        )
    if "FileSystemTask" in step_types:
        names = step_types["FileSystemTask"]
        how_parts.append(
            f"performs {len(names)} file operation(s) ({', '.join(names)})"
        )
    if "ExecutePackageTask" in step_types:
        names = step_types["ExecutePackageTask"]
        how_parts.append(
            f"calls {len(names)} child package(s) ({', '.join(names)})"
        )
    for ttype, names in step_types.items():
        if ttype not in ("ScriptTask", "ExecuteSQLTask", "DataFlowTask",
                         "FileSystemTask", "ExecutePackageTask",
                         "ForEachLoopContainer", "ForLoopContainer",
                         "SequenceContainer"):
            how_parts.append(f"runs {len(names)} {ttype}(s)")

    if "ForEachLoopContainer" in step_types or "ForLoopContainer" in step_types:
        loop_count = len(step_types.get("ForEachLoopContainer", [])) + len(
            step_types.get("ForLoopContainer", []))
        how_parts.append(f"uses {loop_count} loop(s) for iterative processing")

    if how_parts:
        summary += " The workflow " + ", ".join(how_parts) + "."

    # ── Pattern explanation (if plan provides one) ──────────────────────
    pattern = (plan or {}).get("target_pattern")
    if pattern:
        pattern_descriptions = {
            "scheduled_file_drop": "The target ADF design follows a **scheduled file drop** pattern — extracting data from a database on a schedule and landing it as files (CSV/Parquet) in Azure Data Lake Storage.",
            "ingest_file_to_sql": "The target ADF design follows a **file-to-SQL ingestion** pattern — picking up files from a landing zone and loading them into a SQL staging table.",
            "sql_to_sql_copy": "The target ADF design follows a **SQL-to-SQL copy** pattern — replicating tables directly between SQL databases.",
            "incremental_load": "The target ADF design follows an **incremental load** pattern — using a watermark column to process only new or changed rows since the last run.",
            "dimensional_load": "The target ADF design follows a **dimensional load** pattern — building or maintaining star-schema / SCD tables for analytics.",
            "script_heavy": "The target ADF design is **script-heavy** — the majority of logic lives in Script Tasks that must be ported to Azure Functions.",
            "custom": "The target ADF design uses a **custom** (SSIS-faithful) conversion with no pattern-specific simplifications applied.",
        }
        desc = pattern_descriptions.get(pattern)
        if desc:
            summary += f"\n\n{desc}"

    return summary


def _describe_component(task: Any, conn_by_id: dict[str, Any]) -> str:
    """One-line technical description of a task for the engineer persona."""
    if isinstance(task, ExecuteSQLTask):
        cm = conn_by_id.get(task.connection_id or "")
        target = f" on `{cm.server}/{cm.database}`" if cm and cm.server else ""
        sql_preview = (task.sql_statement or "").strip()[:100]
        return f"Executes SQL{target}: `{sql_preview}`"
    if isinstance(task, DataFlowTask):
        srcs = [c.name for c in task.components if (c.component_type or "").lower().find("source") >= 0]
        dsts = [c.name for c in task.components if (c.component_type or "").lower().find("dest") >= 0]
        return f"Data flow: {', '.join(srcs) or '?'} → {', '.join(dsts) or '?'} ({len(task.components)} components)"
    if isinstance(task, ScriptTask):
        return f"Script Task ({task.script_language}, entry: `{task.entry_point}`). Must be ported to Azure Function."
    if isinstance(task, ExecutePackageTask):
        ref = task.project_package_name or task.package_path or "?"
        return f"Executes child package `{ref}`."
    if isinstance(task, FileSystemTask):
        return f"File operation: {task.operation} — `{task.source_path}` → `{task.destination_path}`"
    if isinstance(task, SendMailTask):
        return "Sends email notification. Mapped to Web Activity → Logic App."
    if isinstance(task, ForEachLoopContainer):
        return f"For-each loop ({task.enumerator_type.value if hasattr(task.enumerator_type, 'value') else task.enumerator_type}), {len(task.tasks)} child task(s)."
    if isinstance(task, ForLoopContainer):
        return f"For loop: eval `{task.eval_expression or '?'}`, {len(task.tasks)} child task(s)."
    if isinstance(task, SequenceContainer):
        return f"Sequence container with {len(task.tasks)} child task(s)."
    return ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_predeployment_report(
    entries: list[dict[str, str | Path]],
    *,
    title: str | None = None,
) -> str:
    """Build a consolidated pre-deployment Markdown report.

    Args:
        entries: list of dicts with keys:
            - ``package_path``: absolute path to .dtsx
            - ``adf_dir``: absolute path to ADF output dir for this package
            - ``plan_path`` (optional): absolute path to migration plan JSON
        title: optional report title

    Returns:
        Full Markdown report as a string.
    """
    from ..parsers.readers.local_reader import LocalReader

    reader = LocalReader()
    parts: list[str] = []

    # ── Title ───────────────────────────────────────────────────────────
    report_title = title or "SSIS → ADF Pre-Deployment Report"
    parts.append(f"# {report_title}")
    parts.append("")
    parts.append(f"_Generated: {datetime.now():%Y-%m-%d %H:%M} | "
                 f"Packages: {len(entries)}_")
    parts.append("")
    parts.append("---")
    parts.append("")

    # ── Table of contents ───────────────────────────────────────────────
    parts.append("## Table of Contents")
    parts.append("")
    for i, entry in enumerate(entries, 1):
        pkg_name = Path(entry["package_path"]).stem
        parts.append(f"{i}. [{pkg_name}](#{i}-package-{pkg_name.lower().replace(' ', '-')})")
    parts.append(f"{len(entries) + 1}. [Shared Infrastructure Checklist]"
                 f"(#{len(entries) + 1}-shared-infrastructure-checklist)")
    parts.append("")
    parts.append("---")
    parts.append("")

    # ── Per-package sections ────────────────────────────────────────────
    all_pre_tasks: list[dict[str, str]] = []
    all_post_tasks: list[dict[str, str]] = []

    for i, entry in enumerate(entries, 1):
        pkg_path = Path(entry["package_path"])
        adf_dir = Path(entry["adf_dir"])
        plan_path = entry.get("plan_path")

        package = reader.read(pkg_path)

        plan: dict[str, Any] | None = None
        if plan_path and Path(plan_path).exists():
            plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))

        section = _render_package_section(package, adf_dir, plan, i)
        parts.append(section)
        parts.append("---")
        parts.append("")

        # Collect tasks for shared summary
        adf_outline = build_adf_outline(adf_dir)
        all_pre_tasks.extend(_extract_pre_deployment_tasks(package, adf_outline, plan))
        all_post_tasks.extend(_extract_post_deployment_tasks(package, adf_outline, plan))

    # ── Shared infrastructure checklist ─────────────────────────────────
    parts.append(f"## {len(entries) + 1}. Shared Infrastructure Checklist")
    parts.append("")
    parts.append("This section consolidates all pre- and post-deployment tasks "
                 "across the estate, de-duplicated where possible.")
    parts.append("")

    # De-duplicate by task text
    seen_pre: set[str] = set()
    unique_pre: list[dict[str, str]] = []
    for t in all_pre_tasks:
        key = t["task"]
        if key not in seen_pre:
            seen_pre.add(key)
            unique_pre.append(t)

    seen_post: set[str] = set()
    unique_post: list[dict[str, str]] = []
    for t in all_post_tasks:
        key = t["task"]
        if key not in seen_post:
            seen_post.add(key)
            unique_post.append(t)

    parts.append("### All Pre-Deployment Tasks (de-duplicated)")
    parts.append("")
    if unique_pre:
        parts.append("| # | Category | Task | Detail | Status | Tool |")
        parts.append("|---|---|---|---|---|---|")
        for i, task in enumerate(unique_pre, 1):
            parts.append(_checklist_row(i, task))
    else:
        parts.append("_None._")
    parts.append("")

    parts.append("### All Post-Deployment Tasks (de-duplicated)")
    parts.append("")
    if unique_post:
        parts.append("| # | Category | Task | Detail | Status | Tool |")
        parts.append("|---|---|---|---|---|---|")
        for i, task in enumerate(unique_post, 1):
            parts.append(_checklist_row(i, task))
    else:
        parts.append("_None._")
    parts.append("")

    return "\n".join(parts)
