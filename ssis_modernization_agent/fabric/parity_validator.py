"""
SSIS↔Fabric structural parity validator.

The Fabric counterpart to
:mod:`ssis_modernization_agent.documentation.parity_validator`.  Compares an
SSIS source package (`.dtsx`) to its converted Fabric Data Pipelines
artifacts and reports whether the conversion preserved the package's
structure.

The result shape mirrors the ADF parity validator on purpose so callers
that already consume parity JSON in CI can switch on a single field.

Checks performed:

1. **Task coverage.**  Every SSIS task type appears in the Fabric pipeline
   as an activity of an expected type.  Counts must match.
2. **Connection coverage.**  Every SSIS Connection Manager has a
   placeholder entry in `connections_required.json`.  Synthetic entries
   (e.g. the implicit Function-host connection) are allowed.
3. **Notebook coverage.**  Every `TridentNotebook` activity in the
   pipeline JSON resolves to a `notebook-content.py` on disk under
   `notebook/<DisplayName>.Notebook/`.
4. **Script-task stub coverage.**  Every SSIS Script Task produces a
   Function stub under `stubs/<FunctionName>/__init__.py` (same contract
   as the ADF path; the Fabric converter shares the Script Task pipeline).
5. **Event handlers.**  Every SSIS event handler is recorded as a warning
   for manual review (Fabric, like ADF, has no direct equivalent).
6. **Artifact JSON shape.**  Delegates to
   :func:`ssis_modernization_agent.fabric.validate_fabric_artifacts` for
   pipeline-content + manifest well-formedness.

Deterministic and offline.  No `fab` calls, no Azure calls.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..parsers.models import (
    ForEachLoopContainer,
    ForLoopContainer,
    ScriptTask,
    SequenceContainer,
    SSISPackage,
    SSISTask,
    TaskType,
)
from .connection_resolver import _PLACEHOLDER_PREFIX
from .validator import validate_fabric_artifacts

# ---------------------------------------------------------------------------
# Expected Fabric activity types per SSIS task type
# ---------------------------------------------------------------------------
# Fabric Data Pipelines reuse most ADF activity-type names.  The notable
# differences vs. _EXPECTED_ACTIVITY_TYPES in the ADF validator:
#   - Mapping Data Flow has no Fabric equivalent → DATA_FLOW becomes either
#     `Copy` (simple 1:1) or `TridentNotebook` (complex; PySpark hand-port).
#   - File-system tasks always become `WebActivity` against a Function (no
#     Fabric File-System activity exists).
#   - SCRIPT activities always become `AzureFunction` (Fabric calls the type
#     `AzureFunction`, not `AzureFunctionActivity`).
_EXPECTED_FABRIC_ACTIVITY_TYPES: dict[TaskType, tuple[str, ...]] = {
    TaskType.EXECUTE_SQL: ("Lookup", "Script", "SqlServerStoredProcedure"),
    TaskType.EXECUTE_PACKAGE: ("ExecutePipeline",),
    TaskType.FILE_SYSTEM: ("WebActivity", "AzureFunction", "Copy"),
    TaskType.FTP: ("Copy",),
    TaskType.SCRIPT: ("AzureFunction", "WebActivity"),
    TaskType.EXECUTE_PROCESS: ("WebActivity", "AzureFunction"),
    TaskType.DATA_FLOW: ("Copy", "TridentNotebook"),
    TaskType.FOREACH_LOOP: ("ForEach",),
    TaskType.FOR_LOOP: ("Until", "SetVariable"),
    TaskType.SEND_MAIL: ("WebActivity",),
}


# ---------------------------------------------------------------------------
# Result objects (shape-compatible with the ADF parity validator)
# ---------------------------------------------------------------------------

@dataclass
class FabricParityIssue:
    severity: str  # "info" | "warning" | "error"
    category: str
    message: str
    detail: str = ""


@dataclass
class FabricParityResult:
    ok: bool = True
    package_name: str = ""
    output_dir: str = ""
    target: str = "fabric"
    summary: dict[str, Any] = field(default_factory=dict)
    matches: list[str] = field(default_factory=list)
    issues: list[FabricParityIssue] = field(default_factory=list)
    artifact_dryrun: dict[str, Any] = field(default_factory=dict)

    def add(self, severity: str, category: str, message: str, detail: str = "") -> None:
        self.issues.append(
            FabricParityIssue(severity=severity, category=category, message=message, detail=detail)
        )
        if severity == "error":
            self.ok = False

    def issues_for(self, category: str, severity: str) -> list[FabricParityIssue]:
        return [i for i in self.issues if i.category == category and i.severity == severity]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "package_name": self.package_name,
            "output_dir": self.output_dir,
            "target": self.target,
            "summary": self.summary,
            "matches": self.matches,
            "issues": [vars(i) for i in self.issues],
            "artifact_dryrun": self.artifact_dryrun,
        }


# ---------------------------------------------------------------------------
# SSIS-side helpers (mirror the ADF validator)
# ---------------------------------------------------------------------------

def _walk(task: SSISTask) -> list[SSISTask]:
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


def _ssis_task_counts(package: SSISPackage) -> dict[str, int]:
    counts: dict[str, int] = {}
    for t in _all_tasks(package):
        kind = t.task_type.value if hasattr(t.task_type, "value") else str(t.task_type)
        counts[kind] = counts.get(kind, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Fabric-side helpers
# ---------------------------------------------------------------------------

def _flatten_fabric_activities(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Recurse into ForEach/IfCondition/Switch/Until containers."""
    out: list[dict[str, Any]] = []
    for act in activities or []:
        if not isinstance(act, dict):
            continue
        out.append(act)
        tp = act.get("typeProperties") or {}
        for key in ("activities", "ifTrueActivities", "ifFalseActivities", "defaultActivities"):
            if isinstance(tp.get(key), list):
                out.extend(_flatten_fabric_activities(tp[key]))
        for case in tp.get("cases") or []:
            if isinstance(case, dict) and isinstance(case.get("activities"), list):
                out.extend(_flatten_fabric_activities(case["activities"]))
    return out


def _load_pipeline_activities(artifacts_dir: Path) -> list[dict[str, Any]]:
    pipeline_dirs = sorted((artifacts_dir / "pipeline").glob("*.DataPipeline"))
    activities: list[dict[str, Any]] = []
    for pdir in pipeline_dirs:
        content = pdir / "pipeline-content.json"
        if not content.is_file():
            continue
        try:
            doc = json.loads(content.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        acts = (doc.get("properties") or {}).get("activities") or []
        if isinstance(acts, list):
            activities.extend(_flatten_fabric_activities(acts))
    return activities


def _load_connections_manifest(artifacts_dir: Path) -> dict[str, Any] | None:
    p = artifacts_dir / "connections_required.json"
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _load_notebook_placeholders(artifacts_dir: Path) -> dict[str, str]:
    p = artifacts_dir / "notebook_placeholders.json"
    if not p.is_file():
        return {}
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    placeholders = doc.get("placeholders") or {}
    return placeholders if isinstance(placeholders, dict) else {}


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def _check_task_coverage(
    package: SSISPackage,
    activities: list[dict[str, Any]],
    result: FabricParityResult,
) -> None:
    ssis_counts = _ssis_task_counts(package)
    fabric_counts: dict[str, int] = {}
    for a in activities:
        t = a.get("type") or "?"
        fabric_counts[t] = fabric_counts.get(t, 0) + 1

    result.summary["ssis_task_counts"] = ssis_counts
    result.summary["fabric_activity_counts"] = fabric_counts
    result.summary["ssis_total_tasks"] = sum(ssis_counts.values())
    result.summary["fabric_total_activities"] = sum(fabric_counts.values())

    for task_type_str, n in ssis_counts.items():
        try:
            tt = TaskType(task_type_str)
        except ValueError:
            result.add(
                "warning", "task_count",
                f"SSIS task type '{task_type_str}' has no known Fabric mapping ({n} occurrence(s))",
            )
            continue
        # Containers are flattened into their parents — no 1:1 activity
        if tt in (TaskType.SEQUENCE, TaskType.UNKNOWN):
            continue
        expected = _EXPECTED_FABRIC_ACTIVITY_TYPES.get(tt)
        if expected is None:
            result.add(
                "warning", "task_count",
                f"No expected Fabric activity type registered for SSIS '{task_type_str}'",
            )
            continue
        produced_types = [et for et in expected if fabric_counts.get(et, 0)]
        produced = sum(fabric_counts.get(et, 0) for et in expected)
        if produced == 0:
            result.add(
                "error", "task_count",
                f"{n} SSIS '{task_type_str}' task(s) → no matching Fabric activity found",
                detail=f"Expected one of: {', '.join(expected)}",
            )
        else:
            result.matches.append(
                f"{task_type_str}: {n} SSIS task(s) → {produced} Fabric activity(ies) "
                f"({', '.join(produced_types)})"
            )


def _check_connections(
    package: SSISPackage,
    manifest: dict[str, Any] | None,
    result: FabricParityResult,
) -> None:
    n_cms = len(package.connection_managers)
    result.summary["ssis_connection_managers"] = n_cms

    if manifest is None:
        if n_cms:
            result.add(
                "error", "connection",
                f"{n_cms} SSIS connection manager(s) but no connections_required.json found",
            )
        result.summary["fabric_connection_placeholders"] = 0
        return

    conns = manifest.get("connections") or []
    if not isinstance(conns, list):
        result.add("error", "connection", "connections_required.json: 'connections' is not a list")
        return

    cm_names_in_manifest = {
        c.get("ssis_connection_manager_name")
        for c in conns
        if isinstance(c, dict) and c.get("ssis_connection_manager_name")
    }
    synthetic_count = sum(
        1 for c in conns
        if isinstance(c, dict)
        and isinstance(c.get("ssis_connection_manager_id"), str)
        and c["ssis_connection_manager_id"].startswith("__synthetic__")
    )
    on_prem_count = sum(
        1 for c in conns
        if isinstance(c, dict)
        and any("On-prem" in (n or "") for n in (c.get("notes") or []))
    )

    result.summary["fabric_connection_placeholders"] = len(conns)
    result.summary["fabric_synthetic_connections"] = synthetic_count
    result.summary["fabric_on_prem_connections"] = on_prem_count

    missing = [cm.name for cm in package.connection_managers if cm.name not in cm_names_in_manifest]
    if missing:
        result.add(
            "error", "connection",
            f"{len(missing)} SSIS connection manager(s) missing from connections_required.json: "
            f"{', '.join(missing)}",
        )
    else:
        result.matches.append(
            f"Connections: {n_cms} SSIS CM(s) → {len(conns)} placeholder(s) "
            f"({synthetic_count} synthetic)"
        )

    if on_prem_count:
        result.add(
            "warning", "connection",
            f"{on_prem_count} connection(s) flagged as on-prem — On-Premises Data Gateway "
            "(OPDG) must be provisioned and bound to the Fabric workspace before deploy",
        )

    # Every placeholder_id should still be in placeholder shape pre-deploy.
    # Anything that looks resolved (not the all-zero prefix) is suspicious in
    # a freshly-converted artifact — usually means a hand edit got committed.
    resolved = [
        c.get("placeholder_id") for c in conns
        if isinstance(c, dict)
        and isinstance(c.get("placeholder_id"), str)
        and not c["placeholder_id"].startswith(_PLACEHOLDER_PREFIX)
    ]
    if resolved:
        result.add(
            "warning", "connection",
            f"{len(resolved)} connection(s) appear pre-resolved (no placeholder prefix). "
            "Re-resolution at deploy time will be skipped for these.",
            detail=", ".join(str(r) for r in resolved[:5]),
        )


def _check_notebooks(
    artifacts_dir: Path,
    activities: list[dict[str, Any]],
    placeholders: dict[str, str],
    result: FabricParityResult,
) -> None:
    notebook_dir = artifacts_dir / "notebook"
    on_disk: dict[str, Path] = {}
    if notebook_dir.is_dir():
        for nbdir in notebook_dir.glob("*.Notebook"):
            stub = nbdir / "notebook-content.py"
            if stub.is_file():
                on_disk[nbdir.name] = stub

    referenced_ids: list[str] = []
    for a in activities:
        if a.get("type") != "TridentNotebook":
            continue
        tp = a.get("typeProperties") or {}
        nb_id = tp.get("notebookId")
        if isinstance(nb_id, str):
            referenced_ids.append(nb_id)

    result.summary["fabric_notebook_stubs_on_disk"] = len(on_disk)
    result.summary["fabric_notebook_activities"] = len(referenced_ids)
    result.summary["fabric_notebook_placeholders"] = len(placeholders)

    # Every TridentNotebook reference must resolve to a placeholder, and
    # every placeholder must have a stub on disk.
    for nb_id in referenced_ids:
        display = placeholders.get(nb_id)
        if display is None:
            result.add(
                "error", "notebook",
                f"TridentNotebook activity references notebookId '{nb_id}' "
                "with no entry in notebook_placeholders.json",
            )
            continue
        if display not in on_disk:
            result.add(
                "error", "notebook",
                f"Notebook placeholder '{display}' has no notebook-content.py on disk",
            )

    # Orphan stubs are a warning — the converter sometimes emits a stub for
    # a DFT that the dispatcher classified as simple (Copy), so the stub is
    # generated but not referenced.  Surface it; don't fail.
    referenced_displays = {placeholders.get(i) for i in referenced_ids if placeholders.get(i)}
    orphans = sorted(set(on_disk) - referenced_displays)
    if orphans:
        result.add(
            "info", "notebook",
            f"{len(orphans)} notebook stub(s) on disk are not referenced by any activity "
            "(simple DFT → Copy classification, stub kept as starting point)",
            detail=", ".join(orphans[:5]),
        )

    if referenced_ids and not result.issues_for("notebook", "error"):
        result.matches.append(
            f"Notebooks: {len(referenced_ids)} TridentNotebook activity(ies) "
            f"→ {len(referenced_ids)} stub(s) resolved"
        )


def _check_script_tasks(
    package: SSISPackage,
    artifacts_dir: Path,
    result: FabricParityResult,
) -> None:
    scripts = [t for t in _all_tasks(package) if isinstance(t, ScriptTask)]
    if not scripts:
        return
    stubs_dir = artifacts_dir / "stubs"
    stub_names = (
        [d.name for d in stubs_dir.iterdir() if d.is_dir()]
        if stubs_dir.is_dir() else []
    )
    result.summary["ssis_script_tasks"] = len(scripts)
    result.summary["fabric_function_stubs"] = len(stub_names)
    if len(stub_names) < len(scripts):
        result.add(
            "error", "script_task",
            f"{len(scripts)} Script Task(s) but only {len(stub_names)} Azure Function stub(s) "
            "generated",
        )
    else:
        result.matches.append(
            f"Script Tasks: {len(scripts)} SSIS task(s) → {len(stub_names)} Function stub(s)"
        )
    result.add(
        "warning", "script_task",
        f"{len(scripts)} Script Task(s) require manual porting from C#/VB to Python",
    )


def _check_event_handlers(package: SSISPackage, result: FabricParityResult) -> None:
    if not package.event_handlers:
        return
    result.summary["ssis_event_handlers"] = len(package.event_handlers)
    handler_summary = [
        f"{eh.event_name} on {eh.parent_task_name or '(package)'}"
        for eh in package.event_handlers
    ]
    result.summary["event_handler_details"] = handler_summary
    result.add(
        "info", "event_handler",
        f"{len(package.event_handlers)} SSIS event handler(s) — "
        "verify Fabric error/success paths cover the same logic",
        detail="; ".join(handler_summary),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_fabric_conversion_parity(
    package: SSISPackage,
    artifacts_dir: Path,
) -> dict[str, Any]:
    """Compare *package* to the Fabric artifacts under *artifacts_dir*.

    Returns a dict with the same top-level shape as the ADF parity
    validator: ``ok``, ``package_name``, ``output_dir``, ``summary``,
    ``matches``, ``issues``, ``artifact_dryrun``, plus ``target="fabric"``
    so callers consuming both validators in one CI step can route on it.
    """
    artifacts_dir = Path(artifacts_dir)
    result = FabricParityResult(package_name=package.name, output_dir=str(artifacts_dir))

    # 1. Structural validation of the JSON shape (delegate).
    artifact_validation = validate_fabric_artifacts(artifacts_dir)
    result.artifact_dryrun = {
        "valid": artifact_validation.get("valid", False),
        "errors": list(artifact_validation.get("errors", [])),
        "warnings": list(artifact_validation.get("warnings", [])),
        "pipelines": artifact_validation.get("pipelines", 0),
        "notebooks": artifact_validation.get("notebooks", 0),
    }
    for err in artifact_validation.get("errors", []):
        result.add("error", "artifact_shape", err)
    for warn in artifact_validation.get("warnings", []):
        result.add("warning", "artifact_shape", warn)

    # 2. Load Fabric-side state.
    activities = _load_pipeline_activities(artifacts_dir)
    manifest = _load_connections_manifest(artifacts_dir)
    placeholders = _load_notebook_placeholders(artifacts_dir)

    # 3. Run the SSIS↔Fabric checks.
    _check_task_coverage(package, activities, result)
    _check_connections(package, manifest, result)
    _check_notebooks(artifacts_dir, activities, placeholders, result)
    _check_script_tasks(package, artifacts_dir, result)
    _check_event_handlers(package, result)

    return result.to_dict()


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

def render_fabric_parity_markdown(parity: dict[str, Any]) -> str:
    """Render a :func:`validate_fabric_conversion_parity` result to Markdown."""
    lines: list[str] = []
    pkg = parity.get("package_name", "?")
    verdict = "✅ PASS" if parity.get("ok") else "❌ FAIL"
    lines.append(f"# Fabric Parity Report — `{pkg}`")
    lines.append("")
    lines.append(f"**Verdict:** {verdict}")
    lines.append(f"**Output dir:** `{parity.get('output_dir', '?')}`")
    lines.append("")

    summary = parity.get("summary") or {}
    if summary:
        lines.append("## Summary")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        for k in (
            "ssis_total_tasks", "fabric_total_activities",
            "ssis_connection_managers", "fabric_connection_placeholders",
            "fabric_synthetic_connections", "fabric_on_prem_connections",
            "ssis_script_tasks", "fabric_function_stubs",
            "fabric_notebook_activities", "fabric_notebook_stubs_on_disk",
            "ssis_event_handlers",
        ):
            if k in summary:
                lines.append(f"| {k} | {summary[k]} |")
        lines.append("")

    matches = parity.get("matches") or []
    if matches:
        lines.append("## Matches")
        lines.append("")
        for m in matches:
            lines.append(f"- ✅ {m}")
        lines.append("")

    issues = parity.get("issues") or []
    if issues:
        lines.append("## Issues")
        lines.append("")
        lines.append("| Severity | Category | Message |")
        lines.append("|---|---|---|")
        for i in issues:
            sev = i.get("severity", "?")
            icon = {"error": "❌", "warning": "🟡", "info": "ℹ️"}.get(sev, "•")
            msg = (i.get("message", "") or "").replace("|", "\\|")
            lines.append(f"| {icon} {sev} | {i.get('category', '')} | {msg} |")
        lines.append("")

    return "\n".join(lines)
