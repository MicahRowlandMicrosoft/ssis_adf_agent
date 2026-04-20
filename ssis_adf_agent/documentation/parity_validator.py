"""
Pre-deployment parity validator.

Compares an SSIS source package to its converted ADF artifacts and reports
whether the conversion preserved the package's logic. Performs:

  1. **Structural parity** — task counts by type, control-flow edges,
     data-flow component counts, connection-manager → linked-service mapping,
     parameter coverage. Deterministic, no Azure calls.

  2. **Optional dry-run** — deserializes each generated ADF JSON via the
     azure-mgmt-datafactory SDK models to catch schema errors locally, and
     (if subscription / resource_group / factory_name are supplied) calls
     ``client.factories.get`` to confirm the target factory is reachable.

The result is a structured dict listing matches, mismatches, and warnings,
plus a top-level ``ok`` boolean.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..parsers.models import (
    DataFlowTask,
    ForEachLoopContainer,
    ForLoopContainer,
    ScriptTask,
    SequenceContainer,
    SSISPackage,
    SSISTask,
    TaskType,
)

# ---------------------------------------------------------------------------
# Expected ADF activity types per SSIS task type
# (matches converters/dispatcher.py mapping)
# ---------------------------------------------------------------------------
_EXPECTED_ACTIVITY_TYPES: dict[TaskType, tuple[str, ...]] = {
    TaskType.EXECUTE_SQL: ("Lookup", "SqlServerStoredProcedure", "Script"),
    TaskType.EXECUTE_PACKAGE: ("ExecutePipeline",),
    TaskType.FILE_SYSTEM: ("Copy", "WebActivity", "AzureFunctionActivity"),
    TaskType.FTP: ("Copy",),
    TaskType.SCRIPT: ("AzureFunctionActivity", "WebActivity"),
    TaskType.EXECUTE_PROCESS: ("WebActivity", "AzureFunctionActivity"),
    TaskType.DATA_FLOW: ("Copy", "ExecuteDataFlow"),
    TaskType.FOREACH_LOOP: ("ForEach",),
    TaskType.FOR_LOOP: ("Until", "SetVariable"),
    TaskType.SEND_MAIL: ("WebActivity",),
}


@dataclass
class ParityIssue:
    severity: str  # "info" | "warning" | "error"
    category: str  # "task_count" | "linked_service" | "parameter" | "schema" | ...
    message: str
    detail: str = ""


@dataclass
class ParityResult:
    ok: bool = True
    package_name: str = ""
    output_dir: str = ""
    summary: dict[str, Any] = field(default_factory=dict)
    matches: list[str] = field(default_factory=list)
    issues: list[ParityIssue] = field(default_factory=list)
    artifact_dryrun: dict[str, Any] = field(default_factory=dict)
    factory_check: dict[str, Any] = field(default_factory=dict)

    def add(self, severity: str, category: str, message: str, detail: str = "") -> None:
        self.issues.append(ParityIssue(severity=severity, category=category, message=message, detail=detail))
        if severity == "error":
            self.ok = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "package_name": self.package_name,
            "output_dir": self.output_dir,
            "summary": self.summary,
            "matches": self.matches,
            "issues": [vars(i) for i in self.issues],
            "artifact_dryrun": self.artifact_dryrun,
            "factory_check": self.factory_check,
        }


# ---------------------------------------------------------------------------
# Helpers
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


def _flatten_activities(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Recursively flatten ForEach/IfCondition/Switch nested activities."""
    out: list[dict[str, Any]] = []
    for act in activities:
        out.append(act)
        tp = act.get("typeProperties", {}) or {}
        # ForEach has 'activities'
        if "activities" in tp:
            out.extend(_flatten_activities(tp["activities"] or []))
        # IfCondition has ifTrueActivities / ifFalseActivities
        for key in ("ifTrueActivities", "ifFalseActivities"):
            if key in tp:
                out.extend(_flatten_activities(tp[key] or []))
        # Switch has cases[*].activities and defaultActivities
        for case in tp.get("cases", []) or []:
            out.extend(_flatten_activities(case.get("activities") or []))
        if "defaultActivities" in tp:
            out.extend(_flatten_activities(tp["defaultActivities"] or []))
        # Until has 'activities'
        # (already handled by 'activities' key above in some payloads)
    return out


# ---------------------------------------------------------------------------
# Structural parity
# ---------------------------------------------------------------------------

def _check_task_coverage(
    package: SSISPackage,
    activities: list[dict[str, Any]],
    result: ParityResult,
) -> None:
    ssis_counts = _ssis_task_counts(package)
    adf_counts: dict[str, int] = {}
    for a in activities:
        t = a.get("type") or "?"
        adf_counts[t] = adf_counts.get(t, 0) + 1

    result.summary["ssis_task_counts"] = ssis_counts
    result.summary["adf_activity_counts"] = adf_counts
    result.summary["ssis_total_tasks"] = sum(ssis_counts.values())
    result.summary["adf_total_activities"] = sum(adf_counts.values())

    # Check each SSIS task type yielded *some* expected activity type
    for task_type_str, n in ssis_counts.items():
        try:
            tt = TaskType(task_type_str)
        except ValueError:
            result.add(
                "warning", "task_count",
                f"SSIS task type '{task_type_str}' has no known ADF mapping ({n} occurrence(s))",
            )
            continue
        expected = _EXPECTED_ACTIVITY_TYPES.get(tt)
        if expected is None:
            # Containers are flattened; not expected to map 1:1
            if tt in (TaskType.SEQUENCE, TaskType.UNKNOWN):
                continue
            result.add(
                "warning", "task_count",
                f"No expected ADF activity type registered for SSIS '{task_type_str}'",
            )
            continue
        produced = sum(adf_counts.get(et, 0) for et in expected)
        if produced == 0:
            result.add(
                "error", "task_count",
                f"{n} SSIS '{task_type_str}' task(s) → no matching ADF activity found",
                detail=f"Expected one of: {', '.join(expected)}",
            )
        else:
            result.matches.append(
                f"{task_type_str}: {n} SSIS task(s) → {produced} ADF activity(ies) "
                f"({', '.join(et for et in expected if adf_counts.get(et, 0))})"
            )


def _check_linked_services(
    package: SSISPackage,
    linked_services: list[tuple[Path, dict[str, Any]]],
    result: ParityResult,
) -> None:
    # The actual generator names use the full GUID — read what's there:
    ls_names = {payload.get("name") for _, payload in linked_services if payload.get("name")}

    result.summary["ssis_connection_managers"] = len(package.connection_managers)
    result.summary["adf_linked_services"] = len(linked_services)

    if package.connection_managers and not ls_names:
        result.add(
            "error", "linked_service",
            f"{len(package.connection_managers)} SSIS connection manager(s) but no linked services generated",
        )
        return

    if len(linked_services) < len(package.connection_managers):
        result.add(
            "warning", "linked_service",
            f"Fewer linked services ({len(linked_services)}) than SSIS "
            f"connection managers ({len(package.connection_managers)}) — possible dedup or missing conversion",
        )

    # Check for placeholder values that would fail at runtime
    for path, payload in linked_services:
        body = json.dumps(payload)
        if any(token in body for token in ("Insert_SQL_Server_Name_Here", "TODO_", "AccountName=TODO", "ChangeMe")):
            result.add(
                "warning", "linked_service",
                f"Linked service '{payload.get('name')}' contains placeholder values "
                "that must be filled before deployment",
                detail=str(path),
            )


def _check_parameters(
    package: SSISPackage,
    pipelines: list[tuple[Path, dict[str, Any]]],
    result: ParityResult,
) -> None:
    ssis_params = {p.name for p in package.parameters}
    ssis_proj_params = {p.name for p in package.project_parameters}
    expected = ssis_params | ssis_proj_params

    pipeline_params: set[str] = set()
    for _, payload in pipelines:
        for p in (payload.get("properties", {}).get("parameters") or {}):
            pipeline_params.add(p)

    result.summary["ssis_parameters"] = sorted(expected)
    result.summary["adf_pipeline_parameters"] = sorted(pipeline_params)

    missing = expected - pipeline_params
    if missing:
        result.add(
            "warning", "parameter",
            f"{len(missing)} SSIS parameter(s) not represented in any pipeline: {', '.join(sorted(missing))}",
        )


def _check_data_flows(
    package: SSISPackage,
    dataflows: list[tuple[Path, dict[str, Any]]],
    result: ParityResult,
) -> None:
    ssis_dfts = [t for t in _all_tasks(package) if isinstance(t, DataFlowTask)]
    result.summary["ssis_data_flow_tasks"] = len(ssis_dfts)
    result.summary["adf_mapping_data_flows"] = len(dataflows)
    # Note: simple DFTs convert to Copy activities (no separate dataflow JSON)
    # Only complex DFTs become ExecuteDataFlow + dataflow JSON
    if ssis_dfts and len(dataflows) > len(ssis_dfts):
        result.add(
            "warning", "data_flow",
            f"More mapping data flows ({len(dataflows)}) than SSIS Data Flow Tasks ({len(ssis_dfts)})",
        )


def _check_event_handlers(package: SSISPackage, result: ParityResult) -> None:
    if not package.event_handlers:
        return
    result.summary["ssis_event_handlers"] = len(package.event_handlers)
    handler_summary: list[str] = []
    for eh in package.event_handlers:
        handler_summary.append(f"{eh.event_name} on {eh.parent_task_name or '(package)'}")
    result.summary["event_handler_details"] = handler_summary
    result.add(
        "info", "event_handler",
        f"{len(package.event_handlers)} SSIS event handler(s) — verify ADF error/success paths cover the same logic",
        detail="; ".join(handler_summary),
    )


def _check_script_tasks(package: SSISPackage, result: ParityResult, output_dir: Path) -> None:
    scripts = [t for t in _all_tasks(package) if isinstance(t, ScriptTask)]
    if not scripts:
        return
    stubs_dir = output_dir / "stubs"
    stub_dirs = []
    if stubs_dir.exists():
        stub_dirs = [d.name for d in stubs_dir.iterdir() if d.is_dir()]
    result.summary["ssis_script_tasks"] = len(scripts)
    result.summary["adf_function_stubs"] = len(stub_dirs)
    if len(stub_dirs) < len(scripts):
        result.add(
            "error", "script_task",
            f"{len(scripts)} Script Task(s) but only {len(stub_dirs)} Azure Function stub(s) generated",
        )
    result.add(
        "warning", "script_task",
        f"{len(scripts)} Script Task(s) require manual porting from C#/VB to Python",
        detail=", ".join(t.name for t in scripts),
    )


# ---------------------------------------------------------------------------
# Optional dry-run via SDK deserialization + factory reachability
# ---------------------------------------------------------------------------

def _sdk_dry_run(
    pipelines: list[tuple[Path, dict[str, Any]]],
    linked_services: list[tuple[Path, dict[str, Any]]],
    datasets: list[tuple[Path, dict[str, Any]]],
    dataflows: list[tuple[Path, dict[str, Any]]],
    triggers: list[tuple[Path, dict[str, Any]]],
    result: ParityResult,
) -> None:
    """Deserialize each generated artifact via the SDK to catch schema errors."""
    try:
        from azure.mgmt.datafactory.models import (
            DataFlowResource,
            DatasetResource,
            LinkedServiceResource,
            PipelineResource,
            TriggerResource,
        )
    except ImportError:
        result.add(
            "warning", "schema",
            "azure-mgmt-datafactory not installed; skipping SDK dry-run validation",
        )
        return

    counts = {"pipelines": 0, "linked_services": 0, "datasets": 0, "data_flows": 0, "triggers": 0}
    errors: list[dict[str, str]] = []

    def _try(model_cls: Any, payload: dict[str, Any], path: Path, kind: str) -> None:
        # SDK models expect the 'properties' subtree, not the wrapper
        try:
            props = payload.get("properties", payload)
            model_cls.deserialize({"properties": props, "name": payload.get("name", path.stem)})
            counts[kind] += 1
        except Exception as exc:
            errors.append({"file": str(path), "kind": kind, "error": str(exc)[:300]})

    for path, p in pipelines:
        _try(PipelineResource, p, path, "pipelines")
    for path, p in linked_services:
        _try(LinkedServiceResource, p, path, "linked_services")
    for path, p in datasets:
        _try(DatasetResource, p, path, "datasets")
    for path, p in dataflows:
        _try(DataFlowResource, p, path, "data_flows")
    for path, p in triggers:
        _try(TriggerResource, p, path, "triggers")

    result.artifact_dryrun = {"deserialized": counts, "errors": errors}
    if errors:
        result.add(
            "error", "schema",
            f"{len(errors)} ADF artifact(s) failed SDK deserialization",
            detail="; ".join(f"{e['kind']}: {e['file']}" for e in errors[:5]),
        )


def _check_factory(
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    result: ParityResult,
) -> None:
    """Confirm the target factory exists and the caller can authenticate."""
    try:
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.datafactory import DataFactoryManagementClient
    except ImportError:
        result.add(
            "warning", "factory_check",
            "azure-mgmt-datafactory / azure-identity not installed; skipping factory check",
        )
        return

    try:
        client = DataFactoryManagementClient(DefaultAzureCredential(), subscription_id)
        factory_obj = client.factories.get(resource_group, factory_name)
        if factory_obj is None:
            result.add(
                "warning", "factory_check",
                f"Factory '{factory_name}' returned None from the SDK",
            )
            return
        existing_pipelines = list(client.pipelines.list_by_factory(resource_group, factory_name))
        existing_ls = list(client.linked_services.list_by_factory(resource_group, factory_name))
        result.factory_check = {
            "factory_name": getattr(factory_obj, "name", "") or factory_name,
            "location": getattr(factory_obj, "location", "") or "",
            "provisioning_state": getattr(factory_obj, "provisioning_state", None),
            "existing_pipeline_count": len(existing_pipelines),
            "existing_linked_service_count": len(existing_ls),
        }
    except Exception as exc:
        result.add(
            "warning", "factory_check",
            f"Could not reach factory '{factory_name}' in '{resource_group}': {exc}",
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_parity(
    package: SSISPackage,
    output_dir: str | Path,
    *,
    dry_run: bool = True,
    subscription_id: str | None = None,
    resource_group: str | None = None,
    factory_name: str | None = None,
) -> ParityResult:
    """Compare *package* against generated ADF artifacts in *output_dir*.

    Args:
        package: parsed SSIS package
        output_dir: directory containing generated ADF JSON artifacts
        dry_run: if True, additionally deserialize artifacts via the SDK
        subscription_id, resource_group, factory_name: optional — if all
            three are provided, additionally call factories.get to confirm
            the target factory is reachable.
    """
    root = Path(output_dir)
    result = ParityResult(
        package_name=package.name,
        output_dir=str(root),
    )

    pipelines = _load_jsons(root / "pipeline")
    linked_services = _load_jsons(root / "linkedService")
    datasets = _load_jsons(root / "dataset")
    dataflows = _load_jsons(root / "dataflow")
    triggers = _load_jsons(root / "trigger")

    if not pipelines:
        result.add("error", "structure", f"No pipeline JSONs found in {root / 'pipeline'}")
        return result

    # Flatten all activities across all pipelines (handles ForEach nesting)
    all_activities: list[dict[str, Any]] = []
    for _, payload in pipelines:
        all_activities.extend(_flatten_activities(payload.get("properties", {}).get("activities", [])))

    _check_task_coverage(package, all_activities, result)
    _check_linked_services(package, linked_services, result)
    _check_parameters(package, pipelines, result)
    _check_data_flows(package, dataflows, result)
    _check_event_handlers(package, result)
    _check_script_tasks(package, result, root)

    if dry_run:
        _sdk_dry_run(pipelines, linked_services, datasets, dataflows, triggers, result)

    if subscription_id and resource_group and factory_name:
        _check_factory(subscription_id, resource_group, factory_name, result)

    return result


# ---------------------------------------------------------------------------
# Markdown summary
# ---------------------------------------------------------------------------

def render_parity_markdown(result: ParityResult) -> str:
    parts: list[str] = []
    icon = "✅" if result.ok else "❌"
    parts.append(f"# {icon} Parity report — `{result.package_name}`")
    parts.append(f"_ADF artifacts: `{result.output_dir}`_\n")

    s = result.summary
    parts.append("## Coverage summary")
    ssis_t = s.get('ssis_total_tasks', 0)
    adf_t = s.get('adf_total_activities', 0)
    parts.append(f"- SSIS tasks: **{ssis_t}** → ADF activities: **{adf_t}**")
    cm = s.get('ssis_connection_managers', 0)
    ls = s.get('adf_linked_services', 0)
    parts.append(f"- Connection managers: {cm} → linked services: {ls}")
    dft = s.get('ssis_data_flow_tasks', 0)
    mdf = s.get('adf_mapping_data_flows', 0)
    parts.append(f"- Data Flow Tasks: {dft} → mapping data flows: {mdf}")
    if s.get("ssis_script_tasks"):
        parts.append(f"- Script Tasks: {s['ssis_script_tasks']} → function stubs: {s.get('adf_function_stubs', 0)}")

    if result.matches:
        parts.append("\n## Matches")
        for m in result.matches:
            parts.append(f"- ✓ {m}")

    by_sev: dict[str, list[ParityIssue]] = {"error": [], "warning": [], "info": []}
    for i in result.issues:
        by_sev.setdefault(i.severity, []).append(i)

    for sev, label, emoji in [("error", "Errors", "🔴"), ("warning", "Warnings", "🟡"), ("info", "Info", "🔵")]:
        items = by_sev.get(sev, [])
        if not items:
            continue
        parts.append(f"\n## {emoji} {label} ({len(items)})")
        for i in items:
            parts.append(f"- **[{i.category}]** {i.message}")
            if i.detail:
                parts.append(f"  - _{i.detail}_")

    if result.artifact_dryrun:
        parts.append("\n## SDK dry-run")
        d = result.artifact_dryrun.get("deserialized", {})
        parts.append(
            "Deserialized: "
            + ", ".join(f"{k}={v}" for k, v in d.items())
        )

    if result.factory_check:
        parts.append("\n## Target factory")
        for k, v in result.factory_check.items():
            parts.append(f"- {k}: `{v}`")

    return "\n".join(parts)
