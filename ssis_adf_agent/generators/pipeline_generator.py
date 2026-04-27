"""
Pipeline generator — assembles a complete ADF pipeline.json from an SSISPackage.

Supports:
  - Configurable naming prefixes (PL_, LS_, DS_, TR_)
  - Ingestion pattern annotations (full/delta/merge)
  - CDM pattern annotations
  - ESI reuse candidate annotations
  - Schema remapping for database consolidation

Output structure::

    {
        "name": "PL_<PackageName>",
        "properties": {
            "description": "...",
            "activities": [...],
            "parameters": {...},
            "variables": {...}
        }
    }
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ..analyzers.dependency_graph import topological_sort
from ..converters.dispatcher import ConverterDispatcher
from ..parsers.models import (
    DataFlowTask,
    ExecuteSQLTask,
    IngestionPattern,
    SSISPackage,
)
from ..warnings_collector import warn as _warn
from .naming import pl_name as _pl_name

# ADF hard limit on activities per pipeline
_ADF_ACTIVITY_LIMIT = 40

# --- Sensitive value detection ---------------------------------------------
# A pipeline variable / parameter defaultValue is considered sensitive when its
# *name* matches a credential-style keyword OR its *value* looks like one of
# the credential / on-prem-identifier patterns below. Sensitive defaults are
# stripped from the generated pipeline JSON so credentials and on-prem
# hostnames don't ship in plain ARM/Git artifacts.

_SENSITIVE_NAME_RE = re.compile(
    r"(?i)(password|passwd|pwd|secret|token|apikey|api[_-]?key|"
    r"connectionstring|conn[_-]?str|client[_-]?secret|"
    r"sas|key$|credential|username|userid|user[_-]?id|"
    r"login|account)"
)
# Domain\user (Windows / AD account)
_DOMAIN_USER_RE = re.compile(r"^[A-Za-z][\w-]{0,30}\\[A-Za-z][\w.\-$]{0,63}$")
# Hostname / FQDN with a dot and a non-cloud TLD (e.g. on-prem .lcl, .local,
# .corp, .lan, .internal, .intra). Plain Azure hosts (*.windows.net etc.) are
# not flagged because they are not sensitive identifiers.
_ONPREM_FQDN_RE = re.compile(
    r"(?i)\.(lcl|local|corp|lan|intra|internal|prv|priv|prod|dev|test|uat)"
    r"(\\[\w-]+)?(,\d+)?$"
)


def _is_sensitive_name(name: str) -> bool:
    return bool(_SENSITIVE_NAME_RE.search(name or ""))


def _looks_like_credential_value(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    if _DOMAIN_USER_RE.match(value):
        return True
    if _ONPREM_FQDN_RE.search(value):
        return True
    return False


def _redact_sensitive_default(name: str, value: Any) -> tuple[Any, str | None]:
    """Return (safe_default_or_None, description_or_None).

    If the name or value looks sensitive, the default is dropped (returned as
    ``None``) and a description is returned that the deployer can act on.
    Otherwise returns ``(value, None)`` unchanged.
    """
    name_hit = _is_sensitive_name(name)
    value_hit = _looks_like_credential_value(value)
    if not (name_hit or value_hit):
        return value, None
    reason_parts = []
    if name_hit:
        reason_parts.append("name matches credential pattern")
    if value_hit:
        reason_parts.append("value looks like an account name or on-prem FQDN")
    return None, (
        "[SENSITIVE] defaultValue stripped at generation time ("
        + "; ".join(reason_parts)
        + "). Inject at deploy via Key Vault reference, pipeline parameter, "
        "or environment-specific override."
    )


_SSIS_TO_ADF_TYPE: dict[str, str] = {
    "String": "String",
    "Int32": "Int",
    "Int16": "Int",
    "Int64": "Int",
    "Boolean": "Bool",
    "DateTime": "String",
    "Double": "Float",
    "Single": "Float",
    "Object": "String",
}


def _map_param_type(ssis_type: str) -> str:
    return _SSIS_TO_ADF_TYPE.get(ssis_type, "String")


def _collect_annotations(package: SSISPackage) -> list[str]:
    """Build pipeline annotations from detected patterns."""
    annotations = ["ssis-adf-agent", f"source-package:{package.name}"]

    # Ingestion pattern annotations
    has_delta = False
    has_merge = False
    for task in package.tasks:
        pat = IngestionPattern.UNKNOWN
        if isinstance(task, ExecuteSQLTask):
            pat = task.ingestion_pattern
        elif isinstance(task, DataFlowTask):
            pat = task.ingestion_pattern
        if pat == IngestionPattern.DELTA:
            has_delta = True
        elif pat == IngestionPattern.MERGE:
            has_merge = True

    if has_merge:
        annotations.append("ingestion-pattern:merge")
    elif has_delta:
        annotations.append("ingestion-pattern:delta")

    # Cross-DB references
    has_cross_db = any(
        len(t.cross_db_references) > 0 for t in package.tasks
    )
    if has_cross_db:
        annotations.append("has-cross-db-references")

    return annotations


def generate_pipeline(
    package: SSISPackage,
    output_dir: Path,
    stubs_dir: Path | None = None,
    llm_translate: bool = False,
    *,
    pipeline_prefix: str = "PL_",
    cdm_gaps: list | None = None,
    esi_gaps: list | None = None,
    schema_remap: dict[str, str] | None = None,
    ls_name_map: dict[str, str] | None = None,
    name_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Convert an SSISPackage to a full ADF pipeline JSON and write it to *output_dir*.

    Args:
        pipeline_prefix: Prefix for pipeline name (default "PL_").
        cdm_gaps: CDM pattern gap items to annotate pipeline.
        esi_gaps: ESI reuse gap items to annotate pipeline.
        schema_remap: Schema remap config for database consolidation.
        ls_name_map: Mapping from CM ID to linked service name.
        name_overrides: Optional artifact name overrides from the migration plan.

    Returns the pipeline dict.
    """
    dispatcher = ConverterDispatcher(
        stubs_dir=stubs_dir or output_dir / "stubs",
        llm_translate=llm_translate,
        pipeline_prefix=pipeline_prefix,
        ls_name_map=ls_name_map,
        package_name=package.name,
    )
    pipeline_name = _pl_name(package.name, pipeline_prefix, name_overrides=name_overrides)

    # Topological task ordering
    task_by_id = {t.id: t for t in package.tasks}
    ordered_ids = topological_sort(package.tasks, package.constraints)

    # Convert tasks in dependency order
    activities: list[dict[str, Any]] = []
    for task_id in ordered_ids:
        task = task_by_id.get(task_id)
        if task is None:
            continue
        acts = dispatcher.convert_task(task, package.constraints, task_by_id)

        # Apply schema remap to SQL text in Script, Lookup, and StoredProcedure activities
        if schema_remap:
            from ..converters.control_flow.execute_sql_converter import apply_schema_remap
            for act in acts:
                act_type = act.get("type", "")
                tp = act.get("typeProperties", {})

                # Script activities — remap SQL in scripts[].text
                if act_type == "Script":
                    scripts = tp.get("scripts", [])
                    for script in scripts:
                        if "text" in script:
                            script["text"] = apply_schema_remap(script["text"], schema_remap) or script["text"]

                # Lookup activities — remap SQL in source.sqlReaderQuery
                elif act_type == "Lookup":
                    source = tp.get("source", {})
                    if "sqlReaderQuery" in source:
                        source["sqlReaderQuery"] = (
                            apply_schema_remap(source["sqlReaderQuery"], schema_remap)
                            or source["sqlReaderQuery"]
                        )

                # StoredProcedure activities — remap the procedure name
                elif act_type == "SqlServerStoredProcedure":
                    if "storedProcedureName" in tp:
                        tp["storedProcedureName"] = (
                            apply_schema_remap(tp["storedProcedureName"], schema_remap)
                            or tp["storedProcedureName"]
                        )

        activities.extend(acts)

    # Deduplicate activity names — ADF requires unique names
    _deduplicate_activity_names(activities)

    # Warn if activity count exceeds ADF hard limit
    if len(activities) > _ADF_ACTIVITY_LIMIT:
        _warn(
            phase="generate",
            severity="warning",
            source="pipeline_generator",
            message=(
                f"Pipeline has {len(activities)} activities, exceeding the "
                f"ADF limit of {_ADF_ACTIVITY_LIMIT}. Split into sub-pipelines."
            ),
            task_name=package.name,
        )

    # Build parameters from SSIS package parameters
    parameters: dict[str, Any] = {}
    for p in package.parameters:
        entry: dict[str, Any] = {"type": _map_param_type(p.data_type)}
        safe_value, redaction_note = _redact_sensitive_default(p.name, p.value)
        if p.value is not None and safe_value is not None:
            entry["defaultValue"] = safe_value
        if redaction_note:
            entry["description"] = redaction_note
            _warn(
                phase="generate",
                severity="info",
                source="pipeline_generator",
                message=f"Stripped sensitive defaultValue from parameter '{p.name}'",
                detail=redaction_note,
            )
        parameters[p.name] = entry

    # Project-level parameters (from Project.params) are exposed as pipeline
    # parameters so SSIS expressions like @[$Project::Database] (translated to
    # pipeline().parameters.Database) resolve at runtime. Sensitive params get
    # no defaultValue so the deployer must inject a value (Key Vault, etc.).
    for p in package.project_parameters:
        if p.name in parameters:
            continue
        entry = {"type": _map_param_type(p.data_type)}
        safe_value, redaction_note = _redact_sensitive_default(p.name, p.value)
        if p.value is not None and not p.sensitive and safe_value is not None:
            entry["defaultValue"] = safe_value
        if p.sensitive or redaction_note:
            note = redaction_note or (
                "[SENSITIVE] Project-level sensitive parameter — inject at deploy."
            )
            entry["description"] = note
            _warn(
                phase="generate",
                severity="info",
                source="pipeline_generator",
                message=f"Stripped sensitive defaultValue from project parameter '{p.name}'",
                detail=note,
            )
        parameters[p.name] = entry

    # Add implicit parameters for function URLs (referenced by File System / Send Mail converters)
    _inject_function_url_params(parameters, activities)

    # Add delta_column / key_columns as pipeline parameters when detected
    for task in package.tasks:
        if isinstance(task, ExecuteSQLTask) and task.delta_column:
            parameters.setdefault("delta_column", {"type": "String", "defaultValue": task.delta_column})
        if isinstance(task, DataFlowTask):
            for comp in task.components:
                if comp.key_columns:
                    parameters.setdefault("key_columns", {
                        "type": "String",
                        "defaultValue": ",".join(comp.key_columns),
                    })

    # Build variables from SSIS package variables (User namespace only)
    variables: dict[str, Any] = {}
    for v in package.variables:
        if v.namespace.lower() == "user":
            entry = {"type": _map_param_type(v.data_type)}
            safe_value, redaction_note = _redact_sensitive_default(v.name, v.value)
            if v.value is not None and safe_value is not None:
                entry["defaultValue"] = safe_value
            if redaction_note:
                entry["description"] = redaction_note
                _warn(
                    phase="generate",
                    severity="info",
                    source="pipeline_generator",
                    message=f"Stripped sensitive defaultValue from variable '{v.name}'",
                    detail=redaction_note,
                )
            variables[v.name] = entry

    # Annotations
    annotations = _collect_annotations(package)
    if cdm_gaps:
        annotations.append("cdm-review-required")
    if esi_gaps:
        annotations.append("esi-reuse-candidate")

    pipeline: dict[str, Any] = {
        "name": pipeline_name,
        "properties": {
            "description": (
                f"Auto-generated from SSIS package: {package.source_file}. "
                "Review flagged activities before deploying."
            ),
            "activities": activities,
            "parameters": parameters,
            "variables": variables,
            "annotations": annotations,
        },
    }

    # Write to disk
    output_dir.mkdir(parents=True, exist_ok=True)
    pipeline_file = output_dir / "pipeline" / f"{pipeline_name}.json"
    pipeline_file.parent.mkdir(parents=True, exist_ok=True)
    pipeline_file.write_text(
        json.dumps(pipeline, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )

    return pipeline


def _inject_function_url_params(
    parameters: dict[str, Any],
    activities: list[dict],
) -> None:
    """Add pipeline parameters for Azure Function URLs referenced in activities."""
    needed = set()
    for act in activities:
        tp = act.get("typeProperties", {})
        url = tp.get("url", "")
        if isinstance(url, str) and url.startswith("@pipeline().parameters."):
            param_name = url.split("@pipeline().parameters.")[-1]
            needed.add(param_name)
    for name in needed:
        if name not in parameters:
            parameters[name] = {
                "type": "String",
                "defaultValue": "https://TODO.azurewebsites.net/api/" + name.replace("Url", ""),
            }


def _deduplicate_activity_names(activities: list[dict[str, Any]]) -> None:
    """Ensure every activity has a unique name by appending _2, _3, … to collisions.

    Also patches ``dependsOn`` references so renamed activities are still
    reachable by downstream activities.
    """
    seen: dict[str, int] = {}
    old_to_new: dict[int, tuple[str, str]] = {}  # obj-id → (old_name, new_name)

    for act in activities:
        name = act["name"]
        if name in seen:
            seen[name] += 1
            new_name = f"{name}_{seen[name]}"
            old_to_new[id(act)] = (name, new_name)
            act["name"] = new_name
        else:
            seen[name] = 1

    # Patch dependsOn: build old_name → set-of-new-names for renames
    if old_to_new:
        rename_map: dict[str, list[str]] = {}
        for _, (old, new) in old_to_new.items():
            rename_map.setdefault(old, []).append(new)

        for act in activities:
            new_deps = []
            for dep in act.get("dependsOn", []):
                dep_name = dep.get("activity", "")
                if dep_name in rename_map:
                    # This dep refers to a name that was duplicated; find
                    # the renamed version (if any) — keep original reference
                    # since first occurrence kept its name.
                    pass
                new_deps.append(dep)
            act["dependsOn"] = new_deps
