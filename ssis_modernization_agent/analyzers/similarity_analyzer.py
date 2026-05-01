"""
Similarity analyzer — computes structural fingerprints for SSIS packages and
groups structurally identical packages for consolidation into a single
parameterized ADF pipeline.

Fingerprinting
--------------
A fingerprint captures the *shape* of a package while ignoring the specific
values that vary between instances (SQL text, file paths, connection strings,
variable values).  Two packages with the same fingerprint can be safely
consolidated into one parameterized pipeline driven by a config array.

The fingerprint is a tuple of:
  - Task types in execution order  (e.g. ("ExecuteSQLTask", "FileSystemTask"))
  - Connection manager types       (e.g. ("OLEDB", "FLATFILE"))
  - Result set types for Execute SQL tasks
  - Data flow component topologies
  - Container nesting structure

Grouping
--------
``group_similar_packages`` accepts a list of parsed ``SSISPackage`` objects,
fingerprints each one, and returns groups of 2+ packages that share the same
fingerprint along with the per-package parameter values that differ.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from ..parsers.models import (
    DataFlowComponent,
    DataFlowTask,
    ExecuteSQLTask,
    FileSystemTask,
    ForEachLoopContainer,
    ForLoopContainer,
    FTPTask,
    ScriptTask,
    SequenceContainer,
    SSISPackage,
    SSISTask,
)
from .dependency_graph import topological_sort

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

@dataclass
class PackageFingerprint:
    """Structural fingerprint for one SSIS package."""
    package_name: str
    source_file: str
    digest: str  # hex SHA-256 of the canonical shape
    shape_summary: str  # human-readable description of the shape
    task_type_sequence: tuple[str, ...]
    connection_manager_types: tuple[str, ...]


@dataclass
class ParameterSet:
    """The varying values extracted from one package in a consolidation group."""
    package_name: str
    source_file: str
    values: dict[str, Any]  # param_name → value


@dataclass
class ConsolidationGroup:
    """A group of 2+ structurally identical packages that can share one pipeline."""
    fingerprint: PackageFingerprint
    packages: list[SSISPackage]
    parameter_sets: list[ParameterSet]
    shared_parameter_names: list[str]  # ordered list of extracted parameter names


@dataclass
class SimilarityResult:
    """Full result of similarity analysis across multiple packages."""
    total_packages: int
    groups: list[ConsolidationGroup]
    ungrouped: list[PackageFingerprint]  # packages that have no match


# ---------------------------------------------------------------------------
# Fingerprinting
# ---------------------------------------------------------------------------

def fingerprint_package(package: SSISPackage) -> PackageFingerprint:
    """Compute a structural fingerprint for *package*."""
    # Task types in execution order
    ordered_ids = topological_sort(package.tasks, package.constraints)
    task_by_id = {t.id: t for t in package.tasks}
    task_types = tuple(
        _task_shape(task_by_id[tid])
        for tid in ordered_ids
        if tid in task_by_id
    )

    # Connection manager types (sorted for stability)
    cm_types = tuple(sorted(cm.type.value for cm in package.connection_managers))

    # Build the canonical shape dict
    shape = {
        "task_types": task_types,
        "cm_types": cm_types,
        "variable_count": len([v for v in package.variables if v.namespace.lower() == "user"]),
        "parameter_count": len(package.parameters),
        "event_handler_events": tuple(sorted(eh.event_name for eh in package.event_handlers)),
    }

    canonical = json.dumps(shape, sort_keys=True, ensure_ascii=True)
    digest = hashlib.sha256(canonical.encode()).hexdigest()

    # Human-readable summary
    type_counts: dict[str, int] = {}
    for t in task_types:
        base = t.split("(")[0]
        type_counts[base] = type_counts.get(base, 0) + 1
    parts = [f"{count}x {name}" for name, count in sorted(type_counts.items())]
    shape_summary = ", ".join(parts) or "empty"

    return PackageFingerprint(
        package_name=package.name,
        source_file=package.source_file,
        digest=digest,
        shape_summary=shape_summary,
        task_type_sequence=task_types,
        connection_manager_types=cm_types,
    )


def _task_shape(task: SSISTask) -> str:
    """Return a string describing the structural shape of a task (ignoring values)."""
    base = task.task_type.value

    if isinstance(task, ExecuteSQLTask):
        return f"{base}(result={task.result_set_type})"

    if isinstance(task, DataFlowTask):
        comp_shapes = tuple(sorted(_component_shape(c) for c in task.components))
        return f"{base}(components={comp_shapes})"

    if isinstance(task, FileSystemTask):
        return f"{base}(op={task.operation})"

    if isinstance(task, FTPTask):
        return f"{base}(op={task.operation})"

    if isinstance(task, ScriptTask):
        return f"{base}(lang={task.script_language})"

    if isinstance(task, (ForEachLoopContainer, ForLoopContainer)):
        child_shapes = tuple(
            _task_shape(child) for child in task.tasks
        )
        enum_type = ""
        if isinstance(task, ForEachLoopContainer):
            enum_type = f",enum={task.enumerator_type.value}"
        return f"{base}(children={child_shapes}{enum_type})"

    if isinstance(task, SequenceContainer):
        child_shapes = tuple(_task_shape(child) for child in task.tasks)
        return f"{base}(children={child_shapes})"

    return base


def _component_shape(comp: DataFlowComponent) -> str:
    """Shape of a data flow component (type only, not specific table/file)."""
    return comp.component_type


# ---------------------------------------------------------------------------
# Value extraction
# ---------------------------------------------------------------------------

def _extract_varying_values(package: SSISPackage) -> dict[str, Any]:
    """Extract the values that typically differ between similar packages.

    These become the parameters of the consolidated pipeline.
    """
    values: dict[str, Any] = {}

    # Connection manager details
    for cm in package.connection_managers:
        prefix = f"cm_{cm.name}"
        if cm.connection_string:
            values[f"{prefix}_connectionString"] = cm.connection_string
        if cm.server:
            values[f"{prefix}_server"] = cm.server
        if cm.database:
            values[f"{prefix}_database"] = cm.database
        if cm.file_path:
            values[f"{prefix}_filePath"] = cm.file_path

    # SQL statements from Execute SQL tasks
    ordered_ids = topological_sort(package.tasks, package.constraints)
    task_by_id = {t.id: t for t in package.tasks}
    sql_idx = 0
    fs_idx = 0
    for tid in ordered_ids:
        task = task_by_id.get(tid)
        if task is None:
            continue
        _extract_task_values(task, values, counters={"sql": sql_idx, "fs": fs_idx})
        if isinstance(task, ExecuteSQLTask):
            sql_idx += 1
        elif isinstance(task, FileSystemTask):
            fs_idx += 1

    # User variables and their defaults
    for v in package.variables:
        if v.namespace.lower() == "user" and v.value is not None:
            values[f"var_{v.name}"] = v.value

    # Package parameters
    for p in package.parameters:
        if p.value is not None:
            values[f"param_{p.name}"] = p.value

    return values


def _extract_task_values(
    task: SSISTask,
    values: dict[str, Any],
    counters: dict[str, int],
) -> None:
    """Recursively extract varying values from tasks."""
    if isinstance(task, ExecuteSQLTask):
        idx = counters.get("sql", 0)
        if task.sql_statement:
            values[f"sql_{idx}_statement"] = task.sql_statement
        if task.connection_id:
            values[f"sql_{idx}_connectionId"] = task.connection_id
        counters["sql"] = idx + 1

    elif isinstance(task, FileSystemTask):
        idx = counters.get("fs", 0)
        if task.source_path:
            values[f"fs_{idx}_sourcePath"] = task.source_path
        if task.destination_path:
            values[f"fs_{idx}_destPath"] = task.destination_path
        counters["fs"] = idx + 1

    elif isinstance(task, DataFlowTask):
        for comp in task.components:
            if comp.connection_id:
                values[f"df_{comp.name}_connectionId"] = comp.connection_id
            table = comp.properties.get("OpenRowset") or comp.properties.get("TableOrViewName")
            if table:
                values[f"df_{comp.name}_table"] = table

    elif isinstance(task, (ForEachLoopContainer, ForLoopContainer, SequenceContainer)):
        for child in task.tasks:
            _extract_task_values(child, values, counters)


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------

def group_similar_packages(
    packages: list[SSISPackage],
) -> SimilarityResult:
    """Group structurally identical packages for consolidation.

    Returns a ``SimilarityResult`` with groups of 2+ similar packages
    and any ungrouped (unique) packages.
    """
    # Fingerprint all packages
    fingerprints: list[PackageFingerprint] = []
    by_digest: dict[str, list[int]] = {}
    for i, pkg in enumerate(packages):
        fp = fingerprint_package(pkg)
        fingerprints.append(fp)
        by_digest.setdefault(fp.digest, []).append(i)

    groups: list[ConsolidationGroup] = []
    ungrouped: list[PackageFingerprint] = []

    for digest, indices in by_digest.items():
        if len(indices) < 2:
            ungrouped.append(fingerprints[indices[0]])
            continue

        group_packages = [packages[i] for i in indices]

        # Extract parameter sets
        all_values = [_extract_varying_values(pkg) for pkg in group_packages]

        # Find keys that actually differ across the group
        all_keys = sorted(set().union(*(v.keys() for v in all_values)))
        varying_keys: list[str] = []
        for key in all_keys:
            seen = set()
            for vals in all_values:
                seen.add(str(vals.get(key, "")))
            if len(seen) > 1:
                varying_keys.append(key)

        # If nothing varies, still include shared keys for completeness
        if not varying_keys:
            varying_keys = all_keys

        param_sets = [
            ParameterSet(
                package_name=pkg.name,
                source_file=pkg.source_file,
                values={k: vals.get(k) for k in varying_keys},
            )
            for pkg, vals in zip(group_packages, all_values)
        ]

        groups.append(ConsolidationGroup(
            fingerprint=fingerprints[indices[0]],
            packages=group_packages,
            parameter_sets=param_sets,
            shared_parameter_names=varying_keys,
        ))

    return SimilarityResult(
        total_packages=len(packages),
        groups=groups,
        ungrouped=ungrouped,
    )
