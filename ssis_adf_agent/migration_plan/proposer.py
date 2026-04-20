"""Rule-based v1 proposer for SSIS → ADF migration plans.

Given a parsed ``SSISPackage``, produces a ``MigrationPlan`` describing a
recommended target ADF design. Uses deterministic pattern matching only — no
LLM calls. The output is intentionally **opinionated** (recommends managed
identity, ADLS Gen2, dropping atomic-write FileSystemTask patterns when sinks
are cloud storage, etc.).

The agent calling ``propose_adf_design`` may then use the plan directly or
refine it via LLM reasoning before passing to ``convert_ssis_package``.
"""
from __future__ import annotations

from typing import Iterable

from ..analyzers.complexity_scorer import score_package
from ..analyzers.gap_analyzer import analyze_gaps
from ..parsers.models import (  # type: ignore[attr-defined]
    ConnectionManagerType,
    DataFlowTask,
    ExecuteSQLTask,
    FileSystemTask,
    ForEachLoopContainer,
    ForLoopContainer,
    ScriptTask,
    SequenceContainer,
    SSISConnectionManager,
    SSISPackage,
    SSISTask,
    TaskType,
)
from .models import (
    AuthMode,
    EffortEstimate,
    InfrastructureItem,
    LinkedServiceSpec,
    MigrationPlan,
    RbacAssignment,
    Risk,
    RiskSeverity,
    Simplification,
    SimplificationAction,
    StorageKind,
    TargetPattern,
)


# Connection manager kinds that map to SQL targets / sources
_SQL_CM_TYPES = frozenset({
    ConnectionManagerType.OLEDB,
    ConnectionManagerType.ADO_NET,
})

_FILE_CM_TYPES = frozenset({
    ConnectionManagerType.FLAT_FILE,
})

# File-system operations that exist purely to enable atomic writes on SMB
# (copy a template file, set attributes, rename after the data write completes).
# These are unnecessary when the sink is Azure Blob/ADLS — single PUT is atomic.
_ATOMIC_WRITE_OPS = frozenset({"CopyFile", "MoveFile"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _walk(tasks: Iterable[SSISTask]) -> Iterable[SSISTask]:
    for t in tasks:
        yield t
        if isinstance(t, (SequenceContainer, ForEachLoopContainer, ForLoopContainer)):
            yield from _walk(t.tasks)


def _count_by_type(package: SSISPackage) -> dict[str, int]:
    counts: dict[str, int] = {}
    for t in _walk(package.tasks):
        key = t.task_type.value if hasattr(t.task_type, "value") else str(t.task_type)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _classify_cms(cms: list[SSISConnectionManager]) -> tuple[list[SSISConnectionManager], list[SSISConnectionManager]]:
    """Return (sql_cms, file_cms)."""
    sql = [c for c in cms if c.type in _SQL_CM_TYPES]
    files = [c for c in cms if c.type in _FILE_CM_TYPES]
    return sql, files


# ---------------------------------------------------------------------------
# Pattern detection
# ---------------------------------------------------------------------------

def detect_target_pattern(package: SSISPackage) -> TargetPattern:
    """Best-effort label for the package's intent."""
    counts = _count_by_type(package)
    sql_cms, file_cms = _classify_cms(package.connection_managers)

    n_dataflow = counts.get(TaskType.DATA_FLOW.value, 0)
    n_filesys = counts.get(TaskType.FILE_SYSTEM.value, 0)
    n_script = counts.get(TaskType.SCRIPT.value, 0)
    n_execsql = counts.get(TaskType.EXECUTE_SQL.value, 0)
    n_total = sum(counts.values())

    if n_total == 0:
        return TargetPattern.CUSTOM

    # Script-heavy: more script tasks than anything else
    if n_script >= max(2, n_total // 2):
        return TargetPattern.SCRIPT_HEAVY

    # Single data flow + SQL source + file sink → scheduled file drop
    if n_dataflow == 1 and sql_cms and file_cms:
        return TargetPattern.SCHEDULED_FILE_DROP

    # Single data flow + file source + SQL sink → ingest
    if n_dataflow == 1 and file_cms and sql_cms and n_filesys == 0:
        return TargetPattern.INGEST_FILE_TO_SQL

    # SQL → SQL with no transformation complexity
    if n_dataflow >= 1 and len(sql_cms) >= 2 and not file_cms:
        return TargetPattern.SQL_TO_SQL_COPY

    # Lots of execute SQL + a watermark variable → incremental load (heuristic)
    if n_execsql >= 2 and any(
        "watermark" in (v.name or "").lower() or "lastrun" in (v.name or "").lower()
        for v in package.variables
    ):
        return TargetPattern.INCREMENTAL_LOAD

    return TargetPattern.CUSTOM


# ---------------------------------------------------------------------------
# Simplification rules
# ---------------------------------------------------------------------------

def _atomic_write_simplification(package: SSISPackage, pattern: TargetPattern) -> Simplification | None:
    """If the file sink is going to Azure Blob/ADLS, drop the SMB atomic-write pattern."""
    if pattern not in (TargetPattern.SCHEDULED_FILE_DROP, TargetPattern.INGEST_FILE_TO_SQL):
        return None
    fs_tasks = [t for t in _walk(package.tasks)
                if isinstance(t, FileSystemTask) and t.operation in _ATOMIC_WRITE_OPS]
    # Also pull in non-copy file ops that surround the data flow (Set Attributes, Rename)
    other_fs = [t for t in _walk(package.tasks)
                if isinstance(t, FileSystemTask) and t.operation not in _ATOMIC_WRITE_OPS]
    candidates = fs_tasks + other_fs
    if not candidates:
        return None
    return Simplification(
        action=SimplificationAction.DROP,
        items=[t.name for t in candidates],
        reason=(
            "These FileSystemTasks implement an SMB atomic-write pattern (copy template → "
            "write → rename). Azure Blob/ADLS Gen2 PUT is already atomic, so the dance is "
            "unnecessary when the sink is cloud storage."
        ),
        confidence=0.85,
    )


def _fold_simple_dataflow_simplification(package: SSISPackage, pattern: TargetPattern) -> Simplification | None:
    """Fold a trivial Data Flow Task (1 source + 1 sink + ≤2 light transforms) into a Copy Activity."""
    if pattern not in (TargetPattern.SCHEDULED_FILE_DROP, TargetPattern.INGEST_FILE_TO_SQL,
                       TargetPattern.SQL_TO_SQL_COPY):
        return None
    folds: list[str] = []
    for t in _walk(package.tasks):
        if not isinstance(t, DataFlowTask):
            continue
        comps = t.components or []
        sources = [c for c in comps if "Source" in c.component_type]
        sinks = [c for c in comps if "Destination" in c.component_type]
        transforms = [c for c in comps if c not in sources and c not in sinks]
        # Light transforms: derived column / data conversion only
        light_only = all(
            ct.component_type in {"DerivedColumn", "DataConversion", "RowCount"}
            for ct in transforms
        )
        if len(sources) == 1 and len(sinks) == 1 and len(transforms) <= 2 and light_only:
            folds.append(t.name)
    if not folds:
        return None
    return Simplification(
        action=SimplificationAction.FOLD_TO_COPY_ACTIVITY,
        items=folds,
        reason=(
            "Single source + single sink + only lightweight transforms (derived column / "
            "data conversion). Express the derived columns in the source SQL query and use "
            "a Copy Activity — avoids spinning up a Spark cluster for trivial work."
        ),
        confidence=0.75,
    )


# ---------------------------------------------------------------------------
# Linked-service & infrastructure recommendations
# ---------------------------------------------------------------------------

def _recommend_linked_services(package: SSISPackage) -> list[LinkedServiceSpec]:
    specs: list[LinkedServiceSpec] = []
    sql_cms, file_cms = _classify_cms(package.connection_managers)
    for cm in sql_cms:
        specs.append(LinkedServiceSpec(
            name=f"LS_{cm.name}",
            type="AzureSqlDatabase",
            auth=AuthMode.MANAGED_IDENTITY,
            target_resource=f"sql://{cm.server or '<server>'}/{cm.database or '<db>'}",
            notes="Grant the ADF managed identity db_datareader (or db_datawriter) on the target.",
        ))
    for cm in file_cms:
        specs.append(LinkedServiceSpec(
            name=f"LS_{cm.name}",
            type=StorageKind.ADLS_GEN2.value,
            auth=AuthMode.MANAGED_IDENTITY,
            target_resource=f"storage://<account>/<container>",
            notes=(
                f"Original SSIS path: {cm.file_path or '(unset)'}. "
                "Grant the ADF managed identity 'Storage Blob Data Contributor' on the storage account."
            ),
        ))
    # Other CM types (ODBC, MSMQ, etc.) — pass through SSIS-faithful with a warning
    other = [c for c in package.connection_managers if c.type not in (_SQL_CM_TYPES | _FILE_CM_TYPES)]
    for cm in other:
        specs.append(LinkedServiceSpec(
            name=f"LS_{cm.name}",
            type="Custom",
            auth=AuthMode.MANAGED_IDENTITY,
            notes=f"SSIS connection type '{cm.type.value}' has no direct ADF equivalent. Manual review required.",
        ))
    return specs


def _recommend_infrastructure(linked_services: list[LinkedServiceSpec]) -> list[InfrastructureItem]:
    items: list[InfrastructureItem] = []
    items.append(InfrastructureItem(
        type="Microsoft.DataFactory/factories",
        name_hint="adf-<workload>",
        sku="V2",
        properties={"managedVirtualNetwork": True},
        purpose="The ADF instance that hosts the converted pipelines.",
    ))
    needs_storage = any(ls.type == StorageKind.ADLS_GEN2.value for ls in linked_services)
    if needs_storage:
        items.append(InfrastructureItem(
            type="Microsoft.Storage/storageAccounts",
            name_hint="st<workload>",
            sku="Standard_LRS",
            properties={"isHnsEnabled": True, "minimumTlsVersion": "TLS1_2"},
            purpose="ADLS Gen2 storage for file sources/sinks.",
        ))
    needs_kv = any(ls.auth not in {AuthMode.MANAGED_IDENTITY} for ls in linked_services)
    if needs_kv:
        items.append(InfrastructureItem(
            type="Microsoft.KeyVault/vaults",
            name_hint="kv-<workload>",
            sku="standard",
            properties={"enableRbacAuthorization": True},
            purpose="Holds any non-MI credentials referenced by linked services.",
        ))
    return items


def _recommend_rbac(linked_services: list[LinkedServiceSpec]) -> list[RbacAssignment]:
    rbac: list[RbacAssignment] = []
    for ls in linked_services:
        if ls.auth != AuthMode.MANAGED_IDENTITY:
            continue
        if ls.type == "AzureSqlDatabase":
            rbac.append(RbacAssignment(
                principal="<ADF MI>",
                scope=ls.target_resource or "<sql>",
                role="db_datareader",
                purpose=f"Allow ADF to read from {ls.name}.",
            ))
        elif ls.type == StorageKind.ADLS_GEN2.value:
            rbac.append(RbacAssignment(
                principal="<ADF MI>",
                scope=ls.target_resource or "<storage>",
                role="Storage Blob Data Contributor",
                purpose=f"Allow ADF to read/write {ls.name}.",
            ))
    return rbac


# ---------------------------------------------------------------------------
# Risk detection
# ---------------------------------------------------------------------------

def _detect_risks(package: SSISPackage) -> list[Risk]:
    risks: list[Risk] = []
    # Hard-coded passwords / placeholder credentials
    for cm in package.connection_managers:
        cs = cm.connection_string or ""
        if "password=" in cs.lower() and "TODO" not in cs and "Insert_" not in cs:
            risks.append(Risk(
                severity=RiskSeverity.HIGH,
                message=f"Connection manager '{cm.name}' embeds a password in its connection string.",
                mitigation="Move the password to Key Vault and switch the linked service to Managed Identity if possible.",
            ))
        if "Insert_" in cs or "TODO" in cs:
            risks.append(Risk(
                severity=RiskSeverity.MEDIUM,
                message=f"Connection manager '{cm.name}' contains placeholder values that must be filled in.",
                mitigation="Confirm the real server/database/credential before deployment.",
            ))
    # Script Tasks always carry porting risk
    n_script = sum(1 for t in _walk(package.tasks) if isinstance(t, ScriptTask))
    if n_script > 0:
        risks.append(Risk(
            severity=RiskSeverity.MEDIUM if n_script <= 2 else RiskSeverity.HIGH,
            message=f"{n_script} Script Task(s) require manual porting from C#/VB to Azure Function (Python).",
            mitigation="Use convert_ssis_package with llm_translate=true for an automated first pass; review carefully.",
        ))
    # Cross-database references
    gaps = analyze_gaps(package)
    cross_db = [g for g in gaps if "CrossDatabase" in (g.task_type or "")]
    if cross_db:
        risks.append(Risk(
            severity=RiskSeverity.MEDIUM,
            message=f"{len(cross_db)} cross-database reference(s) detected.",
            mitigation="Confirm the target database is reachable from the ADF managed VNet and update three-part names if consolidating.",
            related_tasks=[g.task_name for g in cross_db if g.task_name],
        ))
    return risks


# ---------------------------------------------------------------------------
# Effort estimate
# ---------------------------------------------------------------------------

def _effort_from_complexity(score: int, n_simplifications: int) -> EffortEstimate:
    if score <= 30:
        bucket = "low"
        dev = 4.0
    elif score <= 55:
        bucket = "medium"
        dev = 12.0
    elif score <= 80:
        bucket = "high"
        dev = 32.0
    else:
        bucket = "very_high"
        dev = 80.0
    arch = max(2.0, dev * 0.15)
    test = max(2.0, dev * 0.25)
    # Simplifications add architectural review time but reduce dev time slightly
    arch += n_simplifications * 0.5
    dev = max(1.0, dev - n_simplifications * 0.5)
    return EffortEstimate(
        architecture_hours=round(arch, 1),
        development_hours=round(dev, 1),
        testing_hours=round(test, 1),
        total_hours=round(arch + dev + test, 1),
        bucket=bucket,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def propose_design(package: SSISPackage) -> MigrationPlan:
    """Build a recommended ``MigrationPlan`` for the given package.

    Deterministic and side-effect-free. The agent may further refine the plan
    using natural-language reasoning before persisting and acting on it.
    """
    pattern = detect_target_pattern(package)

    simplifications: list[Simplification] = []
    for builder in (_atomic_write_simplification, _fold_simple_dataflow_simplification):
        s = builder(package, pattern)
        if s is not None:
            simplifications.append(s)

    linked_services = _recommend_linked_services(package)
    infrastructure = _recommend_infrastructure(linked_services)
    rbac = _recommend_rbac(linked_services)
    risks = _detect_risks(package)

    score = score_package(package).score
    effort = _effort_from_complexity(score, len(simplifications))

    counts = _count_by_type(package)
    summary = (
        f"Pattern: **{pattern.value}**. "
        f"Source has {sum(counts.values())} task(s) across {len(counts)} type(s); "
        f"complexity score {score}. "
        f"Recommended {len(simplifications)} simplification(s), "
        f"{len(linked_services)} linked service(s), "
        f"{len(infrastructure)} infra resource(s), "
        f"{len(rbac)} RBAC assignment(s)."
    )

    return MigrationPlan(
        package_name=package.name,
        package_path=str(package.source_file),
        target_pattern=pattern,
        summary=summary,
        simplifications=simplifications,
        linked_services=linked_services,
        infrastructure_needed=infrastructure,
        rbac_needed=rbac,
        risks=risks,
        effort=effort,
        reasoning_input={
            "complexity_score": score,
            "task_counts": counts,
            "connection_manager_types": [cm.type.value for cm in package.connection_managers],
            "variable_count": len(package.variables),
            "parameter_count": len(package.parameters),
            "event_handler_count": len(package.event_handlers),
        },
    )
