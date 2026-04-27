"""
CDM (Common Data Model) pattern detector.

Detects SSIS patterns that correspond to CDM-layer transformations:
  - Multi-source joins (MergeJoin, Merge with 2+ sources)
  - Aggregation patterns (Aggregate transform)
  - Cross-system enrichment (sources from different connection managers)
  - Denormalization (Lookup transforms used as enrichment)
  - Fact table patterns (large INSERT/SELECT combining multiple tables)

Output is informational only (INFO severity). Does not block or alter conversion.
Adds "cdm-review-required" annotation and gap items for CDM team review.
"""
from __future__ import annotations

from ..parsers.models import (
    DataFlowTask,
    GapItem,
    Severity,
    SSISPackage,
    TaskType,
)

_SOURCE_TYPES = frozenset({
    "OleDbSource", "FlatFileSource", "ExcelSource", "OdbcSource",
    "ADONetSource", "SqlServerSource",
})

_JOIN_TYPES = frozenset({"MergeJoin", "Merge"})
_AGGREGATION_TYPES = frozenset({"Aggregate"})
_LOOKUP_TYPES = frozenset({"Lookup"})


def detect_cdm_patterns(package: SSISPackage) -> list[GapItem]:
    """Detect data flow patterns that may overlap with CDM layer logic.

    Returns GapItems with severity="info" and task_type containing "CDM_PATTERN".
    """
    gaps: list[GapItem] = []

    for task in package.tasks:
        if task.task_type != TaskType.DATA_FLOW:
            continue
        assert isinstance(task, DataFlowTask)

        sources = [c for c in task.components if c.component_type in _SOURCE_TYPES]
        joins = [c for c in task.components if c.component_type in _JOIN_TYPES]
        aggregations = [c for c in task.components if c.component_type in _AGGREGATION_TYPES]
        lookups = [c for c in task.components if c.component_type in _LOOKUP_TYPES]

        # Pattern 1: Multi-source joins
        if len(sources) >= 2 and joins:
            join_names = ", ".join(j.name for j in joins)
            gaps.append(GapItem(
                task_id=task.id,
                task_name=task.name,
                task_type="CDM_PATTERN/MultiSourceJoin",
                severity=Severity.INFO,
                message=(
                    f"Data flow combines {len(sources)} sources via join(s): {join_names}. "
                    "This pattern typically indicates CDM-layer logic."
                ),
                recommendation=(
                    "[CDM REVIEW] Review with DAP CDM team — this logic may already exist "
                    "in the CDM layer or should be coordinated with CDM design."
                ),
            ))

        # Pattern 2: Aggregation
        if aggregations:
            agg_names = ", ".join(a.name for a in aggregations)
            gaps.append(GapItem(
                task_id=task.id,
                task_name=task.name,
                task_type="CDM_PATTERN/Aggregation",
                severity=Severity.INFO,
                message=(
                    f"Data flow contains aggregation transform(s): {agg_names}. "
                    "GROUP BY / SUM / COUNT patterns often belong in the CDM layer."
                ),
                recommendation=(
                    "[CDM REVIEW] Review with DAP CDM team — verify this aggregation "
                    "aligns with CDM layer design or is intentionally in a different zone."
                ),
            ))

        # Pattern 3: Cross-system enrichment (sources from different connection managers)
        source_connections = {c.connection_id for c in sources if c.connection_id}
        if len(source_connections) >= 2:
            gaps.append(GapItem(
                task_id=task.id,
                task_name=task.name,
                task_type="CDM_PATTERN/CrossSystemEnrichment",
                severity=Severity.INFO,
                message=(
                    f"Data flow reads from {len(source_connections)} different connection managers. "
                    "Cross-system data combination is typically CDM-layer logic."
                ),
                recommendation=(
                    "[CDM REVIEW] Review with DAP CDM team — combining data from multiple "
                    "source systems may already be handled by existing CDM entities."
                ),
            ))

        # Pattern 4: Denormalization via Lookups (3+ lookups suggests enrichment)
        if len(lookups) >= 3:
            lookup_names = ", ".join(lk.name for lk in lookups)
            gaps.append(GapItem(
                task_id=task.id,
                task_name=task.name,
                task_type="CDM_PATTERN/Denormalization",
                severity=Severity.INFO,
                message=(
                    f"Data flow has {len(lookups)} lookup transform(s): {lookup_names}. "
                    "Heavy lookup usage often indicates dimension denormalization."
                ),
                recommendation=(
                    "[CDM REVIEW] Review with DAP CDM team — denormalization logic "
                    "may be better placed in the CDM layer."
                ),
            ))

    return gaps
