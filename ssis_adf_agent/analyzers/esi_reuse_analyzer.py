"""
ESI (Enriched Source Image) reuse analyzer.

Detects SSIS data flow sources that target tables already available in the
ESI layer of an existing DAP platform.  When a match is found:
  - The gap analysis reports an INFO item ("esi-reuse-candidate")
  - The pipeline annotation "esi-reuse-candidate" is added
  - Optionally an alternative source pointing to ESI (Azure SQL) is noted

Configuration is supplied via a JSON file mapping source_system → table list::

    {
        "PHINEOS": ["TocPartyAddress", "TLBenefit", "TLPolicy"],
        "SAP": ["BSEG", "KNA1"]
    }
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..parsers.models import (
    DataFlowTask,
    GapItem,
    SSISPackage,
    TaskType,
)


def load_esi_config(config_path: str | Path) -> dict[str, set[str]]:
    """Load ESI tables config from a JSON file.

    Returns a dict mapping source_system (upper-cased) → set of table names (upper-cased).
    """
    path = Path(config_path)
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        k.upper(): {t.upper() for t in v}
        for k, v in data.items()
    }


def _extract_table_name(comp_properties: dict[str, Any]) -> str | None:
    """Get table name from a data flow component's properties."""
    raw = (
        comp_properties.get("OpenRowset")
        or comp_properties.get("TableOrViewName")
        or comp_properties.get("CommandText")
    )
    if not raw:
        return None
    # Strip schema prefix: [dbo].[TableName] → TableName
    parts = raw.strip().strip("[]").split(".")
    return parts[-1].strip("[]")


def analyze_esi_reuse(
    package: SSISPackage,
    esi_config: dict[str, set[str]],
) -> list[GapItem]:
    """Identify data flow sources that match ESI-available tables.

    Returns GapItems with severity="info" for each ESI-reuse candidate.
    """
    if not esi_config:
        return []

    # Build a flat lookup: table_name (upper) → source_system
    table_to_system: dict[str, str] = {}
    for system, tables in esi_config.items():
        for tbl in tables:
            table_to_system[tbl] = system

    gaps: list[GapItem] = []
    source_types = frozenset({
        "OleDbSource", "ADONetSource", "OdbcSource", "SqlServerSource",
    })

    for task in package.tasks:
        if task.task_type != TaskType.DATA_FLOW:
            continue
        assert isinstance(task, DataFlowTask)
        for comp in task.components:
            if comp.component_type not in source_types:
                continue
            table = _extract_table_name(comp.properties)
            if not table:
                continue
            system = table_to_system.get(table.upper())
            if system:
                gaps.append(GapItem(
                    task_id=comp.id,
                    task_name=f"{task.name} / {comp.name}",
                    task_type=f"DataFlow/{comp.component_type}",
                    severity="info",
                    message=(
                        f"Table '{table}' from source system '{system}' is already available in the "
                        f"ESI layer. Consider reading from ESI (Azure SQL) instead of staging from on-prem."
                    ),
                    recommendation=(
                        f"Switch the source linked service to the ESI Azure SQL linked service for table '{table}'. "
                        "This avoids redundant data movement via Self-Hosted IR."
                    ),
                ))

    return gaps
