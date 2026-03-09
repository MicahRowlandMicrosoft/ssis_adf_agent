"""
Data Flow Task → ADF Copy Activity + Mapping Data Flow Activity.

For simple single-source → single-destination flows with no intermediate
transformations, a Copy Activity is generated (cheaper, faster, no Spark overhead).
For flows with transformations, a Mapping Data Flow activity is generated.
The actual Mapping Data Flow JSON is produced by generators/dataflow_generator.py.
"""
from __future__ import annotations

from typing import Any

from ...parsers.models import DataFlowTask, PrecedenceConstraint, SSISTask
from ..base_converter import BaseConverter

# Component types treated as "pure source"
_SOURCE_TYPES = frozenset({
    "OleDbSource", "FlatFileSource", "ExcelSource", "OdbcSource",
    "ADONetSource", "SqlServerSource",
})

# Component types treated as "pure destination"
_DEST_TYPES = frozenset({
    "OleDbDestination", "FlatFileDestination", "ExcelDestination",
    "OdbcDestination", "ADONetDestination", "SqlServerDestination",
    "RecordsetDestination",
})

# Component types that are pure transformations
_TRANSFORM_TYPES = frozenset({
    "Lookup", "DerivedColumn", "ConditionalSplit", "Multicast", "UnionAll",
    "Aggregate", "Sort", "MergeJoin", "Merge", "DataConversion",
    "CharacterMap", "RowCount", "FuzzyLookup", "FuzzyGrouping",
    "TermExtraction", "TermLookup", "ScriptComponent", "Cache",
})


def _is_simple_copy(task: DataFlowTask) -> bool:
    """True if the data flow has exactly one source and one destination, no transforms."""
    sources = [c for c in task.components if c.component_type in _SOURCE_TYPES]
    dests = [c for c in task.components if c.component_type in _DEST_TYPES]
    transforms = [c for c in task.components if c.component_type in _TRANSFORM_TYPES]
    return len(sources) == 1 and len(dests) == 1 and len(transforms) == 0


class DataFlowConverter(BaseConverter):
    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        assert isinstance(task, DataFlowTask)
        depends_on = self._depends_on(task, constraints, task_by_id)
        safe_name = task.name.replace(" ", "_")

        if _is_simple_copy(task):
            return [self._copy_activity(task, safe_name, depends_on)]
        else:
            return [self._mapping_dataflow_activity(task, safe_name, depends_on)]

    def _copy_activity(
        self, task: DataFlowTask, safe_name: str, depends_on: list
    ) -> dict[str, Any]:
        src = next((c for c in task.components if c.component_type in _SOURCE_TYPES), None)
        dst = next((c for c in task.components if c.component_type in _DEST_TYPES), None)

        src_ds = f"DS_{src.name.replace(' ', '_')}" if src else f"DS_src_{safe_name}"
        dst_ds = f"DS_{dst.name.replace(' ', '_')}" if dst else f"DS_dst_{safe_name}"

        return {
            "name": task.name,
            "description": task.description or "",
            "type": "Copy",
            "dependsOn": depends_on,
            "policy": {"timeout": "0.12:00:00", "retry": 0, "retryIntervalInSeconds": 30},
            "typeProperties": {
                "source": {"type": "AzureSqlSource", "queryTimeout": "02:00:00"},
                "sink": {"type": "AzureSqlSink", "writeBehavior": "upsert"},
                "enableStaging": False,
                "translator": {"type": "TabularTranslator", "typeConversion": True},
            },
            "inputs": [{"referenceName": src_ds, "type": "DatasetReference"}],
            "outputs": [{"referenceName": dst_ds, "type": "DatasetReference"}],
        }

    def _mapping_dataflow_activity(
        self, task: DataFlowTask, safe_name: str, depends_on: list
    ) -> dict[str, Any]:
        return {
            "name": task.name,
            "description": task.description or "",
            "type": "ExecuteDataFlow",
            "dependsOn": depends_on,
            "policy": {"timeout": "1.00:00:00", "retry": 0, "retryIntervalInSeconds": 30},
            "typeProperties": {
                "dataflow": {
                    "referenceName": f"DF_{safe_name}",
                    "type": "DataFlowReference",
                },
                "compute": {
                    "coreCount": 8,
                    "computeType": "General",
                },
                "traceLevel": "None",
            },
        }
