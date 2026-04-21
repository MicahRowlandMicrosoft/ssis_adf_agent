"""
Data Flow Task → ADF Copy Activity + Mapping Data Flow Activity.

For simple single-source → single-destination flows with no intermediate
transformations, a Copy Activity is generated (cheaper, faster, no Spark overhead).
For flows with transformations, a Mapping Data Flow activity is generated.
The actual Mapping Data Flow JSON is produced by generators/dataflow_generator.py.

Copy Activity patterns follow Microsoft best practices:
  - Full Load: writeBehavior=insert, tableOption=autoCreate
  - Delta/Upsert: writeBehavior=upsert, upsertSettings with keys from parsed key_columns
  - MERGE: writeBehavior=upsert with detected key columns
  - Retry policy: 2 retries, 60s interval per Microsoft recommended pattern
"""
from __future__ import annotations

from typing import Any

from ...parsers.models import (
    DataFlowTask,
    IngestionPattern,
    PrecedenceConstraint,
    SSISTask,
)
from ...generators.naming import ds_name as _ds_name, df_name as _df_name
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
    """True if the data flow has exactly one source and one destination, no meaningful transforms.

    A DerivedColumn with no output columns is a no-op and doesn't count as
    a transform — the dataflow generator would skip it too.
    """
    sources = [c for c in task.components if c.component_type in _SOURCE_TYPES]
    dests = [c for c in task.components if c.component_type in _DEST_TYPES]
    transforms = [c for c in task.components if c.component_type in _TRANSFORM_TYPES]
    # Filter out no-op DerivedColumns (0 output columns)
    meaningful = [
        t for t in transforms
        if not (t.component_type == "DerivedColumn" and not t.output_columns)
    ]
    return len(sources) == 1 and len(dests) == 1 and len(meaningful) == 0


class DataFlowConverter(BaseConverter):
    def __init__(self, *, package_name: str = "", ls_name_map: dict[str, str] | None = None) -> None:
        self._package_name = package_name
        self._ls_name_map = ls_name_map

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

        src_ds = _ds_name(self._package_name, src.name) if src else f"DS_src_{safe_name}"
        dst_ds = _ds_name(self._package_name, dst.name) if dst else f"DS_dst_{safe_name}"

        # Determine sink pattern based on ingestion pattern
        ingestion = task.ingestion_pattern

        # Collect key columns from destination component
        key_cols: list[str] = []
        if dst and dst.key_columns:
            key_cols = dst.key_columns

        sink: dict[str, Any]
        if ingestion == IngestionPattern.MERGE or (
            ingestion == IngestionPattern.DELTA and key_cols
        ):
            # Upsert pattern with native temp table
            sink = {
                "type": "AzureSqlSink",
                "writeBehavior": "upsert",
                "upsertSettings": {
                    "useTempDB": True,
                    "keys": key_cols or ["TODO_KEY_COLUMN"],
                },
                "sqlWriterUseTableLock": False,
            }
        elif ingestion == IngestionPattern.DELTA:
            # Delta without detected keys — upsert with placeholder
            sink = {
                "type": "AzureSqlSink",
                "writeBehavior": "upsert",
                "upsertSettings": {
                    "useTempDB": True,
                    "keys": ["TODO_KEY_COLUMN"],
                },
                "sqlWriterUseTableLock": False,
            }
        else:
            # Full load — insert with auto-create
            sink = {
                "type": "AzureSqlSink",
                "writeBehavior": "insert",
                "tableOption": "autoCreate",
                "writeBatchSize": 100000,
                "sqlWriterUseTableLock": False,
            }

        return {
            "name": task.name,
            "description": task.description or "",
            "type": "Copy",
            "dependsOn": depends_on,
            "policy": {
                "timeout": "01:00:00",
                "retry": 2,
                "retryIntervalInSeconds": 60,
                "secureOutput": False,
                "secureInput": False,
            },
            "typeProperties": {
                "source": {
                    "type": "AzureSqlSource",
                    "queryTimeout": "02:00:00",
                    "isolationLevel": "ReadUncommitted",
                },
                "sink": sink,
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
            "policy": {
                "timeout": "1.00:00:00",
                "retry": 2,
                "retryIntervalInSeconds": 60,
                "secureOutput": False,
                "secureInput": False,
            },
            "typeProperties": {
                "dataflow": {
                    "referenceName": _df_name(self._package_name, task.name),
                    "type": "DataFlowReference",
                },
                "compute": {
                    "coreCount": 8,
                    "computeType": "General",
                },
                "traceLevel": "None",
            },
        }
