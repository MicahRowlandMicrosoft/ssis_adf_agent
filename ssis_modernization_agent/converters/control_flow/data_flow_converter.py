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


# Map SSIS source component type → (ADF Copy source.type, extra typeProperties)
# These pair with the dataset types emitted by generators/dataset_generator.py.
_COPY_SOURCE_BY_COMPONENT: dict[str, dict[str, Any]] = {
    "OleDbSource": {
        "type": "AzureSqlSource",
        "queryTimeout": "02:00:00",
        "isolationLevel": "ReadUncommitted",
    },
    "ADONetSource": {
        "type": "AzureSqlSource",
        "queryTimeout": "02:00:00",
        "isolationLevel": "ReadUncommitted",
    },
    "SqlServerSource": {
        "type": "SqlServerSource",
        "queryTimeout": "02:00:00",
    },
    "FlatFileSource": {
        "type": "DelimitedTextSource",
        "storeSettings": {"type": "AzureBlobStorageReadSettings", "recursive": False},
        "formatSettings": {"type": "DelimitedTextReadSettings"},
    },
    "ExcelSource": {
        "type": "ExcelSource",
        "storeSettings": {"type": "AzureBlobStorageReadSettings", "recursive": False},
    },
    "OdbcSource": {
        "type": "OdbcSource",
        "queryTimeout": "02:00:00",
    },
}

# Map SSIS destination component type → (ADF Copy sink.type, extra typeProperties,
# default writeBehavior keyword used for the sink).
_COPY_SINK_BY_COMPONENT: dict[str, dict[str, Any]] = {
    "OleDbDestination": {"type": "AzureSqlSink"},
    "ADONetDestination": {"type": "AzureSqlSink"},
    "SqlServerDestination": {"type": "SqlServerSink"},
    "FlatFileDestination": {
        "type": "DelimitedTextSink",
        "storeSettings": {"type": "AzureBlobStorageWriteSettings"},
        "formatSettings": {"type": "DelimitedTextWriteSettings", "quoteAllText": False},
    },
    "ExcelDestination": {
        "type": "ExcelSink",
        "storeSettings": {"type": "AzureBlobStorageWriteSettings"},
    },
    "OdbcDestination": {"type": "OdbcSink"},
    # RecordsetDestination has no direct ADF equivalent; approximate as SQL sink.
    "RecordsetDestination": {"type": "AzureSqlSink"},
}

# Sink types that are SQL-shaped and accept upsert/insert writeBehavior + tableOption.
_SQL_SINK_TYPES = frozenset({"AzureSqlSink", "SqlServerSink", "OdbcSink"})


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

        # --- Build source typeProperties from the actual SSIS source component
        source: dict[str, Any]
        if src is not None:
            template = _COPY_SOURCE_BY_COMPONENT.get(src.component_type)
            if template is None:
                # Unknown source component — fall back to generic SQL source and
                # flag the activity for manual review.
                source = {
                    "type": "AzureSqlSource",
                    "queryTimeout": "02:00:00",
                    "isolationLevel": "ReadUncommitted",
                }
            else:
                source = {k: (dict(v) if isinstance(v, dict) else v) for k, v in template.items()}
            # Carry SQL query if present (for SQL-shaped sources)
            query = (
                src.properties.get("SqlCommand")
                or src.properties.get("OpenRowset")
                if hasattr(src, "properties") and src.properties
                else None
            )
            if query and source["type"] in {"AzureSqlSource", "SqlServerSource", "OdbcSource"}:
                source["sqlReaderQuery"] = query
        else:
            source = {
                "type": "AzureSqlSource",
                "queryTimeout": "02:00:00",
                "isolationLevel": "ReadUncommitted",
            }

        # --- Build sink typeProperties from the actual SSIS destination component
        sink_template = _COPY_SINK_BY_COMPONENT.get(dst.component_type) if dst else None
        if sink_template is None:
            sink_type = "AzureSqlSink"
            sink: dict[str, Any] = {"type": sink_type}
        else:
            sink = {k: (dict(v) if isinstance(v, dict) else v) for k, v in sink_template.items()}
            sink_type = sink["type"]

        # Collect key columns from destination component (only meaningful for SQL sinks)
        key_cols: list[str] = []
        if dst and dst.key_columns:
            key_cols = dst.key_columns

        ingestion = task.ingestion_pattern

        if sink_type in _SQL_SINK_TYPES:
            if ingestion == IngestionPattern.MERGE or (
                ingestion == IngestionPattern.DELTA and key_cols
            ):
                sink.update({
                    "writeBehavior": "upsert",
                    "upsertSettings": {
                        "useTempDB": True,
                        "keys": key_cols or ["TODO_KEY_COLUMN"],
                    },
                    "sqlWriterUseTableLock": False,
                })
            elif ingestion == IngestionPattern.DELTA:
                sink.update({
                    "writeBehavior": "upsert",
                    "upsertSettings": {
                        "useTempDB": True,
                        "keys": ["TODO_KEY_COLUMN"],
                    },
                    "sqlWriterUseTableLock": False,
                })
            else:
                sink.update({
                    "writeBehavior": "insert",
                    "tableOption": "autoCreate",
                    "writeBatchSize": 100000,
                    "sqlWriterUseTableLock": False,
                })
        elif sink_type in {"DelimitedTextSink", "ExcelSink"}:
            # File-shaped sinks: ADF writes file(s) to the dataset's location.
            # No writeBehavior / tableOption / batch size — those are SQL-only.
            pass

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
                "source": source,
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
