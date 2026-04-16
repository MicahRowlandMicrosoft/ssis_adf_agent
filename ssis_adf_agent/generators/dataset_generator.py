"""
Dataset generator — emits ADF dataset JSON files for source/destination components.

Uses Microsoft Recommended format: separate `schema` and `table` properties
instead of the deprecated `tableName` property.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..parsers.models import (
    ConnectionManagerType,
    DataFlowColumn,
    DataFlowComponent,
    DataFlowTask,
    DataType,
    SSISConnectionManager,
    SSISPackage,
    TaskType,
)

# ---------------------------------------------------------------------------
# SSIS DataType → ADF dataset schema type mapping
# ---------------------------------------------------------------------------

_SSIS_TO_ADF_TYPE: dict[DataType, str] = {
    DataType.INT8: "int8",
    DataType.INT16: "int16",
    DataType.INT32: "int32",
    DataType.INT64: "int64",
    DataType.UINT8: "byte",
    DataType.UINT16: "int32",
    DataType.UINT32: "int64",
    DataType.UINT64: "decimal",
    DataType.FLOAT: "float",
    DataType.DOUBLE: "double",
    DataType.CURRENCY: "decimal",
    DataType.DECIMAL: "decimal",
    DataType.BOOLEAN: "boolean",
    DataType.STRING: "string",
    DataType.WSTRING: "string",
    DataType.BYTES: "binary",
    DataType.DATE: "date",
    DataType.DBDATE: "date",
    DataType.DBTIME: "string",  # no native time type
    DataType.DBTIMESTAMP: "datetime",
    DataType.GUID: "string",
    DataType.EMPTY: "string",
}


def _columns_to_schema(columns: list[DataFlowColumn]) -> list[dict[str, Any]]:
    """Convert a list of SSIS DataFlowColumns to an ADF dataset schema array."""
    schema: list[dict[str, Any]] = []
    for col in columns:
        entry: dict[str, Any] = {
            "name": col.name,
            "type": _SSIS_TO_ADF_TYPE.get(col.data_type, "string"),
        }
        # Include precision/scale for decimal types
        if col.data_type in (DataType.DECIMAL, DataType.CURRENCY):
            if col.precision:
                entry["precision"] = col.precision
            if col.scale:
                entry["scale"] = col.scale
        # Include length for string types
        if col.data_type in (DataType.STRING, DataType.WSTRING) and col.length:
            entry["length"] = col.length
        schema.append(entry)
    return schema

_COMP_TO_DS_TYPE: dict[str, str] = {
    "OleDbSource": "AzureSqlTable",
    "OleDbDestination": "AzureSqlTable",
    "ADONetSource": "AzureSqlTable",
    "ADONetDestination": "AzureSqlTable",
    "FlatFileSource": "DelimitedText",
    "FlatFileDestination": "DelimitedText",
    "ExcelSource": "Excel",
    "ExcelDestination": "Excel",
    "OdbcSource": "OdbcTable",
    "OdbcDestination": "OdbcTable",
    "SqlServerSource": "SqlServerTable",
    "SqlServerDestination": "SqlServerTable",
}


def _parse_table_name(raw_name: str | None) -> tuple[str | None, str | None]:
    """Split a possibly-qualified table name into (schema, table).

    Handles: ``[schema].[table]``, ``schema.table``, ``table`` (defaults to dbo).
    """
    if not raw_name:
        return None, None
    # Remove surrounding brackets and whitespace
    name = raw_name.strip().strip("[]")
    if "." in name:
        parts = [p.strip().strip("[]") for p in name.split(".", 1)]
        return parts[0], parts[1]
    return "dbo", name


def _build_dataset(
    name: str,
    ds_type: str,
    linked_service_name: str,
    table_name: str | None = None,
    file_path: str | None = None,
    description: str = "",
    schema_remap: dict[str, str] | None = None,
    columns: list[DataFlowColumn] | None = None,
) -> dict[str, Any]:
    props: dict[str, Any] = {
        "linkedServiceName": {
            "referenceName": linked_service_name,
            "type": "LinkedServiceReference",
        },
        "description": description,
        "annotations": ["ssis-adf-agent"],
        "type": ds_type,
        "typeProperties": {},
        "schema": _columns_to_schema(columns) if columns else [],
    }

    if ds_type in ("AzureSqlTable", "SqlServerTable", "OdbcTable"):
        schema_part, table_part = _parse_table_name(table_name)

        # Apply schema remapping if configured
        if schema_remap and schema_part:
            remap_key = schema_part.lower()
            if remap_key in schema_remap:
                schema_part = schema_remap[remap_key]

        if table_part:
            props["typeProperties"]["schema"] = schema_part or "dbo"
            props["typeProperties"]["table"] = table_part

    elif ds_type == "DelimitedText":
        props["typeProperties"] = {
            "location": {
                "type": "AzureBlobStorageLocation",
                "fileName": file_path or "TODO_filename.csv",
                "folderPath": "TODO_folder",
                "container": "TODO_container",
            },
            "columnDelimiter": ",",
            "rowDelimiter": "\n",
            "firstRowAsHeader": True,
            "quoteChar": "\"",
        }

    elif ds_type == "Excel":
        props["typeProperties"] = {
            "location": {
                "type": "AzureBlobStorageLocation",
                "fileName": file_path or "TODO_file.xlsx",
                "container": "TODO_container",
            },
            "sheetIndex": 0,
            "firstRowAsHeader": True,
        }

    return {"name": name, "properties": props}


def generate_datasets(
    package: SSISPackage,
    output_dir: Path,
    *,
    schema_remap: dict[str, str] | None = None,
    shared_artifacts_dir: Path | None = None,
) -> list[dict[str, Any]]:
    """
    Generate ADF dataset JSON files for every Data Flow source and destination.

    When *shared_artifacts_dir* is set, checks for existing dataset JSON files
    there before creating new ones (cross-package deduplication).

    Files are written to *output_dir*/dataset/.
    Returns the list of dataset dicts.
    """
    ds_dir = output_dir / "dataset"
    ds_dir.mkdir(parents=True, exist_ok=True)

    # Build index of existing shared datasets for dedup
    existing_ds: set[str] = set()
    if shared_artifacts_dir:
        shared_ds_dir = shared_artifacts_dir / "dataset"
        if shared_ds_dir.exists():
            for f in shared_ds_dir.glob("*.json"):
                existing_ds.add(f.stem)

    conn_by_id: dict[str, SSISConnectionManager] = {cm.id: cm for cm in package.connection_managers}
    results: list[dict[str, Any]] = []
    seen: set[str] = set()

    for task in package.tasks:
        if task.task_type != TaskType.DATA_FLOW:
            continue
        assert isinstance(task, DataFlowTask)

        for comp in task.components:
            ds_type = _COMP_TO_DS_TYPE.get(comp.component_type)
            if ds_type is None:
                continue  # transformation — no dataset needed

            ds_name = f"DS_{comp.name.replace(' ', '_')}"
            if ds_name in seen or ds_name in existing_ds:
                continue
            seen.add(ds_name)

            conn = conn_by_id.get(comp.connection_id or "")
            ls_name = f"LS_{comp.connection_id or 'unknown'}"
            table = (
                comp.properties.get("OpenRowset")
                or comp.properties.get("TableOrViewName")
            )
            file_path = conn.file_path if conn else None

            # Use output_columns for sources, input_columns for destinations
            columns = comp.output_columns or comp.input_columns or []

            ds = _build_dataset(
                name=ds_name,
                ds_type=ds_type,
                linked_service_name=ls_name,
                table_name=table,
                file_path=file_path,
                description=f"Dataset for SSIS component: {comp.name}",
                schema_remap=schema_remap,
                columns=columns if columns else None,
            )
            (ds_dir / f"{ds_name}.json").write_text(
                json.dumps(ds, indent=4, ensure_ascii=False),
                encoding="utf-8",
            )
            results.append(ds)

    return results
