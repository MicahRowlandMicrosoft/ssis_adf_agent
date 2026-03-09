"""
Dataset generator — emits ADF dataset JSON files for source/destination components.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..parsers.models import (
    ConnectionManagerType,
    DataFlowComponent,
    DataFlowTask,
    SSISConnectionManager,
    SSISPackage,
    TaskType,
)

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


def _build_dataset(
    name: str,
    ds_type: str,
    linked_service_name: str,
    table_name: str | None = None,
    file_path: str | None = None,
    description: str = "",
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
        "schema": [],
    }

    if ds_type in ("AzureSqlTable", "SqlServerTable", "OdbcTable"):
        if table_name:
            props["typeProperties"]["tableName"] = table_name

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
) -> list[dict[str, Any]]:
    """
    Generate ADF dataset JSON files for every Data Flow source and destination.
    Files are written to *output_dir*/dataset/.

    Returns the list of dataset dicts.
    """
    ds_dir = output_dir / "dataset"
    ds_dir.mkdir(parents=True, exist_ok=True)

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
            if ds_name in seen:
                continue
            seen.add(ds_name)

            conn = conn_by_id.get(comp.connection_id or "")
            ls_name = f"LS_{comp.connection_id or 'unknown'}"
            table = (
                comp.properties.get("OpenRowset")
                or comp.properties.get("TableOrViewName")
            )
            file_path = conn.file_path if conn else None

            ds = _build_dataset(
                name=ds_name,
                ds_type=ds_type,
                linked_service_name=ls_name,
                table_name=table,
                file_path=file_path,
                description=f"Dataset for SSIS component: {comp.name}",
            )
            (ds_dir / f"{ds_name}.json").write_text(
                json.dumps(ds, indent=4, ensure_ascii=False),
                encoding="utf-8",
            )
            results.append(ds)

    return results
