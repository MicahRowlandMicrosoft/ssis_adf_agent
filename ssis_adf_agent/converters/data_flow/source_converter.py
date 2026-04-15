"""
Data Flow source converter — maps SSIS Data Flow source component types to
ADF Mapping Data Flow ``source`` transformation JSON.
"""
from __future__ import annotations

from typing import Any

from ...parsers.models import DataFlowComponent
from ...warnings_collector import warn

# Map SSIS source component type → ADF dataset type
_SOURCE_DATASET_TYPE: dict[str, str] = {
    "OleDbSource": "AzureSqlTable",
    "ADONetSource": "AzureSqlTable",
    "FlatFileSource": "DelimitedText",
    "ExcelSource": "Excel",
    "OdbcSource": "OdbcTable",
    "SqlServerSource": "SqlServerTable",
}

_SOURCE_STORE_SETTINGS: dict[str, dict] = {
    "DelimitedText": {"type": "AzureBlobStorageReadSettings", "recursive": False},
    "Excel": {"type": "AzureBlobStorageReadSettings", "recursive": False},
}


def convert_source(component: DataFlowComponent) -> dict[str, Any]:
    """
    Return an ADF Mapping Data Flow ``source`` transformation dict.

    This dict is embedded in the ``sources`` array of the data flow JSON.
    """
    comp_type = component.component_type
    ds_type = _SOURCE_DATASET_TYPE.get(comp_type, "AzureSqlTable")
    safe_name = component.name.replace(" ", "_")

    conn_ref = component.connection_id
    if not conn_ref:
        warn(
            phase="convert", severity="warning", source="source_converter",
            message=f"Source component '{component.name}' has no connection ID",
            detail="Using fallback 'LS_unknown' — update the linked service reference manually",
        )
        conn_ref = "unknown"

    source: dict[str, Any] = {
        "name": safe_name,
        "description": f"Source from SSIS {comp_type}: {component.name}",
        "dataset": {
            "referenceName": f"DS_{safe_name}",
            "type": "DatasetReference",
        },
        "linkedService": {
            "referenceName": f"LS_{conn_ref}",
            "type": "LinkedServiceReference",
        },
        "typeProperties": {
            "format": {"type": ds_type},
        },
    }

    # Carry over any SQL query
    query = component.properties.get("SqlCommand") or component.properties.get("OpenRowset")
    if query:
        source["typeProperties"]["query"] = query

    return source
