"""
Data Flow destination converter — maps SSIS Data Flow destination component types
to ADF Mapping Data Flow ``sink`` transformation JSON.
"""
from __future__ import annotations

from typing import Any

from ...parsers.models import DataFlowComponent
from ...warnings_collector import warn
from ...generators.naming import ds_name as _ds_name, resolve_ls_name
from ._naming import safe_node_name


_SINK_DATASET_TYPE: dict[str, str] = {
    "OleDbDestination": "AzureSqlTable",
    "ADONetDestination": "AzureSqlTable",
    "FlatFileDestination": "DelimitedText",
    "ExcelDestination": "Excel",
    "OdbcDestination": "OdbcTable",
    "SqlServerDestination": "SqlServerTable",
    "RecordsetDestination": "AzureSqlTable",  # approximation
}

_WRITE_BEHAVIOR: dict[str, str] = {
    "OleDbDestination": "upsert",
    "ADONetDestination": "upsert",
    "FlatFileDestination": "overwrite",
    "ExcelDestination": "overwrite",
    "SqlServerDestination": "insert",
}


def convert_destination(
    component: DataFlowComponent,
    *,
    package_name: str = "",
    ls_name_map: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Return an ADF Mapping Data Flow ``sink`` transformation dict.

    This dict is embedded in the ``sinks`` array of the data flow JSON.
    """
    comp_type = component.component_type
    ds_type = _SINK_DATASET_TYPE.get(comp_type, "AzureSqlTable")
    write_behavior = _WRITE_BEHAVIOR.get(comp_type, "upsert")
    # ADF Mapping Data Flow node names must be alphanumeric only.
    safe_name = safe_node_name(component.name, fallback="Sink")
    # Dataset resource names allow underscores; keep the underscore form for refs.
    ds_ref = _ds_name(package_name, component.name) if package_name else f"DS_{component.name.replace(' ', '_')}"

    # Guard: only allow upsert if the component has key columns defined
    has_keys = bool(component.key_columns)
    allow_upsert = write_behavior == "upsert" and has_keys

    conn_ref = component.connection_id
    if not conn_ref:
        warn(
            phase="convert", severity="warning", source="destination_converter",
            message=f"Destination component '{component.name}' has no connection ID",
            detail="Using fallback 'LS_unknown' — update the linked service reference manually",
        )
        conn_ref = "unknown"

    sink: dict[str, Any] = {
        "name": safe_name,
        "description": f"Sink from SSIS {comp_type}: {component.name}",
        "dataset": {
            "referenceName": ds_ref,
            "type": "DatasetReference",
        },
        "linkedService": {
            "referenceName": resolve_ls_name(conn_ref, ls_name_map),
            "type": "LinkedServiceReference",
        },
        "typeProperties": {
            "format": {"type": ds_type},
            "allowUpsert": allow_upsert,
        },
    }

    # Destination table name
    table = (
        component.properties.get("OpenRowset")
        or component.properties.get("TableOrViewName")
    )
    if table:
        sink["typeProperties"]["tableName"] = table

    # Carry column mapping metadata for DSL script generation
    sink["_input_columns"] = component.input_columns
    sink["_key_columns"] = component.key_columns

    return sink
