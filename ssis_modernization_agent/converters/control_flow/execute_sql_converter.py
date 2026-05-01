"""
Execute SQL Task → ADF Stored Procedure Activity or Lookup Activity.

Mapping rules
-------------
- result_set_type == "None"       → StoredProcedure Activity (fire-and-forget) if sql looks
                                    like a stored procedure call; otherwise Script Activity.
- result_set_type == "SingleRow"  → Lookup Activity (first row only)
- result_set_type == "FullResultSet" → Lookup Activity
- result_set_type == "Xml"        → WebActivity (not supported natively; flagged as warning)

Supports optional schema remapping for database consolidation scenarios.

Parameter bindings
------------------
The SSIS parser extracts ``direction`` (Input / Output / ReturnValue),
``data_type`` (OLE DB type code), and ``parameter_name`` from each binding.
This converter maps them to ADF stored-procedure parameter declarations with
correct ``type`` and ``value`` fields.  Output parameters and return values are
translated but annotated with ``/* OUTPUT */``—ADF stored procedure activities
don't natively support OUT params, so the user may need to restructure.
"""
from __future__ import annotations

import re
from typing import Any

from ...parsers.models import ExecuteSQLTask, PrecedenceConstraint, SSISTask
from ...generators.naming import resolve_ls_name
from ..base_converter import BaseConverter

_PROC_PATTERN = re.compile(
    r"^\s*(exec(?:ute)?\s+|call\s+)(?:\[?[\w\s]+\]?\.)?\[?[\w\s]+\]?",
    re.IGNORECASE,
)

# SSIS OLE DB type codes → ADF parameter type names
# Reference: https://learn.microsoft.com/en-us/dotnet/api/system.data.oledb.oledbtype
_SSIS_OLEDB_TYPE_MAP: dict[str, str] = {
    # Integer types
    "2": "Int16",      # SmallInt
    "3": "Int32",      # Integer
    "16": "Byte",      # TinyInt / SByte
    "17": "Byte",      # UnsignedTinyInt
    "18": "Int16",     # UnsignedSmallInt
    "19": "Int32",     # UnsignedInt
    "20": "Int64",     # BigInt
    "21": "Int64",     # UnsignedBigInt
    # Floating point
    "4": "Single",     # Single
    "5": "Double",     # Double
    "6": "Decimal",    # Currency
    "14": "Decimal",   # Decimal
    "131": "Decimal",  # Numeric
    # Boolean
    "11": "Boolean",   # Boolean
    # Date/time
    "7": "DateTime",   # Date
    "133": "DateTime", # DBDate
    "134": "DateTime", # DBTime
    "135": "DateTime", # DBTimeStamp
    "64": "DateTime",  # FileTime
    # String
    "8": "String",     # BSTR
    "129": "String",   # Char
    "130": "String",   # WChar
    "200": "String",   # VarChar
    "201": "String",   # LongVarChar
    "202": "String",   # VarWChar
    "203": "String",   # LongVarWChar
    # Binary
    "128": "Byte",     # Binary
    # GUID
    "72": "Guid",      # Guid
    # Default
    "0": "String",     # Empty
}


def _oledb_type_to_adf(type_code: str) -> str:
    """Map SSIS OLE DB type code string to ADF parameter type name."""
    return _SSIS_OLEDB_TYPE_MAP.get(type_code, "String")


def _is_stored_proc_call(sql: str | None) -> bool:
    if not sql:
        return False
    return bool(_PROC_PATTERN.match(sql.strip()))


def _extract_proc_name(sql: str) -> str:
    """Best-effort extraction of a stored procedure name from a SQL string."""
    m = re.search(
        r"(?:exec(?:ute)?\s+|call\s+)(?:\[?[\w\s]+\]?\.)?\[?([\w\s]+)\]?",
        sql, re.IGNORECASE,
    )
    return m.group(1).strip() if m else "sp_unknown"


def apply_schema_remap(sql: str | None, schema_remap: dict[str, str] | None) -> str | None:
    """Apply schema remapping to SQL text.

    Replaces three-part names like ``OldDB.dbo.Table`` with ``NewDB.newschema.Table``
    based on the remap dict (key = "OldDB.dbo" → value = "NewDB.newschema").
    """
    if not sql or not schema_remap:
        return sql
    result = sql
    for old_prefix, new_prefix in schema_remap.items():
        # Match [OldDB].[dbo] or OldDB.dbo (with optional brackets)
        parts = old_prefix.split(".", 1)
        if len(parts) == 2:
            db, schema = parts
            pattern = re.compile(
                rf"\[?{re.escape(db)}\]?\.\[?{re.escape(schema)}\]?\.",
                re.IGNORECASE,
            )
            new_parts = new_prefix.split(".", 1)
            if len(new_parts) == 2:
                replacement = f"[{new_parts[0]}].[{new_parts[1]}]."
            else:
                replacement = f"[{new_prefix}]."
            result = pattern.sub(replacement, result)
    return result


class ExecuteSQLConverter(BaseConverter):
    def __init__(self, *, ls_name_map: dict[str, str] | None = None) -> None:
        self._ls_name_map = ls_name_map

    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        assert isinstance(task, ExecuteSQLTask)
        depends_on = self._depends_on(task, constraints, task_by_id)

        result_type = task.result_set_type or "None"

        # Build linked service reference from connection_id placeholder
        linked_service_ref = {
            "referenceName": resolve_ls_name(task.connection_id or "unknown", self._ls_name_map),
            "type": "LinkedServiceReference",
        }

        if result_type in ("SingleRow", "FullResultSet"):
            return [self._lookup_activity(task, linked_service_ref, depends_on)]
        else:
            if _is_stored_proc_call(task.sql_statement):
                return [self._stored_proc_activity(task, linked_service_ref, depends_on)]
            else:
                return [self._script_activity(task, linked_service_ref, depends_on)]

    def _lookup_activity(
        self,
        task: ExecuteSQLTask,
        ls_ref: dict,
        depends_on: list,
    ) -> dict[str, Any]:
        first_row_only = task.result_set_type == "SingleRow"
        return {
            "name": task.name,
            "description": task.description or "",
            "type": "Lookup",
            "dependsOn": depends_on,
            "policy": {
                "timeout": f"0.{task.timeout // 3600:02d}:{(task.timeout % 3600) // 60:02d}:{task.timeout % 60:02d}"
                if task.timeout > 0 else "0.12:00:00",
                "retry": 0,
                "retryIntervalInSeconds": 30,
            },
            "typeProperties": {
                "source": {
                    "type": "AzureSqlSource",
                    "sqlReaderQuery": task.sql_statement or "",
                    "queryTimeout": "02:00:00",
                },
                "dataset": {
                    "referenceName": f"DS_{task.name.replace(' ', '_')}",
                    "type": "DatasetReference",
                },
                "firstRowOnly": first_row_only,
            },
        }

    def _stored_proc_activity(
        self,
        task: ExecuteSQLTask,
        ls_ref: dict,
        depends_on: list,
    ) -> dict[str, Any]:
        proc_name = _extract_proc_name(task.sql_statement or "")
        parameters = _build_sp_parameters(task.parameter_bindings)
        return {
            "name": task.name,
            "description": task.description or "",
            "type": "SqlServerStoredProcedure",
            "dependsOn": depends_on,
            "linkedServiceName": ls_ref,
            "typeProperties": {
                "storedProcedureName": proc_name,
                "storedProcedureParameters": parameters,
            },
        }

    def _script_activity(
        self,
        task: ExecuteSQLTask,
        ls_ref: dict,
        depends_on: list,
    ) -> dict[str, Any]:
        # Build script parameters from input bindings
        script_params = _build_script_parameters(task.parameter_bindings)
        scripts_block: dict[str, Any] = {
            "type": "Query",
            "text": task.sql_statement or "",
        }
        if script_params:
            scripts_block["parameters"] = script_params
        return {
            "name": task.name,
            "description": task.description or "",
            "type": "Script",
            "dependsOn": depends_on,
            "linkedServiceName": ls_ref,
            "typeProperties": {
                "scripts": [scripts_block],
                "logSettings": {
                    "logDestination": "ActivityOutput",
                },
            },
        }


# ---------------------------------------------------------------------------
# Parameter binding helpers
# ---------------------------------------------------------------------------

def _build_sp_parameters(bindings: list[dict[str, Any]]) -> dict[str, Any]:
    """Build ADF storedProcedureParameters from SSIS parameter bindings.

    Handles Input, Output, and ReturnValue directions.
    Maps SSIS OLE DB type codes to ADF type names.
    """
    parameters: dict[str, Any] = {}
    for idx, pb in enumerate(bindings):
        var_raw = pb.get("variable", "")
        var_name = var_raw.split("::")[-1] if "::" in var_raw else var_raw
        direction = (pb.get("direction") or "Input").strip()
        type_code = pb.get("data_type", "0")
        adf_type = _oledb_type_to_adf(type_code)
        param_name = pb.get("parameter_name") or f"param{idx}"

        value_expr = f"@variables('{var_name}')" if var_name else "null"

        entry: dict[str, Any] = {
            "value": value_expr,
            "type": adf_type,
        }

        if direction.lower() in ("output", "returnvalue"):
            entry["direction"] = "Output"
            entry["value"] = (
                f"/* OUTPUT — ADF SP activities don't natively support OUT params; "
                f"capture via Lookup + result binding instead */ {value_expr}"
            )

        parameters[param_name] = entry

    return parameters


def _build_script_parameters(bindings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build ADF Script activity parameters from SSIS bindings (input only)."""
    params: list[dict[str, Any]] = []
    for idx, pb in enumerate(bindings):
        direction = (pb.get("direction") or "Input").strip()
        if direction.lower() not in ("input",):
            continue  # Script activity only supports input parameters
        var_raw = pb.get("variable", "")
        var_name = var_raw.split("::")[-1] if "::" in var_raw else var_raw
        type_code = pb.get("data_type", "0")
        adf_type = _oledb_type_to_adf(type_code)
        param_name = pb.get("parameter_name") or f"param{idx}"

        params.append({
            "name": param_name,
            "value": f"@variables('{var_name}')" if var_name else "null",
            "type": adf_type,
            "direction": "Input",
        })
    return params
