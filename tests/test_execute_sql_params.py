"""Tests for Execute SQL converter — parameter bindings with direction and type mapping."""
from __future__ import annotations

import pytest

from ssis_adf_agent.converters.control_flow.execute_sql_converter import (
    ExecuteSQLConverter,
    _build_sp_parameters,
    _build_script_parameters,
    _oledb_type_to_adf,
    apply_schema_remap,
)
from ssis_adf_agent.parsers.models import ExecuteSQLTask, TaskType


def _make_task(**overrides) -> ExecuteSQLTask:
    defaults = dict(
        id="sql-1",
        name="Run SQL",
        task_type=TaskType.EXECUTE_SQL,
        connection_id="conn-1",
        sql_statement="SELECT 1",
        result_set_type="None",
        parameter_bindings=[],
    )
    defaults.update(overrides)
    return ExecuteSQLTask(**defaults)


# ---------------------------------------------------------------------------
# _oledb_type_to_adf
# ---------------------------------------------------------------------------

class TestOleDbTypeMapping:
    def test_int32(self):
        assert _oledb_type_to_adf("3") == "Int32"

    def test_string_varchar(self):
        assert _oledb_type_to_adf("200") == "String"

    def test_datetime(self):
        assert _oledb_type_to_adf("135") == "DateTime"

    def test_boolean(self):
        assert _oledb_type_to_adf("11") == "Boolean"

    def test_decimal(self):
        assert _oledb_type_to_adf("131") == "Decimal"

    def test_bigint(self):
        assert _oledb_type_to_adf("20") == "Int64"

    def test_guid(self):
        assert _oledb_type_to_adf("72") == "Guid"

    def test_unknown_defaults_to_string(self):
        assert _oledb_type_to_adf("999") == "String"


# ---------------------------------------------------------------------------
# _build_sp_parameters
# ---------------------------------------------------------------------------

class TestBuildSpParameters:
    def test_input_parameter(self):
        bindings = [{
            "variable": "User::CustomerId",
            "direction": "Input",
            "data_type": "3",  # Int32
            "parameter_name": "@CustomerId",
        }]
        params = _build_sp_parameters(bindings)
        assert "@CustomerId" in params
        p = params["@CustomerId"]
        assert p["type"] == "Int32"
        assert p["value"] == "@variables('CustomerId')"
        assert "direction" not in p  # Input is the default, no direction field

    def test_output_parameter(self):
        bindings = [{
            "variable": "User::RowCount",
            "direction": "Output",
            "data_type": "3",
            "parameter_name": "@RowCount",
        }]
        params = _build_sp_parameters(bindings)
        p = params["@RowCount"]
        assert p["direction"] == "Output"
        assert "OUTPUT" in p["value"]
        assert "RowCount" in p["value"]

    def test_return_value(self):
        bindings = [{
            "variable": "User::RetVal",
            "direction": "ReturnValue",
            "data_type": "3",
            "parameter_name": "@RETURN_VALUE",
        }]
        params = _build_sp_parameters(bindings)
        p = params["@RETURN_VALUE"]
        assert p["direction"] == "Output"

    def test_multiple_parameters(self):
        bindings = [
            {"variable": "User::Id", "direction": "Input", "data_type": "3", "parameter_name": "@Id"},
            {"variable": "User::Name", "direction": "Input", "data_type": "200", "parameter_name": "@Name"},
            {"variable": "User::Result", "direction": "Output", "data_type": "11", "parameter_name": "@Result"},
        ]
        params = _build_sp_parameters(bindings)
        assert len(params) == 3
        assert params["@Id"]["type"] == "Int32"
        assert params["@Name"]["type"] == "String"
        assert params["@Result"]["type"] == "Boolean"
        assert params["@Result"]["direction"] == "Output"

    def test_namespace_stripping(self):
        bindings = [{
            "variable": "User::Counter",
            "direction": "Input",
            "data_type": "3",
            "parameter_name": "@p0",
        }]
        params = _build_sp_parameters(bindings)
        assert "Counter" in params["@p0"]["value"]
        assert "User::" not in params["@p0"]["value"]

    def test_defaults_for_missing_fields(self):
        bindings = [{"variable": "", "parameter_name": ""}]
        params = _build_sp_parameters(bindings)
        assert "param0" in params
        p = params["param0"]
        assert p["type"] == "String"
        assert p["value"] == "null"


# ---------------------------------------------------------------------------
# _build_script_parameters
# ---------------------------------------------------------------------------

class TestBuildScriptParameters:
    def test_input_only(self):
        bindings = [
            {"variable": "User::X", "direction": "Input", "data_type": "3", "parameter_name": "@X"},
            {"variable": "User::Y", "direction": "Output", "data_type": "3", "parameter_name": "@Y"},
        ]
        params = _build_script_parameters(bindings)
        assert len(params) == 1  # Output filtered out
        assert params[0]["name"] == "@X"
        assert params[0]["type"] == "Int32"
        assert params[0]["direction"] == "Input"

    def test_empty_bindings(self):
        assert _build_script_parameters([]) == []


# ---------------------------------------------------------------------------
# ExecuteSQLConverter integration
# ---------------------------------------------------------------------------

class TestConverterWithParameters:
    def test_stored_proc_with_typed_params(self):
        task = _make_task(
            sql_statement="EXEC dbo.sp_UpdateStatus @Id, @Status",
            parameter_bindings=[
                {"variable": "User::RecordId", "direction": "Input",
                 "data_type": "20", "parameter_name": "@Id"},
                {"variable": "User::NewStatus", "direction": "Input",
                 "data_type": "200", "parameter_name": "@Status"},
            ],
        )
        converter = ExecuteSQLConverter()
        activities = converter.convert(task, [], {})

        assert len(activities) == 1
        sp = activities[0]
        assert sp["type"] == "SqlServerStoredProcedure"
        params = sp["typeProperties"]["storedProcedureParameters"]
        assert params["@Id"]["type"] == "Int64"
        assert params["@Status"]["type"] == "String"

    def test_script_with_params(self):
        task = _make_task(
            sql_statement="UPDATE t SET x = ? WHERE id = ?",
            parameter_bindings=[
                {"variable": "User::Val", "direction": "Input",
                 "data_type": "200", "parameter_name": "@p0"},
                {"variable": "User::Id", "direction": "Input",
                 "data_type": "3", "parameter_name": "@p1"},
            ],
        )
        converter = ExecuteSQLConverter()
        activities = converter.convert(task, [], {})

        assert activities[0]["type"] == "Script"
        script_block = activities[0]["typeProperties"]["scripts"][0]
        assert "parameters" in script_block
        assert len(script_block["parameters"]) == 2
        assert script_block["parameters"][0]["type"] == "String"
        assert script_block["parameters"][1]["type"] == "Int32"

    def test_script_no_params(self):
        task = _make_task(sql_statement="TRUNCATE TABLE dbo.Staging")
        converter = ExecuteSQLConverter()
        activities = converter.convert(task, [], {})

        script_block = activities[0]["typeProperties"]["scripts"][0]
        assert "parameters" not in script_block

    def test_lookup_unaffected(self):
        task = _make_task(
            result_set_type="SingleRow",
            sql_statement="SELECT COUNT(*) cnt FROM dbo.Orders",
        )
        converter = ExecuteSQLConverter()
        activities = converter.convert(task, [], {})

        assert activities[0]["type"] == "Lookup"
        assert activities[0]["typeProperties"]["firstRowOnly"] is True
