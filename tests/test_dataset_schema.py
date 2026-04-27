"""Tests for dataset column schema generation."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.generators.dataset_generator import (
    _build_dataset,
    _columns_to_schema,
    generate_datasets,
)
from ssis_adf_agent.parsers.models import (
    DataFlowColumn,
    DataFlowComponent,
    DataFlowTask,
    DataType,
    SSISConnectionManager,
    ConnectionManagerType,
    SSISPackage,
    TaskType,
)


# ---------------------------------------------------------------------------
# _columns_to_schema unit tests
# ---------------------------------------------------------------------------

class TestColumnsToSchema:
    def test_empty_columns(self):
        assert _columns_to_schema([]) == []

    def test_string_column(self):
        cols = [DataFlowColumn(name="FirstName", data_type=DataType.WSTRING, length=50)]
        schema = _columns_to_schema(cols)
        assert len(schema) == 1
        assert schema[0]["name"] == "FirstName"
        assert schema[0]["type"] == "string"
        assert schema[0]["length"] == 50

    def test_int_column(self):
        cols = [DataFlowColumn(name="Age", data_type=DataType.INT32)]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "int32"
        assert "length" not in schema[0]

    def test_decimal_with_precision_scale(self):
        cols = [DataFlowColumn(name="Amount", data_type=DataType.DECIMAL, precision=18, scale=2)]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "decimal"
        assert schema[0]["precision"] == 18
        assert schema[0]["scale"] == 2

    def test_currency_type(self):
        cols = [DataFlowColumn(name="Price", data_type=DataType.CURRENCY, precision=19, scale=4)]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "decimal"
        assert schema[0]["precision"] == 19

    def test_boolean_column(self):
        cols = [DataFlowColumn(name="IsActive", data_type=DataType.BOOLEAN)]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "boolean"

    def test_datetime_column(self):
        cols = [DataFlowColumn(name="CreatedAt", data_type=DataType.DBTIMESTAMP)]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "datetime"

    def test_date_column(self):
        cols = [DataFlowColumn(name="BirthDate", data_type=DataType.DBDATE)]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "date"

    def test_guid_column(self):
        cols = [DataFlowColumn(name="RowGuid", data_type=DataType.GUID)]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "string"

    def test_bytes_column(self):
        cols = [DataFlowColumn(name="Photo", data_type=DataType.BYTES)]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "binary"

    def test_float_double(self):
        cols = [
            DataFlowColumn(name="Score", data_type=DataType.FLOAT),
            DataFlowColumn(name="Latitude", data_type=DataType.DOUBLE),
        ]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "float"
        assert schema[1]["type"] == "double"

    def test_int_variants(self):
        cols = [
            DataFlowColumn(name="TinyVal", data_type=DataType.INT8),
            DataFlowColumn(name="SmallVal", data_type=DataType.INT16),
            DataFlowColumn(name="BigVal", data_type=DataType.INT64),
        ]
        schema = _columns_to_schema(cols)
        assert schema[0]["type"] == "int8"
        assert schema[1]["type"] == "int16"
        assert schema[2]["type"] == "int64"

    def test_string_no_length_omitted(self):
        """When length is 0, the length key should not appear."""
        cols = [DataFlowColumn(name="Notes", data_type=DataType.WSTRING, length=0)]
        schema = _columns_to_schema(cols)
        assert "length" not in schema[0]

    def test_decimal_no_precision_omitted(self):
        cols = [DataFlowColumn(name="Val", data_type=DataType.DECIMAL, precision=0, scale=0)]
        schema = _columns_to_schema(cols)
        assert "precision" not in schema[0]
        assert "scale" not in schema[0]

    def test_multiple_columns(self):
        cols = [
            DataFlowColumn(name="ID", data_type=DataType.INT32),
            DataFlowColumn(name="Name", data_type=DataType.WSTRING, length=100),
            DataFlowColumn(name="Balance", data_type=DataType.DECIMAL, precision=18, scale=2),
            DataFlowColumn(name="Active", data_type=DataType.BOOLEAN),
        ]
        schema = _columns_to_schema(cols)
        assert len(schema) == 4
        assert [s["name"] for s in schema] == ["ID", "Name", "Balance", "Active"]
        assert [s["type"] for s in schema] == ["int32", "string", "decimal", "boolean"]


# ---------------------------------------------------------------------------
# _build_dataset with columns
# ---------------------------------------------------------------------------

class TestBuildDatasetSchema:
    def test_schema_populated_when_columns_provided(self):
        cols = [
            DataFlowColumn(name="Id", data_type=DataType.INT32),
            DataFlowColumn(name="Name", data_type=DataType.WSTRING, length=200),
        ]
        ds = _build_dataset(
            name="DS_Test",
            ds_type="AzureSqlTable",
            linked_service_name="LS_Test",
            table_name="dbo.Users",
            columns=cols,
        )
        schema = ds["properties"]["schema"]
        assert len(schema) == 2
        assert schema[0]["name"] == "Id"
        assert schema[0]["type"] == "int32"
        assert schema[1]["name"] == "Name"
        assert schema[1]["type"] == "string"
        assert schema[1]["length"] == 200

    def test_schema_empty_when_no_columns(self):
        ds = _build_dataset(
            name="DS_Test",
            ds_type="AzureSqlTable",
            linked_service_name="LS_Test",
        )
        assert ds["properties"]["schema"] == []

    def test_schema_empty_when_columns_none(self):
        ds = _build_dataset(
            name="DS_Test",
            ds_type="DelimitedText",
            linked_service_name="LS_Test",
            columns=None,
        )
        assert ds["properties"]["schema"] == []


# ---------------------------------------------------------------------------
# generate_datasets end-to-end
# ---------------------------------------------------------------------------

class TestGenerateDatasetsWithSchema:
    def _make_package(self, components: list[DataFlowComponent]) -> SSISPackage:
        return SSISPackage(
            id="pkg-1",
            name="TestPackage",
            source_file="test.dtsx",
            connection_managers=[
                SSISConnectionManager(
                    id="conn-1", name="OleDbConn",
                    type=ConnectionManagerType.OLEDB,
                    connection_string="Server=localhost;Database=TestDb;",
                ),
            ],
            tasks=[
                DataFlowTask(
                    id="df-1", name="Data Flow",
                    task_type=TaskType.DATA_FLOW,
                    components=components,
                ),
            ],
        )

    def test_source_columns_populate_schema(self, tmp_path):
        comp = DataFlowComponent(
            id="src-1", name="OLE Source",
            component_class_id="", component_type="OleDbSource",
            connection_id="conn-1",
            output_columns=[
                DataFlowColumn(name="CustomerId", data_type=DataType.INT32),
                DataFlowColumn(name="Email", data_type=DataType.WSTRING, length=255),
            ],
        )
        pkg = self._make_package([comp])
        datasets = generate_datasets(pkg, tmp_path)

        assert len(datasets) == 1
        schema = datasets[0]["properties"]["schema"]
        assert len(schema) == 2
        assert schema[0] == {"name": "CustomerId", "type": "int32"}
        assert schema[1] == {"name": "Email", "type": "string", "length": 255}

    def test_destination_uses_input_columns(self, tmp_path):
        """Destination components may only have input_columns."""
        comp = DataFlowComponent(
            id="dst-1", name="OLE Dest",
            component_class_id="", component_type="OleDbDestination",
            connection_id="conn-1",
            input_columns=[
                DataFlowColumn(name="OrderId", data_type=DataType.INT64),
                DataFlowColumn(name="Total", data_type=DataType.DECIMAL, precision=18, scale=2),
            ],
            output_columns=[],
        )
        pkg = self._make_package([comp])
        datasets = generate_datasets(pkg, tmp_path)

        schema = datasets[0]["properties"]["schema"]
        assert len(schema) == 2
        assert schema[0] == {"name": "OrderId", "type": "int64"}
        assert schema[1] == {"name": "Total", "type": "decimal", "precision": 18, "scale": 2}

    def test_no_columns_gives_empty_schema(self, tmp_path):
        comp = DataFlowComponent(
            id="src-1", name="Bare Source",
            component_class_id="", component_type="OleDbSource",
            connection_id="conn-1",
        )
        pkg = self._make_package([comp])
        datasets = generate_datasets(pkg, tmp_path)

        assert datasets[0]["properties"]["schema"] == []


class TestLookupDatasetGeneration:
    """Lookup transformations should generate a companion dataset."""

    def _make_package(self, components):
        return SSISPackage(
            id="pkg1", name="TestPkg", source_file="test.dtsx",
            connection_managers=[
                SSISConnectionManager(
                    id="conn-1", name="OleConn", type=ConnectionManagerType.OLEDB,
                    connection_string="Server=.;Database=TestDB;",
                    server=".", database="TestDB",
                ),
            ],
            tasks=[
                DataFlowTask(
                    id="dft1", name="DataFlowWithLookup",
                    components=components,
                ),
            ],
        )

    def test_lookup_dataset_created(self, tmp_path):
        lookup = DataFlowComponent(
            id="lkp-1", name="Lookup Customer",
            component_class_id="", component_type="Lookup",
            connection_id="conn-1",
            properties={"OpenRowset": "[dbo].[Customer]"},
        )
        source = DataFlowComponent(
            id="src-1", name="OLE_SRC",
            component_class_id="", component_type="OleDbSource",
            connection_id="conn-1",
        )
        pkg = self._make_package([source, lookup])
        datasets = generate_datasets(pkg, tmp_path)

        names = [ds["name"] for ds in datasets]
        assert "DS_TestPkg_OLE_SRC" in names
        assert "DS_TestPkg_Lookup_Customer_lookup" in names

        # Verify the lookup dataset has the right table
        lkp_ds = next(d for d in datasets if d["name"] == "DS_TestPkg_Lookup_Customer_lookup")
        assert lkp_ds["properties"]["typeProperties"]["table"] == "Customer"

    def test_schema_written_to_json_file(self, tmp_path):
        comp = DataFlowComponent(
            id="src-1", name="TestSrc",
            component_class_id="", component_type="OleDbSource",
            connection_id="conn-1",
            output_columns=[
                DataFlowColumn(name="Id", data_type=DataType.INT32),
            ],
        )
        pkg = self._make_package([comp])
        generate_datasets(pkg, tmp_path)

        ds_file = tmp_path / "dataset" / "DS_TestPkg_TestSrc.json"
        assert ds_file.exists()
        payload = json.loads(ds_file.read_text(encoding="utf-8"))
        assert len(payload["properties"]["schema"]) == 1
        assert payload["properties"]["schema"][0]["name"] == "Id"

    def test_flat_file_with_columns(self, tmp_path):
        comp = DataFlowComponent(
            id="src-ff", name="CSV Source",
            component_class_id="", component_type="FlatFileSource",
            connection_id="conn-1",
            output_columns=[
                DataFlowColumn(name="Line", data_type=DataType.WSTRING, length=500),
                DataFlowColumn(name="Amount", data_type=DataType.DOUBLE),
            ],
        )
        pkg = self._make_package([comp])
        datasets = generate_datasets(pkg, tmp_path)

        schema = datasets[0]["properties"]["schema"]
        assert len(schema) == 2
        assert schema[0]["type"] == "string"
        assert schema[1]["type"] == "double"
