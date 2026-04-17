"""
Tests for the similarity analyzer and consolidated pipeline generator.
"""
from __future__ import annotations

import json
import pytest
from pathlib import Path

from ssis_adf_agent.parsers.models import (
    ConnectionManagerType,
    ExecuteSQLTask,
    FileSystemTask,
    DataFlowTask,
    DataFlowComponent,
    ScriptTask,
    SSISConnectionManager,
    SSISPackage,
    SSISVariable,
    TaskType,
)
from ssis_adf_agent.analyzers.similarity_analyzer import (
    ConsolidationGroup,
    PackageFingerprint,
    SimilarityResult,
    fingerprint_package,
    group_similar_packages,
)
from ssis_adf_agent.generators.consolidated_pipeline_generator import (
    generate_consolidated_pipelines,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sql_to_csv_package(
    name: str,
    sql: str,
    server: str,
    database: str,
    output_path: str,
) -> SSISPackage:
    """Create a package that represents 'execute SQL → export CSV'."""
    return SSISPackage(
        id=f"pkg-{name}",
        name=name,
        source_file=f"C:\\ssis\\{name}.dtsx",
        connection_managers=[
            SSISConnectionManager(
                id="cm-db",
                name="SourceDB",
                type=ConnectionManagerType.OLEDB,
                server=server,
                database=database,
                connection_string=f"Server={server};Database={database};",
            ),
            SSISConnectionManager(
                id="cm-file",
                name="OutputFile",
                type=ConnectionManagerType.FLAT_FILE,
                file_path=output_path,
            ),
        ],
        variables=[
            SSISVariable(name="OutputPath", namespace="User", value=output_path),
        ],
        tasks=[
            ExecuteSQLTask(
                id=f"sql-{name}",
                name="Run Query",
                connection_id="cm-db",
                sql_statement=sql,
                result_set_type="FullResultSet",
            ),
            FileSystemTask(
                id=f"fs-{name}",
                name="Export Results",
                operation="CopyFile",
                source_path="tempresults.csv",
                destination_path=output_path,
            ),
        ],
    )


def _make_different_package(name: str) -> SSISPackage:
    """Create a structurally different package (only a script task)."""
    return SSISPackage(
        id=f"pkg-{name}",
        name=name,
        source_file=f"C:\\ssis\\{name}.dtsx",
        tasks=[
            ScriptTask(
                id=f"scr-{name}",
                name="Run Script",
                source_code='public void Main() { }',
            ),
        ],
    )


# ===================================================================
# 1. Fingerprinting
# ===================================================================

class TestFingerprinting:

    def test_identical_structures_same_fingerprint(self):
        """Two packages with same structure but different values → same digest."""
        pkg1 = _make_sql_to_csv_package(
            "ExportOrders", "SELECT * FROM Orders", "PROD01", "SalesDB",
            r"\\share\orders.csv",
        )
        pkg2 = _make_sql_to_csv_package(
            "ExportCustomers", "SELECT * FROM Customers", "PROD02", "CustDB",
            r"\\share\customers.csv",
        )
        fp1 = fingerprint_package(pkg1)
        fp2 = fingerprint_package(pkg2)

        assert fp1.digest == fp2.digest

    def test_different_structures_different_fingerprint(self):
        """Structurally different packages → different digest."""
        pkg1 = _make_sql_to_csv_package(
            "ExportOrders", "SELECT * FROM Orders", "PROD01", "SalesDB",
            r"\\share\orders.csv",
        )
        pkg2 = _make_different_package("RunScript")

        fp1 = fingerprint_package(pkg1)
        fp2 = fingerprint_package(pkg2)

        assert fp1.digest != fp2.digest

    def test_fingerprint_has_shape_summary(self):
        pkg = _make_sql_to_csv_package(
            "Test", "SELECT 1", "SRV", "DB", r"\\out.csv",
        )
        fp = fingerprint_package(pkg)

        assert fp.shape_summary  # non-empty
        assert "ExecuteSQLTask" in fp.shape_summary or "FileSystemTask" in fp.shape_summary

    def test_fingerprint_captures_task_sequence(self):
        pkg = _make_sql_to_csv_package(
            "Test", "SELECT 1", "SRV", "DB", r"\\out.csv",
        )
        fp = fingerprint_package(pkg)

        assert len(fp.task_type_sequence) == 2
        assert "ExecuteSQLTask" in fp.task_type_sequence[0]
        assert "FileSystemTask" in fp.task_type_sequence[1]

    def test_fingerprint_captures_connection_types(self):
        pkg = _make_sql_to_csv_package(
            "Test", "SELECT 1", "SRV", "DB", r"\\out.csv",
        )
        fp = fingerprint_package(pkg)

        assert "FLATFILE" in fp.connection_manager_types
        assert "OLEDB" in fp.connection_manager_types

    def test_result_set_type_matters(self):
        """ExecuteSQL with FullResultSet vs None → different fingerprint."""
        pkg1 = _make_sql_to_csv_package(
            "A", "SELECT 1", "SRV", "DB", r"\\a.csv",
        )
        # Change result set type on second package
        pkg2 = _make_sql_to_csv_package(
            "B", "SELECT 1", "SRV", "DB", r"\\b.csv",
        )
        pkg2.tasks[0] = ExecuteSQLTask(
            id="sql-B",
            name="Run Query",
            connection_id="cm-db",
            sql_statement="SELECT 1",
            result_set_type="None",  # different
        )

        fp1 = fingerprint_package(pkg1)
        fp2 = fingerprint_package(pkg2)
        assert fp1.digest != fp2.digest


# ===================================================================
# 2. Grouping
# ===================================================================

class TestGrouping:

    def test_groups_identical_packages(self):
        """Multiple identical-structure packages should be grouped together."""
        packages = [
            _make_sql_to_csv_package(
                f"Export{i}", f"SELECT * FROM Table{i}", f"SRV{i}", f"DB{i}",
                f"\\\\share\\file{i}.csv",
            )
            for i in range(5)
        ]
        result = group_similar_packages(packages)

        assert result.total_packages == 5
        assert len(result.groups) == 1
        assert len(result.ungrouped) == 0
        assert len(result.groups[0].packages) == 5

    def test_separates_different_structures(self):
        """Different-structure packages should not be grouped."""
        packages = [
            _make_sql_to_csv_package("Export1", "SELECT 1", "SRV", "DB", "\\\\a.csv"),
            _make_sql_to_csv_package("Export2", "SELECT 2", "SRV", "DB", "\\\\b.csv"),
            _make_different_package("Script1"),
            _make_different_package("Script2"),
        ]
        result = group_similar_packages(packages)

        assert len(result.groups) == 2  # one group of SQL-to-CSV, one group of scripts
        assert len(result.ungrouped) == 0

    def test_single_package_not_grouped(self):
        """A single unique package should be ungrouped."""
        packages = [
            _make_sql_to_csv_package("Export1", "SELECT 1", "SRV", "DB", "\\\\a.csv"),
            _make_different_package("Unique"),
        ]
        result = group_similar_packages(packages)

        # Export1 is alone in its shape → ungrouped
        # Unique is alone in its shape → ungrouped
        assert len(result.groups) == 0
        assert len(result.ungrouped) == 2

    def test_extracts_varying_parameters(self):
        """Grouped packages should have their differing values extracted."""
        packages = [
            _make_sql_to_csv_package(
                "ExportOrders", "SELECT * FROM Orders", "PROD01", "SalesDB",
                r"\\share\orders.csv",
            ),
            _make_sql_to_csv_package(
                "ExportCustomers", "SELECT * FROM Customers", "PROD02", "CustDB",
                r"\\share\customers.csv",
            ),
        ]
        result = group_similar_packages(packages)

        assert len(result.groups) == 1
        group = result.groups[0]
        assert len(group.parameter_sets) == 2
        assert len(group.shared_parameter_names) > 0

        # The SQL statements should be among the varying params
        ps0_values = group.parameter_sets[0].values
        ps1_values = group.parameter_sets[1].values

        # At least one parameter should differ between the two
        has_difference = any(
            ps0_values.get(k) != ps1_values.get(k)
            for k in group.shared_parameter_names
        )
        assert has_difference

    def test_ten_packages_one_group(self):
        """The customer scenario: 10 identical packages → 1 group."""
        packages = [
            _make_sql_to_csv_package(
                f"Export_{table}",
                f"SELECT * FROM dbo.{table}",
                "SQLPROD01",
                "ReportingDB",
                f"\\\\fileshare\\exports\\{table}.csv",
            )
            for table in [
                "Orders", "Customers", "Products", "Employees", "Invoices",
                "Shipments", "Returns", "Categories", "Suppliers", "Regions",
            ]
        ]
        result = group_similar_packages(packages)

        assert result.total_packages == 10
        assert len(result.groups) == 1
        assert len(result.groups[0].packages) == 10
        assert len(result.groups[0].parameter_sets) == 10


# ===================================================================
# 3. Consolidated pipeline generation
# ===================================================================

class TestConsolidatedPipelineGeneration:

    def test_generates_parent_and_child(self, tmp_path: Path):
        """Should produce both a parent and child pipeline file."""
        packages = [
            _make_sql_to_csv_package(
                "ExportOrders", "SELECT * FROM Orders", "PROD01", "SalesDB",
                r"\\share\orders.csv",
            ),
            _make_sql_to_csv_package(
                "ExportCustomers", "SELECT * FROM Customers", "PROD02", "CustDB",
                r"\\share\customers.csv",
            ),
        ]
        result = group_similar_packages(packages)
        group = result.groups[0]

        summary = generate_consolidated_pipelines(group, tmp_path)

        assert summary["packages_consolidated"] == 2
        assert summary["child_pipeline"]
        assert summary["parent_pipeline"]

        # Files should exist on disk
        child_file = Path(summary["files"]["child_pipeline"])
        parent_file = Path(summary["files"]["parent_pipeline"])
        assert child_file.exists()
        assert parent_file.exists()

    def test_parent_has_foreach_activity(self, tmp_path: Path):
        """Parent pipeline should contain a ForEach activity."""
        packages = [
            _make_sql_to_csv_package("A", "SELECT 1", "SRV", "DB", "\\\\a.csv"),
            _make_sql_to_csv_package("B", "SELECT 2", "SRV", "DB", "\\\\b.csv"),
        ]
        result = group_similar_packages(packages)
        summary = generate_consolidated_pipelines(result.groups[0], tmp_path)

        parent_file = Path(summary["files"]["parent_pipeline"])
        parent = json.loads(parent_file.read_text())

        activities = parent["properties"]["activities"]
        assert len(activities) == 1
        assert activities[0]["type"] == "ForEach"

    def test_parent_foreach_calls_child(self, tmp_path: Path):
        """ForEach should contain an ExecutePipeline referencing the child."""
        packages = [
            _make_sql_to_csv_package("A", "SELECT 1", "SRV", "DB", "\\\\a.csv"),
            _make_sql_to_csv_package("B", "SELECT 2", "SRV", "DB", "\\\\b.csv"),
        ]
        result = group_similar_packages(packages)
        summary = generate_consolidated_pipelines(result.groups[0], tmp_path)

        parent = json.loads(Path(summary["files"]["parent_pipeline"]).read_text())
        inner_activities = parent["properties"]["activities"][0]["typeProperties"]["activities"]
        assert len(inner_activities) == 1
        assert inner_activities[0]["type"] == "ExecutePipeline"

        child_ref = inner_activities[0]["typeProperties"]["pipeline"]["referenceName"]
        assert child_ref == summary["child_pipeline"]

    def test_parent_has_config_items_parameter(self, tmp_path: Path):
        """Parent should have a configItems array parameter with all package entries."""
        packages = [
            _make_sql_to_csv_package(
                f"Export{i}", f"SELECT {i}", "SRV", "DB", f"\\\\{i}.csv"
            )
            for i in range(3)
        ]
        result = group_similar_packages(packages)
        summary = generate_consolidated_pipelines(result.groups[0], tmp_path)

        parent = json.loads(Path(summary["files"]["parent_pipeline"]).read_text())
        params = parent["properties"]["parameters"]

        assert "configItems" in params
        assert params["configItems"]["type"] == "Array"
        config = params["configItems"]["defaultValue"]
        assert len(config) == 3

    def test_child_has_varying_parameters(self, tmp_path: Path):
        """Child pipeline should have parameters for each varying value."""
        packages = [
            _make_sql_to_csv_package(
                "ExportOrders", "SELECT * FROM Orders", "PROD01", "SalesDB",
                r"\\share\orders.csv",
            ),
            _make_sql_to_csv_package(
                "ExportCustomers", "SELECT * FROM Customers", "PROD02", "CustDB",
                r"\\share\customers.csv",
            ),
        ]
        result = group_similar_packages(packages)
        summary = generate_consolidated_pipelines(result.groups[0], tmp_path)

        child = json.loads(Path(summary["files"]["child_pipeline"]).read_text())
        child_params = child["properties"]["parameters"]

        # Should have parameters for the varying values
        assert len(child_params) > 0
        # SQL statement should be parameterized
        sql_params = [k for k in child_params if "sql" in k.lower() or "statement" in k.lower()]
        assert len(sql_params) >= 1

    def test_config_preserves_source_package_name(self, tmp_path: Path):
        """Each config item should identify which source package it came from."""
        packages = [
            _make_sql_to_csv_package("OrdersExport", "SELECT 1", "S", "D", "\\\\a.csv"),
            _make_sql_to_csv_package("CustExport", "SELECT 2", "S", "D", "\\\\b.csv"),
        ]
        result = group_similar_packages(packages)
        summary = generate_consolidated_pipelines(result.groups[0], tmp_path)

        parent = json.loads(Path(summary["files"]["parent_pipeline"]).read_text())
        config = parent["properties"]["parameters"]["configItems"]["defaultValue"]

        names = [item["_source_package"] for item in config]
        assert "OrdersExport" in names
        assert "CustExport" in names

    def test_ten_packages_consolidated(self, tmp_path: Path):
        """Full customer scenario: 10 packages → 1 parent + 1 child."""
        tables = [
            "Orders", "Customers", "Products", "Employees", "Invoices",
            "Shipments", "Returns", "Categories", "Suppliers", "Regions",
        ]
        packages = [
            _make_sql_to_csv_package(
                f"Export_{t}",
                f"SELECT * FROM dbo.{t}",
                "SQLPROD01",
                "ReportingDB",
                f"\\\\fileshare\\exports\\{t}.csv",
            )
            for t in tables
        ]
        result = group_similar_packages(packages)
        summary = generate_consolidated_pipelines(result.groups[0], tmp_path)

        assert summary["packages_consolidated"] == 10

        parent = json.loads(Path(summary["files"]["parent_pipeline"]).read_text())
        config = parent["properties"]["parameters"]["configItems"]["defaultValue"]
        assert len(config) == 10

        # Each config item should reference the right source package
        source_names = {item["_source_package"] for item in config}
        for t in tables:
            assert f"Export_{t}" in source_names
