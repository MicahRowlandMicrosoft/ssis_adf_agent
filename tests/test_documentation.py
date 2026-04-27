"""Smoke tests for documentation + parity tools."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.documentation import (
    build_adf_outline,
    build_pre_migration_pdf,
    build_ssis_outline,
    render_adf_markdown,
    render_ssis_markdown,
    validate_parity,
)
from ssis_adf_agent.documentation.parity_validator import render_parity_markdown
from ssis_adf_agent.parsers.models import (
    ConnectionManagerType,
    DataFlowTask,
    ExecuteSQLTask,
    PrecedenceConstraint,
    SSISConnectionManager,
    SSISPackage,
    SSISParameter,
    TaskType,
)


def _make_minimal_package() -> SSISPackage:
    cm = SSISConnectionManager(
        id="cm1",
        name="MySql",
        type=ConnectionManagerType.OLEDB,
        server="srv",
        database="db",
    )
    t1 = ExecuteSQLTask(
        id="t1",
        name="Truncate staging",
        connection_id="cm1",
        sql_statement="TRUNCATE TABLE dbo.staging",
    )
    t2 = ExecuteSQLTask(
        id="t2",
        name="Load fact",
        connection_id="cm1",
        sql_statement="INSERT INTO dbo.fact SELECT * FROM dbo.staging",
    )
    constraint = PrecedenceConstraint(
        id="pc1", from_task_id="t1", to_task_id="t2",
    )
    return SSISPackage(
        id="pkg",
        name="TestPackage",
        source_file="dummy.dtsx",
        connection_managers=[cm],
        parameters=[SSISParameter(name="LoadDate", data_type="DateTime")],
        tasks=[t1, t2],
        constraints=[constraint],
    )


def _write_minimal_adf(out: Path, package_name: str = "TestPackage") -> None:
    """Write a minimal ADF artifact set that the parity validator can read."""
    (out / "pipeline").mkdir(parents=True)
    (out / "linkedService").mkdir()
    (out / "dataset").mkdir()
    (out / "dataflow").mkdir()
    (out / "trigger").mkdir()
    pipeline = {
        "name": f"PL_{package_name}",
        "properties": {
            "description": "Test pipeline",
            "annotations": ["ssis-adf-agent"],
            "activities": [
                {
                    "name": "Truncate staging",
                    "type": "Script",
                    "typeProperties": {"scripts": [{"text": "TRUNCATE TABLE dbo.staging"}]},
                },
                {
                    "name": "Load fact",
                    "type": "Script",
                    "dependsOn": [{"activity": "Truncate staging", "dependencyConditions": ["Succeeded"]}],
                    "typeProperties": {"scripts": [{"text": "INSERT INTO dbo.fact SELECT * FROM dbo.staging"}]},
                },
            ],
            "parameters": {"LoadDate": {"type": "String"}},
        },
    }
    (out / "pipeline" / f"PL_{package_name}.json").write_text(
        json.dumps(pipeline, indent=2), encoding="utf-8"
    )
    ls = {
        "name": "LS_MySql",
        "properties": {
            "type": "AzureSqlDatabase",
            "typeProperties": {"connectionString": "Server=srv;Database=db"},
        },
    }
    (out / "linkedService" / "LS_MySql.json").write_text(
        json.dumps(ls, indent=2), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# explain_ssis_package
# ---------------------------------------------------------------------------

def test_build_ssis_outline_basic() -> None:
    pkg = _make_minimal_package()
    outline = build_ssis_outline(pkg)
    assert outline["package_name"] == "TestPackage"
    assert outline["totals"]["execute_sql_tasks"] == 2
    assert outline["totals"]["connection_managers"] == 1
    assert len(outline["steps"]) == 2
    # First step should run before the second per the constraint
    assert outline["steps"][0]["task_name"] == "Truncate staging"
    assert outline["steps"][1]["task_name"] == "Load fact"
    # Source vs sink classification
    sys_roles = {s["name"]: s["roles"] for s in outline["systems"]}
    assert "sink" in sys_roles["MySql"]
    # Mermaid diagram is present and references both task ids
    md = outline["diagrams"]["control_flow_mermaid"]
    assert "flowchart" in md
    assert "Truncate" in md or "t1" in md


def test_render_ssis_markdown_smoke() -> None:
    pkg = _make_minimal_package()
    md = render_ssis_markdown(build_ssis_outline(pkg))
    assert "# SSIS package" in md
    assert "## Step-by-step execution" in md
    assert "```mermaid" in md


# ---------------------------------------------------------------------------
# explain_adf_artifacts
# ---------------------------------------------------------------------------

def test_build_adf_outline(tmp_path: Path) -> None:
    _write_minimal_adf(tmp_path)
    outline = build_adf_outline(tmp_path)
    assert outline["totals"]["pipelines"] == 1
    assert outline["totals"]["linked_services"] == 1
    assert outline["pipelines"][0]["activity_count"] == 2
    assert "Script" in outline["pipelines"][0]["activities_by_type"]


def test_render_adf_markdown(tmp_path: Path) -> None:
    _write_minimal_adf(tmp_path)
    md = render_adf_markdown(build_adf_outline(tmp_path))
    assert "# ADF artifacts" in md
    assert "## Pipeline:" in md
    assert "```mermaid" in md


# ---------------------------------------------------------------------------
# validate_conversion_parity
# ---------------------------------------------------------------------------

def test_validate_parity_clean(tmp_path: Path) -> None:
    pkg = _make_minimal_package()
    _write_minimal_adf(tmp_path)
    result = validate_parity(pkg, tmp_path, dry_run=False)
    # 2 ExecuteSQL tasks → 2 Script activities — should match
    assert result.summary["ssis_total_tasks"] == 2
    assert result.summary["adf_total_activities"] == 2
    # No errors
    errs = [i for i in result.issues if i.severity == "error"]
    assert errs == [], f"unexpected errors: {errs}"
    assert result.ok


def test_validate_parity_missing_pipeline(tmp_path: Path) -> None:
    pkg = _make_minimal_package()
    # Empty output dir
    result = validate_parity(pkg, tmp_path, dry_run=False)
    assert not result.ok
    assert any(i.severity == "error" for i in result.issues)


def test_validate_parity_dry_run_with_sdk(tmp_path: Path) -> None:
    pkg = _make_minimal_package()
    _write_minimal_adf(tmp_path)
    result = validate_parity(pkg, tmp_path, dry_run=True)
    # SDK deserialization should populate the dryrun dict
    assert "deserialized" in result.artifact_dryrun


def test_render_parity_markdown(tmp_path: Path) -> None:
    pkg = _make_minimal_package()
    _write_minimal_adf(tmp_path)
    result = validate_parity(pkg, tmp_path, dry_run=False)
    md = render_parity_markdown(result)
    assert "Parity report" in md
    assert "## Coverage summary" in md


# ---------------------------------------------------------------------------
# Pre-migration PDF
# ---------------------------------------------------------------------------

def test_build_pre_migration_pdf(tmp_path: Path) -> None:
    pytest.importorskip("reportlab")
    pkg = _make_minimal_package()
    _write_minimal_adf(tmp_path)
    parity = validate_parity(pkg, tmp_path, dry_run=False)
    pdf_path = tmp_path / "report.pdf"
    written = build_pre_migration_pdf(
        output_pdf=pdf_path,
        ssis_outline=build_ssis_outline(pkg),
        adf_outline=build_adf_outline(tmp_path),
        parity=parity.to_dict(),
        factory_target={"Factory": "MyFactory", "Resource group": "rg-test"},
    )
    assert Path(written).exists()
    assert Path(written).stat().st_size > 1000  # should be a non-trivial PDF
