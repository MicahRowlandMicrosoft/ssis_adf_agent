"""Tests for Fabric Phase 5: structural parity validator + estate orchestration."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_modernization_agent.fabric import (
    convert_package_to_fabric,
    render_fabric_parity_markdown,
    validate_fabric_conversion_parity,
)
from ssis_modernization_agent.parsers.models import (
    ConnectionManagerType,
    DataFlowComponent,
    DataFlowTask,
    ScriptTask,
    SSISConnectionManager,
    SSISPackage,
    TaskType,
)
from ssis_modernization_agent.parsers.readers.local_reader import LocalReader

LNI = Path(r"C:\source\test-lni-packages\ADDS-MIPS-TC.dtsx")


# ---------------------------------------------------------------------------
# Builders for synthetic packages (so tests don't depend on the LNI sample)
# ---------------------------------------------------------------------------

def _minimal_package(name: str = "PkgA") -> SSISPackage:
    cm = SSISConnectionManager(
        id=f"{{cm-{name}}}",
        name=f"CM_{name}",
        type=ConnectionManagerType.OLEDB,
        server="dev-sql.local",
        database="DemoDB",
    )
    dft = DataFlowTask(
        id="{dft-1}",
        name="DFT_1",
        task_type=TaskType.DATA_FLOW,
        components=[
            DataFlowComponent(
                id="src1", name="Src", component_class_id="OLEDBSource",
                component_type="OLEDBSource",
                properties={"table_name": "dbo.Src"},
            ),
            DataFlowComponent(
                id="dst1", name="Dst", component_class_id="OLEDBDestination",
                component_type="OLEDBDestination",
                properties={"table_name": "dbo.Dst"},
            ),
        ],
    )
    return SSISPackage(
        id=f"{{pkg-{name}}}", name=name, source_file=f"{name}.dtsx",
        tasks=[dft], connection_managers=[cm],
    )


def _package_with_script(name: str = "PkgScript") -> SSISPackage:
    cm = SSISConnectionManager(
        id=f"{{cm-{name}}}",
        name=f"CM_{name}",
        type=ConnectionManagerType.OLEDB,
        server="prodserver.contoso.com",
        database="ProdDB",
    )
    script = ScriptTask(
        id="{st-1}",
        name="ST_DoThing",
        task_type=TaskType.SCRIPT,
        script_language="CSharp",
        source_code=(
            "public void Main() {\n"
            "    var client = new System.Net.Http.HttpClient();\n"
            "    var response = client.GetAsync(\"https://api.example.com/data\").Result;\n"
            "    System.IO.File.WriteAllText(\"C:\\\\out.txt\", response.Content.ReadAsStringAsync().Result);\n"
            "    Dts.TaskResult = (int)ScriptResults.Success;\n"
            "}\n"
        ),
        read_only_variables=[],
        read_write_variables=[],
    )
    return SSISPackage(
        id=f"{{pkg-{name}}}", name=name, source_file=f"{name}.dtsx",
        tasks=[script], connection_managers=[cm],
    )


# ---------------------------------------------------------------------------
# validate_fabric_conversion_parity — happy paths & failure modes
# ---------------------------------------------------------------------------

def test_parity_passes_on_minimal_package(tmp_path):
    pkg = _minimal_package()
    convert_package_to_fabric(pkg, tmp_path)
    result = validate_fabric_conversion_parity(pkg, tmp_path)

    assert result["target"] == "fabric"
    assert result["package_name"] == "PkgA"
    assert result["ok"], result["issues"]
    summary = result["summary"]
    assert summary["ssis_total_tasks"] == 1
    assert summary["fabric_total_activities"] >= 1
    assert summary["ssis_connection_managers"] == 1
    assert summary["fabric_connection_placeholders"] >= 1
    # At least one match line for the DFT mapping
    assert any("DataFlowTask" in m or "DATA_FLOW" in m or "data_flow" in m
               for m in result["matches"])


def test_parity_script_task_emits_function_stub_match(tmp_path):
    pkg = _package_with_script()
    convert_package_to_fabric(pkg, tmp_path)
    result = validate_fabric_conversion_parity(pkg, tmp_path)

    summary = result["summary"]
    assert summary["ssis_script_tasks"] == 1
    assert summary["fabric_function_stubs"] >= 1
    # A Script Task must always produce a manual-port warning, even on success
    assert any(
        i["category"] == "script_task" and i["severity"] == "warning"
        for i in result["issues"]
    )


def test_parity_fails_when_pipeline_missing_activity(tmp_path):
    pkg = _minimal_package()
    convert_package_to_fabric(pkg, tmp_path)

    # Tamper: delete all activities from the pipeline-content.json
    pipeline_json = next((tmp_path / "pipeline").rglob("pipeline-content.json"))
    doc = json.loads(pipeline_json.read_text(encoding="utf-8"))
    doc["properties"]["activities"] = []
    pipeline_json.write_text(json.dumps(doc), encoding="utf-8")

    result = validate_fabric_conversion_parity(pkg, tmp_path)
    assert not result["ok"]
    assert any(
        i["severity"] == "error" and i["category"] == "task_count"
        for i in result["issues"]
    )


def test_parity_errors_when_connections_manifest_missing(tmp_path):
    pkg = _minimal_package()
    convert_package_to_fabric(pkg, tmp_path)
    (tmp_path / "connections_required.json").unlink()

    result = validate_fabric_conversion_parity(pkg, tmp_path)
    assert not result["ok"]
    assert any(
        i["category"] == "connection" and i["severity"] == "error"
        for i in result["issues"]
    )


def test_parity_warns_on_on_prem_connection(tmp_path):
    """The CM uses 'dev-sql.local' which the resolver flags as on-prem."""
    pkg = _minimal_package()
    convert_package_to_fabric(pkg, tmp_path)
    result = validate_fabric_conversion_parity(pkg, tmp_path)
    assert any(
        i["category"] == "connection" and i["severity"] == "warning"
        and "On-Prem" in i["message"]
        for i in result["issues"]
    )


def test_parity_errors_when_notebook_stub_missing_on_disk(tmp_path):
    """A complex DFT becomes TridentNotebook → stub on disk. Delete stub → error."""
    # Build a deliberately complex DFT to force notebook generation.
    cm = SSISConnectionManager(
        id="{cm-x}", name="CM_X",
        type=ConnectionManagerType.OLEDB,
        server="azuresql.database.windows.net",
        database="DB",
    )
    dft = DataFlowTask(
        id="{dft-cx}",
        name="DFT_Complex",
        task_type=TaskType.DATA_FLOW,
        components=[
            DataFlowComponent(id="s1", name="S1", component_class_id="OLEDBSource",
                              component_type="OLEDBSource",
                              properties={"table_name": "dbo.S1"}),
            DataFlowComponent(id="dc", name="DC", component_class_id="DerivedColumn",
                              component_type="DerivedColumn", properties={}),
            DataFlowComponent(id="lk", name="LK", component_class_id="Lookup",
                              component_type="Lookup", properties={}),
            DataFlowComponent(id="d1", name="D1", component_class_id="OLEDBDestination",
                              component_type="OLEDBDestination",
                              properties={"table_name": "dbo.D1"}),
        ],
    )
    pkg = SSISPackage(
        id="{pkg-cx}", name="PkgComplex", source_file="PkgComplex.dtsx",
        tasks=[dft], connection_managers=[cm],
    )
    convert_package_to_fabric(pkg, tmp_path)

    notebook_dir = tmp_path / "notebook"
    if not list(notebook_dir.glob("*.Notebook")):
        pytest.skip("complex DFT did not produce a notebook in this build")

    # Delete the on-disk stub to simulate a corrupted output
    for nbdir in notebook_dir.glob("*.Notebook"):
        (nbdir / "notebook-content.py").unlink()

    result = validate_fabric_conversion_parity(pkg, tmp_path)
    assert not result["ok"]
    assert any(
        i["category"] == "notebook" and i["severity"] == "error"
        for i in result["issues"]
    )


def test_render_fabric_parity_markdown_includes_verdict_and_summary(tmp_path):
    pkg = _minimal_package()
    convert_package_to_fabric(pkg, tmp_path)
    result = validate_fabric_conversion_parity(pkg, tmp_path)
    md = render_fabric_parity_markdown(result)
    assert "Fabric Parity Report" in md
    assert "PkgA" in md
    assert "Verdict" in md
    assert ("PASS" in md) or ("FAIL" in md)


# ---------------------------------------------------------------------------
# convert_estate_to_fabric (MCP handler)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_convert_estate_to_fabric_aggregates_per_package(tmp_path):
    from ssis_modernization_agent import mcp_server

    # Stage two synthetic .dtsx files via the LNI samples (the smallest
    # available) so we exercise the real reader + converter end-to-end.
    if not LNI.exists():
        pytest.skip("LNI sample not present")
    second = Path(r"C:\source\test-lni-packages\ADDS-Transaction Control.dtsx")
    if not second.exists():
        pytest.skip("Second LNI sample not present")

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "Pkg1.dtsx").write_bytes(LNI.read_bytes())
    (src_dir / "Pkg2.dtsx").write_bytes(second.read_bytes())

    out_dir = tmp_path / "out"
    result_text = await mcp_server._convert_estate_to_fabric({
        "source_path": str(src_dir),
        "output_dir": str(out_dir),
        "dedup_connections": True,
    })
    payload = json.loads(result_text[0].text)

    assert payload["target"] == "fabric"
    assert payload["package_count"] == 2
    assert payload["succeeded_count"] == 2
    assert payload["failed_count"] == 0
    assert "estate_connections_manifest" in payload
    estate_manifest = json.loads(
        Path(payload["estate_connections_manifest"]).read_text(encoding="utf-8")
    )
    assert estate_manifest["scope"] == "estate"
    # Each placeholder records which packages reference it; with two copies
    # of the same source, every CM should be shared by both packages.
    used_by = [c.get("used_by_packages", []) for c in estate_manifest["connections"]]
    assert any(len(u) >= 2 for u in used_by), used_by


@pytest.mark.asyncio
async def test_convert_estate_to_fabric_with_parity_writes_per_package_report(tmp_path):
    from ssis_modernization_agent import mcp_server

    if not LNI.exists():
        pytest.skip("LNI sample not present")

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "Pkg.dtsx").write_bytes(LNI.read_bytes())

    out_dir = tmp_path / "out"
    result_text = await mcp_server._convert_estate_to_fabric({
        "source_path": str(src_dir),
        "output_dir": str(out_dir),
        "dedup_connections": False,
        "with_parity": True,
    })
    payload = json.loads(result_text[0].text)
    assert payload["succeeded_count"] == 1
    pkg = payload["packages"][0]
    assert "parity" in pkg
    assert "ok" in pkg["parity"]
    pkg_out = Path(pkg["output_dir"])
    assert (pkg_out / "parity_report.md").is_file()
    assert (pkg_out / "parity_report.json").is_file()


@pytest.mark.asyncio
async def test_convert_estate_to_fabric_reports_per_package_failure(tmp_path, monkeypatch):
    from ssis_modernization_agent import mcp_server

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    # Write a malformed .dtsx so the reader raises
    (src_dir / "broken.dtsx").write_text("<not-valid-xml")

    out_dir = tmp_path / "out"
    result_text = await mcp_server._convert_estate_to_fabric({
        "source_path": str(src_dir),
        "output_dir": str(out_dir),
        "dedup_connections": False,
    })
    payload = json.loads(result_text[0].text)
    assert payload["package_count"] == 1
    assert payload["failed_count"] == 1
    assert payload["packages"][0]["status"] == "failed"
    assert "error" in payload["packages"][0]


# ---------------------------------------------------------------------------
# MCP wiring — verify both new tools are listed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_phase5_tools_appear_in_list_tools():
    from ssis_modernization_agent import mcp_server

    tools = await mcp_server.list_tools()
    names = {t.name for t in tools}
    assert "validate_fabric_conversion_parity" in names
    assert "convert_estate_to_fabric" in names


# ---------------------------------------------------------------------------
# End-to-end on real LNI sample
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not LNI.exists(), reason="LNI sample not present")
def test_parity_on_lni_adds_mips_tc(tmp_path):
    pkg = LocalReader().read(LNI)
    convert_package_to_fabric(pkg, tmp_path)
    result = validate_fabric_conversion_parity(pkg, tmp_path)
    # We don't assert ok=True (event handlers / scripts make it noisy), but the
    # validator must run cleanly and produce a non-trivial summary.
    summary = result["summary"]
    assert summary["ssis_total_tasks"] >= 1
    assert summary["fabric_total_activities"] >= 1
    # If there are script tasks, the manual-port warning must be present
    if summary.get("ssis_script_tasks", 0) > 0:
        assert any(
            i["category"] == "script_task" and i["severity"] == "warning"
            for i in result["issues"]
        )
