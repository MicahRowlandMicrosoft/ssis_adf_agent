"""Tests for bulk_analyze MCP tool."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from ssis_adf_agent.mcp_server import _bulk_analyze


_DTSX_TEMPLATE = """<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
    DTS:ExecutableType="Microsoft.Package"
    DTS:DTSID="{11111111-1111-1111-1111-1111111111{n}}"
    DTS:ObjectName="Pkg{n}">
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\T1"
        DTS:ExecutableType="Microsoft.FileSystemTask"
        DTS:DTSID="{AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAA{n}}"
        DTS:ObjectName="T1" />
  </DTS:Executables>
</DTS:Executable>
"""


def test_bulk_analyze_walks_directory_and_aggregates(tmp_path: Path) -> None:
    for i in (1, 2, 3):
        p = tmp_path / f"pkg{i}.dtsx"
        p.write_text(_DTSX_TEMPLATE.replace("{n}", str(i)), encoding="utf-8")

    result = asyncio.run(_bulk_analyze({"source_path": str(tmp_path)}))
    payload = json.loads(result[0].text)
    assert payload["package_count"] == 3
    assert payload["failure_count"] == 0
    assert sum(payload["estate_summary"]["by_complexity_bucket"].values()) == 3
    # All packages should appear in the per-package list, sorted by complexity desc
    scores = [p["complexity_score"] for p in payload["packages"]]
    assert scores == sorted(scores, reverse=True)


def test_bulk_analyze_writes_report_when_output_path_supplied(tmp_path: Path) -> None:
    (tmp_path / "pkg.dtsx").write_text(
        _DTSX_TEMPLATE.replace("{n}", "1"), encoding="utf-8",
    )
    out = tmp_path / "report.json"
    asyncio.run(_bulk_analyze({"source_path": str(tmp_path), "output_path": str(out)}))
    assert out.exists()
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert saved["package_count"] == 1


def test_bulk_analyze_handles_empty_directory(tmp_path: Path) -> None:
    result = asyncio.run(_bulk_analyze({"source_path": str(tmp_path)}))
    payload = json.loads(result[0].text)
    assert payload["package_count"] == 0
    assert payload["failures"] == []


_PROJECT_PARAMS_TEMPLATE = """<?xml version="1.0"?>
<SSIS:Parameters xmlns:SSIS="www.microsoft.com/SqlServer/SSIS">
  <SSIS:Parameter SSIS:Name="DbPassword">
    <SSIS:Properties>
      <SSIS:Property SSIS:Name="DataType">18</SSIS:Property>
      <SSIS:Property SSIS:Name="Sensitive">1</SSIS:Property>
      <SSIS:Property SSIS:Name="Value"></SSIS:Property>
    </SSIS:Properties>
  </SSIS:Parameter>
</SSIS:Parameters>
"""


def test_bulk_analyze_groups_packages_by_project_directory(tmp_path: Path) -> None:
    proj_a = tmp_path / "ProjA"
    proj_b = tmp_path / "ProjB"
    proj_a.mkdir(); proj_b.mkdir()
    # Two packages share Project.params in ProjA
    (proj_a / "Project.params").write_text(_PROJECT_PARAMS_TEMPLATE, encoding="utf-8")
    for i in (1, 2):
        (proj_a / f"pkg{i}.dtsx").write_text(
            _DTSX_TEMPLATE.replace("{n}", str(i)), encoding="utf-8",
        )
    # One package in ProjB without Project.params
    (proj_b / "pkg3.dtsx").write_text(
        _DTSX_TEMPLATE.replace("{n}", "3"), encoding="utf-8",
    )

    result = asyncio.run(_bulk_analyze({"source_path": str(tmp_path)}))
    payload = json.loads(result[0].text)

    assert payload["package_count"] == 3
    assert payload["estate_summary"]["project_count"] == 2

    projects = {p["project_dir"]: p for p in payload["projects"]}
    a = projects[str(proj_a)]
    b = projects[str(proj_b)]

    assert a["package_count"] == 2
    assert a["has_project_params"] is True
    assert "DbPassword" in a["shared_sensitive_params"]

    assert b["package_count"] == 1
    assert b["has_project_params"] is False
    assert b["shared_sensitive_params"] == []

    # ProjA must produce a shared-infra Key Vault recommendation.
    recs = payload["estate_summary"]["shared_infra_recommendations"]
    proj_a_recs = [r for r in recs if r["project_dir"] == str(proj_a)]
    assert any("Key Vault" in r["recommendation"] for r in proj_a_recs)
    # ProjB has only 1 package → no shared-infra recommendation.
    assert not any(r["project_dir"] == str(proj_b) for r in recs)


def test_bulk_analyze_row_includes_project_metadata(tmp_path: Path) -> None:
    (tmp_path / "Project.params").write_text(_PROJECT_PARAMS_TEMPLATE, encoding="utf-8")
    (tmp_path / "pkg.dtsx").write_text(
        _DTSX_TEMPLATE.replace("{n}", "1"), encoding="utf-8",
    )
    result = asyncio.run(_bulk_analyze({"source_path": str(tmp_path)}))
    payload = json.loads(result[0].text)
    row = payload["packages"][0]
    assert row["project_dir"] == str(tmp_path)
    assert row["has_project_params"] is True
    assert row["sensitive_project_params"] == ["DbPassword"]

