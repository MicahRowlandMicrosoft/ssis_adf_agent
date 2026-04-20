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
