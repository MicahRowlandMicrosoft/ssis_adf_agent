"""P5-14: convert_estate(--with_cost_projection=true) emits cost_projection.json."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from ssis_adf_agent.mcp_server import _convert_estate


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


def _seed_packages(tmp_path: Path, count: int = 2) -> Path:
    src = tmp_path / "src"
    src.mkdir()
    for i in range(1, count + 1):
        (src / f"pkg{i}.dtsx").write_text(
            _DTSX_TEMPLATE.replace("{n}", str(i)), encoding="utf-8",
        )
    return src


def test_with_cost_projection_writes_file(tmp_path: Path) -> None:
    src = _seed_packages(tmp_path, count=2)
    out = tmp_path / "out"

    result = asyncio.run(_convert_estate({
        "source_path": str(src),
        "output_dir": str(out),
        "with_cost_projection": True,
    }))
    payload = json.loads(result[0].text)

    assert payload["succeeded_count"] == 2
    assert payload["cost_projection"]["status"] == "written"

    cost_path = out / "cost_projection.json"
    assert cost_path.exists()

    cost = json.loads(cost_path.read_text(encoding="utf-8"))
    assert cost["package_count"] == 2
    assert "monthly_total_usd" in cost
    assert "annual_total_usd" in cost
    assert cost["currency"] == "USD"
    # Summary mirrors the file's bottom-line numbers
    assert payload["cost_projection"]["monthly_total_usd"] == cost["monthly_total_usd"]


def test_default_no_cost_projection(tmp_path: Path) -> None:
    src = _seed_packages(tmp_path, count=1)
    out = tmp_path / "out"

    result = asyncio.run(_convert_estate({
        "source_path": str(src),
        "output_dir": str(out),
    }))
    payload = json.loads(result[0].text)

    assert "cost_projection" not in payload
    assert not (out / "cost_projection.json").exists()


def test_with_cost_projection_skipped_when_save_plans_false(tmp_path: Path) -> None:
    src = _seed_packages(tmp_path, count=1)
    out = tmp_path / "out"

    result = asyncio.run(_convert_estate({
        "source_path": str(src),
        "output_dir": str(out),
        "save_plans": False,
        "with_cost_projection": True,
    }))
    payload = json.loads(result[0].text)

    assert payload["cost_projection"]["status"] == "skipped"
    assert "save_plans" in payload["cost_projection"]["reason"]
    assert not (out / "cost_projection.json").exists()
