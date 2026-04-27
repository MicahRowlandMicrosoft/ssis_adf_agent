"""Regression tests for PrecedenceConstraint endpoint resolution.

SSIS XML stores constraint From/To as RefId paths (e.g. ``Package\\Copy Template``)
while task ``.id`` holds the DTSID GUID. The parser must rewrite those refs to
GUIDs so that topological_sort, ADF dependsOn generation, and explainer diagrams
can match constraints to tasks.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from ssis_adf_agent.analyzers.dependency_graph import (
    get_depends_on_for_task,
    topological_sort,
)
from ssis_adf_agent.parsers.ssis_parser import SSISParser


_DTSX_TEMPLATE = """<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
    DTS:ExecutableType="Microsoft.Package"
    DTS:DTSID="{11111111-1111-1111-1111-111111111111}"
    DTS:ObjectName="TestPkg">
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\Copy Template"
        DTS:ExecutableType="Microsoft.FileSystemTask"
        DTS:DTSID="{AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA}"
        DTS:ObjectName="Copy Template" />
    <DTS:Executable DTS:refId="Package\\Set Attributes"
        DTS:ExecutableType="Microsoft.FileSystemTask"
        DTS:DTSID="{BBBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB}"
        DTS:ObjectName="Set Attributes" />
    <DTS:Executable DTS:refId="Package\\Rename File"
        DTS:ExecutableType="Microsoft.FileSystemTask"
        DTS:DTSID="{CCCCCCCC-CCCC-CCCC-CCCC-CCCCCCCCCCCC}"
        DTS:ObjectName="Rename File" />
  </DTS:Executables>
  <DTS:PrecedenceConstraints>
    <DTS:PrecedenceConstraint DTS:refId="Package.PrecedenceConstraints[c1]"
        DTS:From="Package\\Copy Template" DTS:To="Package\\Set Attributes"
        DTS:Value="1" DTS:EvalOp="2" />
    <DTS:PrecedenceConstraint DTS:refId="Package.PrecedenceConstraints[c2]"
        DTS:From="Package\\Set Attributes" DTS:To="Package\\Rename File"
        DTS:Value="1" DTS:EvalOp="2" />
  </DTS:PrecedenceConstraints>
</DTS:Executable>
"""


def _write_pkg(tmp_path: Path) -> Path:
    p = tmp_path / "test.dtsx"
    p.write_text(textwrap.dedent(_DTSX_TEMPLATE), encoding="utf-8")
    return p


def test_constraint_endpoints_resolve_to_task_guids(tmp_path: Path) -> None:
    pkg = SSISParser().parse(_write_pkg(tmp_path))

    task_ids = {t.id for t in pkg.tasks}
    assert len(pkg.constraints) == 2
    for c in pkg.constraints:
        assert c.from_task_id in task_ids, f"from {c.from_task_id} not in {task_ids}"
        assert c.to_task_id in task_ids, f"to {c.to_task_id} not in {task_ids}"


def test_topological_sort_honors_constraints(tmp_path: Path) -> None:
    pkg = SSISParser().parse(_write_pkg(tmp_path))
    by_id = {t.id: t for t in pkg.tasks}
    sorted_ids = topological_sort(pkg.tasks, pkg.constraints)
    names = [by_id[i].name for i in sorted_ids]
    assert names == ["Copy Template", "Set Attributes", "Rename File"]


def test_get_depends_on_returns_predecessor_name(tmp_path: Path) -> None:
    pkg = SSISParser().parse(_write_pkg(tmp_path))
    by_id = {t.id: t for t in pkg.tasks}
    rename = next(t for t in pkg.tasks if t.name == "Rename File")

    deps = get_depends_on_for_task(rename.id, by_id, pkg.constraints)
    assert any(d.activity == "Set Attributes" for d in deps)
