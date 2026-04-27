"""
Regression: extract VB / C# source code from SSIS 2017+ Script Tasks that
embed the source inline as <ProjectItem> CDATA inside <ScriptProject>
(the LNI dialect, formerly classified as "self-closing stub format").

Before the fix:
- script_language always defaulted to "CSharp"
- source_code was None
- the LLM translator silently no-op'd

After the fix:
- script_language reflects the ScriptProject@Language attribute
- source_code contains the concatenated user-authored .vb / .cs files
- read-only / read-write variables are read from ScriptProject attrs
"""
from __future__ import annotations

from pathlib import Path

from ssis_adf_agent.parsers.models import ScriptTask
from ssis_adf_agent.parsers.ssis_parser import SSISParser


def _build_inline_vb_dtsx() -> str:
    """A minimal SSIS 2017-style package with a Script Task whose source is
    inlined as <ProjectItem> CDATA inside <ScriptProject>."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
                DTS:refId="Package"
                DTS:CreationName="SSIS.Package.3"
                DTS:DTSID="{11111111-1111-1111-1111-111111111111}"
                DTS:ExecutableType="SSIS.Package.3"
                DTS:ObjectName="InlineVbPkg">
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\My VB Script"
                    DTS:CreationName="Microsoft.ScriptTask"
                    DTS:DTSID="{22222222-2222-2222-2222-222222222222}"
                    DTS:ExecutableType="Microsoft.ScriptTask"
                    DTS:ObjectName="My VB Script">
      <DTS:Variables />
      <DTS:ObjectData>
        <ScriptProject Name="ST_demo"
                       VSTAMajorVersion="15"
                       VSTAMinorVersion="0"
                       Language="VisualBasic"
                       ReadOnlyVariables="User::InVar1,User::InVar2"
                       ReadWriteVariables="User::OutVar">
          <ProjectItem Name="ScriptMain.vb" Encoding="UTF8"><![CDATA[Imports System

Public Class ScriptMain
    Public Sub Main()
        Dts.Variables("OutVar").Value = Dts.Variables("InVar1").Value.ToString() & "_" & Dts.Variables("InVar2").Value.ToString()
        Dts.TaskResult = ScriptResults.Success
    End Sub
End Class]]></ProjectItem>
          <ProjectItem Name="project.xml" Encoding="UTF8"><![CDATA[<?xml version="1.0"?><Project/>]]></ProjectItem>
        </ScriptProject>
      </DTS:ObjectData>
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>
"""


def _build_inline_cs_dtsx() -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
                DTS:refId="Package"
                DTS:CreationName="SSIS.Package.3"
                DTS:DTSID="{33333333-3333-3333-3333-333333333333}"
                DTS:ExecutableType="SSIS.Package.3"
                DTS:ObjectName="InlineCsPkg">
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\\My CS Script"
                    DTS:CreationName="Microsoft.ScriptTask"
                    DTS:DTSID="{44444444-4444-4444-4444-444444444444}"
                    DTS:ExecutableType="Microsoft.ScriptTask"
                    DTS:ObjectName="My CS Script">
      <DTS:Variables />
      <DTS:ObjectData>
        <ScriptProject Name="ST_demo_cs"
                       VSTAMajorVersion="15"
                       VSTAMinorVersion="0"
                       Language="CSharp"
                       ReadOnlyVariables="User::Foo"
                       ReadWriteVariables="User::Bar">
          <ProjectItem Name="ScriptMain.cs" Encoding="UTF8"><![CDATA[using System;
public class ScriptMain {
    public void Main() {
        Dts.Variables["Bar"].Value = "hello " + Dts.Variables["Foo"].Value;
    }
}]]></ProjectItem>
        </ScriptProject>
      </DTS:ObjectData>
    </DTS:Executable>
  </DTS:Executables>
</DTS:Executable>
"""


def _find_script_task(pkg) -> ScriptTask:
    return next(t for t in pkg.tasks if isinstance(t, ScriptTask))


class TestInlineProjectItemExtraction:
    def test_vb_language_detected(self) -> None:
        pkg = SSISParser().parse_xml(_build_inline_vb_dtsx())
        task = _find_script_task(pkg)
        assert task.script_language == "VisualBasic"

    def test_vb_source_extracted(self) -> None:
        pkg = SSISParser().parse_xml(_build_inline_vb_dtsx())
        task = _find_script_task(pkg)
        assert task.source_code is not None
        assert "Public Class ScriptMain" in task.source_code
        assert "Dts.Variables(\"OutVar\")" in task.source_code

    def test_vb_xml_project_item_skipped(self) -> None:
        pkg = SSISParser().parse_xml(_build_inline_vb_dtsx())
        task = _find_script_task(pkg)
        assert task.source_code is not None
        # The <Project/> XML stub must not bleed into the captured source.
        assert "<Project" not in task.source_code

    def test_vb_variables_parsed(self) -> None:
        pkg = SSISParser().parse_xml(_build_inline_vb_dtsx())
        task = _find_script_task(pkg)
        assert task.read_only_variables == ["User::InVar1", "User::InVar2"]
        assert task.read_write_variables == ["User::OutVar"]

    def test_cs_language_and_source(self) -> None:
        pkg = SSISParser().parse_xml(_build_inline_cs_dtsx())
        task = _find_script_task(pkg)
        assert task.script_language == "CSharp"
        assert task.source_code is not None
        assert "public class ScriptMain" in task.source_code


class TestRealLniPackage:
    """Smoke test against the real LNI ADDS-MIPS-TC.dtsx if present."""

    LNI_PATH = Path(r"c:\source\test-lni-packages\ADDS-MIPS-TC.dtsx")

    def test_lni_script_tasks_have_vb_source(self) -> None:
        if not self.LNI_PATH.exists():
            import pytest
            pytest.skip(f"LNI sample not available at {self.LNI_PATH}")
        pkg = SSISParser().parse(self.LNI_PATH)
        scripts = [t for t in pkg.tasks if isinstance(t, ScriptTask)]
        assert scripts, "expected at least one Script Task in ADDS-MIPS-TC.dtsx"
        for t in scripts:
            assert t.script_language == "VisualBasic", (
                f"{t.name}: expected VisualBasic, got {t.script_language}"
            )
            assert t.source_code, (
                f"{t.name}: expected inline ProjectItem source to be extracted"
            )
            assert "ScriptMain" in t.source_code, (
                f"{t.name}: expected ScriptMain.vb content in extracted source"
            )
