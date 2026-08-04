from __future__ import annotations

import pytest

from ssis_adf_agent.analyzers.gap_analyzer import analyze_gaps
from ssis_adf_agent.converters.dispatcher import ConverterDispatcher
from ssis_adf_agent.generators.pipeline_generator import generate_pipeline
from ssis_adf_agent.parsers.models import (
    FileSystemTask,
    ForEachEnumeratorType,
    ForEachLoopContainer,
    PrecedenceConstraint,
    PrecedenceEvalOp,
    SequenceContainer,
    Severity,
    SSISPackage,
)
from ssis_adf_agent.parsers.ssis_parser import SSISParser
from ssis_adf_agent.parsers.task_traversal import DisabledTaskBypassError
from ssis_adf_agent.warnings_collector import WarningsCollector


_DISABLED_CHAIN_DTSX = r"""<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
    DTS:ExecutableType="Microsoft.Package"
    DTS:DTSID="{11111111-1111-1111-1111-111111111111}"
    DTS:ObjectName="DisabledChain">
  <DTS:Executables>
    <DTS:Executable DTS:refId="Package\A"
        DTS:ExecutableType="Microsoft.FileSystemTask"
        DTS:DTSID="{AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA}"
        DTS:ObjectName="A" />
    <DTS:Executable DTS:refId="Package\B"
        DTS:ExecutableType="Microsoft.FileSystemTask"
        DTS:DTSID="{BBBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB}"
        DTS:ObjectName="B"
        DTS:Disabled="1" />
    <DTS:Executable DTS:refId="Package\C"
        DTS:ExecutableType="Microsoft.FileSystemTask"
        DTS:DTSID="{CCCCCCCC-CCCC-CCCC-CCCC-CCCCCCCCCCCC}"
        DTS:ObjectName="C" />
  </DTS:Executables>
  <DTS:PrecedenceConstraints>
    <DTS:PrecedenceConstraint DTS:DTSID="{DDDDDDDD-DDDD-DDDD-DDDD-DDDDDDDDDDDD}"
        DTS:From="Package\A" DTS:To="Package\B"
        DTS:Value="0" DTS:EvalOp="1" />
    <DTS:PrecedenceConstraint DTS:DTSID="{EEEEEEEE-EEEE-EEEE-EEEE-EEEEEEEEEEEE}"
        DTS:From="Package\B" DTS:To="Package\C"
        DTS:Value="0" DTS:EvalOp="1" />
  </DTS:PrecedenceConstraints>
</DTS:Executable>
"""


def _task(task_id: str, *, disabled: bool = False) -> FileSystemTask:
    return FileSystemTask(
        id=task_id,
        name=task_id.upper(),
        disabled=disabled,
        operation="DeleteFile",
        source_path=f"/{task_id}.txt",
    )


def _edge(
    edge_id: str,
    source: str,
    target: str,
    *,
    eval_op: PrecedenceEvalOp = PrecedenceEvalOp.CONSTRAINT,
) -> PrecedenceConstraint:
    return PrecedenceConstraint(
        id=edge_id,
        from_task_id=source,
        to_task_id=target,
        eval_op=eval_op,
    )


def _chain_scope():
    return (
        [_task("a"), _task("b", disabled=True), _task("c")],
        [_edge("a-b", "a", "b"), _edge("b-c", "b", "c")],
    )


def test_pipeline_omits_disabled_task_and_rewrites_dependency(tmp_path):
    tasks, constraints = _chain_scope()
    package = SSISPackage(
        id="package",
        name="DisabledChain",
        source_file="DisabledChain.dtsx",
        tasks=tasks,
        constraints=constraints,
    )

    pipeline = generate_pipeline(package, tmp_path)
    activities = pipeline["properties"]["activities"]

    assert [activity["name"] for activity in activities] == ["A", "C"]
    assert activities[0]["dependsOn"] == []
    assert activities[1]["dependsOn"] == [
        {"activity": "A", "dependencyConditions": ["Succeeded"]}
    ]


def test_parsed_dtsx_disabled_task_is_not_generated(tmp_path):
    package = SSISParser().parse_xml(
        _DISABLED_CHAIN_DTSX,
        source_identifier="DisabledChain.dtsx",
    )

    pipeline = generate_pipeline(package, tmp_path)
    activities = pipeline["properties"]["activities"]

    assert [activity["name"] for activity in activities] == ["A", "C"]
    assert activities[1]["dependsOn"] == [
        {"activity": "A", "dependencyConditions": ["Succeeded"]}
    ]


def test_sequence_uses_the_same_disabled_task_normalization():
    tasks, constraints = _chain_scope()
    sequence = SequenceContainer(
        id="sequence",
        name="Sequence",
        tasks=tasks,
        constraints=constraints,
    )

    activities = ConverterDispatcher().convert_scope([sequence], [])

    assert [activity["name"] for activity in activities] == ["A", "C"]
    assert activities[1]["dependsOn"][0]["activity"] == "A"


def test_foreach_uses_the_same_disabled_task_normalization():
    tasks, constraints = _chain_scope()
    loop = ForEachLoopContainer(
        id="loop",
        name="Loop",
        enumerator_type=ForEachEnumeratorType.ITEM,
        enumerator_config={"Items": "[]"},
        tasks=tasks,
        constraints=constraints,
    )

    activities = ConverterDispatcher().convert_scope([loop], [])
    inner = activities[0]["typeProperties"]["activities"]

    assert [activity["name"] for activity in inner] == ["A", "C"]
    assert inner[1]["dependsOn"][0]["activity"] == "A"


def test_direct_disabled_dispatch_emits_no_activity_and_records_disposition():
    task = _task("disabled", disabled=True)

    with WarningsCollector() as warnings:
        activities = ConverterDispatcher().convert_task(task, [], {task.id: task})

    assert activities == []
    assert len(warnings.warnings) == 1
    assert warnings.warnings[0].task_id == "disabled"
    assert "Omitted disabled task" in warnings.warnings[0].message


def test_unsafe_disabled_task_constraint_blocks_conversion():
    disabled = _task("b", disabled=True)

    with pytest.raises(DisabledTaskBypassError, match="Expression semantics"):
        ConverterDispatcher().convert_scope(
            [_task("a"), disabled],
            [_edge(
                "a-b",
                "a",
                "b",
                eval_op=PrecedenceEvalOp.EXPRESSION,
            )],
        )


def test_gap_analysis_reports_safe_disabled_task_omission():
    task = _task("disabled", disabled=True)
    package = SSISPackage(
        id="package",
        name="Package",
        source_file="Package.dtsx",
        tasks=[task],
    )

    gaps = analyze_gaps(package)

    assert len(gaps) == 1
    assert gaps[0].task_id == "disabled"
    assert gaps[0].severity == Severity.INFO
    assert "will be omitted" in gaps[0].message


def test_gap_analysis_reports_unsafe_disabled_task_as_manual_required():
    package = SSISPackage(
        id="package",
        name="Package",
        source_file="Package.dtsx",
        tasks=[_task("a"), _task("b", disabled=True)],
        constraints=[_edge(
            "a-b",
            "a",
            "b",
            eval_op=PrecedenceEvalOp.EXPRESSION,
        )],
    )

    gaps = analyze_gaps(package)

    disabled_gap = next(gap for gap in gaps if gap.task_id == "b")
    assert disabled_gap.severity == Severity.MANUAL_REQUIRED
    assert "cannot be omitted" in disabled_gap.message