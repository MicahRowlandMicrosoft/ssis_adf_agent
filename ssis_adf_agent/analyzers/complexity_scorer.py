"""
Complexity scorer — assigns a numeric complexity score and effort estimate to an SSIS package.

Scoring model
-------------
- Each component type contributes a weighted score.
- Script Tasks are scored based on content analysis (trivial / simple / moderate / complex)
  rather than a flat weight, so that simple variable-assignment scripts don't inflate
  the effort estimate.
- The final score is clamped to 0-100 and mapped to Low / Medium / High / Very High.

Score bands
-----------
0-30  → Low         (simple automation, mostly Execute SQL / File System)
31-55 → Medium      (data flows, loops, parameters)
56-80 → High        (Script Tasks, deep nesting, event handlers)
81+   → Very High   (many Script Tasks, complex data flows, unsupported components)
"""

from __future__ import annotations

from ..parsers.models import (  # type: ignore[attr-defined]
    ComplexityScore,
    CrossDbReferenceType,
    DataFlowTask,
    ForEachLoopContainer,
    ForLoopContainer,
    ScriptTask,
    SequenceContainer,
    SSISPackage,
    SSISTask,
    TaskType,
)
from .script_classifier import classify_script, ScriptClassificationResult

# Weight per task type (contribution to raw score)
# NOTE: Script Tasks are NOT in this table — they use content-aware classification
# via the script_classifier module.
_WEIGHTS: dict[TaskType, int] = {
    TaskType.EXECUTE_SQL: 2,
    TaskType.DATA_FLOW: 5,
    TaskType.FILE_SYSTEM: 2,
    TaskType.FTP: 3,
    TaskType.SEND_MAIL: 4,
    TaskType.EXECUTE_PACKAGE: 3,
    TaskType.EXECUTE_PROCESS: 4,
    TaskType.SEQUENCE: 1,
    TaskType.FOREACH_LOOP: 5,
    TaskType.FOR_LOOP: 5,
    TaskType.UNKNOWN: 10,  # unknown = scary
}

# Extra score per Data Flow *component* (above the base DataFlowTask weight)
_DF_COMPONENT_WEIGHT = 1.5

# Score added per event handler
_EVENT_HANDLER_WEIGHT = 4

# Score added per nesting level (beyond 1)
_NEST_DEPTH_WEIGHT = 3

# Score per cross-database / linked server reference
_LINKED_SERVER_WEIGHT = 8
_CROSS_DB_WEIGHT = 3


def _walk_tasks(tasks: list[SSISTask], depth: int = 0):  # type: ignore[type-arg]
    """Generator: yields (task, depth) pairs recursively."""
    for task in tasks:
        yield task, depth
        if isinstance(task, (SequenceContainer, ForEachLoopContainer, ForLoopContainer)):
            yield from _walk_tasks(task.tasks, depth + 1)


def score_package(package: SSISPackage) -> ComplexityScore:
    """Compute complexity score for a single SSIS package."""
    return _score_package_impl(package).complexity


def score_package_detailed(
    package: SSISPackage,
) -> tuple[ComplexityScore, list[ScriptClassificationResult]]:
    """Compute complexity score and return per-Script-Task classifications."""
    result = _score_package_impl(package)
    return result.complexity, result.script_classifications


class _ScoringResult:
    __slots__ = ("complexity", "script_classifications")

    def __init__(
        self,
        complexity: ComplexityScore,
        script_classifications: list[ScriptClassificationResult],
    ) -> None:
        self.complexity = complexity
        self.script_classifications = script_classifications


def _score_package_impl(package: SSISPackage) -> _ScoringResult:
    """Internal implementation that computes score and collects script classifications."""
    total_tasks = 0
    script_count = 0
    df_count = 0
    df_component_count = 0
    loop_count = 0
    eh_count = len(package.event_handlers)
    unknown_count = 0
    cross_db_count = 0
    linked_server_count = 0
    max_depth = 0
    raw_score = 0.0
    script_classifications: list[ScriptClassificationResult] = []

    for task, depth in _walk_tasks(package.tasks):
        total_tasks += 1
        max_depth = max(max_depth, depth)

        if task.task_type == TaskType.SCRIPT:
            # Content-aware scoring for Script Tasks
            assert isinstance(task, ScriptTask)
            classification = classify_script(task)
            script_classifications.append(classification)
            raw_score += classification.weight
            script_count += 1
        elif task.task_type == TaskType.DATA_FLOW:
            raw_score += _WEIGHTS.get(task.task_type, _WEIGHTS[TaskType.UNKNOWN])
            df_count += 1
            if isinstance(task, DataFlowTask):
                df_component_count += len(task.components)
                raw_score += len(task.components) * _DF_COMPONENT_WEIGHT
        elif task.task_type in (TaskType.FOREACH_LOOP, TaskType.FOR_LOOP):
            raw_score += _WEIGHTS.get(task.task_type, _WEIGHTS[TaskType.UNKNOWN])
            loop_count += 1
        elif task.task_type == TaskType.UNKNOWN:
            raw_score += _WEIGHTS[TaskType.UNKNOWN]
            unknown_count += 1
        else:
            raw_score += _WEIGHTS.get(task.task_type, _WEIGHTS[TaskType.UNKNOWN])

        # Cross-database / linked server references
        for ref in task.cross_db_references:
            if ref.ref_type in (
                CrossDbReferenceType.FOUR_PART,
                CrossDbReferenceType.OPENQUERY,
                CrossDbReferenceType.OPENROWSET,
            ):
                linked_server_count += 1
                raw_score += _LINKED_SERVER_WEIGHT
            elif ref.ref_type == CrossDbReferenceType.THREE_PART:
                cross_db_count += 1
                raw_score += _CROSS_DB_WEIGHT

    # Add event handler contribution
    raw_score += eh_count * _EVENT_HANDLER_WEIGHT

    # Add nesting depth contribution
    if max_depth > 1:
        raw_score += (max_depth - 1) * _NEST_DEPTH_WEIGHT

    # Normalise to 0-100 using a soft-cap curve (log-ish)
    import math

    # Base: 200 raw points → score of 100
    score = min(100, int(math.log1p(raw_score) / math.log1p(200) * 100))

    if score <= 30:
        effort = "Low"
    elif score <= 55:
        effort = "Medium"
    elif score <= 80:
        effort = "High"
    else:
        effort = "Very High"

    return _ScoringResult(
        complexity=ComplexityScore(
            package_name=package.name,
            total_tasks=total_tasks,
            script_task_count=script_count,
            data_flow_task_count=df_count,
            data_flow_component_count=df_component_count,
            loop_container_count=loop_count,
            event_handler_count=eh_count,
            nest_depth=max_depth,
            unknown_task_count=unknown_count,
            cross_db_ref_count=cross_db_count,
            linked_server_ref_count=linked_server_count,
            score=score,
            effort_estimate=effort,
        ),
        script_classifications=script_classifications,
    )
