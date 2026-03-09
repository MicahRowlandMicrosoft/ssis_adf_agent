"""
Complexity scorer — assigns a numeric complexity score and effort estimate to an SSIS package.

Scoring model
-------------
- Each component type contributes a weighted score.
- Script Tasks are the single largest driver of migration effort.
- The final score is clamped to 0-100 and mapped to Low / Medium / High / Very High.

Score bands
-----------
0-20  → Low         (simple automation, mostly Execute SQL / File System)
21-50 → Medium      (data flows, loops, parameters)
51-75 → High        (Script Tasks, deep nesting, event handlers)
76+   → Very High   (many Script Tasks, complex data flows, unsupported components)
"""
from __future__ import annotations

from ..parsers.models import (  # type: ignore[attr-defined]
    ComplexityScore,
    DataFlowTask,
    ForEachLoopContainer,
    ForLoopContainer,
    ScriptTask,
    SequenceContainer,
    SSISPackage,
    SSISTask,
    TaskType,
)

# Weight per task type (contribution to raw score)
_WEIGHTS: dict[TaskType, int] = {
    TaskType.EXECUTE_SQL: 2,
    TaskType.DATA_FLOW: 8,
    TaskType.SCRIPT: 20,          # highest — manual work required
    TaskType.FILE_SYSTEM: 2,
    TaskType.FTP: 3,
    TaskType.SEND_MAIL: 2,
    TaskType.EXECUTE_PACKAGE: 3,
    TaskType.EXECUTE_PROCESS: 4,
    TaskType.SEQUENCE: 1,
    TaskType.FOREACH_LOOP: 5,
    TaskType.FOR_LOOP: 5,
    TaskType.UNKNOWN: 10,         # unknown = scary
}

# Extra score per Data Flow *component* (above the base DataFlowTask weight)
_DF_COMPONENT_WEIGHT = 1.5

# Score added per event handler
_EVENT_HANDLER_WEIGHT = 4

# Score added per nesting level (beyond 1)
_NEST_DEPTH_WEIGHT = 3


def _walk_tasks(tasks: list[SSISTask], depth: int = 0):  # type: ignore[type-arg]
    """Generator: yields (task, depth) pairs recursively."""
    for task in tasks:
        yield task, depth
        if isinstance(task, (SequenceContainer, ForEachLoopContainer, ForLoopContainer)):
            yield from _walk_tasks(task.tasks, depth + 1)


def score_package(package: SSISPackage) -> ComplexityScore:
    """Compute complexity score for a single SSIS package."""
    total_tasks = 0
    script_count = 0
    df_count = 0
    df_component_count = 0
    loop_count = 0
    eh_count = len(package.event_handlers)
    unknown_count = 0
    max_depth = 0
    raw_score = 0.0

    for task, depth in _walk_tasks(package.tasks):
        total_tasks += 1
        max_depth = max(max_depth, depth)
        weight = _WEIGHTS.get(task.task_type, _WEIGHTS[TaskType.UNKNOWN])
        raw_score += weight

        if task.task_type == TaskType.SCRIPT:
            script_count += 1
        elif task.task_type == TaskType.DATA_FLOW:
            df_count += 1
            if isinstance(task, DataFlowTask):
                df_component_count += len(task.components)
                raw_score += len(task.components) * _DF_COMPONENT_WEIGHT
        elif task.task_type in (TaskType.FOREACH_LOOP, TaskType.FOR_LOOP):
            loop_count += 1
        elif task.task_type == TaskType.UNKNOWN:
            unknown_count += 1

    # Add event handler contribution
    raw_score += eh_count * _EVENT_HANDLER_WEIGHT

    # Add nesting depth contribution
    if max_depth > 1:
        raw_score += (max_depth - 1) * _NEST_DEPTH_WEIGHT

    # Normalise to 0-100 using a soft-cap curve (log-ish)
    import math
    # Base: 200 raw points → score of 100
    score = min(100, int(math.log1p(raw_score) / math.log1p(200) * 100))

    if score <= 20:
        effort = "Low"
    elif score <= 50:
        effort = "Medium"
    elif score <= 75:
        effort = "High"
    else:
        effort = "Very High"

    return ComplexityScore(
        package_name=package.name,
        total_tasks=total_tasks,
        script_task_count=script_count,
        data_flow_task_count=df_count,
        data_flow_component_count=df_component_count,
        loop_container_count=loop_count,
        event_handler_count=eh_count,
        nest_depth=max_depth,
        unknown_task_count=unknown_count,
        score=score,
        effort_estimate=effort,
    )
