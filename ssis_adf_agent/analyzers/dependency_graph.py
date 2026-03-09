"""
Dependency graph builder.

Converts SSIS PrecedenceConstraints into a directed acyclic graph (DAG) and
produces a topologically sorted task execution order.  The result maps
directly to ADF activity ``dependsOn`` declarations.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

from ..parsers.models import (  # type: ignore[attr-defined]
    ForEachLoopContainer,
    ForLoopContainer,
    PrecedenceConstraint,
    PrecedenceValue,
    SequenceContainer,
    SSISPackage,
    SSISTask,
)


@dataclass
class TaskNode:
    task_id: str
    task_name: str
    depends_on: list["DependsOnEntry"] = field(default_factory=list)


@dataclass
class DependsOnEntry:
    """Mirrors ADF pipeline activity dependsOn entry."""
    activity: str  # target task name
    dependency_conditions: list[str]  # "Succeeded", "Failed", "Completed", "Skipped"


def _precedence_to_adf_condition(value: PrecedenceValue) -> str:
    return {
        PrecedenceValue.SUCCESS: "Succeeded",
        PrecedenceValue.FAILURE: "Failed",
        PrecedenceValue.COMPLETION: "Completed",
    }.get(value, "Succeeded")


def build_dependency_graph(
    tasks: list[SSISTask],
    constraints: list[PrecedenceConstraint],
) -> dict[str, TaskNode]:
    """
    Build a {task_id: TaskNode} dependency graph from a flat list of tasks
    and their precedence constraints.

    Returns a dict mapping task ID → TaskNode with populated ``depends_on`` lists.
    """
    # Index tasks
    task_by_id: dict[str, SSISTask] = {t.id: t for t in tasks}

    nodes: dict[str, TaskNode] = {
        t.id: TaskNode(task_id=t.id, task_name=t.name)
        for t in tasks
    }

    # Group constraints by target (to_task_id) to handle multi-input AND/OR logic
    constraints_by_target: dict[str, list[PrecedenceConstraint]] = defaultdict(list)
    for c in constraints:
        constraints_by_target[c.to_task_id].append(c)

    for to_id, pcs in constraints_by_target.items():
        if to_id not in nodes:
            continue
        node = nodes[to_id]
        for pc in pcs:
            if pc.from_task_id not in nodes:
                continue
            from_name = task_by_id[pc.from_task_id].name if pc.from_task_id in task_by_id else pc.from_task_id
            condition = _precedence_to_adf_condition(pc.value)
            node.depends_on.append(DependsOnEntry(
                activity=from_name,
                dependency_conditions=[condition],
            ))

    return nodes


def topological_sort(
    tasks: list[SSISTask],
    constraints: list[PrecedenceConstraint],
) -> list[str]:
    """
    Return task IDs in topological execution order (Kahn's algorithm).

    Handles cycles by appending remaining nodes at the end with a warning.
    """
    nodes = build_dependency_graph(tasks, constraints)
    task_ids = [t.id for t in tasks]

    # Build adjacency: from_id → set of to_ids
    adjacency: dict[str, set[str]] = defaultdict(set)
    in_degree: dict[str, int] = {tid: 0 for tid in task_ids}

    for c in constraints:
        if c.from_task_id in in_degree and c.to_task_id in in_degree:
            adjacency[c.from_task_id].add(c.to_task_id)
            in_degree[c.to_task_id] += 1

    queue: deque[str] = deque(tid for tid in task_ids if in_degree[tid] == 0)
    order: list[str] = []

    while queue:
        current = queue.popleft()
        order.append(current)
        for neighbour in adjacency[current]:
            in_degree[neighbour] -= 1
            if in_degree[neighbour] == 0:
                queue.append(neighbour)

    # Detect cycles — append remaining nodes (broken cycle fallback)
    remaining = [tid for tid in task_ids if tid not in set(order)]
    if remaining:
        import warnings
        warnings.warn(
            f"Cycle detected in precedence constraints for tasks: "
            f"{[t.name for t in tasks if t.id in remaining]}. "
            "Appending in original order.",
            stacklevel=2,
        )
        order.extend(remaining)

    return order


def get_depends_on_for_task(
    task_id: str,
    task_by_id: dict[str, SSISTask],
    constraints: list[PrecedenceConstraint],
) -> list[DependsOnEntry]:
    """
    Get the ADF dependsOn list for a single task given the full constraint list.
    """
    result: list[DependsOnEntry] = []
    for c in constraints:
        if c.to_task_id != task_id:
            continue
        if c.from_task_id not in task_by_id:
            continue
        from_name = task_by_id[c.from_task_id].name
        condition = _precedence_to_adf_condition(c.value)
        result.append(DependsOnEntry(activity=from_name, dependency_conditions=[condition]))
    return result


def build_package_dependency_order(package: SSISPackage) -> list[str]:
    """Convenience wrapper: return topological task ID order for an entire package."""
    return topological_sort(package.tasks, package.constraints)
