"""Shared recursive traversal for the parsed SSIS execution tree."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Literal

from .models import (
    EventHandler,
    ForEachLoopContainer,
    ForLoopContainer,
    PrecedenceConstraint,
    PrecedenceEvalOp,
    PrecedenceValue,
    SequenceContainer,
    SSISPackage,
    SSISTask,
)

TaskOwnerKind = Literal["package", "container", "event_handler"]

_CONTAINER_TYPES = (SequenceContainer, ForEachLoopContainer, ForLoopContainer)


@dataclass(frozen=True, slots=True)
class TaskLocation:
    """A task plus the execution scope that directly owns it."""

    task: SSISTask
    owner_kind: TaskOwnerKind
    owner_id: str
    owner_name: str
    scope_path: tuple[str, ...]
    depth: int
    constraints: tuple[PrecedenceConstraint, ...]
    inbound_constraints: tuple[PrecedenceConstraint, ...]
    outbound_constraints: tuple[PrecedenceConstraint, ...]
    event_name: str | None = None
    handler_parent_task_id: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizedTaskScope:
    """An executable scope after safe disabled-task omission."""

    tasks: tuple[SSISTask, ...]
    constraints: tuple[PrecedenceConstraint, ...]
    entry_task_ids: tuple[str, ...]
    terminal_task_ids: tuple[str, ...]
    omitted_disabled_task_ids: tuple[str, ...]


class DisabledTaskBypassError(ValueError):
    """Raised when omitting a disabled task would weaken control flow."""

    def __init__(self, task: SSISTask, reason: str) -> None:
        self.task_id = task.id
        self.task_name = task.name
        self.reason = reason
        super().__init__(
            f"Disabled task '{task.name}' ({task.id}) cannot be bypassed safely: {reason}"
        )


def iter_task_locations(
    package: SSISPackage,
    *,
    include_event_handlers: bool = True,
) -> Iterator[TaskLocation]:
    """Yield package and handler tasks in deterministic preorder."""

    package_scope = (f"package:{package.id}",)
    package_locations = tuple(
        _iter_scope(
            package.tasks,
            constraints=package.constraints,
            owner_kind="package",
            owner_id=package.id,
            owner_name=package.name,
            scope_path=package_scope,
            depth=0,
        )
    )
    yield from package_locations

    if not include_event_handlers:
        return

    locations_by_id = {location.task.id: location for location in package_locations}
    for index, handler in enumerate(package.event_handlers):
        yield from _iter_handler(
            package,
            handler,
            index=index,
            package_scope=package_scope,
            locations_by_id=locations_by_id,
        )


def iter_tasks(
    package: SSISPackage,
    *,
    include_event_handlers: bool = True,
) -> Iterator[SSISTask]:
    """Yield only tasks while retaining the shared traversal semantics."""

    for location in iter_task_locations(
        package,
        include_event_handlers=include_event_handlers,
    ):
        yield location.task


def disabled_task_bypass_issue(
    task: SSISTask,
    constraints: Sequence[PrecedenceConstraint],
) -> str | None:
    """Return why a disabled task cannot be represented as a pass-through."""

    incident = [
        constraint
        for constraint in constraints
        if task.id in (constraint.from_task_id, constraint.to_task_id)
    ]
    for constraint in incident:
        if constraint.eval_op != PrecedenceEvalOp.CONSTRAINT:
            return (
                f"constraint '{constraint.id}' uses {constraint.eval_op.value} semantics"
            )
        if constraint.value != PrecedenceValue.SUCCESS:
            return (
                f"constraint '{constraint.id}' requires "
                f"{constraint.value.name.lower()} rather than success"
            )
        if constraint.expression:
            return f"constraint '{constraint.id}' contains an expression"
        if not constraint.logical_and:
            return f"constraint '{constraint.id}' participates in OR evaluation"
    return None


def normalize_task_scope(
    tasks: Sequence[SSISTask],
    constraints: Sequence[PrecedenceConstraint],
) -> NormalizedTaskScope:
    """Omit safely disabled tasks and compute the remaining scope boundary."""

    task_ids = [task.id for task in tasks]
    if len(set(task_ids)) != len(task_ids):
        raise ValueError("Task IDs must be unique within an execution scope")

    remaining_constraints = list(constraints)
    omitted: list[str] = []
    for task in tasks:
        if not task.disabled:
            continue
        issue = disabled_task_bypass_issue(task, remaining_constraints)
        if issue:
            raise DisabledTaskBypassError(task, issue)

        incoming = [
            constraint
            for constraint in remaining_constraints
            if constraint.to_task_id == task.id
            and constraint.from_task_id != task.id
        ]
        outgoing = [
            constraint
            for constraint in remaining_constraints
            if constraint.from_task_id == task.id
            and constraint.to_task_id != task.id
        ]
        remaining_constraints = [
            constraint
            for constraint in remaining_constraints
            if task.id not in (constraint.from_task_id, constraint.to_task_id)
        ]

        for predecessor in incoming:
            for successor in outgoing:
                if predecessor.from_task_id == successor.to_task_id:
                    raise DisabledTaskBypassError(
                        task,
                        "bypass would introduce a self-dependency",
                    )
                remaining_constraints.append(PrecedenceConstraint(
                    id=(
                        f"disabled-bypass:{task.id}:"
                        f"{predecessor.id}:{successor.id}"
                    ),
                    from_task_id=predecessor.from_task_id,
                    to_task_id=successor.to_task_id,
                    eval_op=PrecedenceEvalOp.CONSTRAINT,
                    value=PrecedenceValue.SUCCESS,
                    logical_and=True,
                ))
        remaining_constraints = _deduplicate_constraints(remaining_constraints)
        omitted.append(task.id)

    enabled_tasks = tuple(task for task in tasks if not task.disabled)
    enabled_ids = {task.id for task in enabled_tasks}
    normalized_constraints = tuple(
        constraint
        for constraint in remaining_constraints
        if constraint.from_task_id in enabled_ids
        and constraint.to_task_id in enabled_ids
    )
    inbound_ids = {constraint.to_task_id for constraint in normalized_constraints}
    outbound_ids = {constraint.from_task_id for constraint in normalized_constraints}

    return NormalizedTaskScope(
        tasks=enabled_tasks,
        constraints=normalized_constraints,
        entry_task_ids=tuple(
            task.id for task in enabled_tasks if task.id not in inbound_ids
        ),
        terminal_task_ids=tuple(
            task.id for task in enabled_tasks if task.id not in outbound_ids
        ),
        omitted_disabled_task_ids=tuple(omitted),
    )


def _deduplicate_constraints(
    constraints: Sequence[PrecedenceConstraint],
) -> list[PrecedenceConstraint]:
    result: list[PrecedenceConstraint] = []
    seen: set[tuple[object, ...]] = set()
    for constraint in constraints:
        key = (
            constraint.from_task_id,
            constraint.to_task_id,
            constraint.eval_op,
            constraint.value,
            constraint.expression,
            constraint.logical_and,
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(constraint)
    return result


def _iter_handler(
    package: SSISPackage,
    handler: EventHandler,
    *,
    index: int,
    package_scope: tuple[str, ...],
    locations_by_id: dict[str, TaskLocation],
) -> Iterator[TaskLocation]:
    parent_location = locations_by_id.get(handler.parent_task_id or "")
    if parent_location is not None:
        base_scope = parent_location.scope_path + (f"task:{parent_location.task.id}",)
        depth = parent_location.depth + 1
    elif handler.parent_task_id:
        base_scope = package_scope + (f"unresolved-task:{handler.parent_task_id}",)
        depth = 0
    else:
        base_scope = package_scope
        depth = 0

    parent_id = handler.parent_task_id or package.id
    handler_id = f"handler:{parent_id}:{handler.event_name}:{index}"
    handler_scope = base_scope + (handler_id,)
    yield from _iter_scope(
        handler.tasks,
        constraints=handler.constraints,
        owner_kind="event_handler",
        owner_id=handler_id,
        owner_name=handler.event_name,
        scope_path=handler_scope,
        depth=depth,
        event_name=handler.event_name,
        handler_parent_task_id=handler.parent_task_id,
    )


def _iter_scope(
    tasks: Sequence[SSISTask],
    *,
    constraints: Sequence[PrecedenceConstraint],
    owner_kind: TaskOwnerKind,
    owner_id: str,
    owner_name: str,
    scope_path: tuple[str, ...],
    depth: int,
    event_name: str | None = None,
    handler_parent_task_id: str | None = None,
) -> Iterator[TaskLocation]:
    local_constraints = tuple(constraints)
    inbound_by_id: dict[str, list[PrecedenceConstraint]] = {}
    outbound_by_id: dict[str, list[PrecedenceConstraint]] = {}
    for constraint in local_constraints:
        inbound_by_id.setdefault(constraint.to_task_id, []).append(constraint)
        outbound_by_id.setdefault(constraint.from_task_id, []).append(constraint)
    for task in tasks:
        yield TaskLocation(
            task=task,
            owner_kind=owner_kind,
            owner_id=owner_id,
            owner_name=owner_name,
            scope_path=scope_path,
            depth=depth,
            constraints=local_constraints,
            inbound_constraints=tuple(inbound_by_id.get(task.id, ())),
            outbound_constraints=tuple(outbound_by_id.get(task.id, ())),
            event_name=event_name,
            handler_parent_task_id=handler_parent_task_id,
        )
        if isinstance(task, _CONTAINER_TYPES):
            yield from _iter_scope(
                task.tasks,
                constraints=task.constraints,
                owner_kind="container",
                owner_id=task.id,
                owner_name=task.name,
                scope_path=scope_path + (f"container:{task.id}",),
                depth=depth + 1,
                event_name=event_name,
                handler_parent_task_id=handler_parent_task_id,
            )


__all__ = [
    "DisabledTaskBypassError",
    "NormalizedTaskScope",
    "TaskLocation",
    "TaskOwnerKind",
    "disabled_task_bypass_issue",
    "iter_task_locations",
    "iter_tasks",
    "normalize_task_scope",
]