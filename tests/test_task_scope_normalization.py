from __future__ import annotations

import pytest

from ssis_adf_agent.parsers.models import (
    PrecedenceConstraint,
    PrecedenceEvalOp,
    PrecedenceValue,
    SSISTask,
)
from ssis_adf_agent.parsers.task_traversal import (
    DisabledTaskBypassError,
    normalize_task_scope,
)


def _task(task_id: str, *, disabled: bool = False) -> SSISTask:
    return SSISTask(id=task_id, name=task_id.upper(), disabled=disabled)


def _edge(
    edge_id: str,
    source: str,
    target: str,
    **changes: object,
) -> PrecedenceConstraint:
    values = {
        "id": edge_id,
        "from_task_id": source,
        "to_task_id": target,
        **changes,
    }
    return PrecedenceConstraint(**values)  # type: ignore[arg-type]


def test_disabled_task_is_bypassed_and_scope_boundary_is_computed():
    graph = normalize_task_scope(
        [_task("a"), _task("b", disabled=True), _task("c")],
        [_edge("a-b", "a", "b"), _edge("b-c", "b", "c")],
    )

    assert [task.id for task in graph.tasks] == ["a", "c"]
    assert graph.omitted_disabled_task_ids == ("b",)
    assert graph.entry_task_ids == ("a",)
    assert graph.terminal_task_ids == ("c",)
    assert [
        (constraint.from_task_id, constraint.to_task_id)
        for constraint in graph.constraints
    ] == [("a", "c")]


def test_disabled_task_fan_in_and_fan_out_preserve_all_success_edges():
    graph = normalize_task_scope(
        [
            _task("a"),
            _task("b"),
            _task("disabled", disabled=True),
            _task("c"),
            _task("d"),
        ],
        [
            _edge("a-x", "a", "disabled"),
            _edge("b-x", "b", "disabled"),
            _edge("x-c", "disabled", "c"),
            _edge("x-d", "disabled", "d"),
        ],
    )

    assert {
        (constraint.from_task_id, constraint.to_task_id)
        for constraint in graph.constraints
    } == {("a", "c"), ("a", "d"), ("b", "c"), ("b", "d")}
    assert graph.entry_task_ids == ("a", "b")
    assert graph.terminal_task_ids == ("c", "d")


def test_chained_disabled_tasks_are_bypassed_transitively():
    graph = normalize_task_scope(
        [
            _task("a"),
            _task("b", disabled=True),
            _task("c", disabled=True),
            _task("d"),
        ],
        [
            _edge("a-b", "a", "b"),
            _edge("b-c", "b", "c"),
            _edge("c-d", "c", "d"),
        ],
    )

    assert [task.id for task in graph.tasks] == ["a", "d"]
    assert graph.omitted_disabled_task_ids == ("b", "c")
    assert [
        (constraint.from_task_id, constraint.to_task_id)
        for constraint in graph.constraints
    ] == [("a", "d")]


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"eval_op": PrecedenceEvalOp.EXPRESSION}, "Expression semantics"),
        ({"value": PrecedenceValue.FAILURE}, "failure rather than success"),
        ({"value": PrecedenceValue.COMPLETION}, "completion rather than success"),
        ({"expression": "@[User::Run] == 1"}, "contains an expression"),
        ({"logical_and": False}, "OR evaluation"),
    ],
)
def test_unsafe_disabled_task_constraints_are_blocking(changes, reason):
    with pytest.raises(DisabledTaskBypassError, match=reason) as exc_info:
        normalize_task_scope(
            [_task("a"), _task("b", disabled=True)],
            [_edge("a-b", "a", "b", **changes)],
        )

    assert exc_info.value.task_id == "b"
    assert exc_info.value.task_name == "B"


def test_isolated_disabled_task_is_omitted_without_constraints():
    graph = normalize_task_scope(
        [_task("disabled", disabled=True), _task("enabled")],
        [],
    )

    assert [task.id for task in graph.tasks] == ["enabled"]
    assert graph.entry_task_ids == ("enabled",)
    assert graph.terminal_task_ids == ("enabled",)