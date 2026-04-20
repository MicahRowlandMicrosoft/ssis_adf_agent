"""Tests for ``migration_plan.applier`` — applying a plan to a parsed package."""
from __future__ import annotations

from ssis_adf_agent.migration_plan import (
    MigrationPlan,
    Simplification,
    SimplificationAction,
    TargetPattern,
    apply_plan,
)
from ssis_adf_agent.parsers.models import (  # type: ignore[attr-defined]
    FileSystemTask,
    PrecedenceConstraint,
    PrecedenceEvalOp,
    PrecedenceValue,
    SSISPackage,
    TaskType,
)


def _chain_package() -> SSISPackage:
    """A → B → C → D where B will be dropped; expect A → C → D after apply."""
    a = FileSystemTask(id="A", name="A", task_type=TaskType.FILE_SYSTEM, operation="CopyFile")
    b = FileSystemTask(id="B", name="B", task_type=TaskType.FILE_SYSTEM, operation="CopyFile")
    c = FileSystemTask(id="C", name="C", task_type=TaskType.FILE_SYSTEM, operation="CopyFile")
    d = FileSystemTask(id="D", name="D", task_type=TaskType.FILE_SYSTEM, operation="CopyFile")
    pcs = [
        PrecedenceConstraint(id="ab", from_task_id="A", to_task_id="B",
                             eval_op=PrecedenceEvalOp.CONSTRAINT, value=PrecedenceValue.SUCCESS),
        PrecedenceConstraint(id="bc", from_task_id="B", to_task_id="C",
                             eval_op=PrecedenceEvalOp.CONSTRAINT, value=PrecedenceValue.SUCCESS),
        PrecedenceConstraint(id="cd", from_task_id="C", to_task_id="D",
                             eval_op=PrecedenceEvalOp.CONSTRAINT, value=PrecedenceValue.SUCCESS),
    ]
    return SSISPackage(id="pkg", name="P", source_file="x.dtsx",
                       tasks=[a, b, c, d], constraints=pcs)


def _drop_b_plan() -> MigrationPlan:
    return MigrationPlan(
        package_name="P", package_path="x.dtsx",
        target_pattern=TargetPattern.CUSTOM,
        simplifications=[Simplification(
            action=SimplificationAction.DROP, items=["B"], reason="test",
        )],
    )


def test_apply_drops_named_tasks_from_top_level() -> None:
    pkg = _chain_package()
    new_pkg, app = apply_plan(pkg, _drop_b_plan())
    names = {t.name for t in new_pkg.tasks}
    assert names == {"A", "C", "D"}
    assert app.dropped_task_count == 1
    assert app.dropped_task_names == ["B"]


def test_apply_does_not_mutate_original_package() -> None:
    pkg = _chain_package()
    original_names = {t.name for t in pkg.tasks}
    apply_plan(pkg, _drop_b_plan())
    assert {t.name for t in pkg.tasks} == original_names


def test_apply_rewires_constraints_around_dropped_task() -> None:
    pkg = _chain_package()
    new_pkg, app = apply_plan(pkg, _drop_b_plan())
    edges = {(c.from_task_id, c.to_task_id) for c in new_pkg.constraints}
    # B → C is gone, A → B is gone; expect a stitched A → C plus the original C → D
    assert ("A", "C") in edges
    assert ("C", "D") in edges
    # The original B-touching constraints must not survive
    assert all("B" not in (c.from_task_id, c.to_task_id) for c in new_pkg.constraints)
    assert app.rewired_constraint_count >= 1


def test_apply_records_non_drop_simplifications_as_deferred() -> None:
    pkg = _chain_package()
    plan = MigrationPlan(
        package_name="P", package_path="x.dtsx",
        target_pattern=TargetPattern.CUSTOM,
        simplifications=[Simplification(
            action=SimplificationAction.FOLD_TO_COPY_ACTIVITY,
            items=["DataFlowTask"], reason="trivial",
        )],
    )
    _, app = apply_plan(pkg, plan)
    assert len(app.deferred_simplifications) == 1
    assert app.deferred_simplifications[0]["action"] == SimplificationAction.FOLD_TO_COPY_ACTIVITY.value
    assert app.dropped_task_count == 0


def test_apply_with_empty_plan_is_noop() -> None:
    pkg = _chain_package()
    new_pkg, app = apply_plan(pkg, MigrationPlan(
        package_name="P", package_path="x.dtsx",
    ))
    assert {t.name for t in new_pkg.tasks} == {"A", "B", "C", "D"}
    assert app.dropped_task_count == 0
    assert app.rewired_constraint_count == 0
