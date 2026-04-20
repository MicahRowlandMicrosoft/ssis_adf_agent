"""Apply a :class:`MigrationPlan` to a parsed :class:`SSISPackage`.

This is the bridge between the design proposer and the code generators. Given a
plan (typically reviewed and possibly edited by the customer/agent), we mutate
a *copy* of the parsed package so downstream generators emit ADF artifacts that
match the agreed target rather than the SSIS-faithful default.

Currently supported actions (v1):

* :attr:`SimplificationAction.DROP` — remove the listed tasks from the package
  (top-level and inside containers / event handlers). Constraints that reference
  the dropped tasks are also rewired or removed.
* All other actions — recorded in the returned :class:`PlanApplication` summary
  but not yet mutating; the agent surfaces them to the user as TODOs.

Linked-service auth recommendations from the plan are exposed as the returned
``linked_service_overrides`` map so the linked-service generator can honor them.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Iterable

from pydantic import BaseModel, Field

from ..parsers.models import (  # type: ignore[attr-defined]
    EventHandler,
    ForEachLoopContainer,
    ForLoopContainer,
    PrecedenceConstraint,
    SequenceContainer,
    SSISPackage,
    SSISTask,
)
from .models import MigrationPlan, SimplificationAction


class PlanApplication(BaseModel):
    """Summary of what ``apply_plan`` did to a package."""

    dropped_task_names: list[str] = Field(default_factory=list)
    dropped_task_count: int = 0
    rewired_constraint_count: int = 0
    deferred_simplifications: list[dict] = Field(default_factory=list)
    linked_service_overrides: dict[str, dict] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _walk_containers(tasks: Iterable[SSISTask]):
    """Yield containers (recursively) so we can mutate their .tasks/.constraints."""
    for t in tasks:
        if isinstance(t, (SequenceContainer, ForEachLoopContainer, ForLoopContainer)):
            yield t
            yield from _walk_containers(t.tasks)


def _collect_drop_targets(plan: MigrationPlan) -> set[str]:
    targets: set[str] = set()
    for s in plan.simplifications:
        if s.action == SimplificationAction.DROP:
            targets.update(s.items)
    return targets


def _drop_from_task_list(tasks: list[SSISTask], drop_names: set[str]) -> tuple[list[SSISTask], list[str]]:
    """Return (kept_tasks, dropped_names) — case-insensitive name match."""
    kept: list[SSISTask] = []
    dropped: list[str] = []
    lc_drop = {n.lower() for n in drop_names}
    for t in tasks:
        if (t.name or "").lower() in lc_drop:
            dropped.append(t.name)
            continue
        kept.append(t)
    return kept, dropped


def _rewire_constraints(
    constraints: list[PrecedenceConstraint],
    dropped_ids: set[str],
    kept_ids: set[str],
) -> tuple[list[PrecedenceConstraint], int]:
    """Remove constraints touching dropped tasks; for chained drops, stitch survivors.

    For a chain A -> B -> C where B is dropped, produces A -> C so execution
    order is preserved. Constraints fully outside the dropped set pass through.
    """
    # Build forward and reverse adjacency among ALL constraints (so we can stitch through chains)
    forward: dict[str, list[str]] = {}
    reverse: dict[str, list[str]] = {}
    for c in constraints:
        forward.setdefault(c.from_task_id, []).append(c.to_task_id)
        reverse.setdefault(c.to_task_id, []).append(c.from_task_id)

    def _live_predecessors(node: str, seen: set[str]) -> set[str]:
        result: set[str] = set()
        for pred in reverse.get(node, []):
            if pred in seen:
                continue
            seen.add(pred)
            if pred in dropped_ids:
                result.update(_live_predecessors(pred, seen))
            elif pred in kept_ids:
                result.add(pred)
        return result

    def _live_successors(node: str, seen: set[str]) -> set[str]:
        result: set[str] = set()
        for succ in forward.get(node, []):
            if succ in seen:
                continue
            seen.add(succ)
            if succ in dropped_ids:
                result.update(_live_successors(succ, seen))
            elif succ in kept_ids:
                result.add(succ)
        return result

    new_constraints: list[PrecedenceConstraint] = []
    rewired = 0
    seen_pairs: set[tuple[str, str]] = set()

    # Pass 1: keep constraints whose endpoints are both alive
    for c in constraints:
        if c.from_task_id in kept_ids and c.to_task_id in kept_ids:
            key = (c.from_task_id, c.to_task_id)
            if key not in seen_pairs:
                new_constraints.append(c)
                seen_pairs.add(key)

    # Pass 2: stitch live predecessors of dropped sinks to live successors of dropped sources
    for c in constraints:
        if c.from_task_id in dropped_ids and c.to_task_id in kept_ids:
            for pred in _live_predecessors(c.from_task_id, set()):
                key = (pred, c.to_task_id)
                if key in seen_pairs:
                    continue
                new_constraints.append(PrecedenceConstraint(
                    id=f"{c.id}__rewired",
                    from_task_id=pred, to_task_id=c.to_task_id,
                    eval_op=c.eval_op, value=c.value,
                    expression=c.expression, logical_and=c.logical_and,
                ))
                seen_pairs.add(key)
                rewired += 1
        elif c.from_task_id in kept_ids and c.to_task_id in dropped_ids:
            for succ in _live_successors(c.to_task_id, set()):
                key = (c.from_task_id, succ)
                if key in seen_pairs:
                    continue
                new_constraints.append(PrecedenceConstraint(
                    id=f"{c.id}__rewired",
                    from_task_id=c.from_task_id, to_task_id=succ,
                    eval_op=c.eval_op, value=c.value,
                    expression=c.expression, logical_and=c.logical_and,
                ))
                seen_pairs.add(key)
                rewired += 1

    return new_constraints, rewired


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_plan(package: SSISPackage, plan: MigrationPlan) -> tuple[SSISPackage, PlanApplication]:
    """Return a modified copy of ``package`` per the plan, plus an application summary.

    The original package is **not** mutated. Generators should consume the
    returned package; the caller can also use the :class:`PlanApplication`
    summary to attach annotations or warnings to the conversion report.
    """
    pkg = deepcopy(package)
    app = PlanApplication()

    drop_names = _collect_drop_targets(plan)

    if drop_names:
        # Build name → id map (current state of pkg, before mutation)
        name_to_id: dict[str, str] = {}

        def _index(items: list[SSISTask]) -> None:
            for t in items:
                if t.name:
                    name_to_id[t.name.lower()] = t.id
                sub = getattr(t, "tasks", None)
                if sub:
                    _index(sub)

        _index(pkg.tasks)
        for eh in pkg.event_handlers:
            _index(eh.tasks)

        dropped_ids: set[str] = {
            name_to_id[n.lower()] for n in drop_names if n.lower() in name_to_id
        }

        # Drop top-level
        pkg.tasks, dropped_top = _drop_from_task_list(pkg.tasks, drop_names)
        # Drop inside containers
        for container in _walk_containers(pkg.tasks):
            container.tasks, dropped_sub = _drop_from_task_list(container.tasks, drop_names)
            dropped_top.extend(dropped_sub)
        # Drop inside event handlers
        for eh in pkg.event_handlers:
            eh.tasks, dropped_sub = _drop_from_task_list(eh.tasks, drop_names)
            dropped_top.extend(dropped_sub)

        app.dropped_task_names = sorted(set(dropped_top))
        app.dropped_task_count = len(app.dropped_task_names)

        if dropped_ids:
            kept_ids: set[str] = set()

            def _collect(items: list[SSISTask]) -> None:
                for t in items:
                    kept_ids.add(t.id)
                    sub = getattr(t, "tasks", None)
                    if sub:
                        _collect(sub)

            _collect(pkg.tasks)
            for eh in pkg.event_handlers:
                _collect(eh.tasks)

            pkg.constraints, rewired_top = _rewire_constraints(pkg.constraints, dropped_ids, kept_ids)
            for container in _walk_containers(pkg.tasks):
                container.constraints, rewired_sub = _rewire_constraints(
                    container.constraints, dropped_ids, kept_ids,
                )
                rewired_top += rewired_sub
            for eh in pkg.event_handlers:
                eh.constraints, rewired_sub = _rewire_constraints(
                    eh.constraints, dropped_ids, kept_ids,
                )
                rewired_top += rewired_sub
            app.rewired_constraint_count = rewired_top

    # Record non-DROP simplifications as deferred so the agent can surface them
    for s in plan.simplifications:
        if s.action == SimplificationAction.DROP:
            continue
        app.deferred_simplifications.append({
            "action": s.action.value,
            "items": s.items,
            "reason": s.reason,
            "note": (
                "Plan v1 records this simplification but does not auto-apply it. "
                "Review the generated artifacts and edit them to match the agreed design."
            ),
        })

    # Surface linked-service auth overrides for the linked-service generator
    for ls in plan.linked_services:
        app.linked_service_overrides[ls.name] = {
            "type": ls.type,
            "auth": ls.auth.value,
            "secret_name": ls.secret_name,
        }

    if app.dropped_task_count:
        app.notes.append(
            f"Dropped {app.dropped_task_count} task(s) per plan: "
            f"{', '.join(app.dropped_task_names)}."
        )
    if app.rewired_constraint_count:
        app.notes.append(
            f"Rewired {app.rewired_constraint_count} precedence constraint(s) "
            "to preserve execution order across dropped tasks."
        )

    return pkg, app


__all__ = ["PlanApplication", "apply_plan"]
