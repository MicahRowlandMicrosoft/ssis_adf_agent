"""
For Loop Container → ADF Until Activity.

SSIS For Loop:
  InitExpression   — sets the counter variable before the loop starts
  EvalExpression   — loop continues while this is True
  AssignExpression — updates the counter after each iteration

ADF Until:
  expression — loop continues UNTIL this is True (i.e. the negation of EvalExpression)
  timeout    — safety timeout
  activities — inner activities + a SetVariable to update the counter
"""
from __future__ import annotations

import re
from typing import Any

from ...parsers.models import (
    ForLoopContainer,
    PrecedenceConstraint,
    SSISTask,
)
from ...translators.control_flow_expression import (
    translate_control_flow_expr,
    strip_variable_namespace,
)
from ..base_converter import BaseConverter


def _negate_ssis_expression(expr: str | None) -> str:
    """
    Wrap the SSIS eval expression in ADF @not(...) to convert from
    "continue while true" → "stop until true".
    """
    if not expr:
        return "@equals(1, 1)  /* TODO: translate loop condition */"
    adf = translate_control_flow_expr(expr)
    return f"@not({adf})"


class ForLoopConverter(BaseConverter):
    def __init__(self, child_converter: "ConverterDispatcher | None" = None) -> None:  # type: ignore[name-defined]
        self._child_converter = child_converter

    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        assert isinstance(task, ForLoopContainer)
        depends_on = self._depends_on(task, constraints, task_by_id)

        inner_activities = self._convert_inner(task)

        # Add a SetVariable activity for the assign expression (counter increment)
        if task.assign_expression:
            inner_activities.append(self._set_variable_activity(task))

        activities: list[dict[str, Any]] = []

        # Emit an initial SetVariable for the init expression (runs before the Until)
        if task.init_expression:
            activities.append(self._init_set_variable(task))

        until_depends: list[dict] = []
        if task.init_expression:
            init_name = f"Init_{task.name.replace(' ', '_')}"
            until_depends.append({"activity": init_name, "dependencyConditions": ["Succeeded"]})
        until_depends.extend(depends_on)

        activities.append({
            "name": task.name,
            "description": (
                f"[Converted from SSIS For Loop] "
                f"Init: {task.init_expression!r} | "
                f"Eval: {task.eval_expression!r} | "
                f"Assign: {task.assign_expression!r}"
                + (" | " + task.description if task.description else "")
            ),
            "type": "Until",
            "dependsOn": until_depends,
            "typeProperties": {
                "expression": {
                    "value": _negate_ssis_expression(task.eval_expression),
                    "type": "Expression",
                },
                "timeout": "0.12:00:00",
                "activities": inner_activities,
            },
        })
        return activities

    def _init_set_variable(self, task: ForLoopContainer) -> dict[str, Any]:
        # Parse @[User::VarName] = value or @(Ns::VarName) = value
        m = re.search(
            r'@(?:\[([\w:]+)\]|\(([\w:]+)\)|(\w+::\w+))\s*=\s*(.+)',
            task.init_expression or "",
        )
        if m:
            raw_var = m.group(1) or m.group(2) or m.group(3) or "counter"
            var_name = strip_variable_namespace(raw_var)
            init_val = translate_control_flow_expr(m.group(4).strip())
        else:
            var_name = "counter"
            init_val = "0"
        return {
            "name": f"Init_{task.name.replace(' ', '_')}",
            "description": f"Initialize counter for For Loop: {task.init_expression}",
            "type": "SetVariable",
            "typeProperties": {
                "variableName": var_name,
                "value": {"value": f"@{init_val}" if not init_val.startswith('@') else init_val, "type": "Expression"},
            },
        }

    def _set_variable_activity(self, task: ForLoopContainer) -> dict[str, Any]:
        m = re.search(
            r'@(?:\[([\w:]+)\]|\(([\w:]+)\)|(\w+::\w+))\s*=\s*(.+)',
            task.assign_expression or "",
        )
        if m:
            raw_var = m.group(1) or m.group(2) or m.group(3) or "counter"
            var_name = strip_variable_namespace(raw_var)
            new_val = translate_control_flow_expr(m.group(4).strip())
        else:
            var_name = "counter"
            new_val = "add(variables('counter'), 1)"
        return {
            "name": f"Increment_{task.name.replace(' ', '_')}",
            "description": f"Increment counter: {task.assign_expression}",
            "type": "SetVariable",
            "typeProperties": {
                "variableName": var_name,
                "value": {"value": f"@{new_val}" if not new_val.startswith('@') else new_val, "type": "Expression"},
            },
        }

    def _convert_inner(self, parent: ForLoopContainer) -> list[dict[str, Any]]:
        if not self._child_converter:
            return [
                {"name": f"activity_{t.name}", "type": "Wait",
                 "typeProperties": {"waitTimeInSeconds": 1}}
                for t in parent.tasks
            ]
        activities = []
        inner_task_by_id = {t.id: t for t in parent.tasks}
        for t in parent.tasks:
            activities.extend(
                self._child_converter.convert_task(t, parent.constraints, inner_task_by_id)
            )
        return activities
