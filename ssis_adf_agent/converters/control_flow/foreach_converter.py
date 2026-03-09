"""
ForEach Loop Container → ADF ForEach Activity.

Mapping:
  ForEachFileEnumerator → ForEach over GetMetadata output items
  ForEachADOEnumerator  → ForEach over Lookup output value
  ForEachItemEnumerator → ForEach over inline items array
  Other types          → ForEach with a placeholder expression (flagged as warning)
"""
from __future__ import annotations

from typing import Any

from ...parsers.models import (
    ForEachEnumeratorType,
    ForEachLoopContainer,
    PrecedenceConstraint,
    SSISTask,
)
from ..base_converter import BaseConverter


class ForEachConverter(BaseConverter):
    def __init__(self, child_converter: "ConverterDispatcher | None" = None) -> None:  # type: ignore[name-defined]
        self._child_converter = child_converter

    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        assert isinstance(task, ForEachLoopContainer)
        depends_on = self._depends_on(task, constraints, task_by_id)

        items_expr = self._build_items_expression(task)
        inner_activities = self._convert_inner(task)

        return [{
            "name": task.name,
            "description": task.description or "",
            "type": "ForEach",
            "dependsOn": depends_on,
            "typeProperties": {
                "items": {
                    "value": items_expr,
                    "type": "Expression",
                },
                "isSequential": True,
                "activities": inner_activities,
            },
        }]

    def _build_items_expression(self, task: ForEachLoopContainer) -> str:
        cfg = task.enumerator_config
        if task.enumerator_type == ForEachEnumeratorType.FILE:
            folder = cfg.get("Folder") or cfg.get("FolderPath") or "@pipeline().parameters.FolderPath"
            return (
                f"@activity('GetMetadata_{task.name.replace(' ', '_')}').output.childItems"
            )
        elif task.enumerator_type == ForEachEnumeratorType.ADO:
            return f"@activity('Lookup_{task.name.replace(' ', '_')}').output.value"
        elif task.enumerator_type == ForEachEnumeratorType.ITEM:
            items_raw = cfg.get("Items") or "[]"
            return f"@json('{items_raw}')"
        elif task.enumerator_type == ForEachEnumeratorType.VARIABLE:
            var_name = cfg.get("VariableName") or "items"
            var_short = var_name.split("::")[-1]
            return f"@variables('{var_short}')"
        else:
            return (
                f"@pipeline().parameters.{task.name.replace(' ', '_')}_Items"
                "  /* TODO: populate this parameter */"
            )

    def _convert_inner(self, parent: ForEachLoopContainer) -> list[dict[str, Any]]:
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
