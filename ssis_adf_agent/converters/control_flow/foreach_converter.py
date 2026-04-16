"""
ForEach Loop Container → ADF ForEach Activity.

Mapping:
  ForEachFileEnumerator → GetMetadata activity + ForEach over childItems
  ForEachADOEnumerator  → Lookup activity + ForEach over output.value
  ForEachItemEnumerator → ForEach over inline items array
  Other types          → ForEach with a placeholder expression (flagged as warning)

Prerequisite activities (GetMetadata / Lookup) are auto-generated and
inserted before the ForEach with a dependsOn chain.
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

        # Build prerequisite activity (GetMetadata / Lookup) if required
        prereq = self._build_prerequisite(task, depends_on)

        items_expr = self._build_items_expression(task)
        inner_activities = self._convert_inner(task)

        # If there's a prerequisite activity the ForEach depends on it,
        # otherwise the ForEach inherits the original dependsOn list.
        foreach_depends_on: list[dict[str, Any]]
        if prereq:
            foreach_depends_on = [{
                "activity": prereq["name"],
                "dependencyConditions": ["Succeeded"],
            }]
        else:
            foreach_depends_on = depends_on

        activities: list[dict[str, Any]] = []
        if prereq:
            activities.append(prereq)

        activities.append({
            "name": task.name,
            "description": task.description or "",
            "type": "ForEach",
            "dependsOn": foreach_depends_on,
            "typeProperties": {
                "items": {
                    "value": items_expr,
                    "type": "Expression",
                },
                "isSequential": True,
                "activities": inner_activities,
            },
        })
        return activities

    # ------------------------------------------------------------------
    # Prerequisite activity builders
    # ------------------------------------------------------------------

    def _build_prerequisite(
        self,
        task: ForEachLoopContainer,
        depends_on: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Return a GetMetadata or Lookup activity that the ForEach depends on,
        or *None* if no prerequisite is needed."""
        if task.enumerator_type == ForEachEnumeratorType.FILE:
            return self._build_get_metadata(task, depends_on)
        if task.enumerator_type == ForEachEnumeratorType.ADO:
            return self._build_lookup(task, depends_on)
        return None

    def _build_get_metadata(
        self,
        task: ForEachLoopContainer,
        depends_on: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Emit a GetMetadata activity that lists childItems for a folder."""
        cfg = task.enumerator_config
        safe = task.name.replace(" ", "_")
        activity_name = f"GetMetadata_{safe}"

        folder = (
            cfg.get("Folder")
            or cfg.get("FolderPath")
            or "@pipeline().parameters.FolderPath"
        )
        file_spec = cfg.get("FileSpec") or "*"

        # Dataset reference — caller should ensure DS_<name> exists or will be
        # created by the dataset generator.  We use a parameterised reference.
        dataset_ref = cfg.get("DatasetRef") or f"DS_{safe}_Folder"

        return {
            "name": activity_name,
            "type": "GetMetadata",
            "dependsOn": depends_on,
            "typeProperties": {
                "fieldList": ["childItems"],
                "dataset": {
                    "referenceName": dataset_ref,
                    "type": "DatasetReference",
                    "parameters": {
                        "FolderPath": folder,
                        "FileSpec": file_spec,
                    },
                },
                "storeSettings": {
                    "type": "AzureBlobStorageReadSettings",
                    "recursive": False,
                    "wildcardFileName": file_spec,
                },
            },
            "policy": {
                "timeout": "0.00:10:00",
                "retry": 2,
                "retryIntervalInSeconds": 30,
            },
        }

    def _build_lookup(
        self,
        task: ForEachLoopContainer,
        depends_on: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Emit a Lookup activity whose output.value feeds the ForEach."""
        cfg = task.enumerator_config
        safe = task.name.replace(" ", "_")
        activity_name = f"Lookup_{safe}"

        # ADO enumerator often holds a variable name containing a recordset
        # produced by an earlier Execute SQL Task.  In ADF the equivalent is
        # a Lookup pointing at the same query/table.
        source_variable = cfg.get("VariableName") or ""
        query = cfg.get("Query") or cfg.get("SqlCommand") or ""
        dataset_ref = cfg.get("DatasetRef") or f"DS_{safe}_Lookup"

        source_props: dict[str, Any] = {
            "type": "AzureSqlSource",
        }
        if query:
            source_props["sqlReaderQuery"] = query
        else:
            source_props["sqlReaderQuery"] = (
                f"/* TODO: replace with query from variable {source_variable} */"
                " SELECT 1 AS placeholder"
            )

        return {
            "name": activity_name,
            "type": "Lookup",
            "dependsOn": depends_on,
            "typeProperties": {
                "source": source_props,
                "dataset": {
                    "referenceName": dataset_ref,
                    "type": "DatasetReference",
                },
                "firstRowOnly": False,
            },
            "policy": {
                "timeout": "0.00:10:00",
                "retry": 2,
                "retryIntervalInSeconds": 30,
            },
        }

    # ------------------------------------------------------------------
    # Items expression
    # ------------------------------------------------------------------

    def _build_items_expression(self, task: ForEachLoopContainer) -> str:
        cfg = task.enumerator_config
        if task.enumerator_type == ForEachEnumeratorType.FILE:
            safe = task.name.replace(" ", "_")
            return f"@activity('GetMetadata_{safe}').output.childItems"
        elif task.enumerator_type == ForEachEnumeratorType.ADO:
            safe = task.name.replace(" ", "_")
            return f"@activity('Lookup_{safe}').output.value"
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
