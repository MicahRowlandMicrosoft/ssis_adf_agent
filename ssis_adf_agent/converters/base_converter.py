"""
Base converter — abstract base class for all SSIS → ADF activity converters.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..parsers.models import PrecedenceConstraint, SSISTask


class BaseConverter(ABC):
    """
    Convert a single SSIS task into one or more ADF activity dictionaries.

    Implementors override ``convert`` and return a list of activity dicts
    (usually one, but complex tasks like ForEach may produce helper activities).
    """

    @abstractmethod
    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        """
        Convert *task* to ADF activity JSON dict(s).

        Args:
            task: The SSIS task to convert.
            constraints: All precedence constraints in the containing scope.
            task_by_id: All sibling tasks keyed by ID (for dependency resolution).

        Returns:
            List of ADF activity dicts (one per activity to emit).
        """

    def _depends_on(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        """Build the ADF ``dependsOn`` list for *task*."""
        result: list[dict[str, Any]] = []
        from ..analyzers.dependency_graph import get_depends_on_for_task

        entries = get_depends_on_for_task(task.id, task_by_id, constraints)
        for entry in entries:
            result.append({
                "activity": entry.activity,
                "dependencyConditions": entry.dependency_conditions,
            })
        return result
