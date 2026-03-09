"""
Event Handler converter.

SSIS event handlers (OnError, OnPreExecute, OnPostExecute, OnWarning, etc.) map to:
  - OnError  → A separate error-handling pipeline referenced via Execute Pipeline
               (failure-path dependsOn in the main pipeline)
  - Others   → Informational note; best-effort inline failure/completion activities
"""
from __future__ import annotations

from typing import Any

from ...parsers.models import EventHandler, PrecedenceConstraint, SSISTask
from ..base_converter import BaseConverter

_ERROR_EVENTS = frozenset({"OnError", "OnTaskFailed"})
_PRE_EVENTS = frozenset({"OnPreExecute", "OnPreValidate"})
_POST_EVENTS = frozenset({"OnPostExecute", "OnPostValidate"})


class EventHandlerConverter(BaseConverter):
    """
    Converts package-level event handlers into either:
      - A dedicated error pipeline + Execute Pipeline activity wired on failure
      - Inline comment activities noting what was present
    """

    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        # EventHandlerConverter is not called for individual tasks;
        # it is invoked by the pipeline_generator for package-level handlers.
        raise NotImplementedError("Use convert_handler() instead.")

    def convert_handler(
        self,
        handler: EventHandler,
        parent_pipeline_name: str,
    ) -> dict[str, Any]:
        """
        Returns a descriptor dict rather than raw ADF JSON because event handlers
        produce an entire sub-pipeline plus a dependsOn entry in the main pipeline.

        Returns a dict with keys:
          - "sub_pipeline_name": str
          - "sub_pipeline_tasks": list[dict]  (ADF activities for the error pipeline)
          - "trigger_condition": str ("Succeeded" | "Failed" | "Completed")
          - "event_name": str
        """
        event = handler.event_name
        sub_pipeline_name = f"PL_{parent_pipeline_name}_EH_{event}"

        trigger_condition: str
        if event in _ERROR_EVENTS:
            trigger_condition = "Failed"
        elif event in _POST_EVENTS:
            trigger_condition = "Succeeded"
        else:
            trigger_condition = "Completed"

        # Convert inner tasks to placeholder activities
        # (a full nested conversion would require ConverterDispatcher — handled at pipeline level)
        sub_activities: list[dict[str, Any]] = []
        for t in handler.tasks:
            sub_activities.append({
                "name": t.name,
                "description": f"[EventHandler:{event}] {t.description or t.name}",
                "type": "Wait",
                "typeProperties": {"waitTimeInSeconds": 1},
            })

        if not sub_activities:
            sub_activities.append({
                "name": f"Log_{event}",
                "description": f"Placeholder for {event} handler. Implement logging here.",
                "type": "Wait",
                "typeProperties": {"waitTimeInSeconds": 1},
            })

        return {
            "sub_pipeline_name": sub_pipeline_name,
            "sub_pipeline_tasks": sub_activities,
            "trigger_condition": trigger_condition,
            "event_name": event,
        }
