"""
Trigger generator — extracts schedule information from SSIS package properties
and emits ADF trigger JSON files when a schedule is found.

Note: SSIS scheduling lives in SQL Agent (not in the .dtsx file itself).
This generator produces a template trigger that can be filled in or left as-is.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..parsers.models import SSISPackage


def generate_triggers(
    package: SSISPackage,
    output_dir: Path,
    cron_expression: str | None = None,
) -> list[dict[str, Any]]:
    """
    Generate an ADF ScheduleTrigger JSON for the pipeline derived from *package*.

    If *cron_expression* is provided it is used directly.  Otherwise a placeholder
    daily-at-midnight schedule is emitted with a TODO comment.

    Files are written to *output_dir*/trigger/.
    Returns the list of trigger dicts.
    """
    trigger_dir = output_dir / "trigger"
    trigger_dir.mkdir(parents=True, exist_ok=True)

    pipeline_name = f"PL_{package.name.replace(' ', '_')}"
    trigger_name = f"TR_{package.name.replace(' ', '_')}"

    recurrence: dict[str, Any]
    if cron_expression:
        recurrence = {
            "frequency": "Minute",
            "interval": 1,
            "schedule": {"quartz": cron_expression},
        }
    else:
        recurrence = {
            "frequency": "Day",
            "interval": 1,
            "schedule": {
                "hours": [0],
                "minutes": [0],
            },
        }

    trigger: dict[str, Any] = {
        "name": trigger_name,
        "properties": {
            "description": (
                f"Auto-generated trigger for pipeline {pipeline_name}. "
                "Adjust schedule to match original SQL Agent job schedule."
            ),
            "annotations": ["ssis-adf-agent"],
            "type": "ScheduleTrigger",
            "typeProperties": {
                "recurrence": {
                    **recurrence,
                    "startTime": "2026-01-01T00:00:00Z",
                    "timeZone": "UTC",
                },
            },
            "pipelines": [
                {
                    "pipelineReference": {
                        "referenceName": pipeline_name,
                        "type": "PipelineReference",
                    },
                    "parameters": {},
                }
            ],
            "runtimeState": "Stopped",
        },
    }

    file_path = trigger_dir / f"{trigger_name}.json"
    file_path.write_text(
        json.dumps(trigger, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )

    return [trigger]
