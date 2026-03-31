"""
Trigger generator — emits ADF trigger JSON from SSIS package schedule metadata.

When a SqlAgentSchedule is available on the package model, generates an accurate
ScheduleTrigger mapping SQL Agent frequency types to ADF recurrence patterns.
Otherwise falls back to a placeholder daily-at-midnight schedule.

Triggers are ALWAYS deployed in Stopped state (domain rule).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..parsers.models import SqlAgentSchedule, SSISPackage


# SQL Agent freq_type → ADF frequency
_FREQ_MAP: dict[int, str] = {
    1: "Minute",   # Once — use one-time; approximate as Minute
    4: "Day",
    8: "Week",
    16: "Month",
    32: "Month",   # Monthly relative — needs manual review
}

# SQL Agent freq_interval bit flags for weekly (freq_type=8) → day names
_WEEKDAY_BITS: dict[int, str] = {
    1: "Sunday",
    2: "Monday",
    4: "Tuesday",
    8: "Wednesday",
    16: "Thursday",
    32: "Friday",
    64: "Saturday",
}


def _schedule_from_agent(sched: SqlAgentSchedule) -> dict[str, Any]:
    """Convert SqlAgentSchedule to ADF recurrence + schedule dicts."""
    freq = _FREQ_MAP.get(sched.frequency_type, "Day")
    interval = 1

    # Parse active_start_time (HHMMSS int)
    hours = sched.active_start_time // 10000
    minutes = (sched.active_start_time % 10000) // 100

    schedule: dict[str, Any] = {
        "hours": [hours],
        "minutes": [minutes],
    }

    if sched.frequency_type == 4:
        # Daily — interval is freq_interval (every N days)
        interval = max(sched.freq_interval, 1)

    elif sched.frequency_type == 8:
        # Weekly — decode day-of-week bitmask
        interval = max(sched.freq_recurrence_factor, 1)
        days = []
        for bit, name in _WEEKDAY_BITS.items():
            if sched.freq_interval & bit:
                days.append(name)
        if days:
            schedule["weekDays"] = days

    elif sched.frequency_type == 16:
        # Monthly — freq_interval is day of month
        interval = max(sched.freq_recurrence_factor, 1)
        schedule["monthDays"] = [sched.freq_interval]

    elif sched.frequency_type == 32:
        # Monthly relative — complex; flag for review
        interval = max(sched.freq_recurrence_factor, 1)

    recurrence: dict[str, Any] = {
        "frequency": freq,
        "interval": interval,
        "schedule": schedule,
    }

    return recurrence


def generate_triggers(
    package: SSISPackage,
    output_dir: Path,
    cron_expression: str | None = None,
) -> list[dict[str, Any]]:
    """
    Generate an ADF ScheduleTrigger JSON for the pipeline derived from *package*.

    Priority:
      1. If the package has a sql_agent_schedule, use it for accurate mapping.
      2. If *cron_expression* is provided, use it.
      3. Otherwise emit a placeholder daily-at-midnight schedule.

    Files are written to *output_dir*/trigger/.
    Returns the list of trigger dicts.
    """
    trigger_dir = output_dir / "trigger"
    trigger_dir.mkdir(parents=True, exist_ok=True)

    pipeline_name = f"PL_{package.name.replace(' ', '_')}"
    trigger_name = f"TR_{package.name.replace(' ', '_')}"

    description_parts = [f"Auto-generated trigger for pipeline {pipeline_name}."]
    recurrence: dict[str, Any]

    if package.sql_agent_schedule:
        recurrence = _schedule_from_agent(package.sql_agent_schedule)
        sched = package.sql_agent_schedule
        description_parts.append(
            f"Mapped from SQL Agent job '{sched.job_name}' schedule '{sched.schedule_name}'."
        )
        if sched.frequency_type == 32:
            description_parts.append(
                "[MANUAL REVIEW] Monthly-relative schedule — verify ADF recurrence matches original."
            )
    elif cron_expression:
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
        description_parts.append(
            "Adjust schedule to match original SQL Agent job schedule."
        )

    # Support pipeline parameters (e.g., windowStart for incremental)
    pipeline_params: dict[str, Any] = {}

    trigger: dict[str, Any] = {
        "name": trigger_name,
        "properties": {
            "description": " ".join(description_parts),
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
                    "parameters": pipeline_params,
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
