"""
Custom-component substitution registry (M7).

Customers running 3rd-party SSIS components (Cozyroc, KingswaySoft, custom
in-house pipeline components) need a deterministic way to tell the agent:

> "When you see component type `Cozyroc.SSIS.SuperLookupTask`, emit a Lookup
> activity that calls our pre-built ADF dataset `DS_SuperLookupReplacement`."

This module loads a JSON file with that mapping and exposes it to:

* `transformation_converter` — for Data Flow components.
* `unknown_task_converter`  — for Control Flow tasks (future hook).

The registry file format is intentionally tiny and human-editable:

```json
{
  "version": "1",
  "data_flow_components": {
    "Cozyroc.SSIS.SuperLookupTask": {
      "adf_type": "Lookup",
      "notes": "Replace with our hand-built ADF Lookup; reviewer must wire columns.",
      "type_properties": {
        "broadcast": "Auto"
      }
    }
  },
  "control_flow_tasks": {
    "{ABC-CUSTOM-TASK-GUID}": {
      "adf_activity_type": "WebActivity",
      "notes": "Custom HTTP task -> use Web Activity, supply URL via parameter."
    }
  }
}
```

Loaders are pure (no I/O outside the explicit ``load_registry`` entry point)
so they're trivial to unit-test and to swap in mock data.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DataFlowSubstitution:
    """Maps a 3rd-party Data Flow component to an ADF MDF transformation."""
    adf_type: str
    notes: str = ""
    type_properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ControlFlowSubstitution:
    """Maps a 3rd-party Control Flow task to an ADF activity."""
    adf_activity_type: str
    notes: str = ""
    type_properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SubstitutionRegistry:
    """Frozen view over the registry; safe to share between conversions."""
    data_flow: dict[str, DataFlowSubstitution] = field(default_factory=dict)
    control_flow: dict[str, ControlFlowSubstitution] = field(default_factory=dict)

    def lookup_data_flow(self, component_type: str) -> DataFlowSubstitution | None:
        return self.data_flow.get(component_type)

    def lookup_control_flow(self, task_creation_name: str) -> ControlFlowSubstitution | None:
        return self.control_flow.get(task_creation_name)


# A module-level empty registry callers can use as a default sentinel.
EMPTY_REGISTRY = SubstitutionRegistry()


def load_registry(path: str | Path) -> SubstitutionRegistry:
    """
    Read and validate a substitution registry from ``path``.

    Raises ``ValueError`` for the few things we cannot recover from
    (unreadable JSON, wrong top-level type, missing ``adf_type`` /
    ``adf_activity_type`` fields). Anything else is treated as a benign
    no-op so a customer can grow the registry incrementally.
    """
    p = Path(path)
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Substitution registry not found: {p}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Substitution registry {p} is not valid JSON: {exc}"
        ) from exc

    if not isinstance(raw, dict):
        raise ValueError(
            f"Substitution registry {p} must be a JSON object at the top level."
        )

    df_raw = raw.get("data_flow_components") or {}
    cf_raw = raw.get("control_flow_tasks") or {}
    if not isinstance(df_raw, dict) or not isinstance(cf_raw, dict):
        raise ValueError(
            f"Substitution registry {p}: 'data_flow_components' and "
            "'control_flow_tasks' must each be a JSON object."
        )

    data_flow: dict[str, DataFlowSubstitution] = {}
    for key, val in df_raw.items():
        if not isinstance(val, dict) or "adf_type" not in val:
            raise ValueError(
                f"data_flow_components['{key}'] must be an object with an 'adf_type' field."
            )
        data_flow[key] = DataFlowSubstitution(
            adf_type=str(val["adf_type"]),
            notes=str(val.get("notes", "")),
            type_properties=dict(val.get("type_properties") or {}),
        )

    control_flow: dict[str, ControlFlowSubstitution] = {}
    for key, val in cf_raw.items():
        if not isinstance(val, dict) or "adf_activity_type" not in val:
            raise ValueError(
                f"control_flow_tasks['{key}'] must be an object with an 'adf_activity_type' field."
            )
        control_flow[key] = ControlFlowSubstitution(
            adf_activity_type=str(val["adf_activity_type"]),
            notes=str(val.get("notes", "")),
            type_properties=dict(val.get("type_properties") or {}),
        )

    return SubstitutionRegistry(data_flow=data_flow, control_flow=control_flow)
