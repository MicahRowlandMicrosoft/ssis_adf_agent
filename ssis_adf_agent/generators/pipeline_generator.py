"""
Pipeline generator — assembles a complete ADF pipeline.json from an SSISPackage.

Output structure::

    {
        "name": "PL_<PackageName>",
        "properties": {
            "description": "...",
            "activities": [...],
            "parameters": {...},
            "variables": {...}
        }
    }
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..parsers.models import (
    SSISPackage,
    SSISParameter,
    SSISVariable,
)
from ..analyzers.dependency_graph import topological_sort
from ..converters.dispatcher import ConverterDispatcher

_SSIS_TO_ADF_TYPE: dict[str, str] = {
    "String": "String",
    "Int32": "Int",
    "Int16": "Int",
    "Int64": "Int",
    "Boolean": "Bool",
    "DateTime": "String",
    "Double": "Float",
    "Single": "Float",
    "Object": "String",
}


def _map_param_type(ssis_type: str) -> str:
    return _SSIS_TO_ADF_TYPE.get(ssis_type, "String")


def generate_pipeline(
    package: SSISPackage,
    output_dir: Path,
    stubs_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Convert an SSISPackage to a full ADF pipeline JSON and write it to *output_dir*.

    Returns the pipeline dict.
    """
    dispatcher = ConverterDispatcher(stubs_dir=stubs_dir or output_dir / "stubs")
    pipeline_name = f"PL_{package.name.replace(' ', '_')}"

    # Topological task ordering
    task_by_id = {t.id: t for t in package.tasks}
    ordered_ids = topological_sort(package.tasks, package.constraints)

    # Convert tasks in dependency order
    activities: list[dict[str, Any]] = []
    for task_id in ordered_ids:
        task = task_by_id.get(task_id)
        if task is None:
            continue
        acts = dispatcher.convert_task(task, package.constraints, task_by_id)
        activities.extend(acts)

    # Build parameters from SSIS package parameters
    parameters: dict[str, Any] = {}
    for p in package.parameters:
        parameters[p.name] = {
            "type": _map_param_type(p.data_type),
            **({"defaultValue": p.value} if p.value is not None else {}),
        }

    # Add implicit parameters for function URLs (referenced by File System / Send Mail converters)
    _inject_function_url_params(parameters, activities)

    # Build variables from SSIS package variables (User namespace only)
    variables: dict[str, Any] = {}
    for v in package.variables:
        if v.namespace.lower() == "user":
            variables[v.name] = {
                "type": _map_param_type(v.data_type),
                **({"defaultValue": v.value} if v.value is not None else {}),
            }

    pipeline: dict[str, Any] = {
        "name": pipeline_name,
        "properties": {
            "description": (
                f"Auto-generated from SSIS package: {package.source_file}. "
                "Review flagged activities before deploying."
            ),
            "activities": activities,
            "parameters": parameters,
            "variables": variables,
            "annotations": [
                "ssis-adf-agent",
                f"source-package:{package.name}",
            ],
        },
    }

    # Write to disk
    output_dir.mkdir(parents=True, exist_ok=True)
    pipeline_file = output_dir / "pipeline" / f"{pipeline_name}.json"
    pipeline_file.parent.mkdir(parents=True, exist_ok=True)
    pipeline_file.write_text(
        json.dumps(pipeline, indent=4, ensure_ascii=False),
        encoding="utf-8",
    )

    return pipeline


def _inject_function_url_params(
    parameters: dict[str, Any],
    activities: list[dict],
) -> None:
    """Add pipeline parameters for Azure Function URLs referenced in activities."""
    needed = set()
    for act in activities:
        tp = act.get("typeProperties", {})
        url = tp.get("url", "")
        if isinstance(url, str) and url.startswith("@pipeline().parameters."):
            param_name = url.split("@pipeline().parameters.")[-1]
            needed.add(param_name)
    for name in needed:
        if name not in parameters:
            parameters[name] = {
                "type": "String",
                "defaultValue": "https://TODO.azurewebsites.net/api/" + name.replace("Url", ""),
            }
