"""
Execute Package Task → ADF Execute Pipeline Activity.
"""
from __future__ import annotations

from typing import Any

from ...parsers.models import ExecutePackageTask, PrecedenceConstraint, SSISTask
from ..base_converter import BaseConverter


class ExecutePackageConverter(BaseConverter):
    def __init__(self, pipeline_prefix: str = "PL_") -> None:
        self._pipeline_prefix = pipeline_prefix

    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        assert isinstance(task, ExecutePackageTask)
        depends_on = self._depends_on(task, constraints, task_by_id)

        # Derive a pipeline name from the referenced package
        if task.use_project_reference and task.project_package_name:
            ref_name = task.project_package_name.replace(".dtsx", "").replace(" ", "_")
        elif task.package_path:
            from pathlib import PurePosixPath, PureWindowsPath
            # Handle both / and \ separators in package paths
            raw = task.package_path or ""
            basename = PureWindowsPath(raw).name if "\\" in raw else PurePosixPath(raw).name
            ref_name = basename.replace(".dtsx", "").replace(" ", "_")
        else:
            ref_name = "UNKNOWN"

        # Apply pipeline prefix consistently
        ref_name = f"{self._pipeline_prefix}{ref_name}"

        parameters: dict[str, Any] = {}
        for pa in task.parameter_assignments:
            param_name = pa.get("parameter", "param")
            var_raw = pa.get("variable", "")
            var_name = var_raw.split("::")[-1] if "::" in var_raw else var_raw
            parameters[param_name] = {
                "value": f"@variables('{var_name}')",
                "type": "Expression",
            }

        return [{
            "name": task.name,
            "description": task.description or "",
            "type": "ExecutePipeline",
            "dependsOn": depends_on,
            "typeProperties": {
                "pipeline": {
                    "referenceName": ref_name,
                    "type": "PipelineReference",
                },
                "waitOnCompletion": True,
                "parameters": parameters,
            },
        }]
