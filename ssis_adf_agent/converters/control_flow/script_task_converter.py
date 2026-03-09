"""
Script Task → ADF Azure Function Activity + Azure Function Python stub generation.

The converter:
1. Generates an AzureFunctionActivity JSON that calls an Azure Function endpoint.
2. Writes a Python Azure Function stub to the ``stubs/`` output directory that
   preserves the original variable interface so the developer can fill in logic.
"""
from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

from ...parsers.models import PrecedenceConstraint, ScriptTask, SSISTask
from ..base_converter import BaseConverter


class ScriptTaskConverter(BaseConverter):
    def __init__(self, stubs_output_dir: Path | None = None) -> None:
        self.stubs_output_dir = stubs_output_dir or Path("stubs")

    def convert(
        self,
        task: SSISTask,
        constraints: list[PrecedenceConstraint],
        task_by_id: dict[str, SSISTask],
    ) -> list[dict[str, Any]]:
        assert isinstance(task, ScriptTask)
        depends_on = self._depends_on(task, constraints, task_by_id)

        func_name = _safe_name(task.name)
        stub_path = self._write_stub(task, func_name)

        return [{
            "name": task.name,
            "description": (
                f"[MANUAL REVIEW REQUIRED] Converted from {task.script_language} Script Task. "
                f"Azure Function stub: {stub_path.name}. "
                + (task.description or "")
            ),
            "type": "AzureFunction",
            "dependsOn": depends_on,
            "linkedServiceName": {
                "referenceName": "LS_AzureFunction",
                "type": "LinkedServiceReference",
            },
            "typeProperties": {
                "functionName": func_name,
                "method": "POST",
                "body": _build_body(task),
            },
        }]

    def _write_stub(self, task: ScriptTask, func_name: str) -> Path:
        self.stubs_output_dir.mkdir(parents=True, exist_ok=True)
        stub_file = self.stubs_output_dir / f"{func_name}/__init__.py"
        stub_file.parent.mkdir(parents=True, exist_ok=True)

        ro_vars = task.read_only_variables
        rw_vars = task.read_write_variables
        all_params = ro_vars + rw_vars

        param_doc = "\n".join(
            f"        {v}: pipeline variable (read-only)" for v in ro_vars
        ) + "\n" + "\n".join(
            f"        {v}: pipeline variable (read-write)" for v in rw_vars
        )

        original_code_block = ""
        if task.source_code:
            original_code_block = textwrap.indent(
                f'"""\nOriginal {task.script_language} source:\n\n'
                + textwrap.indent(task.source_code, "    ")
                + '\n"""',
                "    ",
            )

        param_assignments = "\n".join(
            f'    {_py_name(v)} = body.get("{v}")'
            for v in all_params
        )
        return_dict = "{" + ", ".join(f'"{v}": {_py_name(v)}' for v in rw_vars) + "}"

        stub_content = f'''\
"""
Azure Function stub — auto-generated from SSIS Script Task: {task.name}
Original language: {task.script_language}
Entry point: {task.entry_point}

TODO: Implement the business logic below.  The function receives the SSIS
      variables listed under Args as JSON body fields and returns the
      read-write variables in the JSON response.

Args:
{param_doc or "        (no variables declared)"}
"""
import logging
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Executing {func_name}")

    try:
        body = req.get_json()
    except ValueError:
        body = {{}}

{param_assignments or "    pass  # no variables declared"}

{original_code_block}

    # TODO: implement converted logic here
    raise NotImplementedError(
        "Script Task '{task.name}' has not been implemented yet. "
        "See the original {task.script_language} code above."
    )

    return func.HttpResponse(
        json.dumps({return_dict or "{}"}),
        mimetype="application/json",
        status_code=200,
    )
'''
        stub_file.write_text(stub_content, encoding="utf-8")
        return stub_file


def _safe_name(name: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9_]", "_", name).strip("_") or "ScriptTask"


def _py_name(var: str) -> str:
    """Convert a dotted SSIS variable name (e.g. User::MyVar) to a Python identifier."""
    import re
    parts = var.split("::")
    raw = parts[-1]
    return re.sub(r"[^A-Za-z0-9_]", "_", raw).strip("_").lower() or "var"


def _build_body(task: ScriptTask) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for v in task.read_only_variables:
        body[v] = f"@variables('{v.split('::')[-1]}')"
    for v in task.read_write_variables:
        body[v] = f"@variables('{v.split('::')[-1]}')"
    return body
