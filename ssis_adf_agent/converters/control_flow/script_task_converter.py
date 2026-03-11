"""
Script Task → ADF Azure Function Activity + Azure Function Python stub generation.

The converter:
1. Generates an AzureFunctionActivity JSON that calls an Azure Function endpoint.
2. Writes a Python Azure Function stub to the ``stubs/`` output directory that
   preserves the original variable interface so the developer can fill in logic.
3. Optionally calls Azure OpenAI to translate the original C# source to Python
   (set ``llm_translate=True`` and configure AZURE_OPENAI_* env vars).
"""
from __future__ import annotations

import textwrap
import warnings
from pathlib import Path
from typing import Any

from ...parsers.models import PrecedenceConstraint, ScriptTask, SSISTask
from ..base_converter import BaseConverter


class ScriptTaskConverter(BaseConverter):
    def __init__(
        self,
        stubs_output_dir: Path | None = None,
        llm_translate: bool = False,
    ) -> None:
        self.stubs_output_dir = stubs_output_dir or Path("stubs")
        self.llm_translate = llm_translate

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

        # --- LLM translation attempt ---
        translated_body: str | None = None
        translation_warning: str = ""
        if self.llm_translate and task.source_code:
            translated_body, translation_warning = _attempt_llm_translation(task)
        elif self.llm_translate and not task.source_code:
            translation_warning = (
                f"[LLM translation skipped for '{task.name}': no C# source code was "
                "extracted from the DTSX (package may use self-closing stub format)]"
            )

        # Original C# included as line comments whenever source is available
        original_code_comment = ""
        if task.source_code:
            original_code_comment = textwrap.indent(
                f"# ---- Original {task.script_language} source ----\n"
                + "\n".join(f"# {line}" for line in task.source_code.splitlines()),
                "    ",
            )

        param_assignments = "\n".join(
            f'    {_py_name(v)} = body.get("{v}")'
            for v in all_params
        )
        return_dict = "{" + ", ".join(f'"{v}": {_py_name(v)}' for v in rw_vars) + "}"

        if translated_body:
            impl = textwrap.indent(translated_body, "    ")
            if original_code_comment:
                impl += "\n\n" + original_code_comment
            if translation_warning:
                impl = f"    # {translation_warning}\n" + impl
        else:
            warn_line = f"    # {translation_warning}\n" if translation_warning else ""
            orig_block = original_code_comment + "\n\n" if original_code_comment else ""
            impl = (
                warn_line
                + orig_block
                + "    # TODO: implement converted logic here\n"
                + f"    raise NotImplementedError(\n"
                + f'        "Script Task \'{task.name}\' has not been implemented yet. "\n'
                + f'        "See the original {task.script_language} code above."\n'
                + "    )"
            )

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

{impl}

    return func.HttpResponse(
        json.dumps({return_dict or "{}"}),
        mimetype="application/json",
        status_code=200,
    )
'''
        stub_file.write_text(stub_content, encoding="utf-8")
        return stub_file


def _attempt_llm_translation(task: ScriptTask) -> tuple[str | None, str]:
    """
    Try to translate ``task.source_code`` via Azure OpenAI.

    Returns (translated_python, warning_message).
    On success: (code_str, "").
    On failure: (None, warning_message) — caller falls back to TODO stub.
    """
    from ...translators.csharp_to_python import CSharpToPythonTranslator, TranslationError

    translator = CSharpToPythonTranslator()
    if not translator.is_configured():
        msg = (
            "[LLM translation skipped: AZURE_OPENAI_ENDPOINT and/or AZURE_OPENAI_API_KEY "
            "are not set. Set these env vars to enable automatic C# → Python translation.]"
        )
        warnings.warn(msg, stacklevel=4)
        return None, msg

    try:
        python_code = translator.translate(task.source_code or "", task)
        return python_code, ""
    except TranslationError as exc:
        msg = f"[LLM translation failed for '{task.name}': {exc}. Falling back to TODO stub.]"
        warnings.warn(msg, stacklevel=4)
        return None, msg


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
