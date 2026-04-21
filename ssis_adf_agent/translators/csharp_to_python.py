"""
Azure OpenAI-powered C# → Python translator for SSIS Script Tasks.

Usage::

    translator = CSharpToPythonTranslator()
    if translator.is_configured():
        python_body = translator.translate(source_code, task)

Authentication (one of the following):

* **Microsoft Entra ID (recommended; required when API keys are disabled by
  tenant policy):** set ``AZURE_OPENAI_ENDPOINT`` and run ``az login`` (or use
  any other identity supported by ``DefaultAzureCredential`` — managed
  identity, workload identity, environment service principal, etc.). The
  caller's identity must have the **Cognitive Services OpenAI User** role on
  the Azure OpenAI resource.
* **API key (legacy):** set ``AZURE_OPENAI_API_KEY`` in addition to the
  endpoint. Used automatically when present.

Other environment variables:
    AZURE_OPENAI_ENDPOINT     — e.g. https://my-resource.openai.azure.com/
    AZURE_OPENAI_DEPLOYMENT   — Model deployment name (default: gpt-4o)
    AZURE_OPENAI_API_VERSION  — API version (default: 2024-10-21)
"""
from __future__ import annotations

import os
import textwrap
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..parsers.models import ScriptTask

# Conservative token budget to stay well within model context limits
_MAX_INPUT_CHARS = 18_000   # ~6 000 tokens at ~3 char/token


class TranslationError(Exception):
    """Raised when the LLM call fails for any reason."""

    def __init__(self, message: str, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.cause = cause


class CSharpToPythonTranslator:
    """
    Translates C#/VB SSIS Script Task source code to a Python Azure Function
    implementation body using Azure OpenAI Chat Completions.
    """

    _SYSTEM_PROMPT = textwrap.dedent("""\
        You are an expert in both C# SSIS Script Tasks and Python Azure Functions.
        Your job is to translate C# SSIS automation logic into clean, idiomatic Python
        suitable for an Azure Functions v2 HTTP-triggered function.

        The caller has already handled:
        - HTTP request parsing (body is a dict called `body`)
        - Variable extraction from `body` (already done above the TODO block)
        - JSON serialization of the return dict

        You must output ONLY valid Python code for the implementation body —
        the logic that replaces the `raise NotImplementedError(...)` placeholder.
        Do NOT include function signatures, imports, try/except wrappers, or
        markdown code fences. Just the implementation logic.
    """)

    _USER_TEMPLATE = textwrap.dedent("""\
        Translate the following {language} SSIS Script Task to a Python implementation body.

        Task name: {task_name}
        Read-only input variables (already extracted from `body` dict): {ro_vars}
        Read-write output variables (must appear in returned dict at the end): {rw_vars}

        Translation rules:
        1. Replace any SQL Server / OLE DB calls with:
               # TODO: Replace with Azure SQL / Synapse connection using pyodbc or azure-data-tables
        2. Replace any file system path operations with:
               # TODO: Replace with Azure Blob Storage call using azure-storage-blob SDK
        3. Replace any SMTP / mail sending with:
               # TODO: Replace with Azure Communication Services or Logic App call
        4. Replace any Windows registry or COM interop calls with:
               # TODO: Not supported in Azure Functions — implement alternative
        5. Preserve all business logic, loops, conditionals, and variable transformations as closely as possible.
        6. Output ONLY plain Python code — no markdown fences, no explanations, no imports.

        Original {language} source:
        {source_code}
    """)

    def is_configured(self) -> bool:
        """Return True if Azure OpenAI is reachable.

        Requires only ``AZURE_OPENAI_ENDPOINT``. Authentication is then either
        ``AZURE_OPENAI_API_KEY`` (if set) or ``DefaultAzureCredential`` /
        Entra ID (handled lazily inside ``translate``).
        """
        return bool(os.environ.get("AZURE_OPENAI_ENDPOINT"))

    def translate(self, source_code: str, task: ScriptTask) -> str:
        """
        Call Azure OpenAI to translate ``source_code`` to a Python function body.

        Returns the translated Python code string.
        Raises ``TranslationError`` on any failure (auth, rate limit, timeout, etc.).
        """
        try:
            from openai import APIError, AzureOpenAI  # type: ignore[import-untyped]
        except ImportError as exc:
            raise TranslationError(
                "The 'openai' package is not installed. "
                "Run: pip install 'ssis-adf-agent[llm]'",
                exc,
            ) from exc

        endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
        api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
        deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
        api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-10-21")

        if not endpoint:
            raise TranslationError(
                "AZURE_OPENAI_ENDPOINT must be set to use LLM translation."
            )

        # Truncate large scripts to stay within token budget
        if len(source_code) > _MAX_INPUT_CHARS:
            source_code = (
                source_code[:_MAX_INPUT_CHARS]
                + f"\n\n// ... [TRUNCATED: source exceeded {_MAX_INPUT_CHARS} chars] ...\n"
            )

        ro_vars = task.read_only_variables or []
        rw_vars = task.read_write_variables or []

        user_prompt = self._USER_TEMPLATE.format(
            language=task.script_language,
            task_name=task.name,
            ro_vars=", ".join(ro_vars) if ro_vars else "(none)",
            rw_vars=", ".join(rw_vars) if rw_vars else "(none)",
            source_code=source_code,
        )

        # Build the AzureOpenAI client. Prefer Entra ID (DefaultAzureCredential)
        # when no API key is set — required for tenants where API-key auth on
        # Azure OpenAI is disabled by policy. The caller's identity needs the
        # "Cognitive Services OpenAI User" role on the Azure OpenAI resource.
        client_kwargs: dict = {
            "azure_endpoint": endpoint,
            "api_version": api_version,
        }
        if api_key:
            client_kwargs["api_key"] = api_key
        else:
            try:
                from azure.identity import (  # type: ignore[import-untyped]
                    DefaultAzureCredential,
                    get_bearer_token_provider,
                )
            except ImportError as exc:
                raise TranslationError(
                    "AZURE_OPENAI_API_KEY is not set and the 'azure-identity' "
                    "package is not installed for Entra ID authentication. "
                    "Either set AZURE_OPENAI_API_KEY or install azure-identity "
                    "(pip install azure-identity) and run 'az login'.",
                    exc,
                ) from exc
            token_provider = get_bearer_token_provider(
                DefaultAzureCredential(),
                "https://cognitiveservices.azure.com/.default",
            )
            client_kwargs["azure_ad_token_provider"] = token_provider

        try:
            client = AzureOpenAI(**client_kwargs)
            response = client.chat.completions.create(
                model=deployment,
                messages=[
                    {"role": "system", "content": self._SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=2048,
                temperature=0.2,  # Low temperature — deterministic translation
            )
        except APIError as exc:
            raise TranslationError(
                f"Azure OpenAI API call failed: {exc}", exc
            ) from exc
        except Exception as exc:
            raise TranslationError(
                f"Unexpected error calling Azure OpenAI: {exc}", exc
            ) from exc

        if not response.choices:
            raise TranslationError(
                "Azure OpenAI returned a response with no choices."
            )

        message = response.choices[0].message
        if message is None:
            raise TranslationError(
                "Azure OpenAI returned a choice with no message."
            )

        content = message.content
        if not content:
            raise TranslationError("Azure OpenAI returned an empty response.")

        # Strip any accidental markdown fences the model may have added
        content = _strip_code_fences(content)
        return content


def _strip_code_fences(text: str) -> str:
    """Remove ```python ... ``` or ``` ... ``` wrapping if the model added it."""
    lines = text.strip().splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines)
