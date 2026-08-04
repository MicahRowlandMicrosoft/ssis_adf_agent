"""Translation manifest — work-queue artifact for Script Task → Python translation.

Emitted by ``convert_ssis_package`` whenever ``translation_mode`` is anything
other than ``"none"``. Lives at ``<output_dir>/stubs/translation_manifest.json``
alongside the deterministic stubs the converter already writes.

Two consumers:

* The **host agent** (Copilot/Claude in-session) reads the manifest in
  ``host`` mode, opens each pending stub, and replaces the marked translation
  region with real Python.
* **Tooling / humans** in ``aoai`` mode use the manifest to find tasks where
  the in-process Azure OpenAI call failed and a manual second pass is needed.

The deterministic stub is *always* written first. The manifest is metadata,
not the source of truth — the stub files on disk are.

Schema is versioned (``schema_version``); bump on incompatible changes.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

SCHEMA_VERSION = 1
MANIFEST_TYPE = "ssis_script_translation"

# Region markers embedded in generated stubs; agents replace ONLY the bytes
# between (not including) these lines.  Keep stable — host agents and
# downstream tooling depend on the exact strings.
REGION_BEGIN_MARKER = "# BEGIN SSIS_SCRIPT_TRANSLATION"
REGION_END_MARKER = "# END SSIS_SCRIPT_TRANSLATION"

TranslationMode = Literal["none", "host", "aoai"]

# Lifecycle statuses for a manifest entry.  See docs/conversion/script-task-translation.md
EntryStatus = Literal[
    "pending_host_translation",       # mode=host, awaiting agent edit
    "aoai_translated_needs_review",   # mode=aoai, AOAI succeeded; human review still recommended
    "aoai_failed_pending_manual",     # mode=aoai, AOAI failed; manual port required
    "skipped_no_source",              # source_code missing (encrypted/binary-only)
    "deterministic_stub_only",        # mode=none and an entry was still emitted (rare)
]


class TranslationManifest:
    """Mutable collector. Pass an instance into the script-task converter,
    then ``write()`` it once conversion is done."""

    def __init__(
        self,
        package_name: str,
        package_path: str,
        translation_mode: TranslationMode,
    ) -> None:
        self.package_name = package_name
        self.package_path = package_path
        self.translation_mode: TranslationMode = translation_mode
        self.entries: list[dict[str, Any]] = []

    def add_entry(
        self,
        *,
        task_id: str,
        task_name: str,
        function_name: str,
        stub_path: Path,
        script_language: str,
        read_only_variables: list[str],
        read_write_variables: list[str],
        source_code: str | None,
        status: EntryStatus,
        translation_warning: str = "",
    ) -> None:
        self.entries.append({
            "task_id": task_id,
            "task_name": task_name,
            "function_name": function_name,
            "stub_path": str(stub_path),
            "script_language": script_language,
            "read_only_variables": list(read_only_variables),
            "read_write_variables": list(read_write_variables),
            # Source is stored verbatim — it already lives on disk in the stub
            # comments, so the manifest does not increase exposure surface.
            "source_code": source_code,
            "status": status,
            "translation_warning": translation_warning,
            "region_begin_marker": REGION_BEGIN_MARKER,
            "region_end_marker": REGION_END_MARKER,
        })

    @property
    def has_entries(self) -> bool:
        return bool(self.entries)

    def status_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for entry in self.entries:
            counts[entry["status"]] = counts.get(entry["status"], 0) + 1
        return counts

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "manifest_type": MANIFEST_TYPE,
            "package_name": self.package_name,
            "package_path": self.package_path,
            "translation_mode": self.translation_mode,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "region_begin_marker": REGION_BEGIN_MARKER,
            "region_end_marker": REGION_END_MARKER,
            "entries": self.entries,
        }

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return path


def write_estate_index(
    manifests: list[dict[str, Any]],
    path: Path,
) -> Path:
    """Top-level index emitted by ``convert_estate`` aggregating per-package
    manifests so the host agent can discover them without globbing.

    Each entry: ``package_name``, ``manifest_path``, ``pending_count``,
    ``failed_aoai_count``, ``total_entries``.  Source code is NOT duplicated.
    """
    payload = {
        "schema_version": SCHEMA_VERSION,
        "manifest_type": "ssis_script_translation_index",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "manifests": manifests,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def resolve_translation_mode(
    *,
    translation_mode: str | None,
    legacy_llm_translate: bool,
    no_llm_arg: bool,
    no_llm_env: bool,
) -> tuple[TranslationMode, list[str]]:
    """Apply the documented precedence rules and return (effective_mode, notes).

    Precedence (highest → lowest):
      1. ``no_llm`` arg or ``SSIS_ADF_NO_LLM`` env → forces ``"none"``.
      2. Explicit ``translation_mode`` arg wins over legacy ``llm_translate``.
      3. ``llm_translate=True`` (legacy) maps to ``"aoai"`` when
         ``translation_mode`` is omitted.
      4. Otherwise ``"none"``.

    Notes describe degraded / overridden requests so the caller can surface
    them in the tool response.
    """
    notes: list[str] = []

    if no_llm_arg or no_llm_env:
        if translation_mode and translation_mode != "none":
            reason = "no_llm=true argument" if no_llm_arg else "SSIS_ADF_NO_LLM environment variable"
            notes.append(
                f"translation_mode={translation_mode!r} was requested but is "
                f"overridden by {reason}; effective mode is 'none'."
            )
        if legacy_llm_translate and not (translation_mode and translation_mode != "none"):
            reason = "no_llm=true argument" if no_llm_arg else "SSIS_ADF_NO_LLM environment variable"
            notes.append(
                f"llm_translate=true was requested but is overridden by {reason}; "
                "effective mode is 'none'."
            )
        return "none", notes

    if translation_mode in ("none", "host", "aoai"):
        if legacy_llm_translate and translation_mode != "aoai":
            notes.append(
                f"Both translation_mode={translation_mode!r} and llm_translate=true "
                "were supplied; translation_mode wins (legacy llm_translate ignored)."
            )
        return translation_mode, notes  # type: ignore[return-value]

    if legacy_llm_translate:
        return "aoai", notes

    return "none", notes
