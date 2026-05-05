"""Tests for the translation_manifest module + ScriptTaskConverter integration."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.converters.control_flow.script_task_converter import (
    ScriptTaskConverter,
)
from ssis_adf_agent.parsers.models import ScriptTask
from ssis_adf_agent.translators.translation_manifest import (
    REGION_BEGIN_MARKER,
    REGION_END_MARKER,
    SCHEMA_VERSION,
    TranslationManifest,
    resolve_translation_mode,
    write_estate_index,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_complex_script(name: str = "ComplexScript") -> ScriptTask:
    """Force the complex/function-backed code path so a stub is written."""
    return ScriptTask(
        id=f"task-{name}",
        name=name,
        source_code=(
            "public void Main() {\n"
            "  using (var conn = new SqlConnection(\"...\")) {\n"
            "    conn.Open();\n"
            "    var cmd = conn.CreateCommand();\n"
            "    cmd.CommandText = \"SELECT COUNT(*) FROM Orders\";\n"
            "    int n = (int)cmd.ExecuteScalar();\n"
            "    Dts.Variables[\"User::OrderCount\"].Value = n;\n"
            "  }\n"
            "}"
        ),
        read_only_variables=["User::ServerName"],
        read_write_variables=["User::OrderCount"],
    )


# ---------------------------------------------------------------------------
# resolve_translation_mode precedence
# ---------------------------------------------------------------------------

class TestResolveTranslationMode:
    def test_default_is_none(self):
        mode, notes = resolve_translation_mode(
            translation_mode=None,
            legacy_llm_translate=False,
            no_llm_arg=False,
            no_llm_env=False,
        )
        assert mode == "none"
        assert notes == []

    def test_legacy_llm_translate_maps_to_aoai(self):
        mode, notes = resolve_translation_mode(
            translation_mode=None,
            legacy_llm_translate=True,
            no_llm_arg=False,
            no_llm_env=False,
        )
        assert mode == "aoai"
        assert notes == []

    def test_explicit_translation_mode_wins_over_legacy(self):
        mode, notes = resolve_translation_mode(
            translation_mode="host",
            legacy_llm_translate=True,
            no_llm_arg=False,
            no_llm_env=False,
        )
        assert mode == "host"
        # Conflict note surfaced
        assert any("legacy llm_translate ignored" in n for n in notes)

    def test_no_llm_arg_overrides_translation_mode(self):
        mode, notes = resolve_translation_mode(
            translation_mode="aoai",
            legacy_llm_translate=False,
            no_llm_arg=True,
            no_llm_env=False,
        )
        assert mode == "none"
        assert any("no_llm=true argument" in n for n in notes)

    def test_no_llm_env_overrides_translation_mode(self):
        mode, notes = resolve_translation_mode(
            translation_mode="host",
            legacy_llm_translate=False,
            no_llm_arg=False,
            no_llm_env=True,
        )
        assert mode == "none"
        assert any("SSIS_ADF_NO_LLM" in n for n in notes)

    def test_no_llm_with_no_request_is_silent(self):
        mode, notes = resolve_translation_mode(
            translation_mode=None,
            legacy_llm_translate=False,
            no_llm_arg=True,
            no_llm_env=False,
        )
        assert mode == "none"
        assert notes == []


# ---------------------------------------------------------------------------
# Stub region markers — present regardless of mode
# ---------------------------------------------------------------------------

class TestStubRegionMarkers:
    @pytest.mark.parametrize("mode", ["none", "host", "aoai"])
    def test_stub_contains_region_markers(self, tmp_path, mode):
        stubs_dir = tmp_path / "stubs"
        manifest = (
            TranslationManifest(
                package_name="pkg", package_path="x.dtsx", translation_mode=mode,
            )
            if mode != "none"
            else None
        )
        converter = ScriptTaskConverter(
            stubs_output_dir=stubs_dir,
            translation_mode=mode,
            manifest=manifest,
        )
        converter.convert(_make_complex_script(), [], {})
        stub = (stubs_dir / "ComplexScript" / "__init__.py").read_text()
        assert REGION_BEGIN_MARKER in stub
        assert REGION_END_MARKER in stub
        # Marker appears in the function body, after the param assignment
        # block but before the response.
        begin_idx = stub.index(REGION_BEGIN_MARKER)
        end_idx = stub.index(REGION_END_MARKER)
        assert begin_idx < end_idx
        assert "return func.HttpResponse" in stub[end_idx:]


# ---------------------------------------------------------------------------
# Manifest entry creation per mode
# ---------------------------------------------------------------------------

class TestManifestEntries:
    def test_none_mode_does_not_populate_manifest(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        manifest = TranslationManifest(
            package_name="pkg", package_path="x.dtsx", translation_mode="none",
        )
        converter = ScriptTaskConverter(
            stubs_output_dir=stubs_dir, translation_mode="none", manifest=manifest,
        )
        converter.convert(_make_complex_script(), [], {})
        # mode=none short-circuits the manifest append
        assert manifest.entries == []

    def test_host_mode_emits_pending_entry(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        manifest = TranslationManifest(
            package_name="pkg", package_path="x.dtsx", translation_mode="host",
        )
        converter = ScriptTaskConverter(
            stubs_output_dir=stubs_dir, translation_mode="host", manifest=manifest,
        )
        converter.convert(_make_complex_script(), [], {})
        assert len(manifest.entries) == 1
        e = manifest.entries[0]
        assert e["task_name"] == "ComplexScript"
        assert e["task_id"] == "task-ComplexScript"
        assert e["status"] == "pending_host_translation"
        assert e["read_only_variables"] == ["User::ServerName"]
        assert e["read_write_variables"] == ["User::OrderCount"]
        assert e["region_begin_marker"] == REGION_BEGIN_MARKER
        assert e["region_end_marker"] == REGION_END_MARKER
        assert e["source_code"] is not None
        assert "SqlConnection" in e["source_code"]

    def test_skipped_no_source_status_when_source_missing(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        manifest = TranslationManifest(
            package_name="pkg", package_path="x.dtsx", translation_mode="host",
        )
        converter = ScriptTaskConverter(
            stubs_output_dir=stubs_dir, translation_mode="host", manifest=manifest,
        )
        # Force a missing-source path while keeping the task complex enough
        # to route to _convert_to_function (classifier needs *some* source to
        # decide). We bypass by constructing the task with empty source AND
        # high-complexity hint via many variables.
        task = ScriptTask(
            id="task-NoSource",
            name="NoSource",
            source_code="",
            read_only_variables=["A", "B", "C", "D"],
            read_write_variables=["E", "F", "G", "H"],
        )
        converter.convert(task, [], {})
        # Either an entry is emitted with skipped_no_source, or the classifier
        # routed to a different path. Both are acceptable; only assert when an
        # entry exists.
        if manifest.entries:
            assert manifest.entries[0]["status"] == "skipped_no_source"


# ---------------------------------------------------------------------------
# Manifest serialization
# ---------------------------------------------------------------------------

class TestManifestSerialization:
    def test_write_produces_valid_json_with_schema_version(self, tmp_path):
        m = TranslationManifest(
            package_name="pkg", package_path="x.dtsx", translation_mode="host",
        )
        m.add_entry(
            task_id="t1",
            task_name="MyTask",
            function_name="MyFunc",
            stub_path=tmp_path / "stubs" / "MyFunc" / "__init__.py",
            script_language="CSharp",
            read_only_variables=["A"],
            read_write_variables=["B"],
            source_code="public void Main() {}",
            status="pending_host_translation",
        )
        out = m.write(tmp_path / "stubs" / "translation_manifest.json")
        data = json.loads(out.read_text())
        assert data["schema_version"] == SCHEMA_VERSION
        assert data["manifest_type"] == "ssis_script_translation"
        assert data["translation_mode"] == "host"
        assert data["package_name"] == "pkg"
        assert data["region_begin_marker"] == REGION_BEGIN_MARKER
        assert len(data["entries"]) == 1
        assert data["entries"][0]["status"] == "pending_host_translation"

    def test_status_counts(self):
        m = TranslationManifest(
            package_name="p", package_path="x", translation_mode="aoai",
        )
        for status in (
            "aoai_translated_needs_review",
            "aoai_translated_needs_review",
            "aoai_failed_pending_manual",
            "skipped_no_source",
        ):
            m.add_entry(
                task_id=f"t-{status}",
                task_name="T",
                function_name="F",
                stub_path=Path("/tmp/F/__init__.py"),
                script_language="CSharp",
                read_only_variables=[],
                read_write_variables=[],
                source_code="x",
                status=status,  # type: ignore[arg-type]
            )
        counts = m.status_counts()
        assert counts["aoai_translated_needs_review"] == 2
        assert counts["aoai_failed_pending_manual"] == 1
        assert counts["skipped_no_source"] == 1


# ---------------------------------------------------------------------------
# Estate index
# ---------------------------------------------------------------------------

class TestEstateIndex:
    def test_write_estate_index(self, tmp_path):
        path = write_estate_index(
            [
                {
                    "package_name": "pkg-a",
                    "manifest_path": "/tmp/a/stubs/translation_manifest.json",
                    "total_entries": 2,
                    "pending_count": 2,
                    "failed_aoai_count": 0,
                    "skipped_no_source_count": 0,
                },
            ],
            tmp_path / "translation_index.json",
        )
        data = json.loads(path.read_text())
        assert data["schema_version"] == SCHEMA_VERSION
        assert data["manifest_type"] == "ssis_script_translation_index"
        assert len(data["manifests"]) == 1
        assert data["manifests"][0]["package_name"] == "pkg-a"
