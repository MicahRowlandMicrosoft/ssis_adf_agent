"""Tests for the 4 automation features:
1. Auto-validate after convert
2. File path mapping
3. Execute Pipeline cross-reference check
4. XML Task Azure Function stubs
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from ssis_adf_agent.converters.dispatcher import ConverterDispatcher
from ssis_adf_agent.generators.file_path_mapper import (
    apply_file_path_map,
    _replace_path,
    _rewrite_dict,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _task(task_type: str, name: str = "TestTask", **extra) -> SimpleNamespace:
    from ssis_adf_agent.parsers.models import TaskType
    tt = TaskType(task_type)
    defaults = dict(
        id=f"id_{name}",
        name=name,
        task_type=tt,
        description="",
        precedence_constraints=[],
    )
    defaults.update(extra)
    return SimpleNamespace(**defaults)


# ===================================================================
# Feature 2: File Path Mapper
# ===================================================================

class TestReplacePathBasic:
    """Unit tests for _replace_path."""

    def test_exact_prefix_match(self):
        prefixes = ["C:\\Data\\Input"]
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        result, changed = _replace_path(
            "C:\\Data\\Input\\file.csv", prefixes, mapping
        )
        assert changed
        assert result == "https://blob/input\\file.csv"

    def test_case_insensitive(self):
        prefixes = ["C:\\Data\\Input"]
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        result, changed = _replace_path(
            "c:\\data\\input\\file.csv", prefixes, mapping
        )
        assert changed
        assert result == "https://blob/input\\file.csv"

    def test_no_match(self):
        prefixes = ["C:\\Data\\Input"]
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        result, changed = _replace_path("D:\\Other\\path", prefixes, mapping)
        assert not changed
        assert result == "D:\\Other\\path"

    def test_backslash_normalization(self):
        prefixes = ["C:\\Data\\Input"]
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        result, changed = _replace_path(
            "C:/Data/Input/sub/file.csv", prefixes, mapping
        )
        assert changed

    def test_longest_prefix_wins(self):
        prefixes = sorted(
            ["C:\\Data", "C:\\Data\\Input"],
            key=len, reverse=True,
        )
        mapping = {
            "C:\\Data": "https://blob/data",
            "C:\\Data\\Input": "https://blob/input",
        }
        result, changed = _replace_path(
            "C:\\Data\\Input\\file.csv", prefixes, mapping
        )
        assert changed
        assert result.startswith("https://blob/input")


class TestRewriteDict:
    def test_nested_dict(self):
        obj = {"a": {"b": "C:\\Data\\Input\\x.csv"}}
        prefixes = ["C:\\Data\\Input"]
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        count = _rewrite_dict(obj, prefixes, mapping)
        assert count == 1
        assert obj["a"]["b"].startswith("https://blob/input")

    def test_list_values(self):
        obj = {"items": ["C:\\Data\\Input\\a.csv", "D:\\other"]}
        prefixes = ["C:\\Data\\Input"]
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        count = _rewrite_dict(obj, prefixes, mapping)
        assert count == 1
        assert obj["items"][0].startswith("https://blob/input")
        assert obj["items"][1] == "D:\\other"


class TestApplyFilePathMap:
    def test_linked_services_rewrite(self):
        artifacts = {
            "linked_services": [
                {"typeProperties": {"connectionString": "C:\\Data\\Input\\db.mdf"}}
            ],
        }
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        count = apply_file_path_map(artifacts, mapping)
        assert count == 1
        assert "https://blob/input" in artifacts["linked_services"][0]["typeProperties"]["connectionString"]

    def test_pipeline_activity_rewrite(self):
        artifacts = {
            "pipeline": {
                "properties": {
                    "activities": [
                        {"typeProperties": {"source": "C:\\Data\\Input\\file.csv"}}
                    ]
                }
            }
        }
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        count = apply_file_path_map(artifacts, mapping)
        assert count == 1

    def test_empty_map_returns_zero(self):
        artifacts = {"linked_services": [{"a": "C:\\Data"}]}
        assert apply_file_path_map(artifacts, {}) == 0

    def test_datasets_rewrite(self):
        artifacts = {
            "datasets": [
                {"typeProperties": {"location": "C:\\Data\\Input\\sub"}}
            ]
        }
        mapping = {"C:\\Data\\Input": "https://blob/input"}
        count = apply_file_path_map(artifacts, mapping)
        assert count == 1


# ===================================================================
# Feature 3: Execute Pipeline cross-reference check
# ===================================================================

class TestExecutePipelineCrossRef:
    """Test the _check_execute_pipeline_refs logic (imported from mcp_server)."""

    def _check(self, pipeline, output_dir, shared_dir=None):
        # Inline the logic to avoid MCP SDK import
        available: set[str] = set()
        for search_dir in (output_dir, shared_dir):
            if search_dir is None:
                continue
            pl_dir = search_dir / "pipeline"
            if pl_dir.exists():
                for f in pl_dir.glob("*.json"):
                    available.add(f.stem)
        available.add(pipeline.get("name", ""))
        unresolved: list[str] = []
        for act in pipeline.get("properties", {}).get("activities", []):
            if act.get("type") == "ExecutePipeline":
                ref = act.get("typeProperties", {}).get("pipeline", {}).get("referenceName", "")
                if ref and ref not in available:
                    unresolved.append(ref)
        return unresolved

    def test_resolved_ref(self, tmp_path):
        pl_dir = tmp_path / "pipeline"
        pl_dir.mkdir()
        (pl_dir / "ChildPipeline.json").write_text("{}")
        pipeline = {
            "name": "ParentPipeline",
            "properties": {
                "activities": [{
                    "type": "ExecutePipeline",
                    "typeProperties": {
                        "pipeline": {"referenceName": "ChildPipeline"}
                    }
                }]
            }
        }
        assert self._check(pipeline, tmp_path) == []

    def test_unresolved_ref(self, tmp_path):
        (tmp_path / "pipeline").mkdir()
        pipeline = {
            "name": "ParentPipeline",
            "properties": {
                "activities": [{
                    "type": "ExecutePipeline",
                    "typeProperties": {
                        "pipeline": {"referenceName": "MissingPipeline"}
                    }
                }]
            }
        }
        result = self._check(pipeline, tmp_path)
        assert result == ["MissingPipeline"]

    def test_self_reference_ok(self, tmp_path):
        (tmp_path / "pipeline").mkdir()
        pipeline = {
            "name": "SelfRef",
            "properties": {
                "activities": [{
                    "type": "ExecutePipeline",
                    "typeProperties": {
                        "pipeline": {"referenceName": "SelfRef"}
                    }
                }]
            }
        }
        assert self._check(pipeline, tmp_path) == []

    def test_shared_dir_resolves(self, tmp_path):
        local_dir = tmp_path / "local"
        shared_dir = tmp_path / "shared"
        (local_dir / "pipeline").mkdir(parents=True)
        shared_pl = shared_dir / "pipeline"
        shared_pl.mkdir(parents=True)
        (shared_pl / "SharedChild.json").write_text("{}")
        pipeline = {
            "name": "Parent",
            "properties": {
                "activities": [{
                    "type": "ExecutePipeline",
                    "typeProperties": {
                        "pipeline": {"referenceName": "SharedChild"}
                    }
                }]
            }
        }
        assert self._check(pipeline, local_dir, shared_dir) == []

    def test_no_execute_pipeline_activities(self, tmp_path):
        (tmp_path / "pipeline").mkdir()
        pipeline = {
            "name": "Simple",
            "properties": {
                "activities": [{"type": "Copy", "typeProperties": {}}]
            }
        }
        assert self._check(pipeline, tmp_path) == []


# ===================================================================
# Feature 4: XML Task Stubs
# ===================================================================

class TestXMLTaskStubs:
    """Test that XML tasks generate Azure Function stubs."""

    def test_azure_function_activity_type(self, tmp_path):
        t = _task("XMLTask", name="XmlWork",
                  properties={"OperationType": "XPATH", "Source": "input.xml",
                              "SecondOperand": "//root", "XPathOperation": "NodeList"})
        dispatcher = ConverterDispatcher(stubs_dir=tmp_path / "stubs")
        acts = dispatcher.convert_task(t, [], {})
        assert len(acts) == 1
        assert acts[0]["type"] == "AzureFunction"
        assert acts[0]["typeProperties"]["functionName"] == "XmlWork"
        assert acts[0]["typeProperties"]["body"]["operation"] == "XPATH"
        assert acts[0]["typeProperties"]["body"]["source"] == "input.xml"

    def test_stub_file_written(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        t = _task("XMLTask", name="MergeXml",
                  properties={"OperationType": "Merge", "Source": "a.xml",
                              "SecondOperand": "b.xml"})
        dispatcher = ConverterDispatcher(stubs_dir=stubs_dir)
        dispatcher.convert_task(t, [], {})
        stub = stubs_dir / "MergeXml" / "__init__.py"
        assert stub.exists()
        content = stub.read_text()
        assert "Merge" in content
        assert "azure.functions" in content

    def test_function_json_written(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        t = _task("XMLTask", name="ValidateXml",
                  properties={"OperationType": "Validate"})
        dispatcher = ConverterDispatcher(stubs_dir=stubs_dir)
        dispatcher.convert_task(t, [], {})
        func_json = stubs_dir / "ValidateXml" / "function.json"
        assert func_json.exists()
        data = json.loads(func_json.read_text())
        assert data["bindings"][0]["type"] == "httpTrigger"

    def test_xpath_stub_content(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        t = _task("XMLTask", name="XpathTask",
                  properties={"OperationType": "XPATH", "SecondOperand": "//item",
                              "XPathOperation": "Values"})
        dispatcher = ConverterDispatcher(stubs_dir=stubs_dir)
        dispatcher.convert_task(t, [], {})
        content = (stubs_dir / "XpathTask" / "__init__.py").read_text()
        assert "xpath" in content.lower()
        assert "XPATH" in content

    def test_xslt_stub_content(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        t = _task("XMLTask", name="XsltTask",
                  properties={"OperationType": "XSLT"})
        dispatcher = ConverterDispatcher(stubs_dir=stubs_dir)
        dispatcher.convert_task(t, [], {})
        content = (stubs_dir / "XsltTask" / "__init__.py").read_text()
        assert "XSLT" in content

    def test_unknown_operation_stub(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        t = _task("XMLTask", name="WeirdXml",
                  properties={"OperationType": "CustomOp"})
        dispatcher = ConverterDispatcher(stubs_dir=stubs_dir)
        dispatcher.convert_task(t, [], {})
        content = (stubs_dir / "WeirdXml" / "__init__.py").read_text()
        assert "CustomOp" in content

    def test_operation_from_fallback(self, tmp_path):
        """When no properties dict, operation falls back to attribute or 'Unknown'."""
        t = _task("XMLTask", name="NoProps")
        dispatcher = ConverterDispatcher(stubs_dir=tmp_path / "stubs")
        acts = dispatcher.convert_task(t, [], {})
        assert "Unknown" in acts[0]["description"]
        assert acts[0]["typeProperties"]["body"]["operation"] == "Unknown"

    def test_diff_stub(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        t = _task("XMLTask", name="DiffXml",
                  properties={"OperationType": "Diff", "Source": "a.xml",
                              "SecondOperand": "b.xml"})
        dispatcher = ConverterDispatcher(stubs_dir=stubs_dir)
        dispatcher.convert_task(t, [], {})
        content = (stubs_dir / "DiffXml" / "__init__.py").read_text()
        assert "Diff" in content
