"""Tests for Azure Functions project generation (Phase A).

1. Script Task stubs now include function.json
2. func_project_generator produces host.json, requirements.txt,
   local.settings.json, .funcignore
3. Import scanning detects third-party packages
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.converters.control_flow.script_task_converter import ScriptTaskConverter
from ssis_adf_agent.generators.func_project_generator import (
    generate_func_project,
    _scan_imports,
    _resolve_packages,
)
from ssis_adf_agent.parsers.models import ScriptTask


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_script(
    name: str = "TestScript",
    source_code: str | None = None,
    ro_vars: list[str] | None = None,
    rw_vars: list[str] | None = None,
) -> ScriptTask:
    # Use moderately complex source so the classifier routes to _convert_to_function
    default_source = (
        "public void Main() {\n"
        "  string connStr = Dts.Connections[\"OLEDB\"].ConnectionString;\n"
        "  using (var conn = new SqlConnection(connStr)) {\n"
        "    conn.Open();\n"
        "    var cmd = conn.CreateCommand();\n"
        "    cmd.CommandText = \"SELECT COUNT(*) FROM Orders\";\n"
        "    int count = (int)cmd.ExecuteScalar();\n"
        "    Dts.Variables[\"User::OrderCount\"].Value = count;\n"
        "  }\n"
        "  Dts.TaskResult = (int)ScriptResults.Success;\n"
        "}"
    )
    return ScriptTask(
        id=f"task-{name}",
        name=name,
        source_code=source_code or default_source,
        read_only_variables=ro_vars or [],
        read_write_variables=rw_vars or ["User::OrderCount"],
    )


# ===================================================================
# 1. Script Task stubs — function.json generation
# ===================================================================

class TestScriptTaskFunctionJson:
    def test_function_json_created(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        converter = ScriptTaskConverter(stubs_output_dir=stubs_dir)
        task = _make_script("MyFunc")
        converter.convert(task, [], {})
        func_json = stubs_dir / "MyFunc" / "function.json"
        assert func_json.exists()

    def test_function_json_structure(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        converter = ScriptTaskConverter(stubs_output_dir=stubs_dir)
        task = _make_script("MyFunc")
        converter.convert(task, [], {})
        data = json.loads((stubs_dir / "MyFunc" / "function.json").read_text())
        assert data["scriptFile"] == "__init__.py"
        assert len(data["bindings"]) == 2
        trigger = data["bindings"][0]
        assert trigger["type"] == "httpTrigger"
        assert trigger["direction"] == "in"
        assert "post" in trigger["methods"]
        output = data["bindings"][1]
        assert output["type"] == "http"
        assert output["direction"] == "out"

    def test_init_py_also_exists(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        converter = ScriptTaskConverter(stubs_output_dir=stubs_dir)
        task = _make_script("MyFunc")
        converter.convert(task, [], {})
        assert (stubs_dir / "MyFunc" / "__init__.py").exists()


# ===================================================================
# 2. Azure Functions project generator
# ===================================================================

class TestGenerateFuncProject:
    def _setup_stubs(self, stubs_dir: Path, func_names: list[str]) -> None:
        """Create minimal function directories with __init__.py."""
        for name in func_names:
            func_dir = stubs_dir / name
            func_dir.mkdir(parents=True)
            (func_dir / "__init__.py").write_text(
                "import json\nimport azure.functions as func\n\n"
                "def main(req): pass\n",
                encoding="utf-8",
            )
            (func_dir / "function.json").write_text("{}", encoding="utf-8")

    def test_generates_all_project_files(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        self._setup_stubs(stubs_dir, ["FuncA", "FuncB"])
        result = generate_func_project(stubs_dir)
        assert "host.json" in result
        assert "requirements.txt" in result
        assert "local.settings.json" in result
        assert ".funcignore" in result

    def test_host_json_content(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        self._setup_stubs(stubs_dir, ["FuncA"])
        generate_func_project(stubs_dir)
        data = json.loads((stubs_dir / "host.json").read_text())
        assert data["version"] == "2.0"
        assert "extensionBundle" in data
        assert data["functionTimeout"] == "00:10:00"

    def test_requirements_includes_azure_functions(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        self._setup_stubs(stubs_dir, ["FuncA"])
        generate_func_project(stubs_dir)
        reqs = (stubs_dir / "requirements.txt").read_text()
        assert "azure-functions" in reqs

    def test_local_settings_content(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        self._setup_stubs(stubs_dir, ["FuncA"])
        generate_func_project(stubs_dir)
        data = json.loads((stubs_dir / "local.settings.json").read_text())
        assert data["Values"]["FUNCTIONS_WORKER_RUNTIME"] == "python"
        assert data["IsEncrypted"] is False

    def test_funcignore_content(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        self._setup_stubs(stubs_dir, ["FuncA"])
        generate_func_project(stubs_dir)
        content = (stubs_dir / ".funcignore").read_text()
        assert "local.settings.json" in content
        assert "__pycache__" in content

    def test_empty_stubs_dir_returns_empty(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        stubs_dir.mkdir()
        assert generate_func_project(stubs_dir) == {}

    def test_nonexistent_dir_returns_empty(self, tmp_path):
        assert generate_func_project(tmp_path / "missing") == {}

    def test_idempotent_no_overwrite(self, tmp_path):
        """Existing project files are not overwritten."""
        stubs_dir = tmp_path / "stubs"
        self._setup_stubs(stubs_dir, ["FuncA"])
        # Write a custom host.json first
        (stubs_dir / "host.json").write_text('{"version":"custom"}')
        result = generate_func_project(stubs_dir)
        assert "host.json" not in result  # skipped because it already existed
        assert json.loads((stubs_dir / "host.json").read_text())["version"] == "custom"


# ===================================================================
# 3. Import scanning
# ===================================================================

class TestScanImports:
    def test_detects_standard_imports(self, tmp_path):
        func_dir = tmp_path / "func1"
        func_dir.mkdir()
        (func_dir / "__init__.py").write_text(
            "import json\nimport logging\nfrom lxml import etree\n"
        )
        imports = _scan_imports([func_dir])
        assert "json" in imports
        assert "lxml" in imports

    def test_detects_dotted_imports(self, tmp_path):
        func_dir = tmp_path / "func1"
        func_dir.mkdir()
        (func_dir / "__init__.py").write_text(
            "import azure.functions as func\n"
        )
        imports = _scan_imports([func_dir])
        assert "azure.functions" in imports
        assert "azure" in imports

    def test_handles_missing_init(self, tmp_path):
        func_dir = tmp_path / "func1"
        func_dir.mkdir()
        # No __init__.py
        imports = _scan_imports([func_dir])
        assert imports == set()


class TestResolvePackages:
    def test_maps_known_packages(self):
        packages = _resolve_packages({"lxml", "azure.functions"})
        pkg_names = {p.split(">=")[0] for p in packages if not p.startswith("#")}
        assert "lxml" in pkg_names
        assert "azure-functions" in pkg_names

    def test_excludes_stdlib(self):
        packages = _resolve_packages({"json", "os", "logging"})
        # Should be empty — all are stdlib
        non_comment = {p for p in packages if not p.startswith("#")}
        assert non_comment == set()

    def test_unknown_package_as_comment(self):
        packages = _resolve_packages({"some_exotic_lib"})
        assert any("some_exotic_lib" in p and p.startswith("#") for p in packages)

    def test_lxml_version_pinned(self):
        packages = _resolve_packages({"lxml"})
        lxml_pkg = [p for p in packages if "lxml" in p and not p.startswith("#")]
        assert len(lxml_pkg) == 1
        assert ">=" in lxml_pkg[0]


# ===================================================================
# 4. End-to-end: Script Task converter → project generator
# ===================================================================

class TestEndToEnd:
    def test_full_pipeline(self, tmp_path):
        """Convert a script task, then run project generator — full deploy-ready output."""
        stubs_dir = tmp_path / "stubs"

        # Convert two script tasks
        converter = ScriptTaskConverter(stubs_output_dir=stubs_dir)
        for name in ["ProcessOrders", "SendNotification"]:
            task = _make_script(name)
            converter.convert(task, [], {})

        # Generate project files
        result = generate_func_project(stubs_dir)
        assert "host.json" in result
        assert "requirements.txt" in result

        # Verify complete project structure
        assert (stubs_dir / "host.json").exists()
        assert (stubs_dir / "requirements.txt").exists()
        assert (stubs_dir / "local.settings.json").exists()
        assert (stubs_dir / ".funcignore").exists()
        assert (stubs_dir / "ProcessOrders" / "__init__.py").exists()
        assert (stubs_dir / "ProcessOrders" / "function.json").exists()
        assert (stubs_dir / "SendNotification" / "__init__.py").exists()
        assert (stubs_dir / "SendNotification" / "function.json").exists()

        # requirements.txt should have azure-functions
        reqs = (stubs_dir / "requirements.txt").read_text()
        assert "azure-functions" in reqs
