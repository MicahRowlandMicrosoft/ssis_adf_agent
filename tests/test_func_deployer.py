"""Tests for Azure Functions deployer (Phase B).

Tests cover:
1. Stubs directory validation
2. Zip building (content, exclusions, function discovery)
3. Dry-run mode
4. Error handling (missing SDKs, bad credentials, HTTP errors)
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ssis_adf_agent.deployer.func_deployer import (
    FuncDeployer,
    FuncDeployResult,
    _build_zip,
    _validate_stubs_dir,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_func_project(stubs_dir: Path, func_names: list[str]) -> None:
    """Create a minimal Azure Functions project structure."""
    stubs_dir.mkdir(parents=True, exist_ok=True)
    (stubs_dir / "host.json").write_text(json.dumps({"version": "2.0"}))
    (stubs_dir / "requirements.txt").write_text("azure-functions>=1.17.0\n")
    (stubs_dir / "local.settings.json").write_text(json.dumps({
        "IsEncrypted": False,
        "Values": {"FUNCTIONS_WORKER_RUNTIME": "python"},
    }))
    for name in func_names:
        func_dir = stubs_dir / name
        func_dir.mkdir()
        (func_dir / "__init__.py").write_text(
            f"import azure.functions as func\n\ndef main(req): pass  # {name}\n"
        )
        (func_dir / "function.json").write_text(json.dumps({
            "scriptFile": "__init__.py",
            "bindings": [
                {"type": "httpTrigger", "direction": "in", "name": "req"},
                {"type": "http", "direction": "out", "name": "$return"},
            ],
        }))


# ===================================================================
# 1. Stubs directory validation
# ===================================================================

class TestValidateStubsDir:
    def test_valid_project(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA"])
        assert _validate_stubs_dir(stubs_dir) == []

    def test_missing_directory(self, tmp_path):
        issues = _validate_stubs_dir(tmp_path / "nonexistent")
        assert len(issues) == 1
        assert "does not exist" in issues[0]

    def test_missing_host_json(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA"])
        (stubs_dir / "host.json").unlink()
        issues = _validate_stubs_dir(stubs_dir)
        assert any("host.json" in i for i in issues)

    def test_missing_requirements(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA"])
        (stubs_dir / "requirements.txt").unlink()
        issues = _validate_stubs_dir(stubs_dir)
        assert any("requirements.txt" in i for i in issues)

    def test_no_function_dirs(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        stubs_dir.mkdir()
        (stubs_dir / "host.json").write_text("{}")
        (stubs_dir / "requirements.txt").write_text("")
        issues = _validate_stubs_dir(stubs_dir)
        assert any("No function directories" in i for i in issues)

    def test_function_without_function_json(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        stubs_dir.mkdir()
        (stubs_dir / "host.json").write_text("{}")
        (stubs_dir / "requirements.txt").write_text("")
        func_dir = stubs_dir / "Broken"
        func_dir.mkdir()
        (func_dir / "__init__.py").write_text("pass")
        # No function.json
        issues = _validate_stubs_dir(stubs_dir)
        assert any("No function directories" in i for i in issues)


# ===================================================================
# 2. Zip building
# ===================================================================

class TestBuildZip:
    def test_includes_project_files(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA"])
        zip_bytes, func_names = _build_zip(stubs_dir)
        assert len(zip_bytes) > 0
        assert "FuncA" in func_names

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            assert "host.json" in names
            assert "requirements.txt" in names
            assert "FuncA/__init__.py" in names
            assert "FuncA/function.json" in names

    def test_excludes_local_settings(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA"])
        zip_bytes, _ = _build_zip(stubs_dir)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            assert "local.settings.json" not in zf.namelist()

    def test_excludes_pycache(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA"])
        cache_dir = stubs_dir / "FuncA" / "__pycache__"
        cache_dir.mkdir()
        (cache_dir / "module.cpython-311.pyc").write_bytes(b"\x00")
        zip_bytes, _ = _build_zip(stubs_dir)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            assert not any("__pycache__" in n for n in zf.namelist())
            assert not any(".pyc" in n for n in zf.namelist())

    def test_excludes_venv(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA"])
        venv_dir = stubs_dir / ".venv" / "lib"
        venv_dir.mkdir(parents=True)
        (venv_dir / "site.py").write_text("pass")
        zip_bytes, _ = _build_zip(stubs_dir)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            assert not any(".venv" in n for n in zf.namelist())

    def test_multiple_functions(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA", "FuncB", "FuncC"])
        zip_bytes, func_names = _build_zip(stubs_dir)
        assert sorted(func_names) == ["FuncA", "FuncB", "FuncC"]

    def test_empty_project(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        stubs_dir.mkdir()
        (stubs_dir / "host.json").write_text("{}")
        zip_bytes, func_names = _build_zip(stubs_dir)
        assert func_names == []
        assert len(zip_bytes) > 0  # zip header exists even if empty


# ===================================================================
# 3. Dry-run mode
# ===================================================================

class TestDryRun:
    @patch("ssis_adf_agent.deployer.func_deployer._AZURE_WEB_AVAILABLE", True)
    @patch("ssis_adf_agent.deployer.func_deployer._HTTPX_AVAILABLE", True)
    def test_dry_run_returns_zip_info(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["OrderProcessor", "NotifySender"])
        deployer = FuncDeployer.__new__(FuncDeployer)
        deployer.subscription_id = "sub-123"
        deployer.resource_group = "rg-test"
        deployer.function_app_name = "func-test"
        deployer._credential = MagicMock()
        deployer._web_client = None

        result = deployer.deploy(stubs_dir, dry_run=True)
        assert result.success is True
        assert sorted(result.functions_deployed) == ["NotifySender", "OrderProcessor"]
        assert result.zip_size_bytes > 0
        assert "DRY RUN" in result.error

    @patch("ssis_adf_agent.deployer.func_deployer._AZURE_WEB_AVAILABLE", True)
    @patch("ssis_adf_agent.deployer.func_deployer._HTTPX_AVAILABLE", True)
    def test_dry_run_fails_on_invalid_project(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        stubs_dir.mkdir()  # Empty — no host.json
        deployer = FuncDeployer.__new__(FuncDeployer)
        deployer.subscription_id = "sub-123"
        deployer.resource_group = "rg-test"
        deployer.function_app_name = "func-test"
        deployer._credential = MagicMock()
        deployer._web_client = None

        result = deployer.deploy(stubs_dir, dry_run=True)
        assert result.success is False
        assert "Validation failed" in result.error


# ===================================================================
# 4. Error handling
# ===================================================================

class TestErrorHandling:
    def test_missing_azure_web_sdk(self):
        with patch("ssis_adf_agent.deployer.func_deployer._AZURE_WEB_AVAILABLE", False):
            with pytest.raises(ImportError, match="azure-mgmt-web"):
                FuncDeployer("sub", "rg", "func")

    def test_missing_httpx(self):
        with patch("ssis_adf_agent.deployer.func_deployer._AZURE_WEB_AVAILABLE", True):
            with patch("ssis_adf_agent.deployer.func_deployer._HTTPX_AVAILABLE", False):
                with pytest.raises(ImportError, match="httpx"):
                    FuncDeployer("sub", "rg", "func")

    @patch("ssis_adf_agent.deployer.func_deployer._AZURE_WEB_AVAILABLE", True)
    @patch("ssis_adf_agent.deployer.func_deployer._HTTPX_AVAILABLE", True)
    def test_auth_failure(self, tmp_path):
        stubs_dir = tmp_path / "stubs"
        _make_func_project(stubs_dir, ["FuncA"])

        deployer = FuncDeployer.__new__(FuncDeployer)
        deployer.subscription_id = "sub-123"
        deployer.resource_group = "rg-test"
        deployer.function_app_name = "func-test"
        deployer._credential = MagicMock()

        mock_client = MagicMock()
        from ssis_adf_agent.deployer.func_deployer import ClientAuthenticationError
        mock_client.web_apps.list_publishing_credentials.return_value.result.side_effect = (
            ClientAuthenticationError("No credentials")
        )
        deployer._web_client = mock_client

        result = deployer.deploy(stubs_dir)
        assert result.success is False
        assert "Authentication failed" in result.error

    @patch("ssis_adf_agent.deployer.func_deployer._AZURE_WEB_AVAILABLE", True)
    @patch("ssis_adf_agent.deployer.func_deployer._HTTPX_AVAILABLE", True)
    def test_deploy_result_dataclass(self):
        r = FuncDeployResult(
            success=True,
            function_app_name="myapp",
            functions_deployed=["A", "B"],
            zip_size_bytes=1024,
        )
        assert r.success
        assert r.error is None
        assert r.scm_url is None
        assert len(r.functions_deployed) == 2
