"""Tests for AdfDeployer — pre-validation gate, retry logic, error collection."""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from ssis_adf_agent.deployer.adf_deployer import (
    AdfDeployer,
    DeployResult,
    _retry_delay,
    _TRANSIENT_STATUS_CODES,
    _DEFAULT_MAX_RETRIES,
    _DEFAULT_BASE_DELAY,
    HttpResponseError,
    ClientAuthenticationError,
    ServiceResponseError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_artifacts(tmp_path: Path, files: dict[str, dict]) -> Path:
    """Create a fake artifacts directory.

    *files* maps ``"subdir/name.json"`` → payload dict.
    """
    for rel_path, payload in files.items():
        p = tmp_path / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload), encoding="utf-8")
    return tmp_path


def _valid_pipeline(name: str = "PL_Test") -> dict:
    return {
        "name": name,
        "properties": {
            "activities": [
                {
                    "name": "Wait1",
                    "type": "Wait",
                    "typeProperties": {"waitTimeInSeconds": 1},
                }
            ]
        },
    }


def _valid_linked_service(name: str = "LS_Test") -> dict:
    return {
        "name": name,
        "properties": {
            "type": "AzureBlobStorage",
            "typeProperties": {"connectionString": "DefaultEndpointsProtocol=https;..."},
        },
    }


def _make_http_error(status_code: int, message: str = "error") -> HttpResponseError:
    exc = HttpResponseError(message)
    exc.status_code = status_code
    return exc


def _make_429_error(retry_after: float = 0.05) -> HttpResponseError:
    exc = HttpResponseError("Too Many Requests")
    exc.status_code = 429
    exc.retry_after_seconds = retry_after
    return exc


# ---------------------------------------------------------------------------
# _retry_delay helper
# ---------------------------------------------------------------------------

class TestRetryDelay:
    def test_exponential_backoff(self):
        assert _retry_delay(0, 2.0) == 2.0
        assert _retry_delay(1, 2.0) == 4.0
        assert _retry_delay(2, 2.0) == 8.0

    def test_respects_retry_after_header(self):
        exc = _make_429_error(retry_after=10.0)
        assert _retry_delay(0, 2.0, exc) == 10.0

    def test_falls_back_without_retry_after(self):
        exc = _make_http_error(500)
        assert _retry_delay(1, 2.0, exc) == 4.0


# ---------------------------------------------------------------------------
# Pre-deployment validation gate
# ---------------------------------------------------------------------------

class TestValidationGate:
    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_invalid_artifacts_skipped(self, tmp_path):
        """Files that fail validation should not be deployed."""
        arts = _make_artifacts(tmp_path, {
            "pipeline/Good.json": _valid_pipeline("Good"),
            "pipeline/Bad.json": {"not_valid": True},  # missing name, properties, activities
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        # Mock validate_artifacts to return issues for Bad.json
        bad_file = str((arts / "pipeline" / "Bad.json").resolve())
        deployer.validate_artifacts = MagicMock(return_value=[
            {"file": bad_file, "error": "Missing top-level 'name' field"},
        ])

        results = deployer.deploy_all(arts, dry_run=True, validate_first=True)

        names = {r.name for r in results}
        assert "Bad" in names
        bad = next(r for r in results if r.name == "Bad")
        assert bad.success is False
        assert "Pre-deploy validation" in (bad.error or "")

        # Good pipeline should still be deployed
        good = next(r for r in results if r.name == "Good")
        assert good.success is True

    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_validation_off(self, tmp_path):
        """When validate_first=False, bad artifacts are not pre-filtered."""
        arts = _make_artifacts(tmp_path, {
            "pipeline/Bad.json": {"not_valid": True},
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        results = deployer.deploy_all(arts, dry_run=True, validate_first=False)

        # No pre-validation failure — just a dry-run success
        assert len(results) == 1
        assert results[0].success is True  # dry_run succeeds regardless of content

    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_all_valid_no_extra_failures(self, tmp_path):
        arts = _make_artifacts(tmp_path, {
            "linkedService/LS1.json": _valid_linked_service("LS1"),
            "pipeline/PL1.json": _valid_pipeline("PL1"),
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        # Mock validate_artifacts returning no issues
        deployer.validate_artifacts = MagicMock(return_value=[])

        results = deployer.deploy_all(arts, dry_run=True, validate_first=True)

        assert all(r.success for r in results)
        assert len(results) == 2


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------

class TestRetryLogic:
    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_retries_on_transient_error(self, tmp_path):
        """Transient 503 should be retried and eventually succeed."""
        arts = _make_artifacts(tmp_path, {
            "pipeline/PL1.json": _valid_pipeline("PL1"),
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        call_count = 0

        def _mock_deploy_pipeline(name, payload):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise _make_http_error(503, "Service Unavailable")

        deployer._deploy_pipeline = _mock_deploy_pipeline

        with patch("ssis_adf_agent.deployer.adf_deployer.time.sleep"):
            results = deployer.deploy_all(
                arts, dry_run=False, validate_first=False,
                max_retries=3, retry_base_delay=0.01,
            )

        assert len(results) == 1
        assert results[0].success is True
        assert results[0].retries == 2  # succeeded on 3rd attempt (0-indexed)

    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_exhaust_retries(self, tmp_path):
        """If all retries exhausted, report failure with retry count."""
        arts = _make_artifacts(tmp_path, {
            "pipeline/PL1.json": _valid_pipeline("PL1"),
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        deployer._deploy_pipeline = MagicMock(
            side_effect=_make_http_error(429, "Throttled")
        )

        with patch("ssis_adf_agent.deployer.adf_deployer.time.sleep"):
            results = deployer.deploy_all(
                arts, dry_run=False, validate_first=False,
                max_retries=2, retry_base_delay=0.01,
            )

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].retries == 2
        assert "429" in (results[0].error or "")

    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_auth_error_no_retry(self, tmp_path):
        """ClientAuthenticationError should not be retried."""
        arts = _make_artifacts(tmp_path, {
            "pipeline/PL1.json": _valid_pipeline("PL1"),
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        deployer._deploy_pipeline = MagicMock(
            side_effect=ClientAuthenticationError("bad creds")
        )

        with patch("ssis_adf_agent.deployer.adf_deployer.time.sleep"):
            results = deployer.deploy_all(
                arts, dry_run=False, validate_first=False,
                max_retries=3, retry_base_delay=0.01,
            )

        assert len(results) == 1
        assert results[0].success is False
        assert results[0].retries == 0  # no retries for auth errors
        assert "Authentication failed" in (results[0].error or "")

    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_connection_error_retried(self, tmp_path):
        """Network-level errors should be retried."""
        arts = _make_artifacts(tmp_path, {
            "linkedService/LS1.json": _valid_linked_service("LS1"),
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        call_count = 0

        def _mock(name, payload):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("Network unreachable")

        deployer._deploy_linked_service = _mock

        with patch("ssis_adf_agent.deployer.adf_deployer.time.sleep"):
            results = deployer.deploy_all(
                arts, dry_run=False, validate_first=False,
                max_retries=2, retry_base_delay=0.01,
            )

        assert results[0].success is True
        assert results[0].retries == 1

    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_non_transient_http_error_not_retried(self, tmp_path):
        """A 400 Bad Request should NOT be retried."""
        arts = _make_artifacts(tmp_path, {
            "pipeline/PL1.json": _valid_pipeline("PL1"),
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        deployer._deploy_pipeline = MagicMock(
            side_effect=_make_http_error(400, "Bad Request")
        )

        with patch("ssis_adf_agent.deployer.adf_deployer.time.sleep"):
            results = deployer.deploy_all(
                arts, dry_run=False, validate_first=False,
                max_retries=3, retry_base_delay=0.01,
            )

        assert results[0].success is False
        assert results[0].retries == 0  # no retries for 400
        assert "400" in (results[0].error or "")

    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_429_respects_retry_after(self, tmp_path):
        """429 response with Retry-After should use that delay."""
        arts = _make_artifacts(tmp_path, {
            "pipeline/PL1.json": _valid_pipeline("PL1"),
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        call_count = 0

        def _mock(name, payload):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise _make_429_error(retry_after=5.0)

        deployer._deploy_pipeline = _mock
        sleep_calls = []

        with patch("ssis_adf_agent.deployer.adf_deployer.time.sleep", side_effect=lambda d: sleep_calls.append(d)):
            results = deployer.deploy_all(
                arts, dry_run=False, validate_first=False,
                max_retries=3, retry_base_delay=1.0,
            )

        assert results[0].success is True
        # Sleep should have used the Retry-After value (5.0), not exponential backoff (1.0)
        assert sleep_calls[0] == 5.0


# ---------------------------------------------------------------------------
# Error collection (multiple artifacts)
# ---------------------------------------------------------------------------

class TestErrorCollection:
    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_collects_all_results(self, tmp_path):
        """Deploy should not stop on first error — collect results from all artifacts."""
        arts = _make_artifacts(tmp_path, {
            "linkedService/LS1.json": _valid_linked_service("LS1"),
            "linkedService/LS2.json": _valid_linked_service("LS2"),
            "pipeline/PL1.json": _valid_pipeline("PL1"),
            "pipeline/PL2.json": _valid_pipeline("PL2"),
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        def _fail_ls2(name, payload):
            if name == "LS2":
                raise _make_http_error(400, "Bad LS")

        deployer._deploy_linked_service = _fail_ls2
        deployer._deploy_pipeline = MagicMock()

        with patch("ssis_adf_agent.deployer.adf_deployer.time.sleep"):
            results = deployer.deploy_all(
                arts, dry_run=False, validate_first=False, max_retries=0,
            )

        assert len(results) == 4
        names_and_status = {(r.name, r.success) for r in results}
        assert ("LS1", True) in names_and_status
        assert ("LS2", False) in names_and_status
        assert ("PL1", True) in names_and_status
        assert ("PL2", True) in names_and_status

    @patch("ssis_adf_agent.deployer.adf_deployer._AZURE_AVAILABLE", True)
    def test_deploy_order_respected(self, tmp_path):
        """Linked services before datasets before pipelines."""
        arts = _make_artifacts(tmp_path, {
            "pipeline/PL.json": _valid_pipeline("PL"),
            "linkedService/LS.json": _valid_linked_service("LS"),
            "dataset/DS.json": {"name": "DS", "properties": {"type": "AzureBlob"}},
        })
        deployer = AdfDeployer.__new__(AdfDeployer)
        deployer.subscription_id = "sub"
        deployer.resource_group = "rg"
        deployer.factory_name = "adf"
        deployer._credential = MagicMock()
        deployer._client = MagicMock()

        results = deployer.deploy_all(arts, dry_run=True, validate_first=False)

        types_order = [r.artifact_type for r in results]
        assert types_order == ["linkedService", "dataset", "pipeline"]


# ---------------------------------------------------------------------------
# DeployResult dataclass
# ---------------------------------------------------------------------------

class TestDeployResult:
    def test_defaults(self):
        r = DeployResult(artifact_type="pipeline", name="PL1", success=True)
        assert r.error is None
        assert r.retries == 0

    def test_with_retries(self):
        r = DeployResult(artifact_type="pipeline", name="PL1", success=True, retries=2)
        assert r.retries == 2
