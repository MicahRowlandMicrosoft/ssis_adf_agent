"""
H8: non-destructive deploy mode.

When a customer has hand-edited an artifact in ADF Studio (a linked service
credential, a pipeline they tweaked), re-running deploy_to_adf would silently
overwrite the edit because every dispatch path is a put_or_update.

skip_if_exists=True changes that: the deployer probes the target factory
first, and any artifact that already exists is left alone. The DeployResult
records skipped=True and success=True (it isn't a failure, just a no-op).
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from azure.core.exceptions import HttpResponseError

from ssis_adf_agent.deployer.adf_deployer import AdfDeployer


def _make_404_error() -> HttpResponseError:
    """Construct an HttpResponseError that looks like a 404 from azure-core."""
    err = HttpResponseError(message="Not found")
    err.status_code = 404
    return err


@pytest.fixture
def deployer(monkeypatch):
    monkeypatch.setattr(
        "ssis_adf_agent.deployer.adf_deployer.get_credential",
        lambda: MagicMock(),
    )
    d = AdfDeployer(
        subscription_id="sub-fake",
        resource_group="rg-fake",
        factory_name="adf-fake",
    )
    d._client = MagicMock()
    return d


def _write_pipeline(tmp_path: Path, name: str = "MyPipe") -> Path:
    """Build the smallest deploy-able artifact tree containing one pipeline."""
    art_root = tmp_path / "adf"
    pipeline_dir = art_root / "pipeline"
    pipeline_dir.mkdir(parents=True)
    (pipeline_dir / f"{name}.json").write_text(
        json.dumps({
            "name": name,
            "properties": {"activities": [], "annotations": []},
        }),
        encoding="utf-8",
    )
    return art_root


class TestSkipIfExistsFlag:
    def test_default_overwrite_behavior_unchanged(self, deployer, tmp_path):
        """Without skip_if_exists, deploy still calls create_or_update unconditionally."""
        art_root = _write_pipeline(tmp_path)
        results = deployer.deploy_all(art_root, validate_first=False)
        # Should call create_or_update (overwrite) without first probing get().
        deployer._client.pipelines.create_or_update.assert_called_once()
        assert all(r.success for r in results)
        assert all(not r.skipped for r in results)

    def test_skip_if_exists_skips_existing_pipeline(self, deployer, tmp_path):
        art_root = _write_pipeline(tmp_path)
        # The probe returns successfully -> artifact 'exists'.
        deployer._client.pipelines.get.return_value = MagicMock()
        results = deployer.deploy_all(
            art_root, validate_first=False, skip_if_exists=True
        )
        # We must NOT have called create_or_update.
        deployer._client.pipelines.create_or_update.assert_not_called()
        # The result is still success=True but skipped=True.
        assert len(results) == 1
        assert results[0].success is True
        assert results[0].skipped is True

    def test_skip_if_exists_deploys_when_404(self, deployer, tmp_path):
        art_root = _write_pipeline(tmp_path)
        deployer._client.pipelines.get.side_effect = _make_404_error()
        results = deployer.deploy_all(
            art_root, validate_first=False, skip_if_exists=True
        )
        deployer._client.pipelines.create_or_update.assert_called_once()
        assert results[0].success is True
        assert results[0].skipped is False

    def test_skip_if_exists_falls_through_on_unknown_error(self, deployer, tmp_path):
        """Probe failures (not 404) should not block the deploy — surface the real error later."""
        art_root = _write_pipeline(tmp_path)
        # Non-404 error — treat as 'unknown' -> attempt the write.
        unknown_err = HttpResponseError(message="boom")
        unknown_err.status_code = 500
        deployer._client.pipelines.get.side_effect = unknown_err
        results = deployer.deploy_all(
            art_root, validate_first=False, skip_if_exists=True
        )
        deployer._client.pipelines.create_or_update.assert_called_once()
        assert results[0].success is True
        assert results[0].skipped is False
