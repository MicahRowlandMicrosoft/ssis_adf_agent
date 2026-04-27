"""Deploy a generated Bicep template to an Azure resource group.

Uses the Azure Resource Manager SDK rather than shelling out to ``az``, so the
behavior is consistent across platforms and gives us programmatic access to
deployment outputs. Authentication is via :class:`DefaultAzureCredential`
(``az login`` on a developer machine, or managed identity / service principal
env vars in CI).

Bicep is compiled to ARM JSON via the ``az bicep build`` CLI (the only path
Microsoft officially supports — there is no Python-native Bicep compiler).
If the Azure CLI is not on ``PATH``, the function raises a clear error.
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class BicepCompilerNotFound(RuntimeError):
    """Raised when the ``az`` CLI (or its ``bicep`` extension) is not available."""


def _compile_bicep(bicep_path: Path) -> Path:
    """Compile a .bicep file to ARM JSON. Returns the path to the JSON output."""
    az = shutil.which("az")
    if not az:
        raise BicepCompilerNotFound(
            "The Azure CLI ('az') is required to compile Bicep templates. "
            "Install it from https://aka.ms/azure-cli and ensure it is on PATH."
        )
    json_path = bicep_path.with_suffix(".json")
    cmd = [az, "bicep", "build", "--file", str(bicep_path), "--outfile", str(json_path)]
    logger.info("Compiling Bicep: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, shell=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Bicep compilation failed (exit {result.returncode}):\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return json_path


def deploy_bicep(
    *,
    bicep_source: str | Path,
    subscription_id: str,
    resource_group: str,
    deployment_name: str = "ssis-migration-copilot",
    parameters: dict[str, Any] | None = None,
    dry_run: bool = False,
    location: str | None = None,
) -> dict[str, Any]:
    """Compile a Bicep template and deploy it to ``resource_group``.

    :param bicep_source: Either a path to a ``.bicep`` file, or the Bicep source
        as a string (which will be written to a temp file before compilation).
    :param subscription_id: Azure subscription ID.
    :param resource_group: Target resource group (must already exist).
    :param deployment_name: Name for the ARM deployment record.
    :param parameters: Mapping of Bicep parameter name → value. Wrapped to ARM's
        ``{"name": {"value": ...}}`` shape automatically.
    :param dry_run: If True, compile and validate the deployment but do not
        execute it. Returns the validated deployment shape and outputs (empty).
    :param location: Required if the resource group does not exist (currently
        we do not auto-create it; this parameter is reserved for future use).
    :return: ``{"status": "succeeded"|"validated"|"failed", "outputs": {...},
        "deployment_id": "...", "details": "..."}``
    """
    from azure.mgmt.resource import ResourceManagementClient

    from ..credential import get_credential
    from azure.mgmt.resource.resources.models import (
        Deployment,
        DeploymentMode,
        DeploymentProperties,
    )

    # Materialize Bicep source to a file if it came in as a string
    cleanup_dir: tempfile.TemporaryDirectory | None = None
    if isinstance(bicep_source, str) and not bicep_source.endswith(".bicep"):
        cleanup_dir = tempfile.TemporaryDirectory(prefix="ssis_adf_bicep_")
        bicep_path = Path(cleanup_dir.name) / "main.bicep"
        bicep_path.write_text(bicep_source, encoding="utf-8")
    else:
        bicep_path = Path(bicep_source)

    try:
        json_path = _compile_bicep(bicep_path)
        template = json.loads(json_path.read_text(encoding="utf-8"))

        wrapped_params = {k: {"value": v} for k, v in (parameters or {}).items()}

        credential = get_credential()
        client = ResourceManagementClient(credential, subscription_id)

        deployment = Deployment(
            properties=DeploymentProperties(
                mode=DeploymentMode.INCREMENTAL,
                template=template,
                parameters=wrapped_params,
            )
        )

        if dry_run:
            logger.info("Validating deployment '%s' in %s/%s", deployment_name, subscription_id, resource_group)
            poller = client.deployments.begin_validate(
                resource_group_name=resource_group,
                deployment_name=deployment_name,
                parameters=deployment,
            )
            result = poller.result()
            err = getattr(result, "error", None)
            if err is not None:
                return {
                    "status": "failed",
                    "outputs": {},
                    "deployment_id": None,
                    "details": str(err),
                }
            return {
                "status": "validated",
                "outputs": {},
                "deployment_id": None,
                "details": "Template validated successfully (no resources changed).",
            }

        logger.info("Deploying '%s' to %s/%s", deployment_name, subscription_id, resource_group)
        poller = client.deployments.begin_create_or_update(
            resource_group_name=resource_group,
            deployment_name=deployment_name,
            parameters=deployment,
        )
        result = poller.result()
        outputs_raw = (result.properties.outputs or {}) if result.properties else {}
        outputs = {k: v.get("value") if isinstance(v, dict) else v for k, v in outputs_raw.items()}
        return {
            "status": "succeeded",
            "outputs": outputs,
            "deployment_id": result.id,
            "details": f"Provisioning state: {result.properties.provisioning_state}",
        }
    finally:
        if cleanup_dir is not None:
            cleanup_dir.cleanup()


__all__ = ["BicepCompilerNotFound", "deploy_bicep"]
