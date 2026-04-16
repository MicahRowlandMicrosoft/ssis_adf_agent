"""
Azure Functions deployer — zip-deploys generated function stubs to an
existing Azure Function App.

This module handles:
1. Packaging the stubs directory into a deployment zip
2. Fetching publish credentials from the Function App
3. Uploading via the Kudu /api/zipdeploy endpoint

Prerequisites:
- An existing Azure Function App (Python runtime, Consumption or Premium)
- DefaultAzureCredential with Contributor or Website Contributor role
- ``azure-mgmt-web`` for fetching publish credentials

Usage::

    deployer = FuncDeployer(
        subscription_id="...",
        resource_group="rg-myproject",
        function_app_name="func-myproject-stubs",
    )
    result = deployer.deploy(stubs_dir=Path("output/stubs"))
"""
from __future__ import annotations

import io
import json
import logging
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.web import WebSiteManagementClient
    from azure.core.exceptions import (
        HttpResponseError,
        ClientAuthenticationError,
    )
    _AZURE_WEB_AVAILABLE = True
except ImportError:
    _AZURE_WEB_AVAILABLE = False

    class HttpResponseError(Exception): ...  # type: ignore[no-redef]
    class ClientAuthenticationError(Exception): ...  # type: ignore[no-redef]

try:
    import httpx
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False

# Files to exclude from the deployment zip
_EXCLUDE_PATTERNS = frozenset({
    "local.settings.json",
    ".venv",
    "__pycache__",
    ".git",
    ".gitignore",
})


@dataclass
class FuncDeployResult:
    """Result of a Function App deployment."""
    success: bool
    function_app_name: str
    functions_deployed: list[str] = field(default_factory=list)
    zip_size_bytes: int = 0
    error: str | None = None
    scm_url: str | None = None


def _validate_stubs_dir(stubs_dir: Path) -> list[str]:
    """Check that *stubs_dir* looks like a valid Azure Functions project.

    Returns a list of issues (empty = valid).
    """
    issues: list[str] = []
    if not stubs_dir.exists():
        issues.append(f"Stubs directory does not exist: {stubs_dir}")
        return issues
    if not (stubs_dir / "host.json").exists():
        issues.append("Missing host.json — run convert first or generate with func_project_generator")
    if not (stubs_dir / "requirements.txt").exists():
        issues.append("Missing requirements.txt — run convert first or generate with func_project_generator")

    # Check for at least one function
    func_dirs = [
        d for d in stubs_dir.iterdir()
        if d.is_dir() and (d / "__init__.py").exists() and (d / "function.json").exists()
    ]
    if not func_dirs:
        issues.append("No function directories found (need __init__.py + function.json)")
    return issues


def _build_zip(stubs_dir: Path) -> tuple[bytes, list[str]]:
    """Create an in-memory zip of the stubs directory.

    Returns (zip_bytes, list_of_function_names).
    Excludes files matching ``_EXCLUDE_PATTERNS``.
    """
    buf = io.BytesIO()
    func_names: list[str] = []

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(stubs_dir.rglob("*")):
            if not file_path.is_file():
                continue
            # Check exclusions
            rel = file_path.relative_to(stubs_dir)
            parts = rel.parts
            if any(p in _EXCLUDE_PATTERNS for p in parts):
                continue
            if file_path.suffix == ".pyc":
                continue

            zf.write(file_path, rel.as_posix())

            # Track function names
            if file_path.name == "__init__.py" and len(parts) == 2:
                func_names.append(parts[0])

    return buf.getvalue(), func_names


class FuncDeployer:
    """Deploy Azure Function stubs to an existing Function App via zip deploy."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        function_app_name: str,
        credential: Any = None,
    ) -> None:
        if not _AZURE_WEB_AVAILABLE:
            raise ImportError(
                "azure-mgmt-web is required for FuncDeployer. "
                "Install with: pip install azure-mgmt-web"
            )
        if not _HTTPX_AVAILABLE:
            raise ImportError(
                "httpx is required for FuncDeployer. "
                "Install with: pip install httpx"
            )
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.function_app_name = function_app_name
        self._credential = credential or DefaultAzureCredential()
        self._web_client: Any = None

    @property
    def web_client(self) -> "WebSiteManagementClient":
        if self._web_client is None:
            self._web_client = WebSiteManagementClient(
                self._credential, self.subscription_id
            )
        return self._web_client

    def deploy(
        self,
        stubs_dir: Path,
        *,
        dry_run: bool = False,
    ) -> FuncDeployResult:
        """Zip-deploy the stubs directory to the Function App.

        Args:
            stubs_dir: Path to the stubs directory (must contain host.json,
                requirements.txt, and function directories).
            dry_run: If True, build the zip and validate but don't upload.

        Returns:
            FuncDeployResult with deployment status.
        """
        # Validate
        issues = _validate_stubs_dir(stubs_dir)
        if issues:
            return FuncDeployResult(
                success=False,
                function_app_name=self.function_app_name,
                error=f"Validation failed: {'; '.join(issues)}",
            )

        # Build zip
        zip_bytes, func_names = _build_zip(stubs_dir)
        logger.info(
            "Built deployment zip: %d bytes, %d functions: %s",
            len(zip_bytes), len(func_names), func_names,
        )

        if dry_run:
            return FuncDeployResult(
                success=True,
                function_app_name=self.function_app_name,
                functions_deployed=func_names,
                zip_size_bytes=len(zip_bytes),
                error="[DRY RUN] Zip built but not uploaded.",
            )

        # Get publish credentials
        try:
            creds = self.web_client.web_apps.list_publishing_credentials(
                self.resource_group, self.function_app_name
            ).result()
            scm_url = f"https://{self.function_app_name}.scm.azurewebsites.net"
            username = creds.publishing_user_name
            password = creds.publishing_password
        except ClientAuthenticationError as exc:
            return FuncDeployResult(
                success=False,
                function_app_name=self.function_app_name,
                error=f"Authentication failed: {exc}. Run 'az login' or set AZURE_* env vars.",
            )
        except HttpResponseError as exc:
            return FuncDeployResult(
                success=False,
                function_app_name=self.function_app_name,
                error=f"Failed to get publish credentials: {exc}",
            )

        # Zip deploy via Kudu
        deploy_url = f"{scm_url}/api/zipdeploy"
        try:
            resp = httpx.post(
                deploy_url,
                content=zip_bytes,
                auth=(username, password),
                headers={"Content-Type": "application/zip"},
                timeout=300.0,
            )
            if resp.status_code in (200, 202):
                logger.info("Zip deploy succeeded: %s", resp.status_code)
                return FuncDeployResult(
                    success=True,
                    function_app_name=self.function_app_name,
                    functions_deployed=func_names,
                    zip_size_bytes=len(zip_bytes),
                    scm_url=scm_url,
                )
            else:
                body = resp.text[:500]
                return FuncDeployResult(
                    success=False,
                    function_app_name=self.function_app_name,
                    functions_deployed=func_names,
                    zip_size_bytes=len(zip_bytes),
                    error=f"Zip deploy failed (HTTP {resp.status_code}): {body}",
                    scm_url=scm_url,
                )
        except httpx.HTTPError as exc:
            return FuncDeployResult(
                success=False,
                function_app_name=self.function_app_name,
                error=f"HTTP error during zip deploy: {exc}",
            )
