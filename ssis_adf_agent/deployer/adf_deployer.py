"""
ADF Deployer — deploys generated ADF JSON artifacts to an existing Azure Data Factory
using the azure-mgmt-datafactory SDK.

Authentication uses DefaultAzureCredential (supports:
  - Local: az login
  - CI/CD: AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET env vars
  - Managed Identity: on Azure-hosted compute)

Supported artifact types: pipeline, linkedService, dataset, dataFlow, trigger.
"""
from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.datafactory.models import (
        DatasetResource,
        DataFlowResource,
        LinkedServiceResource,
        PipelineResource,
        TriggerResource,
    )
    from azure.core.exceptions import (
        HttpResponseError,
        ServiceResponseError,
        ClientAuthenticationError,
    )
    _AZURE_AVAILABLE = True
except ImportError:
    _AZURE_AVAILABLE = False

    # Stubs so retry logic can reference the exception types when SDK absent
    class HttpResponseError(Exception): ...  # type: ignore[no-redef]
    class ServiceResponseError(Exception): ...  # type: ignore[no-redef]
    class ClientAuthenticationError(Exception): ...  # type: ignore[no-redef]

# HTTP status codes considered transient (safe to retry)
_TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}

# Default retry configuration
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BASE_DELAY = 2.0  # seconds
_DEFAULT_MAX_DELAY = 60.0  # cap to avoid excessively long waits
_DEFAULT_JITTER = 0.5  # ±50% randomisation


@dataclass
class DeployResult:
    artifact_type: str
    name: str
    success: bool
    error: str | None = None
    retries: int = 0


def _retry_delay(
    attempt: int,
    base_delay: float,
    exc: Exception | None = None,
    *,
    max_delay: float = _DEFAULT_MAX_DELAY,
    jitter: float = _DEFAULT_JITTER,
) -> float:
    """Return the delay (in seconds) before the next retry.

    Respects the ``Retry-After`` header for 429 responses when available,
    otherwise falls back to exponential back-off: *base_delay* × 2^attempt,
    capped at *max_delay* and randomised by +/- *jitter* (0-1 fraction).
    """
    if exc is not None:
        retry_after = getattr(exc, "retry_after_seconds", None)
        if retry_after is not None:
            return float(retry_after)
    delay = min(base_delay * (2 ** attempt), max_delay)
    # Apply jitter: multiply by a random factor in [1 - jitter, 1 + jitter]
    if jitter > 0:
        delay *= 1.0 + random.uniform(-jitter, jitter)
    return max(delay, 0.0)


class AdfDeployer:
    """
    Deploys ADF artifacts (pipelines, linked services, datasets, data flows, triggers)
    from a directory of JSON files to an Azure Data Factory.

    Usage::

        deployer = AdfDeployer(
            subscription_id="...",
            resource_group="rg-myproject",
            factory_name="adf-myproject",
        )
        results = deployer.deploy_all(Path("output/MyPackage"))
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        factory_name: str,
        credential: Any = None,
    ) -> None:
        if not _AZURE_AVAILABLE:
            raise ImportError(
                "azure-mgmt-datafactory and azure-identity are required for AdfDeployer. "
                "Install with: pip install azure-mgmt-datafactory azure-identity"
            )
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self._credential = credential or DefaultAzureCredential()
        self._client: DataFactoryManagementClient | None = None

    @property
    def client(self) -> "DataFactoryManagementClient":
        if self._client is None:
            self._client = DataFactoryManagementClient(
                self._credential, self.subscription_id
            )
        return self._client

    def deploy_all(
        self,
        artifacts_dir: Path,
        dry_run: bool = False,
        *,
        validate_first: bool = True,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        retry_base_delay: float = _DEFAULT_BASE_DELAY,
    ) -> list[DeployResult]:
        """
        Discover and deploy all ADF JSON artifacts in *artifacts_dir*.

        Deployment order: linkedServices → datasets → dataFlows → pipelines → triggers.
        Triggers are left in Stopped state and must be activated manually.

        Args:
            artifacts_dir: Root output directory produced by the generators.
            dry_run: If True, validate files but do not call Azure APIs.
            validate_first: If True (default), run structural validation before
                deploying.  Any validation errors are returned as failed
                DeployResults and the corresponding files are skipped.
            max_retries: Maximum retries for transient Azure API failures
                (429, 500, 502, 503, 504).  Default: 3.
            retry_base_delay: Base delay in seconds between retries
                (exponential back-off).  Default: 2.0.

        Returns:
            List of DeployResult per artifact (including validation failures).
        """
        order = ["linkedService", "dataset", "dataflow", "pipeline", "trigger"]
        results: list[DeployResult] = []

        # --- Pre-deployment validation gate ---
        skip_files: set[str] = set()
        if validate_first:
            issues = self.validate_artifacts(artifacts_dir)
            for issue in issues:
                file_path = issue.get("file", "")
                error_msg = issue.get("error", "Validation failed")
                # Derive artifact type and name from file path
                try:
                    rel = Path(file_path).relative_to(artifacts_dir)
                    a_type = rel.parts[0] if len(rel.parts) > 1 else "unknown"
                    a_name = rel.stem if len(rel.parts) > 1 else Path(file_path).stem
                except (ValueError, IndexError):
                    a_type = "unknown"
                    a_name = Path(file_path).stem
                results.append(DeployResult(
                    artifact_type=a_type, name=a_name, success=False,
                    error=f"Pre-deploy validation: {error_msg}",
                ))
                skip_files.add(str(Path(file_path).resolve()))

        # --- Deploy artifacts in dependency order ---
        for artifact_type in order:
            sub_dir = artifacts_dir / artifact_type
            if not sub_dir.exists():
                continue
            for json_file in sorted(sub_dir.glob("*.json")):
                if str(json_file.resolve()) in skip_files:
                    continue
                result = self._deploy_file(
                    json_file, artifact_type, dry_run,
                    max_retries=max_retries,
                    retry_base_delay=retry_base_delay,
                )
                results.append(result)

        return results

    def _deploy_file(
        self,
        json_file: Path,
        artifact_type: str,
        dry_run: bool,
        *,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        retry_base_delay: float = _DEFAULT_BASE_DELAY,
    ) -> DeployResult:
        name = json_file.stem
        try:
            payload = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            return DeployResult(artifact_type=artifact_type, name=name, success=False,
                                error=f"Failed to read JSON: {exc}")

        if dry_run:
            logger.info("[DRY RUN] Would deploy %s: %s", artifact_type, name)
            return DeployResult(artifact_type=artifact_type, name=name, success=True)

        dispatch = {
            "linkedservice": self._deploy_linked_service,
            "dataset": self._deploy_dataset,
            "dataflow": self._deploy_data_flow,
            "pipeline": self._deploy_pipeline,
            "trigger": self._deploy_trigger,
        }
        fn = dispatch.get(artifact_type.lower())
        if fn is None:
            return DeployResult(artifact_type=artifact_type, name=name, success=False,
                                error=f"Unknown artifact type: {artifact_type}")

        last_error: str | None = None
        for attempt in range(max_retries + 1):
            try:
                fn(name, payload)
                logger.info("Deployed %s: %s", artifact_type, name)
                return DeployResult(
                    artifact_type=artifact_type, name=name, success=True,
                    retries=attempt,
                )
            except ClientAuthenticationError:
                # Auth errors are not transient — fail immediately
                logger.error("Authentication failed deploying %s %s", artifact_type, name)
                return DeployResult(
                    artifact_type=artifact_type, name=name, success=False,
                    error="Authentication failed — check credentials",
                    retries=attempt,
                )
            except HttpResponseError as exc:
                status = getattr(exc, "status_code", None)
                last_error = f"HTTP {status}: {exc}"
                if status in _TRANSIENT_STATUS_CODES and attempt < max_retries:
                    delay = _retry_delay(attempt, retry_base_delay, exc)
                    logger.warning(
                        "Transient error deploying %s %s (HTTP %s), "
                        "retrying in %.1fs (attempt %d/%d)",
                        artifact_type, name, status, delay, attempt + 1, max_retries,
                    )
                    time.sleep(delay)
                    continue
                break
            except (ServiceResponseError, ConnectionError, TimeoutError) as exc:
                last_error = str(exc)
                if attempt < max_retries:
                    delay = _retry_delay(attempt, retry_base_delay)
                    logger.warning(
                        "Connection error deploying %s %s, "
                        "retrying in %.1fs (attempt %d/%d): %s",
                        artifact_type, name, delay, attempt + 1, max_retries, exc,
                    )
                    time.sleep(delay)
                    continue
                break
            except Exception as exc:
                # Non-transient / unknown errors — don't retry
                last_error = str(exc)
                break

        logger.error("Failed to deploy %s %s after %d attempts: %s",
                      artifact_type, name, attempt + 1, last_error)
        return DeployResult(
            artifact_type=artifact_type, name=name, success=False,
            error=last_error, retries=attempt,
        )

    def _deploy_linked_service(self, name: str, payload: dict) -> None:
        resource = LinkedServiceResource(properties=payload.get("properties", payload))
        self.client.linked_services.create_or_update(
            self.resource_group, self.factory_name, name, resource
        )

    def _deploy_dataset(self, name: str, payload: dict) -> None:
        resource = DatasetResource(properties=payload.get("properties", payload))
        self.client.datasets.create_or_update(
            self.resource_group, self.factory_name, name, resource
        )

    def _deploy_data_flow(self, name: str, payload: dict) -> None:
        resource = DataFlowResource(properties=payload.get("properties", payload))
        self.client.data_flows.create_or_update(
            self.resource_group, self.factory_name, name, resource
        )

    def _deploy_pipeline(self, name: str, payload: dict) -> None:
        resource = PipelineResource(**payload.get("properties", payload))
        self.client.pipelines.create_or_update(
            self.resource_group, self.factory_name, name, resource
        )

    def _deploy_trigger(self, name: str, payload: dict) -> None:
        resource = TriggerResource(properties=payload.get("properties", payload))
        self.client.triggers.create_or_update(
            self.resource_group, self.factory_name, name, resource
        )
        # Leave in Stopped state — user must activate manually
        logger.info("Trigger %s deployed in Stopped state. Activate manually in ADF Studio.", name)

    def validate_artifacts(self, artifacts_dir: Path) -> list[dict[str, Any]]:
        """
        Validate JSON files in *artifacts_dir* against basic ADF schema requirements.
        Returns a list of validation issues (empty = all good).
        """
        import jsonschema  # type: ignore[import-untyped]

        issues: list[dict[str, Any]] = []
        for json_file in artifacts_dir.rglob("*.json"):
            try:
                payload = json.loads(json_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                issues.append({
                    "file": str(json_file),
                    "error": f"Invalid JSON: {exc}",
                })
                continue

            # Basic structural checks
            relative = json_file.relative_to(artifacts_dir)
            artifact_type = relative.parts[0].lower() if len(relative.parts) > 1 else "unknown"

            if "name" not in payload:
                issues.append({"file": str(json_file), "error": "Missing top-level 'name' field"})
            if "properties" not in payload:
                issues.append({"file": str(json_file), "error": "Missing top-level 'properties' field"})
            elif artifact_type == "pipeline" and "activities" not in payload.get("properties", {}):
                issues.append({"file": str(json_file), "error": "Pipeline missing 'activities' array"})

        return issues
