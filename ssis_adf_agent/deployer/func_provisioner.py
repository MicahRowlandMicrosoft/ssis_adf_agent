"""
Azure Function App provisioner — creates the Azure infrastructure required to
host generated function stubs.

Creates:
1. Storage Account (required by Azure Functions runtime)
2. Application Insights (optional, for monitoring)
3. App Service Plan (Consumption / Y1)
4. Function App (Python 3.11, Linux)

All resources are created in the same resource group and location.
Uses ``DefaultAzureCredential`` for authentication.

Usage::

    provisioner = FuncProvisioner(
        subscription_id="...",
        resource_group="rg-myproject",
        location="eastus2",
    )
    result = provisioner.provision(
        function_app_name="func-myproject-stubs",
    )
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

try:
    from azure.core.exceptions import (
        ClientAuthenticationError,
        HttpResponseError,
        ResourceExistsError,
    )
    from azure.identity import DefaultAzureCredential
    from azure.mgmt.applicationinsights import ApplicationInsightsManagementClient
    from azure.mgmt.applicationinsights.models import (
        ApplicationInsightsComponent,
    )
    from azure.mgmt.storage import StorageManagementClient
    from azure.mgmt.storage.models import (
        Kind as StorageKind,
    )
    from azure.mgmt.storage.models import (
        Sku as StorageSku,
    )
    from azure.mgmt.storage.models import (
        StorageAccountCreateParameters,
    )
    from azure.mgmt.web import WebSiteManagementClient
    from azure.mgmt.web.models import (
        AppServicePlan,
        NameValuePair,
        Site,
        SiteConfig,
        SkuDescription,
    )
    _AZURE_AVAILABLE = True
except ImportError:
    _AZURE_AVAILABLE = False

    class HttpResponseError(Exception):  # type: ignore[no-redef]
        def __init__(self, message="", **kwargs):
            self.message = message
            super().__init__(message)
    class ClientAuthenticationError(Exception): ...  # type: ignore[no-redef]
    class ResourceExistsError(Exception): ...  # type: ignore[no-redef]

    # Stub model classes so methods can reference them when mocked
    class StorageAccountCreateParameters:  # type: ignore[no-redef]
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class StorageSku:  # type: ignore[no-redef]
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class StorageKind:  # type: ignore[no-redef]
        STORAGE_V2 = "StorageV2"
    class AppServicePlan:  # type: ignore[no-redef]
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class Site:  # type: ignore[no-redef]
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class SiteConfig:  # type: ignore[no-redef]
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class NameValuePair:  # type: ignore[no-redef]
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class SkuDescription:  # type: ignore[no-redef]
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
    class ApplicationInsightsComponent:  # type: ignore[no-redef]
        def __init__(self, **kwargs): self.__dict__.update(kwargs)


# Storage account names: 3-24 chars, lowercase letters + digits only
_STORAGE_NAME_RE = re.compile(r"^[a-z0-9]{3,24}$")


@dataclass
class ProvisionResult:
    """Result of provisioning Azure infrastructure."""
    success: bool
    function_app_name: str
    resource_group: str
    location: str
    storage_account_name: str | None = None
    app_insights_name: str | None = None
    app_service_plan_name: str | None = None
    function_app_url: str | None = None
    resources_created: list[str] = field(default_factory=list)
    error: str | None = None


def _derive_storage_name(function_app_name: str) -> str:
    """Derive a valid storage account name from the function app name.

    Storage accounts: 3-24 chars, lowercase alphanumeric only.
    """
    base = re.sub(r"[^a-z0-9]", "", function_app_name.lower())
    # Append 'st' suffix, truncate to 24 chars
    name = (base + "st")[:24]
    # Ensure at least 3 chars
    if len(name) < 3:
        name = name.ljust(3, "0")
    return name


def _derive_plan_name(function_app_name: str) -> str:
    """Derive an App Service Plan name from the function app name."""
    return f"{function_app_name}-plan"


def _derive_insights_name(function_app_name: str) -> str:
    """Derive an Application Insights name from the function app name."""
    return f"{function_app_name}-insights"


class FuncProvisioner:
    """Provision Azure resources for hosting Function stubs."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        location: str,
        credential: Any = None,
    ) -> None:
        if not _AZURE_AVAILABLE:
            raise ImportError(
                "Azure SDK packages required for provisioning. Install with: "
                "pip install azure-mgmt-web azure-mgmt-storage "
                "azure-mgmt-applicationinsights azure-identity"
            )
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.location = location
        self._credential = credential or DefaultAzureCredential()
        self._web_client: Any = None
        self._storage_client: Any = None
        self._insights_client: Any = None

    @property
    def web_client(self) -> WebSiteManagementClient:
        if self._web_client is None:
            self._web_client = WebSiteManagementClient(
                self._credential, self.subscription_id
            )
        return self._web_client

    @property
    def storage_client(self) -> StorageManagementClient:
        if self._storage_client is None:
            self._storage_client = StorageManagementClient(
                self._credential, self.subscription_id
            )
        return self._storage_client

    @property
    def insights_client(self) -> ApplicationInsightsManagementClient:
        if self._insights_client is None:
            self._insights_client = ApplicationInsightsManagementClient(
                self._credential, self.subscription_id
            )
        return self._insights_client

    def provision(
        self,
        function_app_name: str,
        *,
        storage_account_name: str | None = None,
        app_service_plan_name: str | None = None,
        app_insights_name: str | None = None,
        skip_app_insights: bool = False,
        python_version: str = "3.11",
        dry_run: bool = False,
    ) -> ProvisionResult:
        """Provision all resources needed for a Function App.

        Args:
            function_app_name: Name for the Function App (globally unique).
            storage_account_name: Override derived storage account name.
            app_service_plan_name: Override derived plan name.
            app_insights_name: Override derived App Insights name.
            skip_app_insights: If True, skip creating Application Insights.
            python_version: Python runtime version (default "3.11").
            dry_run: If True, report what would be created without provisioning.

        Returns:
            ProvisionResult with created resource details.
        """
        sa_name = storage_account_name or _derive_storage_name(function_app_name)
        plan_name = app_service_plan_name or _derive_plan_name(function_app_name)
        insights_name = app_insights_name or _derive_insights_name(function_app_name)

        if not _STORAGE_NAME_RE.match(sa_name):
            return ProvisionResult(
                success=False,
                function_app_name=function_app_name,
                resource_group=self.resource_group,
                location=self.location,
                error=(
                    f"Invalid storage account name '{sa_name}': "
                    "must be 3-24 lowercase alphanumeric characters."
                ),
            )

        result = ProvisionResult(
            success=False,
            function_app_name=function_app_name,
            resource_group=self.resource_group,
            location=self.location,
            storage_account_name=sa_name,
            app_service_plan_name=plan_name,
            app_insights_name=insights_name if not skip_app_insights else None,
        )

        if dry_run:
            result.success = True
            result.resources_created = [
                f"Storage Account: {sa_name}",
                f"App Service Plan: {plan_name} (Consumption/Y1)",
                f"Function App: {function_app_name} (Python {python_version}, Linux)",
            ]
            if not skip_app_insights:
                result.resources_created.insert(
                    2, f"Application Insights: {insights_name}"
                )
            result.function_app_url = f"https://{function_app_name}.azurewebsites.net"
            result.error = "[DRY RUN] No resources created."
            return result

        try:
            # 1. Storage Account
            logger.info("Creating storage account: %s", sa_name)
            sa_conn_str = self._create_storage_account(sa_name)
            result.resources_created.append(f"Storage Account: {sa_name}")

            # 2. Application Insights (optional)
            instrumentation_key = None
            connection_string_ai = None
            if not skip_app_insights:
                logger.info("Creating Application Insights: %s", insights_name)
                instrumentation_key, connection_string_ai = (
                    self._create_app_insights(insights_name)
                )
                result.resources_created.append(
                    f"Application Insights: {insights_name}"
                )

            # 3. App Service Plan (Consumption)
            logger.info("Creating App Service Plan: %s (Consumption/Y1)", plan_name)
            plan_id = self._create_app_service_plan(plan_name)
            result.resources_created.append(
                f"App Service Plan: {plan_name} (Consumption/Y1)"
            )

            # 4. Function App
            logger.info(
                "Creating Function App: %s (Python %s)", function_app_name, python_version
            )
            self._create_function_app(
                function_app_name=function_app_name,
                plan_id=plan_id,
                storage_connection_string=sa_conn_str,
                instrumentation_key=instrumentation_key,
                app_insights_connection_string=connection_string_ai,
                python_version=python_version,
            )
            result.resources_created.append(
                f"Function App: {function_app_name} (Python {python_version}, Linux)"
            )
            result.function_app_url = (
                f"https://{function_app_name}.azurewebsites.net"
            )
            result.success = True

        except ClientAuthenticationError as exc:
            result.error = (
                f"Authentication failed: {exc}. "
                "Run 'az login' or set AZURE_CLIENT_ID/SECRET/TENANT_ID."
            )
        except HttpResponseError as exc:
            result.error = f"Azure API error: {exc.message}"
        except Exception as exc:
            result.error = f"Provisioning failed: {exc}"

        return result

    def _create_storage_account(self, name: str) -> str:
        """Create a Storage Account and return its connection string."""
        poller = self.storage_client.storage_accounts.begin_create(
            self.resource_group,
            name,
            StorageAccountCreateParameters(
                sku=StorageSku(name="Standard_LRS"),
                kind=StorageKind.STORAGE_V2,
                location=self.location,
                enable_https_traffic_only=True,
                minimum_tls_version="TLS1_2",
            ),
        )
        poller.result()  # wait for completion

        # Fetch connection string
        keys = self.storage_client.storage_accounts.list_keys(
            self.resource_group, name
        )
        key = keys.keys[0].value
        return (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={name};"
            f"AccountKey={key};"
            f"EndpointSuffix=core.windows.net"
        )

    def _create_app_insights(
        self, name: str
    ) -> tuple[str, str]:
        """Create Application Insights. Returns (instrumentation_key, connection_string)."""
        component = self.insights_client.components.create_or_update(
            self.resource_group,
            name,
            ApplicationInsightsComponent(
                location=self.location,
                application_type="web",
                kind="web",
            ),
        )
        return component.instrumentation_key, component.connection_string

    def _create_app_service_plan(self, name: str) -> str:
        """Create a Consumption (Y1) App Service Plan. Returns the plan resource ID."""
        plan = self.web_client.app_service_plans.begin_create_or_update(
            self.resource_group,
            name,
            AppServicePlan(
                location=self.location,
                sku=SkuDescription(name="Y1", tier="Dynamic"),
                reserved=True,  # Linux
                kind="functionapp",
            ),
        ).result()
        return plan.id

    def _create_function_app(
        self,
        function_app_name: str,
        plan_id: str,
        storage_connection_string: str,
        instrumentation_key: str | None,
        app_insights_connection_string: str | None,
        python_version: str,
    ) -> None:
        """Create the Function App."""
        app_settings = [
            NameValuePair(name="AzureWebJobsStorage", value=storage_connection_string),
            NameValuePair(name="FUNCTIONS_EXTENSION_VERSION", value="~4"),
            NameValuePair(name="FUNCTIONS_WORKER_RUNTIME", value="python"),
            NameValuePair(
                name="WEBSITE_RUN_FROM_PACKAGE", value="1"
            ),
        ]
        if instrumentation_key:
            app_settings.append(
                NameValuePair(
                    name="APPINSIGHTS_INSTRUMENTATIONKEY",
                    value=instrumentation_key,
                )
            )
        if app_insights_connection_string:
            app_settings.append(
                NameValuePair(
                    name="APPLICATIONINSIGHTS_CONNECTION_STRING",
                    value=app_insights_connection_string,
                )
            )

        self.web_client.web_apps.begin_create_or_update(
            self.resource_group,
            function_app_name,
            Site(
                location=self.location,
                server_farm_id=plan_id,
                kind="functionapp,linux",
                reserved=True,
                site_config=SiteConfig(
                    app_settings=app_settings,
                    linux_fx_version=f"Python|{python_version}",
                    ftps_state="Disabled",
                    min_tls_version="1.2",
                    http20_enabled=True,
                ),
            ),
        ).result()
