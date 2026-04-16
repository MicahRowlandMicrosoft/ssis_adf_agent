"""Tests for Azure Functions provisioner (Phase C).

Tests cover:
1. Name derivation helpers
2. Storage name validation
3. Dry-run mode
4. Full provisioning flow (mocked Azure SDK)
5. Error handling (auth failures, API errors)
6. Skip App Insights option
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from ssis_adf_agent.deployer.func_provisioner import (
    FuncProvisioner,
    ProvisionResult,
    _derive_storage_name,
    _derive_plan_name,
    _derive_insights_name,
)


# ===================================================================
# 1. Name derivation
# ===================================================================

class TestNameDerivation:
    def test_storage_name_simple(self):
        assert _derive_storage_name("func-myapp") == "funcmyappst"

    def test_storage_name_strips_special_chars(self):
        assert _derive_storage_name("func-my_app.test") == "funcmyapptestst"

    def test_storage_name_uppercase_lowered(self):
        assert _derive_storage_name("FuncMyApp") == "funcmyappst"

    def test_storage_name_truncated_to_24_chars(self):
        name = _derive_storage_name("a" * 30)
        assert len(name) <= 24
        assert name.endswith("st") or len(name) == 24

    def test_storage_name_short_padded(self):
        name = _derive_storage_name("a")
        assert len(name) >= 3

    def test_plan_name(self):
        assert _derive_plan_name("func-myapp") == "func-myapp-plan"

    def test_insights_name(self):
        assert _derive_insights_name("func-myapp") == "func-myapp-insights"


# ===================================================================
# 2. Storage name validation
# ===================================================================

class TestStorageNameValidation:
    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_invalid_storage_name_rejected(self):
        provisioner = FuncProvisioner.__new__(FuncProvisioner)
        provisioner.subscription_id = "sub-1"
        provisioner.resource_group = "rg-test"
        provisioner.location = "eastus2"
        provisioner._credential = MagicMock()
        provisioner._web_client = None
        provisioner._storage_client = None
        provisioner._insights_client = None

        result = provisioner.provision(
            function_app_name="myapp",
            storage_account_name="INVALID-NAME!",  # uppercase + special chars
        )
        assert result.success is False
        assert "Invalid storage account name" in result.error


# ===================================================================
# 3. Dry-run mode
# ===================================================================

class TestDryRun:
    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_dry_run_reports_resources(self):
        provisioner = FuncProvisioner.__new__(FuncProvisioner)
        provisioner.subscription_id = "sub-1"
        provisioner.resource_group = "rg-test"
        provisioner.location = "eastus2"
        provisioner._credential = MagicMock()
        provisioner._web_client = None
        provisioner._storage_client = None
        provisioner._insights_client = None

        result = provisioner.provision(
            function_app_name="func-myapp",
            dry_run=True,
        )
        assert result.success is True
        assert "DRY RUN" in result.error
        assert len(result.resources_created) == 4  # storage, insights, plan, func app
        assert result.function_app_url == "https://func-myapp.azurewebsites.net"
        assert result.storage_account_name == "funcmyappst"
        assert result.app_service_plan_name == "func-myapp-plan"
        assert result.app_insights_name == "func-myapp-insights"

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_dry_run_skip_insights(self):
        provisioner = FuncProvisioner.__new__(FuncProvisioner)
        provisioner.subscription_id = "sub-1"
        provisioner.resource_group = "rg-test"
        provisioner.location = "eastus2"
        provisioner._credential = MagicMock()
        provisioner._web_client = None
        provisioner._storage_client = None
        provisioner._insights_client = None

        result = provisioner.provision(
            function_app_name="func-myapp",
            skip_app_insights=True,
            dry_run=True,
        )
        assert result.success is True
        assert len(result.resources_created) == 3  # no insights
        assert result.app_insights_name is None
        assert not any("Insights" in r for r in result.resources_created)

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_dry_run_custom_python_version(self):
        provisioner = FuncProvisioner.__new__(FuncProvisioner)
        provisioner.subscription_id = "sub-1"
        provisioner.resource_group = "rg-test"
        provisioner.location = "eastus2"
        provisioner._credential = MagicMock()
        provisioner._web_client = None
        provisioner._storage_client = None
        provisioner._insights_client = None

        result = provisioner.provision(
            function_app_name="func-myapp",
            python_version="3.10",
            dry_run=True,
        )
        assert any("Python 3.10" in r for r in result.resources_created)


# ===================================================================
# 4. Full provisioning flow (mocked SDK)
# ===================================================================

class TestProvisionFlow:
    def _make_provisioner(self):
        """Create a provisioner with fully mocked SDK clients."""
        provisioner = FuncProvisioner.__new__(FuncProvisioner)
        provisioner.subscription_id = "sub-1"
        provisioner.resource_group = "rg-test"
        provisioner.location = "eastus2"
        provisioner._credential = MagicMock()

        # Mock storage client
        mock_storage = MagicMock()
        mock_storage.storage_accounts.begin_create.return_value.result.return_value = None
        mock_keys = MagicMock()
        mock_keys.keys = [MagicMock(value="fakekey123==")]
        mock_storage.storage_accounts.list_keys.return_value = mock_keys
        provisioner._storage_client = mock_storage

        # Mock insights client
        mock_insights = MagicMock()
        mock_component = MagicMock()
        mock_component.instrumentation_key = "fake-ikey-1234"
        mock_component.connection_string = "InstrumentationKey=fake-ikey-1234"
        mock_insights.components.create_or_update.return_value = mock_component
        provisioner._insights_client = mock_insights

        # Mock web client
        mock_web = MagicMock()
        mock_plan = MagicMock()
        mock_plan.id = "/subscriptions/sub-1/resourceGroups/rg-test/providers/Microsoft.Web/serverfarms/plan"
        mock_web.app_service_plans.begin_create_or_update.return_value.result.return_value = mock_plan
        mock_web.web_apps.begin_create_or_update.return_value.result.return_value = None
        provisioner._web_client = mock_web

        return provisioner

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_full_provision_success(self):
        provisioner = self._make_provisioner()
        result = provisioner.provision(function_app_name="func-test")

        assert result.success is True
        assert result.function_app_name == "func-test"
        assert result.function_app_url == "https://func-test.azurewebsites.net"
        assert len(result.resources_created) == 4
        assert result.error is None

        # Verify storage was created
        provisioner._storage_client.storage_accounts.begin_create.assert_called_once()
        # Verify insights was created
        provisioner._insights_client.components.create_or_update.assert_called_once()
        # Verify plan was created
        provisioner._web_client.app_service_plans.begin_create_or_update.assert_called_once()
        # Verify function app was created
        provisioner._web_client.web_apps.begin_create_or_update.assert_called_once()

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_provision_skip_insights(self):
        provisioner = self._make_provisioner()
        result = provisioner.provision(
            function_app_name="func-test",
            skip_app_insights=True,
        )

        assert result.success is True
        assert len(result.resources_created) == 3  # no insights
        provisioner._insights_client.components.create_or_update.assert_not_called()

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_provision_custom_storage_name(self):
        provisioner = self._make_provisioner()
        result = provisioner.provision(
            function_app_name="func-test",
            storage_account_name="mycustomstorage",
        )

        assert result.success is True
        assert result.storage_account_name == "mycustomstorage"

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_provision_creates_storage_with_tls12(self):
        provisioner = self._make_provisioner()
        provisioner.provision(function_app_name="func-test")

        call_args = provisioner._storage_client.storage_accounts.begin_create.call_args
        params = call_args[0][2]  # StorageAccountCreateParameters
        assert params.minimum_tls_version == "TLS1_2"
        assert params.enable_https_traffic_only is True

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_provision_creates_consumption_plan(self):
        provisioner = self._make_provisioner()
        provisioner.provision(function_app_name="func-test")

        call_args = provisioner._web_client.app_service_plans.begin_create_or_update.call_args
        plan = call_args[0][2]  # AppServicePlan
        assert plan.sku.name == "Y1"
        assert plan.sku.tier == "Dynamic"
        assert plan.reserved is True  # Linux

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_provision_configures_function_app(self):
        provisioner = self._make_provisioner()
        provisioner.provision(
            function_app_name="func-test",
            python_version="3.11",
        )

        call_args = provisioner._web_client.web_apps.begin_create_or_update.call_args
        site = call_args[0][2]  # Site
        assert site.kind == "functionapp,linux"
        assert site.reserved is True
        assert site.site_config.linux_fx_version == "Python|3.11"
        assert site.site_config.ftps_state == "Disabled"
        assert site.site_config.min_tls_version == "1.2"

        # Check app settings
        settings = {s.name: s.value for s in site.site_config.app_settings}
        assert settings["FUNCTIONS_WORKER_RUNTIME"] == "python"
        assert settings["FUNCTIONS_EXTENSION_VERSION"] == "~4"
        assert "AzureWebJobsStorage" in settings
        assert settings["APPINSIGHTS_INSTRUMENTATIONKEY"] == "fake-ikey-1234"


# ===================================================================
# 5. Error handling
# ===================================================================

class TestErrorHandling:
    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_auth_error(self):
        from ssis_adf_agent.deployer.func_provisioner import ClientAuthenticationError

        provisioner = FuncProvisioner.__new__(FuncProvisioner)
        provisioner.subscription_id = "sub-1"
        provisioner.resource_group = "rg-test"
        provisioner.location = "eastus2"
        provisioner._credential = MagicMock()
        provisioner._web_client = None
        provisioner._insights_client = None

        mock_storage = MagicMock()
        mock_storage.storage_accounts.begin_create.side_effect = ClientAuthenticationError(
            "Token expired"
        )
        provisioner._storage_client = mock_storage

        result = provisioner.provision(function_app_name="func-test")
        assert result.success is False
        assert "Authentication failed" in result.error

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_api_error(self):
        from ssis_adf_agent.deployer.func_provisioner import HttpResponseError

        provisioner = FuncProvisioner.__new__(FuncProvisioner)
        provisioner.subscription_id = "sub-1"
        provisioner.resource_group = "rg-test"
        provisioner.location = "eastus2"
        provisioner._credential = MagicMock()
        provisioner._web_client = None
        provisioner._insights_client = None

        mock_storage = MagicMock()
        mock_storage.storage_accounts.begin_create.side_effect = HttpResponseError(
            "Resource group not found"
        )
        provisioner._storage_client = mock_storage

        result = provisioner.provision(function_app_name="func-test")
        assert result.success is False
        assert "Azure API error" in result.error

    @patch("ssis_adf_agent.deployer.func_provisioner._AZURE_AVAILABLE", True)
    def test_partial_failure_reports_created_resources(self):
        """If Function App creation fails, already-created resources are reported."""
        from ssis_adf_agent.deployer.func_provisioner import HttpResponseError

        provisioner = FuncProvisioner.__new__(FuncProvisioner)
        provisioner.subscription_id = "sub-1"
        provisioner.resource_group = "rg-test"
        provisioner.location = "eastus2"
        provisioner._credential = MagicMock()

        # Storage succeeds
        mock_storage = MagicMock()
        mock_storage.storage_accounts.begin_create.return_value.result.return_value = None
        mock_keys = MagicMock()
        mock_keys.keys = [MagicMock(value="key123==")]
        mock_storage.storage_accounts.list_keys.return_value = mock_keys
        provisioner._storage_client = mock_storage

        # Insights succeeds
        mock_insights = MagicMock()
        mock_component = MagicMock()
        mock_component.instrumentation_key = "ikey"
        mock_component.connection_string = "cs"
        mock_insights.components.create_or_update.return_value = mock_component
        provisioner._insights_client = mock_insights

        # Plan succeeds
        mock_web = MagicMock()
        mock_plan = MagicMock()
        mock_plan.id = "/subs/plan-id"
        mock_web.app_service_plans.begin_create_or_update.return_value.result.return_value = mock_plan
        # Function App fails
        mock_web.web_apps.begin_create_or_update.side_effect = HttpResponseError(
            "Quota exceeded"
        )
        provisioner._web_client = mock_web

        result = provisioner.provision(function_app_name="func-test")
        assert result.success is False
        assert "Quota exceeded" in result.error
        # Storage + Insights + Plan were created before the failure
        assert len(result.resources_created) == 3


# ===================================================================
# 6. ProvisionResult dataclass
# ===================================================================

class TestProvisionResult:
    def test_defaults(self):
        r = ProvisionResult(
            success=True,
            function_app_name="func-test",
            resource_group="rg",
            location="eastus",
        )
        assert r.resources_created == []
        assert r.error is None
        assert r.storage_account_name is None
