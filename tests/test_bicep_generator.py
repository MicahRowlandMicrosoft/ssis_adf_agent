"""Tests for the Bicep generator (no Azure calls)."""
from __future__ import annotations

from ssis_adf_agent.migration_plan import (
    AuthMode,
    InfrastructureItem,
    LinkedServiceSpec,
    MigrationPlan,
    RbacAssignment,
    StorageKind,
    TargetPattern,
    generate_bicep,
)


def _full_plan() -> MigrationPlan:
    return MigrationPlan(
        package_name="test", package_path="x.dtsx",
        target_pattern=TargetPattern.SCHEDULED_FILE_DROP,
        linked_services=[
            LinkedServiceSpec(name="LS_SQL", type="AzureSqlDatabase", auth=AuthMode.MANAGED_IDENTITY),
            LinkedServiceSpec(name="LS_Sink", type=StorageKind.ADLS_GEN2.value, auth=AuthMode.MANAGED_IDENTITY),
        ],
        infrastructure_needed=[
            InfrastructureItem(
                type="Microsoft.DataFactory/factories",
                name_hint="adf-test", sku="V2",
                properties={"managedVirtualNetwork": True},
                purpose="ADF instance",
            ),
            InfrastructureItem(
                type="Microsoft.Storage/storageAccounts",
                name_hint="sttest", sku="Standard_LRS",
                properties={"isHnsEnabled": True, "minimumTlsVersion": "TLS1_2"},
                purpose="ADLS Gen2",
            ),
        ],
        rbac_needed=[
            RbacAssignment(
                principal="<ADF MI>", scope="<storage>",
                role="Storage Blob Data Contributor", purpose="ADF reads/writes blobs",
            ),
            RbacAssignment(
                principal="<ADF MI>", scope="sql://srv/db",
                role="db_datareader", purpose="SQL-side; should be skipped",
            ),
        ],
    )


def test_generates_bicep_with_factory_storage_and_rbac() -> None:
    bicep = generate_bicep(_full_plan(), name_prefix="acme")
    assert "Microsoft.DataFactory/factories" in bicep
    assert "Microsoft.Storage/storageAccounts" in bicep
    # ADF gets a system-assigned MI
    assert "SystemAssigned" in bicep
    # HNS request is honored
    assert "isHnsEnabled: true" in bicep
    # The storage RBAC is emitted (built-in role is resolvable)
    assert "ba92f5b4-2d11-453d-a403-e96b0029c9fe" in bicep
    # The SQL-side RBAC (db_datareader) is skipped — surfaced as a comment, not a resource
    assert "db_datareader" in bicep
    assert "// Skipped RBAC" in bicep
    # Outputs include the factory and storage names
    assert "output factoryName" in bicep
    assert "output storageAccountName" in bicep
    # Prefix parameter is set
    assert "param prefix string = 'acme'" in bicep


def test_empty_plan_emits_minimal_template() -> None:
    plan = MigrationPlan(package_name="empty", package_path="x.dtsx")
    bicep = generate_bicep(plan)
    # Still has scope + the prefix parameter
    assert "targetScope = 'resourceGroup'" in bicep
    assert "param prefix string" in bicep
    # No resources defined
    assert "Microsoft.DataFactory" not in bicep
    assert "Microsoft.Storage" not in bicep


def test_with_observability_emits_diagnostic_settings() -> None:
    """P5-7: when with_observability is set, emit diagnosticSettings child."""
    workspace_id = (
        "/subscriptions/s/resourceGroups/rg/providers/"
        "Microsoft.OperationalInsights/workspaces/law-prod"
    )
    bicep = generate_bicep(
        _full_plan(),
        name_prefix="acme",
        observability_workspace_id=workspace_id,
    )
    assert "Microsoft.Insights/diagnosticSettings" in bicep
    assert workspace_id in bicep
    # All five docs/operations/observability.md categories present
    for cat in (
        "PipelineRuns",
        "ActivityRuns",
        "TriggerRuns",
        "PipelineActivityRuns",
        "AllMetrics",
    ):
        assert cat in bicep
    # Section header is consistent
    assert "// ---- Observability ----" in bicep


def test_observability_without_factory_emits_skip_comment() -> None:
    """If the plan provisions no factory, do not silently drop the request."""
    plan = MigrationPlan(package_name="empty", package_path="x.dtsx")
    bicep = generate_bicep(
        plan,
        observability_workspace_id="/subscriptions/s/resourceGroups/rg/providers/"
        "Microsoft.OperationalInsights/workspaces/law-prod",
    )
    # No actual resource block, but the skip comment names the dropped feature
    assert "Microsoft.Insights/diagnosticSettings" not in bicep
    assert "Skipped diagnosticSettings" in bicep


def test_default_no_observability() -> None:
    """No observability flag -> no diagnosticSettings resource emitted."""
    bicep = generate_bicep(_full_plan())
    assert "diagnosticSettings" not in bicep
    assert "Observability" not in bicep
