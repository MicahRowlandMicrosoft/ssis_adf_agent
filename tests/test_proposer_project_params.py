"""Tests for Project.params-aware Key Vault linked-service emission in the proposer."""
from __future__ import annotations

from ssis_adf_agent.migration_plan import propose_design
from ssis_adf_agent.parsers.models import (  # type: ignore[attr-defined]
    ConnectionManagerType,
    SSISConnectionManager,
    SSISPackage,
    SSISParameter,
)


def _pkg(project_params: list[SSISParameter]) -> SSISPackage:
    cm = SSISConnectionManager(
        id="cm-sql", name="SQL", type=ConnectionManagerType.OLEDB,
        server="srv", database="db",
    )
    return SSISPackage(
        id="pkg", name="P", source_file="p.dtsx",
        connection_managers=[cm], tasks=[],
        project_parameters=project_params,
    )


def test_sensitive_credential_param_creates_key_vault_linked_service() -> None:
    plan = propose_design(_pkg([
        SSISParameter(name="DbPassword", sensitive=True),
    ]))
    kv = [ls for ls in plan.linked_services if ls.type == "AzureKeyVaultSecret"]
    assert len(kv) == 1
    assert kv[0].name == "LS_KV_DbPassword"
    assert kv[0].secret_name == "ssis-dbpassword"
    # Infrastructure must include Key Vault.
    types = {item.type for item in plan.infrastructure_needed}
    assert "Microsoft.KeyVault/vaults" in types
    # RBAC must include Key Vault Secrets User on the KV LS.
    roles = {(r.role, r.scope) for r in plan.rbac_needed}
    assert any(role == "Key Vault Secrets User" for role, _ in roles)


def test_non_sensitive_param_does_not_create_kv_ls() -> None:
    plan = propose_design(_pkg([
        SSISParameter(name="DbPassword", sensitive=False),
    ]))
    assert not any(ls.type == "AzureKeyVaultSecret" for ls in plan.linked_services)


def test_sensitive_non_credential_param_does_not_create_kv_ls() -> None:
    plan = propose_design(_pkg([
        SSISParameter(name="BatchId", sensitive=True),
    ]))
    assert not any(ls.type == "AzureKeyVaultSecret" for ls in plan.linked_services)


def test_multiple_credential_params_create_one_ls_each() -> None:
    plan = propose_design(_pkg([
        SSISParameter(name="DbPassword", sensitive=True),
        SSISParameter(name="ApiToken", sensitive=True),
        SSISParameter(name="ClientSecret", sensitive=True),
    ]))
    kv_names = sorted(
        ls.name for ls in plan.linked_services if ls.type == "AzureKeyVaultSecret"
    )
    assert kv_names == ["LS_KV_ApiToken", "LS_KV_ClientSecret", "LS_KV_DbPassword"]
