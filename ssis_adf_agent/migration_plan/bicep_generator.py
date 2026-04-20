"""Generate a Bicep template from a :class:`MigrationPlan`.

Reads ``plan.infrastructure_needed`` and ``plan.rbac_needed`` and emits a
self-contained Bicep file that, when deployed, provisions the Azure resources
the plan requires and grants the listed RBAC role assignments.

Scope is intentionally focused on the resource types the proposer emits today:

* ``Microsoft.DataFactory/factories``
* ``Microsoft.Storage/storageAccounts`` (with HNS for ADLS Gen2 when requested)
* ``Microsoft.KeyVault/vaults``

RBAC scopes use logical placeholders (``<storage>``, ``<sql>``, ``<ADF MI>``)
which the generator resolves to Bicep symbol names. Unknown principals or
scopes are passed through as parameters so the deployer can supply them.
"""
from __future__ import annotations

from textwrap import dedent

from .models import (
    AuthMode,
    InfrastructureItem,
    MigrationPlan,
    RbacAssignment,
)


# Common built-in role definition IDs (so we don't have to look them up at deploy time)
_BUILTIN_ROLE_IDS: dict[str, str] = {
    "Storage Blob Data Contributor": "ba92f5b4-2d11-453d-a403-e96b0029c9fe",
    "Storage Blob Data Reader":      "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1",
    "Key Vault Secrets User":        "4633458b-17de-408a-b874-0445c86b69e6",
    "Key Vault Secrets Officer":     "b86a8fe4-44ce-4948-aee5-eccb2c155cd7",
    "Reader":                        "acdd72a7-3385-48ef-bd42-f606fba81ae7",
    "Contributor":                   "b24988ac-6180-42a0-ab88-20f7382dd24c",
}


def _slug(name_hint: str) -> str:
    """Turn a name hint like 'adf-<workload>' into a Bicep-friendly identifier."""
    return (
        name_hint.replace("<", "")
        .replace(">", "")
        .replace("-", "_")
        .replace(" ", "_")
        .lower()
    )


def _factory_resource(item: InfrastructureItem, sym: str) -> str:
    managed_vnet = bool(item.properties.get("managedVirtualNetwork", False))
    return dedent(f"""
        resource {sym} 'Microsoft.DataFactory/factories@2018-06-01' = {{
          name: take(toLower('${{prefix}}adf${{uniqueString(resourceGroup().id)}}'), 63)
          location: location
          identity: {{ type: 'SystemAssigned' }}
          properties: {{}}
        }}
        """).strip()


def _storage_resource(item: InfrastructureItem, sym: str) -> str:
    hns = bool(item.properties.get("isHnsEnabled", False))
    return dedent(f"""
        resource {sym} 'Microsoft.Storage/storageAccounts@2023-05-01' = {{
          name: take(toLower('${{prefix}}st${{uniqueString(resourceGroup().id)}}'), 24)
          location: location
          sku: {{ name: '{item.sku or "Standard_LRS"}' }}
          kind: 'StorageV2'
          properties: {{
            minimumTlsVersion: 'TLS1_2'
            allowBlobPublicAccess: false
            supportsHttpsTrafficOnly: true
            isHnsEnabled: {str(hns).lower()}
          }}
        }}
        """).strip()


def _keyvault_resource(item: InfrastructureItem, sym: str) -> str:
    rbac = bool(item.properties.get("enableRbacAuthorization", True))
    return dedent(f"""
        resource {sym} 'Microsoft.KeyVault/vaults@2023-07-01' = {{
          name: take(toLower('${{prefix}}kv${{uniqueString(resourceGroup().id)}}'), 24)
          location: location
          properties: {{
            tenantId: subscription().tenantId
            sku: {{ family: 'A', name: '{item.sku or "standard"}' }}
            enableRbacAuthorization: {str(rbac).lower()}
            enableSoftDelete: true
            softDeleteRetentionInDays: 7
          }}
        }}
        """).strip()


_RESOURCE_BUILDERS = {
    "Microsoft.DataFactory/factories":   _factory_resource,
    "Microsoft.Storage/storageAccounts": _storage_resource,
    "Microsoft.KeyVault/vaults":         _keyvault_resource,
}


def _resolve_principal(principal: str, factory_sym: str | None) -> str:
    """Return a Bicep expression for the principal id."""
    p = principal.strip().lower()
    if "adf" in p and "mi" in p and factory_sym:
        return f"{factory_sym}.identity.principalId"
    return "principalIdParam"  # external principal — passed in via parameter


def _resolve_scope(scope: str, sym_by_type: dict[str, str]) -> tuple[str, str]:
    """Return (scope expression, scope-id expression for guid())."""
    s = scope.strip().lower()
    if s.startswith("storage://") and "Microsoft.Storage/storageAccounts" in sym_by_type:
        sym = sym_by_type["Microsoft.Storage/storageAccounts"]
        return sym, f"{sym}.id"
    if s.startswith("sql://"):
        # External SQL — expressed via parameter; cannot scope an RBAC inside this
        # template at the SQL level. Fall through to subscription-level guid only.
        return "resourceGroup()", "resourceGroup().id"
    if "<storage>" in s and "Microsoft.Storage/storageAccounts" in sym_by_type:
        sym = sym_by_type["Microsoft.Storage/storageAccounts"]
        return sym, f"{sym}.id"
    if "key" in s and "vault" in s and "Microsoft.KeyVault/vaults" in sym_by_type:
        sym = sym_by_type["Microsoft.KeyVault/vaults"]
        return sym, f"{sym}.id"
    return "resourceGroup()", "resourceGroup().id"


def _rbac_resource(rbac: RbacAssignment, idx: int, factory_sym: str | None,
                   sym_by_type: dict[str, str]) -> str | None:
    role_id = _BUILTIN_ROLE_IDS.get(rbac.role)
    if not role_id:
        # Skip roles we can't resolve to a built-in (e.g. SQL-server-side roles
        # like db_datareader, which must be granted via T-SQL, not RBAC).
        return None
    scope_sym, scope_id = _resolve_scope(rbac.scope, sym_by_type)
    principal_expr = _resolve_principal(rbac.principal, factory_sym)
    # The role-assignment name (a GUID) must be deterministic at the start of
    # the deployment, so we cannot read principalId off the factory there. Use
    # the factory's resource id (or 'principalIdParam') as a stable seed.
    if factory_sym and "identity.principalId" in principal_expr:
        guid_seed = f"{factory_sym}.id"
    else:
        guid_seed = principal_expr
    sym = f"rbac{idx}"
    return dedent(f"""
        resource {sym} 'Microsoft.Authorization/roleAssignments@2022-04-01' = {{
          name: guid({scope_id}, {guid_seed}, '{role_id}')
          scope: {scope_sym}
          properties: {{
            principalId: {principal_expr}
            principalType: 'ServicePrincipal'
            roleDefinitionId: subscriptionResourceId(
              'Microsoft.Authorization/roleDefinitions', '{role_id}'
            )
          }}
        }}
        """).strip()


def generate_bicep(plan: MigrationPlan, *, name_prefix: str = "ssismig") -> str:
    """Render a Bicep template from the plan's infrastructure + RBAC sections.

    The result is a single-file template you can deploy with::

        az deployment group create -g <rg> --template-file plan.bicep \\
            --parameters prefix=<short> principalIdParam=<oid>

    ``principalIdParam`` is required only if the plan references a principal
    other than the factory's own managed identity (e.g. an external SP).
    """
    header = dedent(f"""
        // Auto-generated from MigrationPlan for {plan.package_name}
        // schema_version: {plan.schema_version}
        // target_pattern: {plan.target_pattern.value}
        //
        // Deploy:
        //   az deployment group create -g <rg> --template-file <this-file> \\
        //       --parameters prefix={name_prefix}

        targetScope = 'resourceGroup'

        @description('Azure region for all resources.')
        param location string = resourceGroup().location

        @description('Short prefix used in generated resource names (3-11 chars, lowercase).')
        @minLength(3)
        @maxLength(11)
        param prefix string = '{name_prefix}'

        @description('Optional: object ID of an external principal that needs RBAC grants.')
        param principalIdParam string = ''
        """).lstrip()

    sym_by_type: dict[str, str] = {}
    resources: list[str] = []
    factory_sym: str | None = None

    for i, item in enumerate(plan.infrastructure_needed):
        builder = _RESOURCE_BUILDERS.get(item.type)
        if builder is None:
            resources.append(f"// Skipped unsupported resource type: {item.type}")
            continue
        sym = f"{_slug(item.name_hint)}_{i}"
        sym_by_type[item.type] = sym
        if item.type == "Microsoft.DataFactory/factories":
            factory_sym = sym
        resources.append(builder(item, sym))

    rbac_blocks: list[str] = []
    skipped_rbac: list[str] = []
    for i, r in enumerate(plan.rbac_needed):
        block = _rbac_resource(r, i, factory_sym, sym_by_type)
        if block is None:
            skipped_rbac.append(
                f"// Skipped RBAC: {r.principal} -> {r.role} on {r.scope} "
                f"(role not resolvable to a built-in Azure RBAC id; grant manually)"
            )
        else:
            rbac_blocks.append(block)

    outputs: list[str] = []
    if factory_sym:
        outputs.append(f"output factoryName string = {factory_sym}.name")
        outputs.append(f"output factoryPrincipalId string = {factory_sym}.identity.principalId")
    if "Microsoft.Storage/storageAccounts" in sym_by_type:
        ssym = sym_by_type["Microsoft.Storage/storageAccounts"]
        outputs.append(f"output storageAccountName string = {ssym}.name")
    if "Microsoft.KeyVault/vaults" in sym_by_type:
        ksym = sym_by_type["Microsoft.KeyVault/vaults"]
        outputs.append(f"output keyVaultName string = {ksym}.name")

    parts = [header, "", "// ---- Resources ----", *resources]
    if rbac_blocks or skipped_rbac:
        parts += ["", "// ---- RBAC ----", *rbac_blocks, *skipped_rbac]
    if outputs:
        parts += ["", "// ---- Outputs ----", *outputs]
    return "\n\n".join(parts).rstrip() + "\n"


__all__ = ["generate_bicep"]
