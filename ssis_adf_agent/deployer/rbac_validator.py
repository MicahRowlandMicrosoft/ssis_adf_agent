"""P5-12: validate_deployer_rbac — read-only RBAC compliance check.

Compares an identity's actual role assignments against the per-tool minimum
roles documented in RBAC.md. Reports which planned tools the identity can
run today and which it cannot, without creating any resource or attempting
any deployment. All Azure SDK calls are read-only listings (no writes).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# RBAC.md "Quick reference — minimum role per tool".
# Each entry is the *minimum* set of role names the deploying identity must
# hold somewhere up the scope chain (subscription | RG | resource) for the
# tool to succeed. "Or" alternatives are inner lists.
#
# Roles are matched by their built-in display name (the column you see in the
# Azure portal IAM blade). KV data-plane is matched separately because it
# uses either the RBAC role 'Key Vault Secrets User' / 'Key Vault Secrets
# Officer' or a legacy access-policy entry; we only check the RBAC side.
_TOOL_ROLE_REQUIREMENTS: dict[str, dict[str, Any]] = {
    "deploy_to_adf": {
        "arm": [["Data Factory Contributor"]],
        "kv": [],
        "scope_hint": "target ADF",
        "rbac_md_anchor": "deploy_to_adf (deploy)",
    },
    "deploy_to_adf_preflight": {
        "arm": [["Reader"]],
        "kv": [["Key Vault Secrets User", "Key Vault Secrets Officer"]],
        "scope_hint": "target ADF + each referenced KV",
        "rbac_md_anchor": "deploy_to_adf (pre_flight=true, P4-6)",
    },
    "provision_adf_environment": {
        "arm": [["Contributor"], ["User Access Administrator"]],
        "kv": [],
        "scope_hint": "target RG",
        "rbac_md_anchor": "provision_adf_environment",
    },
    "provision_function_app": {
        "arm": [["Contributor"]],
        "kv": [],
        "scope_hint": "target RG",
        "rbac_md_anchor": "provision_function_app",
    },
    "deploy_function_stubs": {
        "arm": [["Website Contributor", "Contributor"]],
        "kv": [],
        "scope_hint": "target Function App",
        "rbac_md_anchor": "deploy_function_stubs",
    },
    "activate_triggers": {
        "arm": [["Data Factory Contributor"]],
        "kv": [],
        "scope_hint": "target ADF",
        "rbac_md_anchor": "activate_triggers (H7)",
    },
    "smoke_test_pipeline": {
        "arm": [["Data Factory Contributor"]],
        "kv": [],
        "scope_hint": "target ADF",
        "rbac_md_anchor": "smoke_test_pipeline",
    },
    "smoke_test_wave": {
        "arm": [["Data Factory Contributor"]],
        "kv": [],
        "scope_hint": "target ADF",
        "rbac_md_anchor": "smoke_test_wave (N1)",
    },
    "upload_encrypted_secrets": {
        "arm": [],
        "kv": [["Key Vault Secrets Officer"]],
        "scope_hint": "target KV",
        "rbac_md_anchor": "upload_encrypted_secrets (P4-4)",
    },
    "validate_adf_artifacts_introspect": {
        "arm": [["Reader"]],
        "kv": [],
        "scope_hint": "target ADF",
        "rbac_md_anchor": "validate_adf_artifacts (with deployed factory introspection)",
    },
    "compare_dataflow_output": {
        "arm": [["Storage Blob Data Reader"]],
        "kv": [],
        "scope_hint": "source/target dataset stores",
        "rbac_md_anchor": "compare_dataflow_output (P4-1)",
    },
    "compare_estimates_to_actuals": {
        "arm": [["Cost Management Reader"]],
        "kv": [],
        "scope_hint": "subscription / billing scope",
        "rbac_md_anchor": "compare_estimates_to_actuals (P4-5)",
    },
}


@dataclass
class ToolRbacFinding:
    tool: str
    status: str  # "ok" | "missing_arm" | "missing_kv" | "missing_both" | "unknown"
    missing_arm_alternatives: list[list[str]] = field(default_factory=list)
    missing_kv_alternatives: list[list[str]] = field(default_factory=list)
    held_arm_roles: list[str] = field(default_factory=list)
    held_kv_roles: list[str] = field(default_factory=list)
    scope_hint: str = ""
    rbac_md_anchor: str = ""


def _alt_satisfied(alternatives: list[list[str]], held: set[str]) -> list[list[str]]:
    """Return any alternative groups not satisfied by `held`."""
    missing: list[list[str]] = []
    for alt in alternatives:
        # `alt` is a disjunction: any one role in `alt` satisfies the slot.
        if not any(role in held for role in alt):
            missing.append(alt)
    return missing


def evaluate_rbac(
    *,
    held_arm_roles: list[str],
    held_kv_roles: list[str],
    planned_tools: list[str],
) -> dict[str, Any]:
    """Compare held roles against required roles for each planned tool.

    Args:
        held_arm_roles: ARM role display names already assigned to the
            deploying identity (anywhere up the chain — caller should
            pre-filter to assignments that *cover* the target scope).
        held_kv_roles: Same, for Key Vault data-plane RBAC roles.
        planned_tools: Subset of _TOOL_ROLE_REQUIREMENTS keys.

    Returns:
        Structured report with per-tool status, summary counts, and a
        deduplicated list of missing role assignments to grant.
    """
    held_arm = set(held_arm_roles)
    held_kv = set(held_kv_roles)

    findings: list[ToolRbacFinding] = []
    for tool in planned_tools:
        spec = _TOOL_ROLE_REQUIREMENTS.get(tool)
        if spec is None:
            findings.append(ToolRbacFinding(
                tool=tool, status="unknown",
                rbac_md_anchor="(not in RBAC.md matrix)",
            ))
            continue
        missing_arm = _alt_satisfied(spec["arm"], held_arm)
        missing_kv = _alt_satisfied(spec["kv"], held_kv)
        if not missing_arm and not missing_kv:
            status = "ok"
        elif missing_arm and missing_kv:
            status = "missing_both"
        elif missing_arm:
            status = "missing_arm"
        else:
            status = "missing_kv"
        findings.append(ToolRbacFinding(
            tool=tool, status=status,
            missing_arm_alternatives=missing_arm,
            missing_kv_alternatives=missing_kv,
            held_arm_roles=sorted(held_arm),
            held_kv_roles=sorted(held_kv),
            scope_hint=spec["scope_hint"],
            rbac_md_anchor=spec["rbac_md_anchor"],
        ))

    ok_count = sum(1 for f in findings if f.status == "ok")
    return {
        "planned_tools": planned_tools,
        "held_arm_roles": sorted(held_arm),
        "held_kv_roles": sorted(held_kv),
        "summary": {
            "tools_evaluated": len(findings),
            "tools_ok": ok_count,
            "tools_blocked": len(findings) - ok_count,
        },
        "findings": [f.__dict__ for f in findings],
        "rbac_md": "RBAC.md — Quick reference — minimum role per tool",
    }


def list_known_tools() -> list[str]:
    """Tools whose RBAC requirements this validator understands."""
    return sorted(_TOOL_ROLE_REQUIREMENTS)


def fetch_held_roles(
    *,
    subscription_id: str,
    resource_group: str | None,
    factory_name: str | None,
    key_vault_name: str | None,
    principal_object_id: str,
    credential: Any = None,
) -> dict[str, list[str]]:
    """Read role assignments for `principal_object_id` from Azure (read-only).

    Returns ``{"arm": [...], "kv": [...]}`` of built-in role display names
    the principal holds at any scope that *covers* the supplied resource
    coordinates (subscription, RG, factory, KV).

    All SDK calls are list/get only — never write.
    """
    from azure.mgmt.authorization import AuthorizationManagementClient
    from ..credential import get_credential

    cred = credential or get_credential()
    auth = AuthorizationManagementClient(cred, subscription_id)

    # Build the set of scope strings that "cover" the request.
    sub_scope = f"/subscriptions/{subscription_id}"
    covering_scopes = {sub_scope}
    if resource_group:
        rg_scope = f"{sub_scope}/resourceGroups/{resource_group}"
        covering_scopes.add(rg_scope)
        if factory_name:
            covering_scopes.add(
                f"{rg_scope}/providers/Microsoft.DataFactory/factories/{factory_name}"
            )
        if key_vault_name:
            covering_scopes.add(
                f"{rg_scope}/providers/Microsoft.KeyVault/vaults/{key_vault_name}"
            )

    # Collect role definitions to map IDs -> display names lazily.
    role_def_cache: dict[str, str] = {}

    def _role_name(role_def_id: str, scope: str) -> str | None:
        if role_def_id in role_def_cache:
            return role_def_cache[role_def_id]
        try:
            # role_def_id is the full ARM id; AuthorizationManagementClient
            # accepts the trailing GUID + the scope.
            guid = role_def_id.rsplit("/", 1)[-1]
            rd = auth.role_definitions.get(scope=scope, role_definition_id=guid)
            name = rd.role_name  # type: ignore[union-attr]
            role_def_cache[role_def_id] = name
            return name
        except Exception as exc:  # pragma: no cover — defensive
            logger.debug("Could not resolve role definition %s: %s", role_def_id, exc)
            return None

    arm_roles: set[str] = set()
    kv_roles: set[str] = set()

    # List all role assignments for the principal at subscription scope and
    # filter down to ones whose scope covers our targets.
    assignments = auth.role_assignments.list_for_subscription(
        filter=f"principalId eq '{principal_object_id}'",
    )
    for ra in assignments:
        scope = ra.scope  # type: ignore[union-attr]
        if not any(scope == s or s.startswith(scope + "/") or scope == s for s in covering_scopes):
            # The assignment covers our target only if its scope is at or
            # above one of the covering scopes (i.e. covering scope starts
            # with the assignment scope).
            if not any(s.startswith(scope) for s in covering_scopes):
                continue
        name = _role_name(ra.role_definition_id, scope)  # type: ignore[union-attr]
        if not name:
            continue
        if name.startswith("Key Vault "):
            kv_roles.add(name)
        else:
            arm_roles.add(name)

    return {"arm": sorted(arm_roles), "kv": sorted(kv_roles)}
