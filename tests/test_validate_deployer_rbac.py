"""P5-12: validate_deployer_rbac unit tests (no Azure calls).

Covers the offline path. Live path (`principal_object_id` set) is tested in
the deployer integration suite, not here, because it requires a real
Azure subscription. The handler is exercised through `_validate_deployer_rbac`
to also lock the MCP wiring.
"""
from __future__ import annotations

import asyncio
import json

from ssis_adf_agent.deployer.rbac_validator import (
    evaluate_rbac,
    list_known_tools,
)
from ssis_adf_agent.mcp_server import _validate_deployer_rbac


def test_known_tools_includes_core_deployment_surface() -> None:
    known = list_known_tools()
    for must in (
        "deploy_to_adf",
        "provision_adf_environment",
        "provision_function_app",
        "deploy_function_stubs",
        "activate_triggers",
        "smoke_test_pipeline",
        "upload_encrypted_secrets",
        "compare_dataflow_output",
        "compare_estimates_to_actuals",
    ):
        assert must in known, f"{must} missing from RBAC matrix"


def test_evaluate_ok_when_all_required_roles_held() -> None:
    report = evaluate_rbac(
        held_arm_roles=["Data Factory Contributor", "Reader"],
        held_kv_roles=[],
        planned_tools=["deploy_to_adf", "activate_triggers"],
    )
    assert report["summary"]["tools_blocked"] == 0
    assert all(f["status"] == "ok" for f in report["findings"])


def test_evaluate_blocks_when_arm_role_missing() -> None:
    report = evaluate_rbac(
        held_arm_roles=["Reader"],  # not enough for deploy_to_adf
        held_kv_roles=[],
        planned_tools=["deploy_to_adf"],
    )
    assert report["summary"]["tools_blocked"] == 1
    finding = report["findings"][0]
    assert finding["status"] == "missing_arm"
    assert finding["missing_arm_alternatives"] == [["Data Factory Contributor"]]


def test_evaluate_provision_adf_requires_both_contributor_and_uaa() -> None:
    """provision_adf_environment requires Contributor AND User Access Administrator."""
    only_contrib = evaluate_rbac(
        held_arm_roles=["Contributor"],
        held_kv_roles=[],
        planned_tools=["provision_adf_environment"],
    )
    assert only_contrib["findings"][0]["status"] == "missing_arm"
    assert only_contrib["findings"][0]["missing_arm_alternatives"] == [
        ["User Access Administrator"]
    ]

    both = evaluate_rbac(
        held_arm_roles=["Contributor", "User Access Administrator"],
        held_kv_roles=[],
        planned_tools=["provision_adf_environment"],
    )
    assert both["findings"][0]["status"] == "ok"


def test_evaluate_keyvault_alternative_either_role_satisfies() -> None:
    """deploy_to_adf_preflight needs Reader (ARM) AND a KV secrets-read role."""
    user = evaluate_rbac(
        held_arm_roles=["Reader"],
        held_kv_roles=["Key Vault Secrets User"],
        planned_tools=["deploy_to_adf_preflight"],
    )
    officer = evaluate_rbac(
        held_arm_roles=["Reader"],
        held_kv_roles=["Key Vault Secrets Officer"],
        planned_tools=["deploy_to_adf_preflight"],
    )
    assert user["findings"][0]["status"] == "ok"
    assert officer["findings"][0]["status"] == "ok"

    none = evaluate_rbac(
        held_arm_roles=["Reader"],
        held_kv_roles=[],
        planned_tools=["deploy_to_adf_preflight"],
    )
    assert none["findings"][0]["status"] == "missing_kv"


def test_evaluate_unknown_tool_reported_not_crashed() -> None:
    report = evaluate_rbac(
        held_arm_roles=["Owner"],
        held_kv_roles=[],
        planned_tools=["does_not_exist"],
    )
    assert report["findings"][0]["status"] == "unknown"


def test_mcp_handler_offline_mode_returns_findings() -> None:
    """Exercise the MCP wiring end-to-end in offline mode."""
    result = asyncio.run(_validate_deployer_rbac({
        "planned_tools": ["deploy_to_adf", "activate_triggers"],
        "held_arm_roles": ["Data Factory Contributor"],
        "held_kv_roles": [],
    }))
    payload = json.loads(result[0].text)
    assert payload["mode"] == "offline"
    assert payload["summary"]["tools_evaluated"] == 2
    assert payload["summary"]["tools_blocked"] == 0


def test_mcp_handler_requires_planned_tools() -> None:
    import pytest

    with pytest.raises(ValueError, match="planned_tools"):
        asyncio.run(_validate_deployer_rbac({"planned_tools": []}))


def test_mcp_handler_live_mode_requires_subscription() -> None:
    import pytest

    with pytest.raises(ValueError, match="subscription_id"):
        asyncio.run(_validate_deployer_rbac({
            "planned_tools": ["deploy_to_adf"],
            "principal_object_id": "00000000-0000-0000-0000-000000000000",
        }))
