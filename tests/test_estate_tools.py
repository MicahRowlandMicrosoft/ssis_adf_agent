"""Tests for the estate-level helpers in migration_plan.estate_tools."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.migration_plan import (
    AuthMode,
    LinkedServiceSpec,
    MigrationPlan,
    PlanEditError,
    SimplificationAction,
    TargetPattern,
    edit_migration_plan,
    estimate_adf_costs,
    plan_migration_waves,
)


# ---------------------------------------------------------------------------
# plan_migration_waves
# ---------------------------------------------------------------------------

def _make_pkg(name, bucket, pattern, score=50, hours=8.0):
    return {
        "package_name": name,
        "complexity_bucket": bucket,
        "target_pattern": pattern,
        "complexity_score": score,
        "estimated_total_hours": hours,
        "manual_required_count": 0,
    }


def test_plan_migration_waves_groups_bulk_first_then_review_by_pattern():
    estate = {
        "packages": [
            _make_pkg("low_a", "low", "scheduled_file_drop"),
            _make_pkg("med_b", "medium", "scheduled_file_drop"),
            _make_pkg("low_c", "low", "ingest_file_to_sql"),
            _make_pkg("hi_x", "high", "scheduled_file_drop", score=70, hours=20),
            _make_pkg("vh_y", "very_high", "script_heavy", score=90, hours=40),
        ],
        "failures": [],
    }
    plan = plan_migration_waves(estate)
    waves = plan["waves"]
    assert plan["wave_count"] == 4
    # First waves are bulk_convert, sorted by pattern alphabetically
    bulk_waves = [w for w in waves if w["strategy"] == "bulk_convert"]
    assert len(bulk_waves) == 2
    assert bulk_waves[0]["target_pattern"] == "ingest_file_to_sql"  # alphabetical
    assert bulk_waves[1]["target_pattern"] == "scheduled_file_drop"
    review_waves = [w for w in waves if w["strategy"] == "design_review"]
    assert len(review_waves) == 2
    # Total estate hours preserved
    assert plan["total_estimated_hours"] == pytest.approx(8 + 8 + 8 + 20 + 40, abs=0.01)


def test_plan_migration_waves_chunks_large_groups():
    pkgs = [_make_pkg(f"p{i}", "low", "scheduled_file_drop") for i in range(25)]
    plan = plan_migration_waves({"packages": pkgs, "failures": []}, max_packages_per_wave=10)
    waves = plan["waves"]
    assert plan["wave_count"] == 3
    assert [w["package_count"] for w in waves] == [10, 10, 5]


def test_plan_migration_waves_appends_triage_for_failures():
    plan = plan_migration_waves({
        "packages": [_make_pkg("ok", "low", "scheduled_file_drop")],
        "failures": [{"path": "/bad.dtsx", "error": "boom"}],
    })
    assert plan["waves"][-1]["strategy"] == "triage"
    assert plan["waves"][-1]["packages"] == ["/bad.dtsx"]


# ---------------------------------------------------------------------------
# estimate_adf_costs
# ---------------------------------------------------------------------------

def test_estimate_adf_costs_returns_breakdown_and_totals():
    estate = {"package_count": 3}
    est = estimate_adf_costs(estate_report=estate, runs_per_day=2, storage_gb=50)
    assert est["package_count"] == 3
    assert est["currency"] == "USD"
    assert len(est["line_items"]) == 5
    total = sum(li["monthly_usd"] for li in est["line_items"])
    assert est["monthly_total_usd"] == pytest.approx(total, abs=0.01)
    assert est["annual_total_usd"] == pytest.approx(total * 12, abs=0.01)


def test_estimate_adf_costs_honours_rate_overrides():
    est = estimate_adf_costs(
        estate_report={"package_count": 1},
        runs_per_day=1,
        rates={"storage_gb_month_hot": 1.0},  # absurd rate to make storage dominant
        storage_gb=100,
    )
    storage = next(li for li in est["line_items"] if "storage" in li["name"].lower())
    assert storage["monthly_usd"] == pytest.approx(100.0, abs=0.01)


# ---------------------------------------------------------------------------
# edit_migration_plan
# ---------------------------------------------------------------------------

def _base_plan() -> MigrationPlan:
    return MigrationPlan(
        package_name="P",
        package_path="/tmp/p.dtsx",
        target_pattern=TargetPattern.CUSTOM,
        linked_services=[
            LinkedServiceSpec(name="LS_Sql", type="AzureSqlDatabase", auth=AuthMode.SQL_AUTH),
        ],
    )


def test_edit_migration_plan_set_auth_mode_updates_all_linked_services():
    plan = _base_plan()
    new = edit_migration_plan(plan, {"set_auth_mode": "ManagedIdentity"})
    assert new.linked_services[0].auth == AuthMode.MANAGED_IDENTITY
    # Original is untouched
    assert plan.linked_services[0].auth == AuthMode.SQL_AUTH


def test_edit_migration_plan_add_and_drop_simplification():
    plan = _base_plan()
    added = edit_migration_plan(plan, {
        "add_simplification": {
            "action": SimplificationAction.DROP.value,
            "items": ["TaskA"],
            "reason": "dead",
        },
    })
    assert len(added.simplifications) == 1
    dropped = edit_migration_plan(added, {"drop_simplification": SimplificationAction.DROP.value})
    assert dropped.simplifications == []


def test_edit_migration_plan_rejects_unknown_keys():
    with pytest.raises(PlanEditError):
        edit_migration_plan(_base_plan(), {"set_color": "blue"})


def test_edit_migration_plan_rejects_invalid_auth_mode():
    with pytest.raises(PlanEditError):
        edit_migration_plan(_base_plan(), {"set_auth_mode": "Bogus"})


def test_edit_migration_plan_set_target_pattern_and_summary():
    new = edit_migration_plan(_base_plan(), {
        "set_target_pattern": "scheduled_file_drop",
        "set_summary": "Customer-approved file drop",
        "set_customer_decision": {"region": "eastus2"},
    })
    assert new.target_pattern == TargetPattern.SCHEDULED_FILE_DROP
    assert new.summary == "Customer-approved file drop"
    assert new.customer_decisions == {"region": "eastus2"}
