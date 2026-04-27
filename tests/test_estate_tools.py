"""Tests for the estate-level helpers in migration_plan.estate_tools."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.migration_plan import (
    AuthMode,
    EffortEstimate,
    LinkedServiceSpec,
    MigrationPlan,
    PlanEditError,
    Simplification,
    SimplificationAction,
    TargetPattern,
    edit_migration_plan,
    estimate_adf_costs,
    plan_migration_waves,
)


# ---------------------------------------------------------------------------
# helpers — build MigrationPlan fixtures for wave / cost tests
# ---------------------------------------------------------------------------

def _make_plan(
    name: str,
    bucket: str,
    pattern: str,
    score: int = 50,
    hours: float = 8.0,
    *,
    task_counts: dict[str, int] | None = None,
    simplifications: list[Simplification] | None = None,
    linked_services: list[LinkedServiceSpec] | None = None,
) -> MigrationPlan:
    return MigrationPlan(
        package_name=name,
        package_path=f"/tmp/{name}.dtsx",
        target_pattern=TargetPattern(pattern),
        effort=EffortEstimate(
            total_hours=hours,
            bucket=bucket,
        ),
        reasoning_input={
            "complexity_score": score,
            "task_counts": task_counts or {},
        },
        simplifications=simplifications or [],
        linked_services=linked_services or [],
    )


# ---------------------------------------------------------------------------
# plan_migration_waves
# ---------------------------------------------------------------------------

def test_plan_migration_waves_groups_bulk_first_then_review_by_pattern():
    plans = [
        _make_plan("low_a", "low", "scheduled_file_drop"),
        _make_plan("med_b", "medium", "scheduled_file_drop"),
        _make_plan("low_c", "low", "ingest_file_to_sql"),
        _make_plan("hi_x", "high", "scheduled_file_drop", score=70, hours=20),
        _make_plan("vh_y", "very_high", "script_heavy", score=90, hours=40),
    ]
    result = plan_migration_waves(plans)
    waves = result["waves"]
    assert result["wave_count"] == 4
    # First waves are bulk_convert, sorted by pattern alphabetically
    bulk_waves = [w for w in waves if w["strategy"] == "bulk_convert"]
    assert len(bulk_waves) == 2
    assert bulk_waves[0]["target_pattern"] == "ingest_file_to_sql"  # alphabetical
    assert bulk_waves[1]["target_pattern"] == "scheduled_file_drop"
    review_waves = [w for w in waves if w["strategy"] == "design_review"]
    assert len(review_waves) == 2
    # Total estate hours preserved
    assert result["total_estimated_hours"] == pytest.approx(8 + 8 + 8 + 20 + 40, abs=0.01)


def test_plan_migration_waves_chunks_large_groups():
    plans = [_make_plan(f"p{i}", "low", "scheduled_file_drop") for i in range(25)]
    result = plan_migration_waves(plans, max_packages_per_wave=10)
    waves = result["waves"]
    assert result["wave_count"] == 3
    assert [w["package_count"] for w in waves] == [10, 10, 5]


def test_plan_migration_waves_empty_plans():
    result = plan_migration_waves([])
    assert result["wave_count"] == 0
    assert result["total_packages"] == 0
    assert result["waves"] == []


# ---------------------------------------------------------------------------
# estimate_adf_costs
# ---------------------------------------------------------------------------

def test_estimate_adf_costs_returns_breakdown_and_totals():
    plans = [
        _make_plan(
            "p1", "low", "scheduled_file_drop",
            task_counts={"DataFlowTask": 1, "ExecuteSQLTask": 2},
            linked_services=[
                LinkedServiceSpec(name="LS_Sql", type="AzureSqlDatabase"),
                LinkedServiceSpec(name="LS_Blob", type="AzureBlobStorage"),
            ],
        ),
        _make_plan(
            "p2", "medium", "ingest_file_to_sql",
            task_counts={"ExecuteSQLTask": 3},
            linked_services=[LinkedServiceSpec(name="LS_Sql2", type="AzureSqlDatabase")],
        ),
        _make_plan(
            "p3", "low", "sql_to_sql_copy",
            task_counts={"ExecuteSQLTask": 1},
            linked_services=[LinkedServiceSpec(name="LS_Sql3", type="AzureSqlDatabase")],
        ),
    ]
    est = estimate_adf_costs(plans=plans, runs_per_day=2, storage_gb=50)
    assert est["package_count"] == 3
    assert est["currency"] == "USD"
    assert len(est["line_items"]) == 5
    total = sum(li["monthly_usd"] for li in est["line_items"])
    assert est["monthly_total_usd"] == pytest.approx(total, abs=0.01)
    assert est["annual_total_usd"] == pytest.approx(total * 12, abs=0.01)
    # Activity mix should be derived from plans
    assert "activity_mix" in est
    assert est["activity_mix"]["total_dataflow_activities"] == 1


def test_estimate_adf_costs_honours_rate_overrides():
    plans = [_make_plan("p1", "low", "scheduled_file_drop")]
    est = estimate_adf_costs(
        plans=plans,
        runs_per_day=1,
        rates={"storage_gb_month_hot": 1.0},  # absurd rate to make storage dominant
        storage_gb=100,
    )
    storage = next(li for li in est["line_items"] if "storage" in li["name"].lower())
    assert storage["monthly_usd"] == pytest.approx(100.0, abs=0.01)


def test_estimate_adf_costs_fold_simplification_shifts_df_to_copy():
    """When fold_to_copy_activity is in simplifications, DFs become copies."""
    plans = [
        _make_plan(
            "p1", "low", "scheduled_file_drop",
            task_counts={"DataFlowTask": 2},
            simplifications=[
                Simplification(
                    action=SimplificationAction.FOLD_TO_COPY_ACTIVITY,
                    items=["DFT_1"],
                    reason="trivial",
                ),
            ],
        ),
    ]
    est = estimate_adf_costs(plans=plans)
    mix = est["activity_mix"]
    # All DFs get folded to copies when fold simplification is present
    assert mix["total_dataflow_activities"] == 0
    assert mix["total_copy_activities"] >= 2


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


def test_edit_migration_plan_set_name_override():
    plan = _base_plan()
    new = edit_migration_plan(plan, {
        "set_name_override": {
            "LS:MyConn": "LS_CustomSql",
            "PL": "PL_MyPipeline",
        },
    })
    assert new.name_overrides == {"LS:MyConn": "LS_CustomSql", "PL": "PL_MyPipeline"}
    # Original is untouched
    assert plan.name_overrides == {}


def test_edit_migration_plan_remove_name_override():
    plan = _base_plan()
    plan = edit_migration_plan(plan, {
        "set_name_override": {"LS:A": "LS_X", "LS:B": "LS_Y", "TR": "TR_Z"},
    })
    new = edit_migration_plan(plan, {"remove_name_override": "LS:A"})
    assert "LS:A" not in new.name_overrides
    assert new.name_overrides == {"LS:B": "LS_Y", "TR": "TR_Z"}


def test_edit_migration_plan_remove_name_override_list():
    plan = edit_migration_plan(_base_plan(), {
        "set_name_override": {"LS:A": "LS_X", "LS:B": "LS_Y"},
    })
    new = edit_migration_plan(plan, {"remove_name_override": ["LS:A", "LS:B"]})
    assert new.name_overrides == {}


def test_edit_migration_plan_rejects_invalid_name_override_prefix():
    with pytest.raises(PlanEditError):
        edit_migration_plan(_base_plan(), {
            "set_name_override": {"XX:bad": "nope"},
        })
