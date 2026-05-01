"""Tests for Phase 3 — Fabric-aware migration plan + cost projection."""
from __future__ import annotations

from pathlib import Path

import pytest

from ssis_modernization_agent.migration_plan import propose_design
from ssis_modernization_agent.migration_plan.fabric_costs import (
    _f_sku_for_cu,
    estimate_costs_dispatch,
    estimate_fabric_costs,
)
from ssis_modernization_agent.migration_plan.models import (
    MigrationPlan,
    MigrationTarget,
    PLAN_SCHEMA_VERSION,
    Simplification,
    SimplificationAction,
)
from ssis_modernization_agent.migration_plan.persistence import save_plan, load_plan
from ssis_modernization_agent.parsers.readers.local_reader import LocalReader

LNI = Path(r"C:\source\test-lni-packages\ADDS-MIPS-TC.dtsx")


# ---------------------------------------------------------------------------
# Migration plan model
# ---------------------------------------------------------------------------

def test_default_target_is_adf():
    plan = MigrationPlan(package_name="P", package_path="x.dtsx")
    assert plan.target == MigrationTarget.ADF


def test_target_field_round_trips_via_persistence(tmp_path):
    plan = MigrationPlan(
        package_name="P", package_path="x.dtsx",
        target=MigrationTarget.FABRIC,
    )
    out = tmp_path / "plan.json"
    save_plan(plan, out)
    loaded = load_plan(out)
    assert loaded.target == MigrationTarget.FABRIC
    assert loaded.schema_version == PLAN_SCHEMA_VERSION


# ---------------------------------------------------------------------------
# Proposer with target parameter
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not LNI.exists(), reason="LNI sample not present")
def test_proposer_default_target_adf():
    package = LocalReader().read(LNI)
    plan = propose_design(package)
    assert plan.target == MigrationTarget.ADF
    # Reasoning input records the target so downstream tools can inspect it
    assert plan.reasoning_input.get("target") == "adf"


@pytest.mark.skipif(not LNI.exists(), reason="LNI sample not present")
def test_proposer_fabric_target_adds_pyspark_summary():
    package = LocalReader().read(LNI)
    plan = propose_design(package, target=MigrationTarget.FABRIC)
    assert plan.target == MigrationTarget.FABRIC
    assert plan.reasoning_input.get("target") == "fabric"
    assert "Fabric" in plan.summary
    assert "PySpark" in plan.summary


@pytest.mark.skipif(not LNI.exists(), reason="LNI sample not present")
def test_proposer_fabric_target_adds_dataflow_porting_risk():
    package = LocalReader().read(LNI)
    plan = propose_design(package, target=MigrationTarget.FABRIC)
    df_risks = [r for r in plan.risks if "PySpark" in r.message]
    # Whether risk is present depends on whether the package has data flows;
    # ADDS-MIPS-TC does, so assert presence.
    if any(getattr(t, "task_type", None) and t.task_type.value == "DataFlowTask" for t in package.tasks):
        assert df_risks, "Expected a Fabric data-flow porting risk for a package with Data Flow Tasks"


# ---------------------------------------------------------------------------
# Fabric cost projection — pure functions
# ---------------------------------------------------------------------------

def test_f_sku_picks_smallest_with_headroom():
    # peak 1.0 CU at 30% headroom -> needs >= 1.3 CU -> F2 (= 2 CU)
    assert _f_sku_for_cu(1.0, headroom_pct=30.0) == 2


def test_f_sku_climbs_with_load():
    # 40 × 1.30 = 52 -> needs >= 52 CU -> F64
    assert _f_sku_for_cu(40.0, headroom_pct=30.0) == 64
    # 100 × 1.30 = 130 -> needs >= 130 CU -> F256 (F128 is only 128)
    assert _f_sku_for_cu(100.0, headroom_pct=30.0) == 256


def test_f_sku_returns_none_when_workload_exceeds_max():
    assert _f_sku_for_cu(5000.0, headroom_pct=30.0) is None


def test_estimate_fabric_costs_handles_empty_estate():
    result = estimate_fabric_costs(plans=[])
    assert result["platform"] == "fabric"
    assert result["package_count"] == 0
    assert result["recommended_sku"].startswith("F")


def test_estimate_fabric_costs_returns_expected_shape():
    plan = MigrationPlan(
        package_name="P", package_path="x.dtsx",
        target=MigrationTarget.FABRIC,
        reasoning_input={"task_counts": {"DataFlowTask": 2, "ExecuteSQLTask": 3}},
    )
    result = estimate_fabric_costs(plans=[plan], runs_per_day=4)
    assert result["platform"] == "fabric"
    assert "peak_cu" in result
    assert result["recommended_sku"].startswith("F")
    line_names = {li["name"] for li in result["line_items"]}
    assert any("reserved capacity" in n for n in line_names)
    assert any("OneLake" in n for n in line_names)
    assert any("Pay-as-you-go" in n for n in line_names)


def test_estimate_fabric_costs_scales_with_notebook_minutes():
    plan = MigrationPlan(
        package_name="P", package_path="x.dtsx",
        target=MigrationTarget.FABRIC,
        reasoning_input={"task_counts": {"DataFlowTask": 1}},
    )
    cheap = estimate_fabric_costs(plans=[plan], avg_notebook_minutes=1.0)
    expensive = estimate_fabric_costs(plans=[plan], avg_notebook_minutes=60.0)
    assert expensive["peak_cu"] > cheap["peak_cu"]


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def test_dispatch_infers_target_from_first_plan():
    fabric_plan = MigrationPlan(
        package_name="F", package_path="f.dtsx",
        target=MigrationTarget.FABRIC,
        reasoning_input={"task_counts": {"DataFlowTask": 1}},
    )
    result = estimate_costs_dispatch(plans=[fabric_plan])
    assert result["platform"] == "fabric"


def test_dispatch_infers_adf_when_no_target_specified():
    adf_plan = MigrationPlan(
        package_name="A", package_path="a.dtsx",
        target=MigrationTarget.ADF,
        reasoning_input={"task_counts": {"DataFlowTask": 1}},
    )
    result = estimate_costs_dispatch(plans=[adf_plan])
    # ADF result shape doesn't carry "platform" key — distinguishable by line items
    line_names = {li["name"] for li in result["line_items"]}
    assert any("DIU" in n for n in line_names)


def test_dispatch_explicit_target_overrides_plan_field():
    adf_plan = MigrationPlan(
        package_name="A", package_path="a.dtsx",
        target=MigrationTarget.ADF,
        reasoning_input={"task_counts": {"DataFlowTask": 1}},
    )
    result = estimate_costs_dispatch(plans=[adf_plan], target=MigrationTarget.FABRIC)
    assert result["platform"] == "fabric"


def test_dispatch_filters_kwargs_per_target():
    """ADF kwargs should not leak into estimate_fabric_costs and vice versa."""
    plan = MigrationPlan(
        package_name="P", package_path="x.dtsx",
        target=MigrationTarget.FABRIC,
        reasoning_input={"task_counts": {"DataFlowTask": 1}},
    )
    # Pass both ADF and Fabric kwargs — should not raise
    result = estimate_costs_dispatch(
        plans=[plan],
        target=MigrationTarget.FABRIC,
        avg_copy_diu=8.0,         # ADF-only
        avg_dataflow_vcores=16,   # ADF-only
        avg_notebook_minutes=5.0, # Fabric-only
        headroom_pct=50.0,        # Fabric-only
    )
    assert result["platform"] == "fabric"
    assert result["assumptions"]["avg_notebook_minutes"] == 5.0
    assert result["assumptions"]["headroom_pct"] == 50.0
