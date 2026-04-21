"""Regression tests for the improved effort-estimation model.

Covers:
- EffortEstimate now has low/likely/high range and script/dataflow breakdowns.
- ScriptTask LOC-weighted porting hours add real dev time.
- Data Flow component-type weighting distinguishes heavy from light transforms.
- plan_migration_waves opt-in setup surcharge + learning curve.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from ssis_adf_agent.migration_plan import (
    EffortEstimate,
    LinkedServiceSpec,
    MigrationPlan,
    TargetPattern,
    plan_migration_waves,
)
from ssis_adf_agent.migration_plan.proposer import (
    _dataflow_hours,
    _effort_from_package,
    _script_loc,
    _script_porting_hours,
    propose_design,
)
from ssis_adf_agent.parsers.models import (
    ConnectionManagerType,
    DataFlowComponent,
    DataFlowTask,
    ProtectionLevel,
    ScriptTask,
    SSISPackage,
    TaskType,
)


# ---------------------------------------------------------------------------
# Low-level primitives
# ---------------------------------------------------------------------------

def test_script_loc_ignores_blank_and_comment_lines():
    task = ScriptTask(
        id="s1", name="Test", task_type=TaskType.SCRIPT,
        source_code=(
            "// header comment\n"
            "using System;\n"
            "\n"
            "public void Main() {\n"
            "    // inline\n"
            "    int x = 1;\n"
            "}\n"
        ),
    )
    # 4 real lines: using, public void Main() {, int x = 1;, }
    assert _script_loc(task) == 4


def test_script_porting_hours_scales_with_loc_for_complex_scripts():
    # Build a Script Task that will classify as 'complex' (uses SqlConnection)
    trivial = ScriptTask(
        id="s1", name="Trivial", task_type=TaskType.SCRIPT,
        source_code="Dts.Variables[\"x\"].Value = 1;",
    )
    trivial_hours, _ = _script_porting_hours(trivial)

    big_complex = ScriptTask(
        id="s2", name="Big", task_type=TaskType.SCRIPT,
        source_code="var conn = new SqlConnection(\"...\");\n" + "x = x + 1;\n" * 300,
    )
    big_hours, _ = _script_porting_hours(big_complex)

    # Trivial is capped near its floor; complex scales with LOC and is much larger.
    assert trivial_hours <= 1.0
    assert big_hours >= 10.0
    assert big_hours <= 40.0  # capped


def test_dataflow_hours_heavier_for_fuzzy_lookup_than_derived_column():
    simple_df = DataFlowTask(id="d1", name="Simple", components=[
        DataFlowComponent(id="c1", name="src", component_class_id="x", component_type="OLEDBSource"),
        DataFlowComponent(id="c2", name="dc", component_class_id="x", component_type="DerivedColumn"),
        DataFlowComponent(id="c3", name="dst", component_class_id="x", component_type="OLEDBDestination"),
    ])
    heavy_df = DataFlowTask(id="d2", name="Heavy", components=[
        DataFlowComponent(id="c1", name="src", component_class_id="x", component_type="OLEDBSource"),
        DataFlowComponent(id="c2", name="fz",  component_class_id="x", component_type="FuzzyLookup"),
        DataFlowComponent(id="c3", name="scd", component_class_id="x", component_type="SlowlyChangingDimension"),
        DataFlowComponent(id="c4", name="dst", component_class_id="x", component_type="OLEDBDestination"),
    ])
    simple_hours, s_h, s_m, s_l = _dataflow_hours(simple_df)
    heavy_hours, h_h, h_m, h_l = _dataflow_hours(heavy_df)
    assert s_h == 0 and s_m == 0 and s_l == 3
    assert h_h == 2 and h_l == 2
    # Heavy DF should be at least 3x the simple one.
    assert heavy_hours >= simple_hours * 3


# ---------------------------------------------------------------------------
# End-to-end via propose_design
# ---------------------------------------------------------------------------

def _simple_package(name: str = "pkg", tasks=None) -> SSISPackage:
    return SSISPackage(
        id=f"id-{name}",
        name=name,
        source_file=f"/tmp/{name}.dtsx",
        protection_level=ProtectionLevel.DONT_SAVE_SENSITIVE,
        tasks=tasks or [],
    )


def test_effort_has_range_and_breakdown():
    pkg = _simple_package(tasks=[
        DataFlowTask(id="d1", name="df1", components=[
            DataFlowComponent(id="c1", name="src", component_class_id="x", component_type="OLEDBSource"),
            DataFlowComponent(id="c2", name="dst", component_class_id="x", component_type="OLEDBDestination"),
        ])
    ])
    plan = propose_design(pkg)
    e = plan.effort
    assert e.total_hours > 0
    assert e.low_hours > 0 and e.high_hours > 0
    # Range is asymmetric: low < likely < high
    assert e.low_hours < e.total_hours < e.high_hours
    # Coarse bounds: low is ~70%, high is ~160%.
    assert e.low_hours == pytest.approx(e.total_hours * 0.7, rel=0.02)
    assert e.high_hours == pytest.approx(e.total_hours * 1.6, rel=0.02)


def test_complex_script_dominates_estimate():
    simple_pkg = _simple_package(name="simple", tasks=[
        DataFlowTask(id="d1", name="df1", components=[
            DataFlowComponent(id="c1", name="src", component_class_id="x", component_type="OLEDBSource"),
            DataFlowComponent(id="c2", name="dst", component_class_id="x", component_type="OLEDBDestination"),
        ])
    ])
    script_pkg = _simple_package(name="scripty", tasks=[
        ScriptTask(
            id="s1", name="Heavy", task_type=TaskType.SCRIPT,
            source_code=(
                "var conn = new SqlConnection(\"...\");\n"
                + "var cmd = new SqlCommand();\n"
                + "x = x + 1;\n" * 200
            ),
        )
    ])
    simple_eff = propose_design(simple_pkg).effort
    script_eff = propose_design(script_pkg).effort
    # Script porting dominates
    assert script_eff.script_porting_hours > 5.0
    assert script_eff.total_hours > simple_eff.total_hours * 2


# ---------------------------------------------------------------------------
# Wave-level surcharge + learning curve
# ---------------------------------------------------------------------------

def _plan(name: str, bucket: str, pattern: str, hours: float = 8.0, score: int = 25) -> MigrationPlan:
    return MigrationPlan(
        package_name=name,
        package_path=f"/tmp/{name}.dtsx",
        target_pattern=TargetPattern(pattern),
        effort=EffortEstimate(total_hours=hours, bucket=bucket,
                              low_hours=round(hours * 0.7, 1),
                              high_hours=round(hours * 1.6, 1)),
        reasoning_input={"complexity_score": score, "task_counts": {}},
    )


def test_plan_migration_waves_defaults_unchanged():
    # Back-compat: without the new knobs, totals match the legacy behaviour.
    plans = [_plan(f"p{i}", "low", "scheduled_file_drop") for i in range(3)]
    result = plan_migration_waves(plans)
    assert result["total_estimated_hours"] == pytest.approx(24.0)
    assert result["estate_setup_hours"] == 0
    assert result["learning_curve_applied"] is False
    assert "setup_surcharge_hours" not in result["waves"][0]


def test_estate_setup_surcharge_applied_to_wave_one():
    plans = [_plan(f"p{i}", "low", "scheduled_file_drop") for i in range(3)]
    result = plan_migration_waves(plans, estate_setup_hours=24)
    assert result["estate_setup_hours"] == 24
    assert result["waves"][0]["setup_surcharge_hours"] == 24
    # Wave 1 hours = 3 * 8 + 24 = 48
    assert result["waves"][0]["estimated_hours"] == pytest.approx(48.0)
    assert result["total_estimated_hours"] == pytest.approx(48.0)


def test_learning_curve_discounts_packages_within_wave():
    # Five identical 10-hour packages in a single wave.
    plans = [_plan(f"p{i}", "low", "scheduled_file_drop", hours=10.0) for i in range(5)]
    straight = plan_migration_waves(plans)
    discounted = plan_migration_waves(plans, apply_learning_curve=True)
    assert straight["total_estimated_hours"] == pytest.approx(50.0)
    # Curve sequence: 1.00, 0.90, 0.85, 0.80, 0.75 → sum=4.30 × 10h = 43h
    assert discounted["total_estimated_hours"] == pytest.approx(43.0, abs=0.1)
    assert discounted["learning_curve_applied"] is True
