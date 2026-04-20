"""Tests for the SSIS Migration Copilot — proposer + persistence + MCP wiring."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.migration_plan import (
    PLAN_SCHEMA_VERSION,
    AuthMode,
    MigrationPlan,
    SimplificationAction,
    TargetPattern,
    load_plan,
    propose_design,
    save_plan,
)
from ssis_adf_agent.parsers.models import (  # type: ignore[attr-defined]
    ConnectionManagerType,
    DataFlowComponent,
    DataFlowTask,
    FileSystemTask,
    SSISConnectionManager,
    SSISPackage,
    TaskType,
)


def _build_scheduled_file_drop_package() -> SSISPackage:
    """Build a synthetic package matching the SCHEDULED_FILE_DROP pattern."""
    sql_cm = SSISConnectionManager(
        id="cm-sql", name="SrcSQL", type=ConnectionManagerType.OLEDB,
        server="srv", database="db",
    )
    file_cm = SSISConnectionManager(
        id="cm-file", name="SinkFlat", type=ConnectionManagerType.FLAT_FILE,
        file_path=r"\\fileserver\share\out.csv",
    )
    src = DataFlowComponent(
        id="src", name="Src", component_class_id="x",
        component_type="OLEDBSource", connection_id="cm-sql",
    )
    dc = DataFlowComponent(
        id="dc", name="DC", component_class_id="x",
        component_type="DerivedColumn",
    )
    sink = DataFlowComponent(
        id="dst", name="Dst", component_class_id="x",
        component_type="FlatFileDestination", connection_id="cm-file",
    )
    df = DataFlowTask(
        id="df-1", name="Data Flow Task", task_type=TaskType.DATA_FLOW,
        components=[src, dc, sink],
    )
    fs1 = FileSystemTask(
        id="fs-1", name="Copy Template", task_type=TaskType.FILE_SYSTEM,
        operation="CopyFile",
    )
    fs2 = FileSystemTask(
        id="fs-2", name="Set Attributes", task_type=TaskType.FILE_SYSTEM,
        operation="SetAttributes",
    )
    fs3 = FileSystemTask(
        id="fs-3", name="Rename File", task_type=TaskType.FILE_SYSTEM,
        operation="RenameFile",
    )
    return SSISPackage(
        id="pkg-1", name="TestPkg", source_file="test.dtsx",
        connection_managers=[sql_cm, file_cm],
        tasks=[fs1, fs2, fs3, df],
    )


def test_proposer_detects_scheduled_file_drop_pattern() -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    assert plan.target_pattern == TargetPattern.SCHEDULED_FILE_DROP


def test_proposer_recommends_dropping_atomic_write_filesystem_tasks() -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    drops = [s for s in plan.simplifications if s.action == SimplificationAction.DROP]
    assert len(drops) == 1
    assert set(drops[0].items) == {"Copy Template", "Set Attributes", "Rename File"}


def test_proposer_recommends_folding_simple_dataflow_to_copy_activity() -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    folds = [s for s in plan.simplifications
             if s.action == SimplificationAction.FOLD_TO_COPY_ACTIVITY]
    assert len(folds) == 1
    assert folds[0].items == ["Data Flow Task"]


def test_proposer_recommends_managed_identity_by_default() -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    assert all(ls.auth == AuthMode.MANAGED_IDENTITY for ls in plan.linked_services)


def test_proposer_emits_storage_and_factory_in_infra() -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    types_ = {item.type for item in plan.infrastructure_needed}
    assert "Microsoft.DataFactory/factories" in types_
    assert "Microsoft.Storage/storageAccounts" in types_


def test_proposer_emits_rbac_for_each_mi_linked_service() -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    roles = {r.role for r in plan.rbac_needed}
    assert "db_datareader" in roles
    assert "Storage Blob Data Contributor" in roles


def test_plan_roundtrip_to_disk(tmp_path: Path) -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    p = tmp_path / "plan.json"
    save_plan(plan, p)
    loaded = load_plan(p)
    assert loaded.package_name == plan.package_name
    assert loaded.target_pattern == plan.target_pattern
    assert len(loaded.simplifications) == len(plan.simplifications)
    assert loaded.schema_version == PLAN_SCHEMA_VERSION


def test_load_plan_rejects_incompatible_major_version(tmp_path: Path) -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    p = tmp_path / "plan.json"
    save_plan(plan, p)
    raw = json.loads(p.read_text(encoding="utf-8"))
    raw["schema_version"] = "99.0"
    p.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="incompatible schema_version"):
        load_plan(p)


def test_plan_renders_markdown_summary() -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    md = plan.render_markdown()
    assert "Migration plan" in md
    assert "scheduled_file_drop" in md
    assert "Effort estimate" in md


def test_proposer_includes_reasoning_input_for_agent_consumption() -> None:
    pkg = _build_scheduled_file_drop_package()
    plan = propose_design(pkg)
    assert "complexity_score" in plan.reasoning_input
    assert "task_counts" in plan.reasoning_input
    assert plan.reasoning_input["task_counts"][TaskType.DATA_FLOW.value] == 1
