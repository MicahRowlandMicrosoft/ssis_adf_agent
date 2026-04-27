"""Tests for the estate-level consolidation/dedup analyzer."""
from __future__ import annotations

import pytest

from ssis_adf_agent.analyzers.consolidation_analyzer import (
    analyze_estate_consolidation,
    find_consolidation_candidates,
    find_dedup_candidates,
)
from ssis_adf_agent.parsers.models import (
    ConnectionManagerType,
    DataFlowComponent,
    DataFlowTask,
    ProtectionLevel,
    SSISConnectionManager,
    SSISPackage,
)


def _pkg(name, cms, tasks=None):
    return SSISPackage(
        id=f"id-{name}", name=name, source_file=f"/tmp/{name}.dtsx",
        protection_level=ProtectionLevel.DONT_SAVE_SENSITIVE,
        connection_managers=cms, tasks=tasks or [],
    )


def _sql_cm(name, server, db):
    return SSISConnectionManager(
        id=f"cm-{name}", name=name, type=ConnectionManagerType.OLEDB,
        server=server, database=db,
    )


def _flat_cm(name, path):
    return SSISConnectionManager(
        id=f"cm-{name}", name=name, type=ConnectionManagerType.FLAT_FILE,
        file_path=path,
    )


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def test_dedup_finds_shared_sql_server_across_packages():
    pkgs = [
        _pkg("p1", [_sql_cm("Conn1", ".\\sql2016", "AdventureWorks2016")]),
        _pkg("p2", [_sql_cm("Conn1", ".\\sql2016", "AdventureWorks2016")]),
        _pkg("p3", [_sql_cm("AW", ".\\sql2016", "AdventureWorks2016")]),  # different SSIS name
    ]
    candidates = find_dedup_candidates(pkgs)
    assert len(candidates) == 1
    c = candidates[0]
    assert c["duplicate_count"] == 3
    assert c["connection_type"] == ConnectionManagerType.OLEDB.value
    # Hours saved = (3-1) * 0.5 = 1.0
    assert c["hours_saved_if_shared"] == pytest.approx(1.0)
    # Picks up the friendly-name variants
    assert set(c["ssis_names"]) == {"Conn1", "AW"}


def test_dedup_normalizes_case_and_whitespace():
    pkgs = [
        _pkg("p1", [_sql_cm("a", ".\\SQL2016", "AdventureWorks2016")]),
        _pkg("p2", [_sql_cm("b", ".\\sql2016", "  AdventureWorks2016 ")]),
    ]
    assert len(find_dedup_candidates(pkgs)) == 1


def test_dedup_skips_unique_connections():
    pkgs = [
        _pkg("p1", [_sql_cm("a", "server1", "db1")]),
        _pkg("p2", [_sql_cm("b", "server2", "db2")]),
    ]
    assert find_dedup_candidates(pkgs) == []


def test_dedup_groups_flat_files_by_path():
    pkgs = [
        _pkg("p1", [_flat_cm("f1", r"c:\data\input.csv")]),
        _pkg("p2", [_flat_cm("f2", r"C:\Data\Input.csv")]),
        _pkg("p3", [_flat_cm("f3", r"c:\data\other.csv")]),
    ]
    candidates = find_dedup_candidates(pkgs)
    assert len(candidates) == 1
    assert candidates[0]["duplicate_count"] == 2
    assert "input.csv" in candidates[0]["file_path"]


# ---------------------------------------------------------------------------
# Consolidation (uses the existing similarity_analyzer)
# ---------------------------------------------------------------------------

def test_consolidation_finds_structurally_identical_packages():
    # Two packages with the exact same component shape: source + lookup + dest
    df_components = [
        DataFlowComponent(id="c1", name="src", component_class_id="x", component_type="OLEDBSource"),
        DataFlowComponent(id="c2", name="lkp", component_class_id="x", component_type="Lookup"),
        DataFlowComponent(id="c3", name="dst", component_class_id="x", component_type="OLEDBDestination"),
    ]
    pkgs = [
        _pkg("p1", [], tasks=[DataFlowTask(id="d1", name="DF", components=list(df_components))]),
        _pkg("p2", [], tasks=[DataFlowTask(id="d1", name="DF", components=list(df_components))]),
        _pkg("p3", [], tasks=[DataFlowTask(id="d1", name="DF", components=list(df_components))]),
    ]
    candidates = find_consolidation_candidates(pkgs)
    assert len(candidates) == 1
    g = candidates[0]
    assert g["package_count"] == 3
    # Net = 3*4.0 - 8.0 = 4.0
    assert g["estimated_hours_saved"] == pytest.approx(4.0)
    assert "ownership" in " ".join(g["tradeoffs"]).lower()


def test_consolidation_yields_no_groups_for_unique_packages():
    pkgs = [
        _pkg("p1", [], tasks=[DataFlowTask(id="d1", name="DF", components=[
            DataFlowComponent(id="c1", name="src", component_class_id="x", component_type="OLEDBSource"),
        ])]),
        _pkg("p2", [], tasks=[DataFlowTask(id="d1", name="DF", components=[
            DataFlowComponent(id="c1", name="src", component_class_id="x", component_type="FlatFileSource"),
        ])]),
    ]
    assert find_consolidation_candidates(pkgs) == []


# ---------------------------------------------------------------------------
# Top-level wrapper
# ---------------------------------------------------------------------------

def test_analyze_estate_consolidation_returns_both_buckets():
    pkgs = [
        _pkg("p1", [_sql_cm("a", "srv", "db")], tasks=[]),
        _pkg("p2", [_sql_cm("b", "srv", "db")], tasks=[]),
        _pkg("p3", [_sql_cm("c", "srv", "db")], tasks=[]),
    ]
    result = analyze_estate_consolidation(pkgs)
    assert "deduplication" in result and "consolidation" in result
    assert result["deduplication"]["candidate_count"] == 1
    assert result["deduplication"]["total_hours_saved"] == pytest.approx(1.0)
    # The similarity analyzer may or may not group empty packages depending on
    # its threshold; just sanity-check the structure is present.
    assert isinstance(result["consolidation"]["candidate_group_count"], int)
    # Recommended-action text mentions shared_artifacts_dir
    assert "shared_artifacts_dir" in result["deduplication"]["recommended_action"]
