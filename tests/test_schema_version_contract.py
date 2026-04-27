"""P5-6: schema_version contract for lineage.json and migration_plan.json.

Both files carry top-level ``schema_version`` and both loaders share the
same forward-compatibility policy: incompatible *major* version is
rejected with a clear message; unknown *minor* version is accepted with
a warning. This pins the contract so a future minor bump cannot
silently break downstream CI tooling.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from ssis_adf_agent.generators.lineage_generator import (
    LINEAGE_SCHEMA_VERSION,
    load_lineage,
)
from ssis_adf_agent.migration_plan.models import (
    PLAN_SCHEMA_VERSION,
    MigrationPlan,
)
from ssis_adf_agent.migration_plan.persistence import load_plan, save_plan


def _write_lineage(path: Path, schema_version: str) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": schema_version,
                "generated_at": "2026-04-24T00:00:00+00:00",
                "agent_version": "test",
                "source": {"package_name": "p"},
                "artifacts": {},
                "activity_origins": [],
            }
        ),
        encoding="utf-8",
    )


def test_lineage_current_version_loads(tmp_path: Path) -> None:
    f = tmp_path / "lineage.json"
    _write_lineage(f, LINEAGE_SCHEMA_VERSION)
    assert load_lineage(f)["schema_version"] == LINEAGE_SCHEMA_VERSION


def test_lineage_unknown_minor_warns_but_loads(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    f = tmp_path / "lineage.json"
    major = LINEAGE_SCHEMA_VERSION.split(".")[0]
    future_minor = f"{major}.999"
    _write_lineage(f, future_minor)
    with caplog.at_level(logging.WARNING):
        out = load_lineage(f)
    assert out["schema_version"] == future_minor
    assert any("forward-compat" in r.message for r in caplog.records)


def test_lineage_unknown_major_rejected(tmp_path: Path) -> None:
    f = tmp_path / "lineage.json"
    major = int(LINEAGE_SCHEMA_VERSION.split(".")[0])
    bad = f"{major + 1}.0"
    _write_lineage(f, bad)
    with pytest.raises(ValueError, match="incompatible schema_version"):
        load_lineage(f)


def _make_plan() -> MigrationPlan:
    return MigrationPlan(package_name="p", package_path="/tmp/p.dtsx")


def test_plan_current_version_loads(tmp_path: Path) -> None:
    f = save_plan(_make_plan(), tmp_path / "plan.json")
    loaded = load_plan(f)
    assert loaded.schema_version == PLAN_SCHEMA_VERSION


def test_plan_unknown_minor_warns_but_loads(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    f = tmp_path / "plan.json"
    raw = json.loads(_make_plan().model_dump_json())
    major = PLAN_SCHEMA_VERSION.split(".")[0]
    raw["schema_version"] = f"{major}.999"
    f.write_text(json.dumps(raw), encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        loaded = load_plan(f)
    assert loaded.schema_version == f"{major}.999"
    assert any("differs from current" in r.message for r in caplog.records)


def test_plan_unknown_major_rejected(tmp_path: Path) -> None:
    f = tmp_path / "plan.json"
    raw = json.loads(_make_plan().model_dump_json())
    major = int(PLAN_SCHEMA_VERSION.split(".")[0])
    raw["schema_version"] = f"{major + 1}.0"
    f.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="incompatible schema_version"):
        load_plan(f)
