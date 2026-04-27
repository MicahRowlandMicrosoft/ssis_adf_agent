"""
M1: lineage manifest.

Every conversion writes ``lineage.json`` next to the ADF artifacts so an
auditor can answer "which .dtsx produced this pipeline" and
"where is it deployed in Azure" without re-running the parser.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from ssis_adf_agent.generators.lineage_generator import (
    generate_lineage_manifest,
    update_lineage_with_deployment,
)
from ssis_adf_agent.parsers.models import SSISPackage


def _make_package(tmp_path: Path) -> SSISPackage:
    src = tmp_path / "MyPackage.dtsx"
    src.write_bytes(b"<DTS:Executable>fake</DTS:Executable>")
    return SSISPackage(
        id="{ABC-123}",
        name="MyPackage",
        source_file=str(src),
    )


def _seed_artifacts(output_dir: Path) -> None:
    """Create a minimal artifact tree under output_dir."""
    for sub, name in [
        ("linkedService", "LS_Sql"),
        ("dataset", "DS_Customer"),
        ("dataflow", "DF_Customer"),
        ("trigger", "TR_Daily"),
    ]:
        d = output_dir / sub
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{name}.json").write_text(
            json.dumps({"name": name, "properties": {}}), encoding="utf-8"
        )


def _pipeline_with_one_activity() -> dict:
    return {
        "name": "PL_MyPackage",
        "properties": {
            "activities": [{
                "name": "Stored_LoadCustomer",
                "type": "SqlServerStoredProcedure",
                "userProperties": [
                    {"name": "ssis_task_id", "value": "{TASK-1}"},
                    {"name": "ssis_task_name", "value": "Load Customer SP"},
                ],
            }],
        },
    }


class TestGenerateLineageManifest:
    def test_writes_lineage_json_with_source_metadata(self, tmp_path):
        pkg = _make_package(tmp_path)
        out = tmp_path / "adf"
        out.mkdir()
        _seed_artifacts(out)

        manifest = generate_lineage_manifest(pkg, out, _pipeline_with_one_activity())

        on_disk = json.loads((out / "lineage.json").read_text(encoding="utf-8"))
        assert on_disk == manifest
        assert manifest["schema_version"] == "1.0"
        assert manifest["source"]["package_name"] == "MyPackage"
        assert manifest["source"]["package_id"] == "{ABC-123}"
        assert len(manifest["source"]["sha256"]) == 64

    def test_lists_every_artifact_type(self, tmp_path):
        pkg = _make_package(tmp_path)
        out = tmp_path / "adf"
        out.mkdir()
        _seed_artifacts(out)

        manifest = generate_lineage_manifest(pkg, out, _pipeline_with_one_activity())

        assert manifest["artifacts"]["linked_services"][0]["name"] == "LS_Sql"
        assert manifest["artifacts"]["datasets"][0]["name"] == "DS_Customer"
        assert manifest["artifacts"]["data_flows"][0]["name"] == "DF_Customer"
        assert manifest["artifacts"]["triggers"][0]["name"] == "TR_Daily"
        # Pre-deploy: every azure_resource_id is empty.
        for type_key in ("linked_services", "datasets", "data_flows", "triggers"):
            for row in manifest["artifacts"][type_key]:
                assert row["azure_resource_id"] == ""

    def test_traces_activity_back_to_ssis_task(self, tmp_path):
        pkg = _make_package(tmp_path)
        out = tmp_path / "adf"
        out.mkdir()

        manifest = generate_lineage_manifest(pkg, out, _pipeline_with_one_activity())

        origins = manifest["activity_origins"]
        assert len(origins) == 1
        assert origins[0]["adf_activity"] == "Stored_LoadCustomer"
        assert origins[0]["ssis_task_id"] == "{TASK-1}"
        assert origins[0]["ssis_task_name"] == "Load Customer SP"

    def test_walks_into_foreach_container(self, tmp_path):
        pkg = _make_package(tmp_path)
        out = tmp_path / "adf"
        out.mkdir()
        pipeline = {
            "name": "PL_MyPackage",
            "properties": {
                "activities": [{
                    "name": "ForEachFiles",
                    "type": "ForEach",
                    "typeProperties": {
                        "activities": [{
                            "name": "Copy_File",
                            "type": "Copy",
                            "userProperties": [
                                {"name": "ssis_task_id", "value": "{INNER-1}"},
                            ],
                        }],
                    },
                }],
            },
        }
        manifest = generate_lineage_manifest(pkg, out, pipeline)
        names = [o["adf_activity"] for o in manifest["activity_origins"]]
        assert "ForEachFiles" in names
        assert "Copy_File" in names


@dataclass
class _FakeResult:
    artifact_type: str
    name: str
    success: bool = True
    skipped: bool = False
    error: str | None = None
    retries: int = 0


class TestUpdateLineageWithDeployment:
    def test_backfills_resource_ids_for_successful_deploys(self, tmp_path):
        pkg = _make_package(tmp_path)
        out = tmp_path / "adf"
        out.mkdir()
        _seed_artifacts(out)
        generate_lineage_manifest(pkg, out, _pipeline_with_one_activity())

        results = [
            _FakeResult("linkedService", "LS_Sql"),
            _FakeResult("dataset", "DS_Customer"),
            _FakeResult("pipeline", "PL_MyPackage"),
            _FakeResult("trigger", "TR_Daily"),
        ]
        path = update_lineage_with_deployment(
            out, results,
            subscription_id="sub-1",
            resource_group="rg-1",
            factory_name="adf-1",
        )
        assert path is not None
        manifest = json.loads(path.read_text(encoding="utf-8"))
        ls_id = manifest["artifacts"]["linked_services"][0]["azure_resource_id"]
        assert ls_id == (
            "/subscriptions/sub-1/resourceGroups/rg-1"
            "/providers/Microsoft.DataFactory/factories/adf-1"
            "/linkedservices/LS_Sql"
        )
        pipe_id = manifest["artifacts"]["pipeline"][0]["azure_resource_id"]
        assert pipe_id.endswith("/pipelines/PL_MyPackage")

    def test_failed_deploy_leaves_resource_id_blank(self, tmp_path):
        pkg = _make_package(tmp_path)
        out = tmp_path / "adf"
        out.mkdir()
        _seed_artifacts(out)
        generate_lineage_manifest(pkg, out, _pipeline_with_one_activity())

        results = [_FakeResult("linkedService", "LS_Sql", success=False, error="boom")]
        update_lineage_with_deployment(
            out, results,
            subscription_id="sub-1",
            resource_group="rg-1",
            factory_name="adf-1",
        )
        manifest = json.loads((out / "lineage.json").read_text(encoding="utf-8"))
        assert manifest["artifacts"]["linked_services"][0]["azure_resource_id"] == ""

    def test_returns_none_when_no_lineage_present(self, tmp_path):
        out = tmp_path / "adf"
        out.mkdir()
        path = update_lineage_with_deployment(
            out, [],
            subscription_id="sub-1",
            resource_group="rg-1",
            factory_name="adf-1",
        )
        assert path is None
