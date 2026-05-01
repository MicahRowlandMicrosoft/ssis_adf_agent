"""Tests for the Fabric deployer (Phase 2).

The fab CLI is mocked via FabRunner — these tests do NOT shell out to the
real `fab` binary. They verify subprocess argument shaping, idempotent
workspace creation, placeholder GUID substitution in pipeline JSON, and the
overall deploy orchestration order (notebooks first, then discover real GUIDs,
then substitute, then pipelines).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_modernization_agent.deployer.fabric_deployer import (
    FabResult,
    FabricDeployer,
    apply_substitutions_in_place,
    load_connections_manifest,
    provision_workspace,
    resolve_connection_substitutions,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeFab:
    """Recording / scripted FabRunner."""

    def __init__(self, responses: dict[str, FabResult] | None = None) -> None:
        self.calls: list[list[str]] = []
        self._responses = responses or {}
        self.default = FabResult(args=[], returncode=0, stdout="ok")

    def __call__(self, args: list[str], cwd: Path | None = None) -> FabResult:
        self.calls.append(list(args))
        # Match by joined args
        key = " ".join(args)
        for pattern, response in self._responses.items():
            if pattern in key:
                return FabResult(
                    args=args, returncode=response.returncode,
                    stdout=response.stdout, stderr=response.stderr,
                )
        return FabResult(args=args, returncode=self.default.returncode, stdout=self.default.stdout)


# ---------------------------------------------------------------------------
# Manifest + substitution helpers
# ---------------------------------------------------------------------------

def test_load_manifest_missing_returns_empty(tmp_path):
    m = load_connections_manifest(tmp_path)
    assert m == {"schema_version": "1.0", "connections": []}


def test_load_manifest_reads_file(tmp_path):
    payload = {"schema_version": "1.0", "connections": [{"placeholder_id": "p1"}]}
    (tmp_path / "connections_required.json").write_text(json.dumps(payload))
    m = load_connections_manifest(tmp_path)
    assert m == payload


def test_resolve_returns_substitutions_and_unresolved():
    manifest = {
        "schema_version": "1.0",
        "connections": [
            {"placeholder_id": "ph_A", "ssis_connection_manager_name": "CM_A"},
            {"placeholder_id": "ph_B", "ssis_connection_manager_name": "CM_B"},
        ],
    }
    sub, unresolved = resolve_connection_substitutions(
        manifest, {"CM_A": "real-guid-A"},
    )
    assert sub == {"ph_A": "real-guid-A"}
    assert unresolved == ["CM_B"]


def test_apply_substitutions_rewrites_pipeline_json(tmp_path):
    pdir = tmp_path / "pipeline" / "PL_X.DataPipeline"
    pdir.mkdir(parents=True)
    doc = {
        "properties": {
            "activities": [
                {
                    "name": "RunSP",
                    "type": "SqlServerStoredProcedure",
                    "externalReferences": {"connection": "ph_A"},
                    "typeProperties": {},
                },
                {
                    "name": "Loop",
                    "type": "ForEach",
                    "typeProperties": {
                        "activities": [
                            {
                                "name": "Inner",
                                "type": "TridentNotebook",
                                "typeProperties": {"notebookId": "ph_NB"},
                            }
                        ],
                    },
                },
            ],
        }
    }
    (pdir / "pipeline-content.json").write_text(json.dumps(doc))
    counts = apply_substitutions_in_place(
        tmp_path, {"ph_A": "real-A", "ph_NB": "real-NB"},
    )
    assert sum(counts.values()) == 2
    rewritten = json.loads((pdir / "pipeline-content.json").read_text())
    activities = rewritten["properties"]["activities"]
    assert activities[0]["externalReferences"]["connection"] == "real-A"
    inner = activities[1]["typeProperties"]["activities"][0]
    assert inner["typeProperties"]["notebookId"] == "real-NB"


# ---------------------------------------------------------------------------
# Workspace provisioning
# ---------------------------------------------------------------------------

def test_provision_returns_existed_when_ls_succeeds():
    fab = FakeFab()  # default returncode=0 -> ls succeeds -> exists
    result = provision_workspace("ws1", capacity_id="cap1", runner=fab)
    assert result.existed
    assert not result.created
    assert fab.calls == [["ls", "/workspaces/ws1"]]


def test_provision_creates_when_ls_fails():
    fab = FakeFab({"ls /workspaces/ws1": FabResult(args=[], returncode=1, stderr="not found")})
    result = provision_workspace("ws1", capacity_id="cap1", runner=fab)
    assert not result.existed
    assert result.created
    assert any("create" in c[0] for c in fab.calls)
    create_call = next(c for c in fab.calls if c[0] == "create")
    assert "capacityId=cap1" in " ".join(create_call)


def test_provision_refuses_to_create_without_capacity():
    fab = FakeFab({"ls /workspaces/ws1": FabResult(args=[], returncode=1)})
    result = provision_workspace("ws1", runner=fab)
    assert not result.created
    assert "capacity_id" in (result.error or "")


def test_provision_dry_run_does_not_invoke_create():
    fab = FakeFab({"ls /workspaces/ws1": FabResult(args=[], returncode=1)})
    result = provision_workspace("ws1", capacity_id="cap1", runner=fab, dry_run=True)
    assert result.created
    assert all(c[0] != "create" for c in fab.calls)


def test_provision_rejects_empty_workspace():
    with pytest.raises(ValueError):
        provision_workspace("", capacity_id="cap1")


# ---------------------------------------------------------------------------
# FabricDeployer
# ---------------------------------------------------------------------------

def _build_minimal_artifacts(tmp_path: Path) -> Path:
    """Build a tiny artifacts directory for deploy tests."""
    pdir = tmp_path / "pipeline" / "PL_X.DataPipeline"
    pdir.mkdir(parents=True)
    pipeline_doc = {
        "properties": {
            "activities": [
                {
                    "name": "RunNB",
                    "type": "TridentNotebook",
                    "typeProperties": {"notebookId": "ph_NB"},
                },
                {
                    "name": "RunSP",
                    "type": "SqlServerStoredProcedure",
                    "externalReferences": {"connection": "ph_CONN"},
                    "typeProperties": {},
                },
            ],
        }
    }
    (pdir / "pipeline-content.json").write_text(json.dumps(pipeline_doc))
    (pdir / ".platform").write_text("{}")

    nbdir = tmp_path / "notebook" / "NB_X.Notebook"
    nbdir.mkdir(parents=True)
    (nbdir / "notebook-content.py").write_text("# stub")
    (nbdir / ".platform").write_text("{}")

    (tmp_path / "connections_required.json").write_text(json.dumps({
        "schema_version": "1.0",
        "connections": [
            {"placeholder_id": "ph_CONN", "ssis_connection_manager_name": "CM_DB"},
        ],
    }))
    (tmp_path / "notebook_placeholders.json").write_text(json.dumps({
        "schema_version": "1.0",
        "placeholders": {"ph_NB": "NB_X.Notebook"},
    }))
    return tmp_path


def test_deployer_rejects_empty_workspace():
    with pytest.raises(ValueError):
        FabricDeployer(workspace="")


def test_deploy_pushes_notebooks_then_substitutes_then_pipelines(tmp_path):
    art = _build_minimal_artifacts(tmp_path)
    fab = FakeFab({
        "get -q id /workspaces/ws/NB_X.Notebook":
            FabResult(args=[], returncode=0, stdout="real-NB-guid\n"),
    })
    deployer = FabricDeployer("ws", runner=fab)
    summary = deployer.deploy(art, connection_name_to_guid={"CM_DB": "real-CONN-guid"})

    # Both notebook + pipeline imported
    assert summary.items_attempted == 2
    assert summary.items_succeeded == 2
    assert summary.items_failed == 0

    # Substitutions: 1 connection + 1 notebook = 2
    assert summary.substitutions_applied == 2
    assert summary.unresolved_connections == []

    # Notebook id discovered
    assert summary.notebook_id_map == {"ph_NB": "real-NB-guid"}

    # Order: notebook import, then notebook discovery, then pipeline import
    op_seq = [c[0] for c in fab.calls]
    assert op_seq.index("import") < op_seq.index("get")
    # Two import calls (notebook + pipeline)
    import_calls = [c for c in fab.calls if c[0] == "import"]
    assert len(import_calls) == 2
    assert any("NB_X.Notebook" in " ".join(c) for c in import_calls)
    assert any("PL_X.DataPipeline" in " ".join(c) for c in import_calls)

    # Pipeline JSON was rewritten
    rewritten = json.loads(
        (art / "pipeline" / "PL_X.DataPipeline" / "pipeline-content.json").read_text()
    )
    activities = rewritten["properties"]["activities"]
    assert activities[0]["typeProperties"]["notebookId"] == "real-NB-guid"
    assert activities[1]["externalReferences"]["connection"] == "real-CONN-guid"


def test_deploy_reports_unresolved_connections(tmp_path):
    art = _build_minimal_artifacts(tmp_path)
    fab = FakeFab({
        "get -q id /workspaces/ws/NB_X.Notebook":
            FabResult(args=[], returncode=0, stdout="real-NB"),
    })
    deployer = FabricDeployer("ws", runner=fab)
    summary = deployer.deploy(art, connection_name_to_guid={})  # no real guids
    assert summary.unresolved_connections == ["CM_DB"]


def test_deploy_dry_run_invokes_no_imports(tmp_path):
    art = _build_minimal_artifacts(tmp_path)
    fab = FakeFab()
    deployer = FabricDeployer("ws", runner=fab, dry_run=True)
    summary = deployer.deploy(art, connection_name_to_guid={"CM_DB": "real"})
    assert summary.items_succeeded == 2
    # In dry_run, no real fab calls are made for import or discovery
    assert all(c[0] not in ("import", "get") for c in fab.calls)


def test_deploy_propagates_import_failure(tmp_path):
    art = _build_minimal_artifacts(tmp_path)
    fab = FakeFab({
        "import": FabResult(args=[], returncode=1, stderr="permission denied"),
    })
    deployer = FabricDeployer("ws", runner=fab)
    summary = deployer.deploy(art, connection_name_to_guid={"CM_DB": "real"})
    assert summary.items_failed == 2
    assert all(not r.success for r in summary.results)
    assert any("permission denied" in r.fab_stderr for r in summary.results)


def test_deploy_force_flag_present_for_idempotency(tmp_path):
    art = _build_minimal_artifacts(tmp_path)
    fab = FakeFab()
    deployer = FabricDeployer("ws", runner=fab)
    deployer.deploy(art, connection_name_to_guid={"CM_DB": "real"})
    import_calls = [c for c in fab.calls if c[0] == "import"]
    for call in import_calls:
        assert "--force" in call


def test_check_fab_installed_calls_version():
    fab = FakeFab()
    deployer = FabricDeployer("ws", runner=fab)
    deployer.check_fab_installed()
    assert fab.calls == [["--version"]]


# ---------------------------------------------------------------------------
# Notebook placeholder sidecar end-to-end (after convert)
# ---------------------------------------------------------------------------

def test_convert_writes_notebook_placeholders_sidecar(tmp_path):
    """Verify the converter emits notebook_placeholders.json so the deployer
    can later resolve real GUIDs."""
    pytest.importorskip("ssis_modernization_agent.parsers.readers.local_reader")
    LNI = Path(r"C:\source\test-lni-packages\ADDS-MIPS-TC.dtsx")
    if not LNI.exists():
        pytest.skip("LNI sample not present")
    from ssis_modernization_agent.fabric import convert_package_to_fabric
    from ssis_modernization_agent.parsers.readers.local_reader import LocalReader
    package = LocalReader().read(LNI)
    convert_package_to_fabric(package, tmp_path)
    sidecar = tmp_path / "notebook_placeholders.json"
    assert sidecar.exists()
    doc = json.loads(sidecar.read_text())
    assert doc["schema_version"] == "1.0"
    assert doc["placeholders"]
    for ph, item in doc["placeholders"].items():
        assert ph.startswith("00000000-0000-4000-9000-")
        assert item.endswith(".Notebook")
