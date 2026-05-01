"""Tests for the Microsoft Fabric target (Phase 1)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_modernization_agent.fabric import (
    ConnectionResolver,
    convert_package_to_fabric,
    validate_fabric_artifacts,
)
from ssis_modernization_agent.fabric.connection_resolver import (
    make_placeholder_connection_id,
)
from ssis_modernization_agent.fabric.notebook_stub_generator import (
    notebook_display_name,
    write_notebook_stub,
)
from ssis_modernization_agent.fabric.pipeline_translator import translate_pipeline
from ssis_modernization_agent.parsers.models import (
    ConnectionManagerType,
    DataFlowComponent,
    DataFlowTask,
    SSISConnectionManager,
    SSISPackage,
    TaskType,
)
from ssis_modernization_agent.parsers.readers.local_reader import LocalReader


# ---------------------------------------------------------------------------
# Connection placeholder shaping
# ---------------------------------------------------------------------------

def test_placeholder_id_is_deterministic():
    assert make_placeholder_connection_id("X") == make_placeholder_connection_id("X")


def test_placeholder_id_is_recognizable():
    pid = make_placeholder_connection_id("CM-A")
    assert pid.startswith("00000000-0000-4000-8000-")
    assert len(pid) == 36


def test_placeholder_id_distinct_per_input():
    a = make_placeholder_connection_id("CM-A")
    b = make_placeholder_connection_id("CM-B")
    assert a != b


def test_placeholder_id_rejects_empty():
    with pytest.raises(ValueError):
        make_placeholder_connection_id("")


# ---------------------------------------------------------------------------
# ConnectionResolver
# ---------------------------------------------------------------------------

def _cm(id_: str, name: str, type_: ConnectionManagerType, server: str | None = None) -> SSISConnectionManager:
    return SSISConnectionManager(id=id_, name=name, type=type_, server=server)


def test_resolver_register_idempotent():
    r = ConnectionResolver()
    cm = _cm("ID1", "Source", ConnectionManagerType.OLEDB, "srv01.lcl")
    a = r.register(cm)
    b = r.register(cm)
    assert a == b
    assert len(r) == 1


def test_resolver_on_prem_note():
    r = ConnectionResolver()
    r.register(_cm("ID1", "OnPrem", ConnectionManagerType.OLEDB, "srv.corp"))
    manifest = r.manifest()
    notes = manifest["connections"][0]["notes"]
    assert any("On-Premises Data Gateway" in n for n in notes)


def test_resolver_azure_no_on_prem_note():
    r = ConnectionResolver()
    r.register(_cm("ID1", "Az", ConnectionManagerType.OLEDB, "x.database.windows.net"))
    entry = r.manifest()["connections"][0]
    assert "notes" not in entry or not any("Gateway" in n for n in entry["notes"])


def test_resolver_unknown_type_note():
    r = ConnectionResolver()
    r.register(_cm("ID1", "Mystery", ConnectionManagerType.UNKNOWN))
    notes = r.manifest()["connections"][0]["notes"]
    assert any("UNKNOWN" in n for n in notes)


def test_resolver_synthetic_entry():
    r = ConnectionResolver()
    pid = r.register_synthetic("LS_Func", "Web", "synth note")
    assert pid.startswith("00000000-0000-4000-8000-")
    manifest = r.manifest()
    assert manifest["connections"][0]["fabric_connection_type"] == "Web"
    assert manifest["connections"][0]["ssis_connection_manager_id"].startswith("__synthetic__:")


def test_resolver_manifest_schema_version():
    r = ConnectionResolver()
    r.register(_cm("ID1", "X", ConnectionManagerType.OLEDB))
    assert r.manifest()["schema_version"] == "1.0"


# ---------------------------------------------------------------------------
# Pipeline translator
# ---------------------------------------------------------------------------

def _adf_pipeline_with_copy(ls_name="LS_SQL", ds_name="DS_T") -> dict:
    return {
        "name": "PL_X",
        "properties": {
            "activities": [
                {
                    "name": "CopyT",
                    "type": "Copy",
                    "dependsOn": [],
                    "typeProperties": {
                        "source": {"type": "AzureSqlSource", "sqlReaderQuery": "SELECT 1"},
                        "sink": {"type": "AzureSqlSink"},
                    },
                    "inputs": [{"referenceName": ds_name, "type": "DatasetReference"}],
                    "outputs": [{"referenceName": ds_name, "type": "DatasetReference"}],
                }
            ],
            "parameters": {},
            "variables": {},
        },
    }


def test_translator_inlines_dataset_in_copy():
    resolver = ConnectionResolver()
    resolver.register(_cm("CMID", "Src", ConnectionManagerType.OLEDB))
    ls_name_map = {"CMID": "LS_SQL"}
    datasets = [{
        "name": "DS_T",
        "properties": {
            "type": "AzureSqlTable",
            "linkedServiceName": {"referenceName": "LS_SQL", "type": "LinkedServiceReference"},
            "schema": [],
            "typeProperties": {"schema": "dbo", "table": "T"},
        },
    }]
    fabric = translate_pipeline(_adf_pipeline_with_copy(), datasets, ls_name_map, resolver)
    act = fabric["properties"]["activities"][0]
    assert "inputs" not in act
    assert "outputs" not in act
    src_settings = act["typeProperties"]["source"]["datasetSettings"]
    assert src_settings["type"] == "AzureSqlTable"
    assert src_settings["externalReferences"]["connection"] == resolver.manifest()["connections"][0]["placeholder_id"]


def test_translator_replaces_executedataflow_with_notebook():
    resolver = ConnectionResolver()
    pipeline = {
        "name": "PL_X",
        "properties": {
            "activities": [
                {
                    "name": "DF1",
                    "type": "ExecuteDataFlow",
                    "dependsOn": [],
                    "typeProperties": {"dataflow": {"referenceName": "DF_X"}},
                }
            ],
        },
    }
    notebook_id = "00000000-0000-4000-9000-abcdef012345"
    fabric = translate_pipeline(
        pipeline, [], {}, resolver,
        notebook_id_by_dataflow_name={"DF_X": notebook_id},
    )
    act = fabric["properties"]["activities"][0]
    assert act["type"] == "TridentNotebook"
    assert act["typeProperties"]["notebookId"] == notebook_id


def test_translator_passes_through_executesql():
    resolver = ConnectionResolver()
    resolver.register(_cm("CMID", "Src", ConnectionManagerType.OLEDB))
    pipeline = {
        "name": "PL_X",
        "properties": {
            "activities": [
                {
                    "name": "RunProc",
                    "type": "SqlServerStoredProcedure",
                    "dependsOn": [],
                    "linkedServiceName": {"referenceName": "LS_SQL", "type": "LinkedServiceReference"},
                    "typeProperties": {"storedProcedureName": "dbo.X"},
                }
            ],
        },
    }
    fabric = translate_pipeline(pipeline, [], {"CMID": "LS_SQL"}, resolver)
    act = fabric["properties"]["activities"][0]
    assert act["type"] == "SqlServerStoredProcedure"
    assert "linkedServiceName" not in act
    assert act["externalReferences"]["connection"].startswith("00000000-0000-4000-8000-")


def test_translator_recurses_into_foreach():
    resolver = ConnectionResolver()
    resolver.register(_cm("CMID", "Src", ConnectionManagerType.OLEDB))
    pipeline = {
        "name": "PL_X",
        "properties": {
            "activities": [
                {
                    "name": "Loop",
                    "type": "ForEach",
                    "typeProperties": {
                        "items": {"value": "@variables('items')", "type": "Expression"},
                        "activities": [
                            {
                                "name": "Inner",
                                "type": "SqlServerStoredProcedure",
                                "linkedServiceName": {"referenceName": "LS_SQL", "type": "LinkedServiceReference"},
                                "typeProperties": {"storedProcedureName": "dbo.Y"},
                            }
                        ],
                    },
                }
            ],
        },
    }
    fabric = translate_pipeline(pipeline, [], {"CMID": "LS_SQL"}, resolver)
    inner = fabric["properties"]["activities"][0]["typeProperties"]["activities"][0]
    assert "linkedServiceName" not in inner
    assert "externalReferences" in inner


def test_translator_appends_fabric_target_annotation():
    resolver = ConnectionResolver()
    pipeline = {"name": "PL_X", "properties": {"activities": [], "annotations": ["src"]}}
    fabric = translate_pipeline(pipeline, [], {}, resolver)
    assert "fabric-target" in fabric["properties"]["annotations"]
    assert "src" in fabric["properties"]["annotations"]


# ---------------------------------------------------------------------------
# Notebook stub generator
# ---------------------------------------------------------------------------

def _df_task(name="DFT", components=None) -> DataFlowTask:
    return DataFlowTask(
        id="t1", name=name, task_type=TaskType.DATA_FLOW,
        components=components or [],
    )


def test_notebook_stub_writes_two_files(tmp_path):
    task = _df_task(components=[
        DataFlowComponent(
            id="c1", name="Src", component_class_id="OLEDBSource",
            component_type="OleDbSource", properties={"OpenRowset": "dbo.X"},
        ),
        DataFlowComponent(
            id="c2", name="Dst", component_class_id="OLEDBDestination",
            component_type="OleDbDestination", properties={"OpenRowset": "dbo.Y"},
        ),
    ])
    out = write_notebook_stub(task, "PkgA", tmp_path)
    assert (out / "notebook-content.py").exists()
    assert (out / ".platform").exists()
    content = (out / "notebook-content.py").read_text(encoding="utf-8")
    assert "PkgA" in content
    assert "DFT" in content
    assert "TODO" in content


def test_notebook_stub_includes_transform_hints(tmp_path):
    task = _df_task(components=[
        DataFlowComponent(
            id="c1", name="Src", component_class_id="OLEDBSource",
            component_type="OleDbSource",
        ),
        DataFlowComponent(
            id="c2", name="DC", component_class_id="DerivedColumn",
            component_type="DerivedColumn",
        ),
        DataFlowComponent(
            id="c3", name="Dst", component_class_id="OLEDBDestination",
            component_type="OleDbDestination",
        ),
    ])
    out = write_notebook_stub(task, "P", tmp_path)
    content = (out / "notebook-content.py").read_text(encoding="utf-8")
    assert "DerivedColumn" in content


def test_notebook_display_name_stable():
    task = _df_task(name="My Flow")
    n1 = notebook_display_name(task, "Pkg A")
    n2 = notebook_display_name(task, "Pkg A")
    assert n1 == n2
    assert " " not in n1


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

def test_validator_flags_missing_pipeline(tmp_path):
    result = validate_fabric_artifacts(tmp_path)
    assert not result["valid"]
    assert any("no .DataPipeline" in e for e in result["errors"])


def test_validator_accepts_valid_layout(tmp_path):
    pdir = tmp_path / "pipeline" / "PL_X.DataPipeline"
    pdir.mkdir(parents=True)
    (pdir / "pipeline-content.json").write_text(json.dumps({
        "properties": {"activities": [{"name": "A", "type": "Wait", "typeProperties": {"waitTimeInSeconds": 1}}]}
    }))
    (pdir / ".platform").write_text("{}")
    (tmp_path / "connections_required.json").write_text(json.dumps({"schema_version": "1.0", "connections": []}))
    result = validate_fabric_artifacts(tmp_path)
    assert result["valid"], result["errors"]


def test_validator_catches_duplicate_activity_names(tmp_path):
    pdir = tmp_path / "pipeline" / "PL_X.DataPipeline"
    pdir.mkdir(parents=True)
    (pdir / "pipeline-content.json").write_text(json.dumps({
        "properties": {"activities": [
            {"name": "A", "type": "Wait", "typeProperties": {}},
            {"name": "A", "type": "Wait", "typeProperties": {}},
        ]}
    }))
    (pdir / ".platform").write_text("{}")
    (tmp_path / "connections_required.json").write_text(json.dumps({"schema_version": "1.0", "connections": []}))
    result = validate_fabric_artifacts(tmp_path)
    assert not result["valid"]
    assert any("duplicate activity name" in e for e in result["errors"])


def test_validator_warns_on_translator_markers(tmp_path):
    pdir = tmp_path / "pipeline" / "PL_X.DataPipeline"
    pdir.mkdir(parents=True)
    (pdir / "pipeline-content.json").write_text(json.dumps({
        "properties": {"activities": [
            {"name": "A", "type": "Wait", "typeProperties": {}, "__connection_placeholder_missing": "LS_X"},
        ]}
    }))
    (pdir / ".platform").write_text("{}")
    (tmp_path / "connections_required.json").write_text(json.dumps({"schema_version": "1.0", "connections": []}))
    result = validate_fabric_artifacts(tmp_path)
    assert result["valid"]
    assert any("LS_X" in w for w in result["warnings"])


# ---------------------------------------------------------------------------
# End-to-end on the real LNI sample (skipped if package missing)
# ---------------------------------------------------------------------------

LNI = Path(r"C:\source\test-lni-packages\ADDS-MIPS-TC.dtsx")


@pytest.mark.skipif(not LNI.exists(), reason="LNI sample not present in workspace")
def test_e2e_lni_package_converts_and_validates(tmp_path):
    package = LocalReader().read(LNI)
    result = convert_package_to_fabric(package, tmp_path)
    counts = result["artifacts_generated"]
    assert counts["pipelines"] == 1
    assert counts["notebooks"] >= 1
    assert counts["connections_required"] >= len(package.connection_managers)

    v = validate_fabric_artifacts(tmp_path)
    assert v["valid"], v["errors"]
    assert v["warnings"] == [], v["warnings"]

    pipeline_json = next(
        (tmp_path / "pipeline").rglob("pipeline-content.json")
    )
    doc = json.loads(pipeline_json.read_text(encoding="utf-8"))
    # No raw linkedServiceName references should survive translation
    serialized = json.dumps(doc)
    assert "\"linkedServiceName\"" not in serialized
    # fabric-target annotation present
    assert "fabric-target" in doc["properties"]["annotations"]
