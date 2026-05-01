"""Tests for Phase 4 — Fabric integrations across the wider product surface.

This module pins the small Fabric-aware extensions added in Phase 4:

* `render_diff_markdown(target_label="Fabric")` cosmetic label on the
  behavioral parity report — the underlying JSON diff shape is unchanged
  (callers parsing `adf_row_count` etc. in CI keep working).
* End-to-end Fabric conversion of the LNI ADDS-MIPS-TC sample is the
  evidence backing the worked case study under
  docs/case-studies/fabric_conversion_adds_mips_tc/.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_modernization_agent.fabric import (
    convert_package_to_fabric,
    validate_fabric_artifacts,
)
from ssis_modernization_agent.parity import (
    CapturedOutputRunner,
    compare_dataflow_output,
    render_diff_markdown,
)
from ssis_modernization_agent.parsers.readers.local_reader import LocalReader

LNI = Path(r"C:\source\test-lni-packages\ADDS-MIPS-TC.dtsx")


# ---------------------------------------------------------------------------
# Behavioral parity report — Fabric target label
# ---------------------------------------------------------------------------

@pytest.fixture
def _fake_pkg_and_df(tmp_path: Path) -> tuple[Path, Path, Path]:
    pkg = tmp_path / "DFT.dtsx"
    pkg.write_text("<dummy/>", encoding="utf-8")
    df = tmp_path / "DF.json"
    df.write_text(json.dumps({"name": "DF"}), encoding="utf-8")
    inp = tmp_path / "input.csv"
    inp.write_text("id,v\n1,a\n", encoding="utf-8")
    return pkg, df, inp


def _captured_csv(tmp_path: Path, name: str, body: str) -> Path:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return p


def test_report_target_label_defaults_to_adf(tmp_path: Path, _fake_pkg_and_df) -> None:
    pkg, df, inp = _fake_pkg_and_df
    ssis = _captured_csv(tmp_path, "s.csv", "id,v\n1,a\n")
    adf = _captured_csv(tmp_path, "a.csv", "id,v\n1,a\n")
    cmp = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(ssis, name="ssis"),
        adf_runner=CapturedOutputRunner(adf, name="adf"),
        package_path=pkg,
        dataflow_task_name="DFT",
        adf_dataflow_path=df,
        input_dataset_path=inp,
        key_columns=("id",),
    )
    md = render_diff_markdown(cmp.to_dict())
    assert "(SSIS → ADF)" in md
    assert "ADF artifact" in md


def test_report_target_label_can_be_fabric(tmp_path: Path, _fake_pkg_and_df) -> None:
    pkg, df, inp = _fake_pkg_and_df
    ssis = _captured_csv(tmp_path, "s.csv", "id,v\n1,a\n")
    adf = _captured_csv(tmp_path, "a.csv", "id,v\n1,b\n")
    cmp = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(ssis, name="ssis"),
        adf_runner=CapturedOutputRunner(adf, name="fabric-notebook"),
        package_path=pkg,
        dataflow_task_name="DFT",
        adf_dataflow_path=df,
        input_dataset_path=inp,
        key_columns=("id",),
    )
    md = render_diff_markdown(cmp.to_dict(), target_label="Fabric")
    assert "(SSIS → Fabric)" in md
    assert "Fabric artifact" in md
    assert "Fabric value" in md  # diff-table column header
    # Underlying JSON diff shape is NOT renamed — buyer CI assertions stay valid
    payload = cmp.to_dict()
    assert "adf_row_count" in payload["diff"]
    assert "missing_in_adf" not in payload["diff"]["summary"]  # no missing here


def test_report_target_label_blank_falls_back_to_adf(tmp_path: Path, _fake_pkg_and_df) -> None:
    pkg, df, inp = _fake_pkg_and_df
    ssis = _captured_csv(tmp_path, "s.csv", "id,v\n1,a\n")
    adf = _captured_csv(tmp_path, "a.csv", "id,v\n1,a\n")
    cmp = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(ssis, name="ssis"),
        adf_runner=CapturedOutputRunner(adf, name="adf"),
        package_path=pkg,
        dataflow_task_name="DFT",
        adf_dataflow_path=df,
        input_dataset_path=inp,
        key_columns=("id",),
    )
    md = render_diff_markdown(cmp.to_dict(), target_label="   ")
    assert "(SSIS → ADF)" in md


# ---------------------------------------------------------------------------
# Worked case study — pin the artifact counts the README cites
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not LNI.exists(), reason="LNI sample not present")
def test_case_study_lni_fabric_conversion_artifact_counts(tmp_path: Path) -> None:
    """The case study at docs/case-studies/fabric_conversion_adds_mips_tc/
    cites these exact artifact counts. Pin them so the doc cannot drift."""
    package = LocalReader().read(LNI)
    result = convert_package_to_fabric(package, tmp_path)
    counts = result["artifacts_generated"]
    assert counts["pipelines"] == 1
    assert counts["notebooks"] == 1  # exactly one DFT in the package
    assert counts["function_stubs"] == 2  # exactly two Script Tasks
    # 2 SSIS CMs + 1 synthetic LS_AzureFunction connection
    assert counts["connections_required"] == 3

    v = validate_fabric_artifacts(tmp_path)
    assert v["valid"], v["errors"]
    assert v["warnings"] == []

    # Connections manifest carries the on-prem warning the case study highlights
    cm = json.loads((tmp_path / "connections_required.json").read_text(encoding="utf-8"))
    sql_entries = [c for c in cm["connections"] if c["fabric_connection_type"] == "SQL"]
    assert sql_entries, "Expected a SQL connection placeholder for the OLE DB CM"
    assert any("On-prem" in n for n in sql_entries[0].get("notes", []))

    # Notebook stub has hand-port TODOs
    nb = next((tmp_path / "notebook").rglob("notebook-content.py"))
    body = nb.read_text(encoding="utf-8")
    assert "TODO" in body
    assert "MUST be hand-ported" in body
