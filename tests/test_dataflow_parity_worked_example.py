"""Worked example for the behavioral parity harness (P4-1).

Demonstrates the harness end-to-end against a synthetic Sales Data Flow:

* SSIS output (captured): ``sales_ssis_output.csv``
* ADF output (captured, correct): ``sales_adf_output.csv``  → PASS
* ADF output (captured, regressed): ``sales_adf_output_buggy.csv`` → FAIL with
  a value mismatch on `net_amount` (row 4: discount mis-applied) and a value
  mismatch on `tier` (row 6: tier classification regressed).

The fixtures are stored as CSVs so a buyer skeptical of the harness can:

1. Read the inputs and the expected outputs.
2. Read this test (no SSIS or Azure required).
3. Run ``python -m pytest tests/test_dataflow_parity_worked_example.py -v``.
4. Confirm that the *failing* case actually fails the way the report says.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.parity import (
    CapturedOutputRunner,
    compare_dataflow_output,
    render_diff_markdown,
)

FIXTURES = Path(__file__).parent / "fixtures" / "dataflow_parity"


@pytest.fixture
def fake_ssis_package(tmp_path: Path) -> Path:
    p = tmp_path / "DFT_Sales.dtsx"
    p.write_text("<dummy/>", encoding="utf-8")
    return p


@pytest.fixture
def fake_adf_dataflow(tmp_path: Path) -> Path:
    p = tmp_path / "DF_Sales.json"
    p.write_text(json.dumps({"name": "DF_Sales"}), encoding="utf-8")
    return p


def test_worked_example_passes_for_correct_conversion(
    tmp_path: Path,
    fake_ssis_package: Path,
    fake_adf_dataflow: Path,
) -> None:
    comparison = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(FIXTURES / "sales_ssis_output.csv", name="ssis-captured"),
        adf_runner=CapturedOutputRunner(FIXTURES / "sales_adf_output.csv", name="adf-captured"),
        package_path=fake_ssis_package,
        dataflow_task_name="DFT_Sales",
        adf_dataflow_path=fake_adf_dataflow,
        input_dataset_path=FIXTURES / "sales_input.csv",
        key_columns=("id",),
        numeric_tolerance=0.001,
    )
    assert comparison.diff["ok"] is True
    assert comparison.diff["matched_row_count"] == 6

    md = render_diff_markdown(comparison.to_dict())
    (tmp_path / "report_pass.md").write_text(md, encoding="utf-8")
    assert "✅ PASS" in md


def test_worked_example_catches_regression(
    tmp_path: Path,
    fake_ssis_package: Path,
    fake_adf_dataflow: Path,
) -> None:
    comparison = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(FIXTURES / "sales_ssis_output.csv", name="ssis-captured"),
        adf_runner=CapturedOutputRunner(FIXTURES / "sales_adf_output_buggy.csv", name="adf-captured"),
        package_path=fake_ssis_package,
        dataflow_task_name="DFT_Sales",
        adf_dataflow_path=fake_adf_dataflow,
        input_dataset_path=FIXTURES / "sales_input.csv",
        key_columns=("id",),
        numeric_tolerance=0.001,
    )
    diff = comparison.diff
    assert diff["ok"] is False
    assert diff["summary"] == {"value_mismatch": 2}

    # Both expected mismatches must be in the diff list.
    mismatches = {
        (tuple(d["key"]), d["column"]): (d["ssis_value"], d["adf_value"])
        for d in diff["diffs"]
    }
    assert (("4",), "net_amount") in mismatches
    assert (("6",), "tier") in mismatches

    md = render_diff_markdown(comparison.to_dict())
    (tmp_path / "report_fail.md").write_text(md, encoding="utf-8")
    assert "❌ FAIL" in md
    assert "value_mismatch" in md
