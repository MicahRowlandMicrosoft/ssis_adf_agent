"""Unit tests for runners and the orchestrator (using fakes / captured CSVs)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssis_adf_agent.parity import (
    CapturedOutputRunner,
    compare_dataflow_output,
    render_diff_markdown,
)
from ssis_adf_agent.parity.runners import RunnerResult, _read_csv, _write_csv


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


@pytest.fixture
def fake_package(tmp_path: Path) -> Path:
    p = tmp_path / "Package.dtsx"
    _write_text(p, "<dummy/>")
    return p


@pytest.fixture
def fake_dataflow(tmp_path: Path) -> Path:
    p = tmp_path / "DF_Sales.json"
    p.write_text(json.dumps({"name": "DF_Sales"}), encoding="utf-8")
    return p


@pytest.fixture
def input_csv(tmp_path: Path) -> Path:
    p = tmp_path / "input.csv"
    _write_text(p, "id,amount\n1,100\n2,200\n")
    return p


def test_captured_runner_reads_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "out.csv"
    _write_text(csv_path, "id,name\n1,Alice\n2,Bob\n")
    runner = CapturedOutputRunner(csv_path, name="ssis-test")
    result = runner.run()
    assert result.runner_name == "ssis-test"
    assert result.rows == [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    assert str(csv_path) in result.artifact_paths


def test_captured_runner_missing_file_raises(tmp_path: Path) -> None:
    runner = CapturedOutputRunner(tmp_path / "nope.csv")
    with pytest.raises(FileNotFoundError):
        runner.run()


def test_orchestrator_pass(
    tmp_path: Path,
    fake_package: Path,
    fake_dataflow: Path,
    input_csv: Path,
) -> None:
    ssis_csv = tmp_path / "ssis.csv"
    adf_csv = tmp_path / "adf.csv"
    _write_text(ssis_csv, "id,name,amount\n1,Alice,100\n2,Bob,200\n")
    _write_text(adf_csv, "id,name,amount\n1,Alice,100\n2,Bob,200\n")

    comparison = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(ssis_csv, name="ssis"),
        adf_runner=CapturedOutputRunner(adf_csv, name="adf"),
        package_path=fake_package,
        dataflow_task_name="DFT_Sales",
        adf_dataflow_path=fake_dataflow,
        input_dataset_path=input_csv,
        key_columns=("id",),
    )

    assert comparison.diff["ok"] is True
    assert comparison.ssis_run["row_count"] == 2
    assert comparison.adf_run["row_count"] == 2


def test_orchestrator_fail_value_mismatch(
    tmp_path: Path,
    fake_package: Path,
    fake_dataflow: Path,
    input_csv: Path,
) -> None:
    ssis_csv = tmp_path / "ssis.csv"
    adf_csv = tmp_path / "adf.csv"
    _write_text(ssis_csv, "id,name,amount\n1,Alice,100\n")
    _write_text(adf_csv, "id,name,amount\n1,Alice,999\n")

    comparison = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(ssis_csv, name="ssis"),
        adf_runner=CapturedOutputRunner(adf_csv, name="adf"),
        package_path=fake_package,
        dataflow_task_name="DFT_Sales",
        adf_dataflow_path=fake_dataflow,
        input_dataset_path=input_csv,
        key_columns=("id",),
    )

    assert comparison.diff["ok"] is False
    assert comparison.diff["summary"] == {"value_mismatch": 1}


def test_orchestrator_with_pluggable_fake_runner(
    tmp_path: Path,
    fake_package: Path,
    fake_dataflow: Path,
    input_csv: Path,
) -> None:
    """Confirms any object satisfying the runner protocol works."""

    class FakeRunner:
        name = "fake"

        def __init__(self, rows: list[dict]) -> None:
            self.rows = rows

        def run(self, **_: object) -> RunnerResult:
            return RunnerResult(rows=self.rows, runner_name=self.name)

    ssis_runner = FakeRunner([{"id": "1", "v": "a"}])
    adf_runner = FakeRunner([{"id": "1", "v": "a"}])

    comparison = compare_dataflow_output(
        ssis_runner=ssis_runner,
        adf_runner=adf_runner,
        package_path=fake_package,
        dataflow_task_name="DFT",
        adf_dataflow_path=fake_dataflow,
        input_dataset_path=input_csv,
        key_columns=("id",),
    )
    assert comparison.diff["ok"] is True


def test_render_diff_markdown_pass(
    tmp_path: Path,
    fake_package: Path,
    fake_dataflow: Path,
    input_csv: Path,
) -> None:
    ssis_csv = tmp_path / "ssis.csv"
    _write_text(ssis_csv, "id,v\n1,a\n")
    comparison = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(ssis_csv, name="ssis"),
        adf_runner=CapturedOutputRunner(ssis_csv, name="adf"),
        package_path=fake_package,
        dataflow_task_name="DFT",
        adf_dataflow_path=fake_dataflow,
        input_dataset_path=input_csv,
        key_columns=("id",),
    )
    md = render_diff_markdown(comparison.to_dict())
    assert "Behavioral Parity Report" in md
    assert "✅ PASS" in md
    assert "## Row counts" in md


def test_render_diff_markdown_fail(
    tmp_path: Path,
    fake_package: Path,
    fake_dataflow: Path,
    input_csv: Path,
) -> None:
    ssis_csv = tmp_path / "ssis.csv"
    adf_csv = tmp_path / "adf.csv"
    _write_text(ssis_csv, "id,v\n1,a\n")
    _write_text(adf_csv, "id,v\n1,b\n")
    comparison = compare_dataflow_output(
        ssis_runner=CapturedOutputRunner(ssis_csv, name="ssis"),
        adf_runner=CapturedOutputRunner(adf_csv, name="adf"),
        package_path=fake_package,
        dataflow_task_name="DFT",
        adf_dataflow_path=fake_dataflow,
        input_dataset_path=input_csv,
        key_columns=("id",),
    )
    md = render_diff_markdown(comparison.to_dict())
    assert "❌ FAIL" in md
    assert "value_mismatch" in md


def test_csv_round_trip(tmp_path: Path) -> None:
    rows = [{"a": "1", "b": "x"}, {"a": "2", "b": "y"}]
    p = tmp_path / "rt.csv"
    _write_csv(p, rows)
    assert _read_csv(p) == rows


def test_orchestrator_writes_report_and_json(
    tmp_path: Path,
    fake_package: Path,
    fake_dataflow: Path,
    input_csv: Path,
) -> None:
    """Drive the MCP-server handler path that writes the report+JSON to disk."""
    import asyncio
    from ssis_adf_agent.mcp_server import _compare_dataflow_output

    ssis_csv = tmp_path / "ssis.csv"
    adf_csv = tmp_path / "adf.csv"
    _write_text(ssis_csv, "id,v\n1,a\n")
    _write_text(adf_csv, "id,v\n1,a\n")
    report_path = tmp_path / "report.md"
    diff_json_path = tmp_path / "diff.json"

    asyncio.run(
        _compare_dataflow_output(
            {
                "package_path": str(fake_package),
                "dataflow_task_name": "DFT",
                "adf_dataflow_path": str(fake_dataflow),
                "input_dataset_path": str(input_csv),
                "key_columns": ["id"],
                "mode": "captured",
                "ssis_captured_csv": str(ssis_csv),
                "adf_captured_csv": str(adf_csv),
                "report_path": str(report_path),
                "diff_json_path": str(diff_json_path),
            }
        )
    )
    assert report_path.is_file()
    assert diff_json_path.is_file()
    payload = json.loads(diff_json_path.read_text(encoding="utf-8"))
    assert payload["diff"]["ok"] is True
