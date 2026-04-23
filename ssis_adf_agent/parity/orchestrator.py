"""Orchestrator for behavioral data-flow parity comparison (P4-1).

Glues an :class:`SSISDataFlowRunner` and an :class:`AdfDataFlowRunner` to
the pure :func:`diff_rows` engine, producing a single
:class:`ParityComparison` result that the MCP tool returns to the caller.
"""
from __future__ import annotations

import logging
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Sequence

from .diff import DataFlowDiff, diff_rows
from .runners import AdfDataFlowRunner, RunnerResult, SSISDataFlowRunner

logger = logging.getLogger(__name__)


@dataclass
class ParityComparison:
    package_path: str
    dataflow_task_name: str
    adf_dataflow_path: str
    input_dataset_path: str
    ssis_runner_name: str
    adf_runner_name: str
    ssis_run: dict[str, Any] = field(default_factory=dict)
    adf_run: dict[str, Any] = field(default_factory=dict)
    diff: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _runner_summary(result: RunnerResult) -> dict[str, Any]:
    return {
        "runner_name": result.runner_name,
        "row_count": len(result.rows),
        "artifact_paths": list(result.artifact_paths),
        "log_tail": (result.log or "").splitlines()[-20:],
    }


def compare_dataflow_output(
    *,
    ssis_runner: SSISDataFlowRunner,
    adf_runner: AdfDataFlowRunner,
    package_path: Path,
    dataflow_task_name: str,
    adf_dataflow_path: Path,
    input_dataset_path: Path,
    key_columns: Sequence[str],
    compare_columns: Sequence[str] | None = None,
    ignore_columns: Sequence[str] = (),
    ignore_case: bool = False,
    strip_whitespace: bool = True,
    numeric_tolerance: float = 0.0,
    work_dir: Path | None = None,
) -> ParityComparison:
    """Run both sides of a data-flow comparison and return a structured diff.

    ``ssis_runner`` and ``adf_runner`` are protocols, so callers can plug in
    :class:`~ssis_adf_agent.parity.runners.DtexecRunner` /
    :class:`~ssis_adf_agent.parity.runners.AdfDebugRunner` for live runs, or
    :class:`~ssis_adf_agent.parity.runners.CapturedOutputRunner` for replay
    (used by the worked example and the unit-test suite).
    """
    package_path = Path(package_path)
    adf_dataflow_path = Path(adf_dataflow_path)
    input_dataset_path = Path(input_dataset_path)

    if not adf_dataflow_path.is_file():
        raise FileNotFoundError(f"ADF dataflow not found: {adf_dataflow_path}")
    if not input_dataset_path.is_file():
        raise FileNotFoundError(f"Input dataset not found: {input_dataset_path}")

    work_root: Path
    cleanup = False
    if work_dir is None:
        work_root = Path(tempfile.mkdtemp(prefix="adf_parity_"))
        cleanup = True
    else:
        work_root = Path(work_dir)
        work_root.mkdir(parents=True, exist_ok=True)

    try:
        ssis_work = work_root / "ssis"
        adf_work = work_root / "adf"
        ssis_work.mkdir(parents=True, exist_ok=True)
        adf_work.mkdir(parents=True, exist_ok=True)

        logger.info("Parity: running SSIS side via %s", ssis_runner.name)
        ssis_result = ssis_runner.run(
            package_path=package_path,
            dataflow_task_name=dataflow_task_name,
            input_dataset_path=input_dataset_path,
            work_dir=ssis_work,
        )
        logger.info("Parity: running ADF side via %s", adf_runner.name)
        adf_result = adf_runner.run(
            adf_dataflow_path=adf_dataflow_path,
            input_dataset_path=input_dataset_path,
            work_dir=adf_work,
        )

        diff: DataFlowDiff = diff_rows(
            ssis_result.rows,
            adf_result.rows,
            key_columns=key_columns,
            compare_columns=compare_columns,
            ignore_columns=ignore_columns,
            ignore_case=ignore_case,
            strip_whitespace=strip_whitespace,
            numeric_tolerance=numeric_tolerance,
        )
    finally:
        if cleanup:
            # Leave artifacts in place when the runner produced files but
            # the caller didn't supply work_dir; we still want to surface
            # them in the report for inspection.
            logger.debug("Parity: temporary work_dir=%s (left in place)", work_root)

    return ParityComparison(
        package_path=str(package_path),
        dataflow_task_name=dataflow_task_name,
        adf_dataflow_path=str(adf_dataflow_path),
        input_dataset_path=str(input_dataset_path),
        ssis_runner_name=ssis_runner.name,
        adf_runner_name=adf_runner.name,
        ssis_run=_runner_summary(ssis_result),
        adf_run=_runner_summary(adf_result),
        diff=diff.to_dict(),
    )
