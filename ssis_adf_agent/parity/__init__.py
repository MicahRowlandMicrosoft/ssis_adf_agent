"""Behavioral parity harness (P4-1).

Compares the *output* of an SSIS Data Flow Task to the output of its converted
ADF Mapping Data Flow on a controlled input set, producing a row-and-column
diff report.

This package answers the buyer concern that
:mod:`ssis_adf_agent.documentation.parity_validator` only checks *structural*
parity: it counts tasks, linked services, and parameters but never compares
actual data values. ``compare_dataflow_output`` closes that gap.

Public surface:

* :func:`compare_dataflow_output` — orchestrator that runs both sides and diffs.
* :class:`DataFlowDiff` — pure result object produced by :func:`diff_rows`.
* :func:`diff_rows` — pure row-and-column diff engine (no Azure / no dtexec).
* :class:`SSISDataFlowRunner`, :class:`AdfDataFlowRunner` — runner protocols.
* :class:`DtexecRunner`, :class:`AdfDebugRunner` — default real-environment impls.
* :class:`CapturedOutputRunner` — replays previously-captured output rows
  (used by the worked example and unit tests; lets users demo the harness
  without dtexec or live ADF).
"""
from __future__ import annotations

from .diff import DataFlowDiff, RowDiff, diff_rows
from .orchestrator import ParityComparison, compare_dataflow_output
from .report import render_diff_markdown
from .runners import (
    AdfDataFlowRunner,
    AdfDebugRunner,
    CapturedOutputRunner,
    DtexecRunner,
    RunnerResult,
    SSISDataFlowRunner,
)

__all__ = [
    "AdfDataFlowRunner",
    "AdfDebugRunner",
    "CapturedOutputRunner",
    "DataFlowDiff",
    "DtexecRunner",
    "ParityComparison",
    "RowDiff",
    "RunnerResult",
    "SSISDataFlowRunner",
    "compare_dataflow_output",
    "diff_rows",
    "render_diff_markdown",
]
