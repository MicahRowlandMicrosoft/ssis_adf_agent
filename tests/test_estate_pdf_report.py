"""Tests for the estate-level PDF report generator."""
from __future__ import annotations

from pathlib import Path

import pytest

reportlab = pytest.importorskip("reportlab")

from ssis_adf_agent.documentation.estate_pdf_report import build_estate_report_pdf  # noqa: E402


def test_build_estate_report_pdf_writes_file(tmp_path: Path) -> None:
    estate_report = {
        "scanned_path": str(tmp_path),
        "package_count": 2,
        "failure_count": 0,
        "estate_summary": {
            "by_complexity_bucket": {"low": 1, "medium": 0, "high": 1, "very_high": 0},
            "by_target_pattern": {"scheduled_file_drop": 2},
            "estimated_total_hours": 50.0,
            "manual_required_total": 1,
            "bulk_convertible_count": 1,
            "needs_design_review_count": 1,
        },
        "packages": [
            {"package_name": "P1", "complexity_bucket": "low", "target_pattern": "scheduled_file_drop",
             "complexity_score": 20, "estimated_total_hours": 8, "manual_required_count": 0},
            {"package_name": "P2", "complexity_bucket": "high", "target_pattern": "scheduled_file_drop",
             "complexity_score": 70, "estimated_total_hours": 42, "manual_required_count": 1},
        ],
        "failures": [],
    }
    waves = {
        "wave_count": 2,
        "total_packages": 2,
        "total_estimated_hours": 50.0,
        "waves": [
            {"wave": 1, "label": "Bulk \u2014 file_drop", "strategy": "bulk_convert",
             "package_count": 1, "estimated_hours": 8.0, "target_pattern": "scheduled_file_drop",
             "packages": ["P1"]},
            {"wave": 2, "label": "Review \u2014 file_drop", "strategy": "design_review",
             "package_count": 1, "estimated_hours": 42.0, "target_pattern": "scheduled_file_drop",
             "packages": ["P2"]},
        ],
    }
    cost = {
        "monthly_total_usd": 123.45,
        "annual_total_usd": 1481.40,
        "currency": "USD",
        "note": "List-price US East",
        "line_items": [
            {"name": "Orchestration", "monthly_usd": 50.0, "basis": "50k runs"},
            {"name": "Storage", "monthly_usd": 73.45, "basis": "100 GB"},
        ],
    }
    out_pdf = tmp_path / "estate.pdf"
    result = build_estate_report_pdf(
        output_pdf=out_pdf,
        estate_report=estate_report,
        waves=waves,
        cost_estimate=cost,
        customer_name="Contoso",
    )
    assert Path(result).exists()
    assert Path(result).stat().st_size > 1000  # non-trivial PDF
