"""Unit tests for the pure diff engine (no I/O)."""
from __future__ import annotations

import pytest

from ssis_adf_agent.parity.diff import diff_rows


def test_identical_rows_pass() -> None:
    rows = [
        {"id": "1", "name": "Alice", "amount": 100},
        {"id": "2", "name": "Bob", "amount": 200},
    ]
    result = diff_rows(rows, rows, key_columns=("id",))
    assert result.ok is True
    assert result.matched_row_count == 2
    assert result.summary == {}


def test_value_mismatch_reported() -> None:
    ssis = [{"id": "1", "name": "Alice", "amount": 100}]
    adf = [{"id": "1", "name": "Alice", "amount": 999}]
    result = diff_rows(ssis, adf, key_columns=("id",))
    assert result.ok is False
    assert result.summary == {"value_mismatch": 1}
    diff = result.diffs[0]
    assert diff.kind == "value_mismatch"
    assert diff.column == "amount"
    assert diff.ssis_value == 100
    assert diff.adf_value == 999


def test_missing_in_adf_reported() -> None:
    ssis = [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}]
    adf = [{"id": "1", "v": "a"}]
    result = diff_rows(ssis, adf, key_columns=("id",))
    assert result.ok is False
    assert result.summary == {"missing_in_adf": 1}
    assert result.diffs[0].kind == "missing_in_adf"
    assert result.diffs[0].key == ("2",)


def test_extra_in_adf_reported() -> None:
    ssis = [{"id": "1", "v": "a"}]
    adf = [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}]
    result = diff_rows(ssis, adf, key_columns=("id",))
    assert result.ok is False
    assert result.summary == {"extra_in_adf": 1}
    assert result.diffs[0].kind == "extra_in_adf"


def test_duplicate_count_drift_reported() -> None:
    ssis = [{"id": "1"}, {"id": "1"}]
    adf = [{"id": "1"}]
    result = diff_rows(ssis, adf, key_columns=("id",))
    assert result.ok is False
    assert result.summary == {"duplicate_count": 1}


def test_schema_drift_reported_and_fails() -> None:
    ssis = [{"id": "1", "amount": 100}]
    adf = [{"id": "1", "amount": 100, "extra_col": "x"}]
    result = diff_rows(ssis, adf, key_columns=("id",))
    assert result.ok is False
    assert result.columns_only_in_adf == ("extra_col",)
    assert result.columns_only_in_ssis == ()


def test_ignore_columns_excludes_field() -> None:
    ssis = [{"id": "1", "v": "a", "load_ts": "2024-01-01"}]
    adf = [{"id": "1", "v": "a", "load_ts": "2026-04-23"}]
    result = diff_rows(ssis, adf, key_columns=("id",), ignore_columns=("load_ts",))
    assert result.ok is True


def test_compare_columns_subset() -> None:
    ssis = [{"id": "1", "v": "a", "noisy": 1}]
    adf = [{"id": "1", "v": "a", "noisy": 2}]
    result = diff_rows(ssis, adf, key_columns=("id",), compare_columns=("v",))
    assert result.ok is True


def test_strip_whitespace_default() -> None:
    ssis = [{"id": "1", "name": "Alice "}]
    adf = [{"id": "1", "name": "Alice"}]
    result = diff_rows(ssis, adf, key_columns=("id",))
    assert result.ok is True


def test_ignore_case_off_by_default() -> None:
    ssis = [{"id": "1", "name": "Alice"}]
    adf = [{"id": "1", "name": "alice"}]
    result = diff_rows(ssis, adf, key_columns=("id",))
    assert result.ok is False
    result_ci = diff_rows(ssis, adf, key_columns=("id",), ignore_case=True)
    assert result_ci.ok is True


def test_numeric_tolerance() -> None:
    ssis = [{"id": "1", "v": 1.000001}]
    adf = [{"id": "1", "v": 1.0}]
    assert diff_rows(ssis, adf, key_columns=("id",)).ok is False
    assert diff_rows(ssis, adf, key_columns=("id",), numeric_tolerance=0.001).ok is True


def test_max_diffs_truncates_list_but_keeps_summary() -> None:
    ssis = [{"id": str(i), "v": 1} for i in range(50)]
    adf = [{"id": str(i), "v": 2} for i in range(50)]
    result = diff_rows(ssis, adf, key_columns=("id",), max_diffs=5)
    assert result.summary == {"value_mismatch": 50}
    assert len(result.diffs) == 5


def test_composite_key_columns() -> None:
    ssis = [{"region": "EU", "id": "1", "v": "a"}, {"region": "US", "id": "1", "v": "b"}]
    adf = [{"region": "EU", "id": "1", "v": "a"}, {"region": "US", "id": "1", "v": "b"}]
    assert diff_rows(ssis, adf, key_columns=("region", "id")).ok is True


def test_empty_inputs() -> None:
    result = diff_rows([], [], key_columns=("id",))
    assert result.ok is True
    assert result.ssis_row_count == 0
    assert result.adf_row_count == 0


def test_to_dict_round_trip() -> None:
    ssis = [{"id": "1", "v": "a"}]
    adf = [{"id": "1", "v": "b"}]
    payload = diff_rows(ssis, adf, key_columns=("id",)).to_dict()
    assert payload["ok"] is False
    assert payload["summary"]["value_mismatch"] == 1
    assert payload["diffs"][0]["column"] == "v"


def test_key_columns_required() -> None:
    with pytest.raises(ValueError):
        diff_rows([], [], key_columns=())
