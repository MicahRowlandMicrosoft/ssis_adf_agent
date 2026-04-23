"""Pure row-and-column diff engine for the behavioral parity harness.

Inputs are two row-sets (lists of dicts).  The engine groups rows by a
``key_columns`` tuple, then within each group reports column-level value
mismatches, missing rows, extra rows, and schema (column-set) drift.

No I/O, no Azure, no SSIS — fully deterministic and unit-testable.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

# ---------------------------------------------------------------------------
# Result objects
# ---------------------------------------------------------------------------


@dataclass
class RowDiff:
    """A single row-level discrepancy."""

    kind: str  # "value_mismatch" | "missing_in_adf" | "extra_in_adf" | "duplicate_count"
    key: tuple[Any, ...]
    column: str | None = None
    ssis_value: Any = None
    adf_value: Any = None
    detail: str = ""


@dataclass
class DataFlowDiff:
    ok: bool
    ssis_row_count: int
    adf_row_count: int
    matched_row_count: int
    key_columns: tuple[str, ...]
    columns_compared: tuple[str, ...]
    columns_only_in_ssis: tuple[str, ...]
    columns_only_in_adf: tuple[str, ...]
    diffs: list[RowDiff] = field(default_factory=list)
    summary: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "ssis_row_count": self.ssis_row_count,
            "adf_row_count": self.adf_row_count,
            "matched_row_count": self.matched_row_count,
            "key_columns": list(self.key_columns),
            "columns_compared": list(self.columns_compared),
            "columns_only_in_ssis": list(self.columns_only_in_ssis),
            "columns_only_in_adf": list(self.columns_only_in_adf),
            "summary": dict(self.summary),
            "diffs": [
                {
                    "kind": d.kind,
                    "key": list(d.key),
                    "column": d.column,
                    "ssis_value": d.ssis_value,
                    "adf_value": d.adf_value,
                    "detail": d.detail,
                }
                for d in self.diffs
            ],
        }


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def _normalize_value(
    value: Any,
    *,
    ignore_case: bool,
    strip_whitespace: bool,
    numeric_tolerance: float,
) -> Any:
    """Normalize a single cell value for comparison.

    Returns a stable representation so that ``"Foo "`` == ``"foo"`` when
    ``ignore_case`` and ``strip_whitespace`` are enabled, and so that
    ``1.000001`` == ``1.0`` when ``numeric_tolerance`` > 0.
    """
    if value is None:
        return None
    if isinstance(value, str):
        out = value
        if strip_whitespace:
            out = out.strip()
        if ignore_case:
            out = out.casefold()
        return out
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and numeric_tolerance > 0:
        # Quantize to the tolerance so equal-within-tolerance values hash equal.
        return round(float(value) / numeric_tolerance) * numeric_tolerance
    return value


def _values_equal(
    a: Any,
    b: Any,
    *,
    ignore_case: bool,
    strip_whitespace: bool,
    numeric_tolerance: float,
) -> bool:
    na = _normalize_value(
        a,
        ignore_case=ignore_case,
        strip_whitespace=strip_whitespace,
        numeric_tolerance=numeric_tolerance,
    )
    nb = _normalize_value(
        b,
        ignore_case=ignore_case,
        strip_whitespace=strip_whitespace,
        numeric_tolerance=numeric_tolerance,
    )
    return na == nb


def _row_key(row: dict[str, Any], key_columns: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row.get(c) for c in key_columns)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def diff_rows(
    ssis_rows: Iterable[dict[str, Any]],
    adf_rows: Iterable[dict[str, Any]],
    *,
    key_columns: Sequence[str],
    compare_columns: Sequence[str] | None = None,
    ignore_columns: Sequence[str] = (),
    ignore_case: bool = False,
    strip_whitespace: bool = True,
    numeric_tolerance: float = 0.0,
    max_diffs: int = 1000,
) -> DataFlowDiff:
    """Compare two row-sets keyed by ``key_columns``.

    :param ssis_rows: Output rows from the SSIS Data Flow Task.
    :param adf_rows: Output rows from the converted ADF Mapping Data Flow.
    :param key_columns: Tuple of columns that uniquely identify each row
        (typically the destination's natural key).  Required.
    :param compare_columns: Optional subset of columns to compare.  If omitted,
        all columns common to both sides are compared.
    :param ignore_columns: Columns to exclude from comparison
        (e.g. ``("LoadDateTime",)`` for non-deterministic timestamps).
    :param ignore_case: Case-insensitive string comparison.
    :param strip_whitespace: Trim leading/trailing whitespace on strings.
    :param numeric_tolerance: Absolute tolerance for numeric comparison.
        ``0.0`` means exact equality.  ``0.01`` would treat ``1.005`` and
        ``1.014`` as equal but ``1.005`` and ``1.025`` as different.
    :param max_diffs: Cap on the number of per-cell diffs collected (the
        summary counts remain accurate even when the diff list is truncated).

    :returns: A :class:`DataFlowDiff` with ``ok=True`` only when both sides
        have the same row counts, the same set of compared columns, and zero
        value mismatches.
    """
    if not key_columns:
        raise ValueError("key_columns must contain at least one column name.")

    ssis_list = list(ssis_rows)
    adf_list = list(adf_rows)

    # Column reconciliation.
    ssis_cols: set[str] = set()
    for r in ssis_list:
        ssis_cols.update(r.keys())
    adf_cols: set[str] = set()
    for r in adf_list:
        adf_cols.update(r.keys())

    only_ssis = tuple(sorted(ssis_cols - adf_cols))
    only_adf = tuple(sorted(adf_cols - ssis_cols))

    if compare_columns is not None:
        cols_to_check = tuple(c for c in compare_columns if c not in ignore_columns)
    else:
        cols_to_check = tuple(
            sorted((ssis_cols & adf_cols) - set(ignore_columns) - set(key_columns))
        )

    diffs: list[RowDiff] = []
    summary = Counter[str]()

    def _push(diff: RowDiff) -> None:
        summary[diff.kind] += 1
        if len(diffs) < max_diffs:
            diffs.append(diff)

    # Index by key.  Rows with duplicate keys are kept as lists so we can
    # report duplicate-count drift instead of silently picking one.
    ssis_index: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in ssis_list:
        ssis_index.setdefault(_row_key(row, key_columns), []).append(row)
    adf_index: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in adf_list:
        adf_index.setdefault(_row_key(row, key_columns), []).append(row)

    matched_rows = 0

    for key, ssis_bucket in ssis_index.items():
        adf_bucket = adf_index.get(key)
        if adf_bucket is None:
            for row in ssis_bucket:
                _push(
                    RowDiff(
                        kind="missing_in_adf",
                        key=key,
                        detail=f"Row present in SSIS output but absent from ADF output: {row}",
                    )
                )
            continue
        if len(ssis_bucket) != len(adf_bucket):
            _push(
                RowDiff(
                    kind="duplicate_count",
                    key=key,
                    ssis_value=len(ssis_bucket),
                    adf_value=len(adf_bucket),
                    detail="Different number of rows for the same key.",
                )
            )
        # Compare element-wise across the shorter pairing.
        for ssis_row, adf_row in zip(ssis_bucket, adf_bucket):
            row_ok = True
            for col in cols_to_check:
                if not _values_equal(
                    ssis_row.get(col),
                    adf_row.get(col),
                    ignore_case=ignore_case,
                    strip_whitespace=strip_whitespace,
                    numeric_tolerance=numeric_tolerance,
                ):
                    row_ok = False
                    _push(
                        RowDiff(
                            kind="value_mismatch",
                            key=key,
                            column=col,
                            ssis_value=ssis_row.get(col),
                            adf_value=adf_row.get(col),
                        )
                    )
            if row_ok:
                matched_rows += 1

    for key, adf_bucket in adf_index.items():
        if key not in ssis_index:
            for row in adf_bucket:
                _push(
                    RowDiff(
                        kind="extra_in_adf",
                        key=key,
                        detail=f"Row present in ADF output but absent from SSIS output: {row}",
                    )
                )

    schema_drift = bool(only_ssis or only_adf)
    has_value_or_row_diffs = bool(summary)

    ok = (
        len(ssis_list) == len(adf_list)
        and not schema_drift
        and not has_value_or_row_diffs
    )

    return DataFlowDiff(
        ok=ok,
        ssis_row_count=len(ssis_list),
        adf_row_count=len(adf_list),
        matched_row_count=matched_rows,
        key_columns=tuple(key_columns),
        columns_compared=cols_to_check,
        columns_only_in_ssis=only_ssis,
        columns_only_in_adf=only_adf,
        diffs=diffs,
        summary=dict(summary),
    )
