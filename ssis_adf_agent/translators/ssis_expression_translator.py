"""
Translate SSIS Data Flow expressions to ADF Mapping Data Flow expressions.

SSIS expressions use syntax like ``[ColumnName]``, ``UPPER([col])``,
``(DT_STR,10,1252)[col]``.  ADF Mapping Data Flow expressions use a
different syntax: ``col``, ``upper(col)``, ``toString(col)``.

This module handles the most common subset.  Expressions that cannot be
fully translated are returned with ``/* TODO ... */`` annotations so the
developer can finish them.
"""
from __future__ import annotations

import re

from ..warnings_collector import warn as _warn

# ---------------------------------------------------------------------------
# Function name mapping: SSIS → ADF
# ---------------------------------------------------------------------------

_FUNC_MAP: dict[str, str] = {
    # String
    "UPPER": "upper",
    "LOWER": "lower",
    "LTRIM": "ltrim",
    "RTRIM": "rtrim",
    "TRIM": "trim",
    "LEN": "length",
    "SUBSTRING": "substring",
    "REPLACE": "replace",
    "REVERSE": "reverse",
    "REPLICATE": "lpad",  # approximate — lpad(str, len, str) vs replicate(str, n)
    "RIGHT": "right",
    "LEFT": "left",
    "FINDSTRING": "locate",
    # Null
    "ISNULL": "isNull",
    "REPLACENULL": "coalesce",
    # Math
    "ABS": "abs",
    "CEILING": "ceil",
    "FLOOR": "floor",
    "ROUND": "round",
    "POWER": "power",
    "SQRT": "sqrt",
    "SIGN": "sign",
    # Date/time
    "GETDATE": "currentTimestamp",
    "GETUTCDATE": "currentUTC",
    "YEAR": "year",
    "MONTH": "month",
    "DAY": "dayOfMonth",
    "DATEPART": "dayOfMonth",  # simplified — datepart has a part arg
    "DATEDIFF": "/* TODO: DATEDIFF — map manually */",
    # Type conversion helpers
    "HEX": "hex",
}

# DATEADD sub-mappings: SSIS datepart → ADF function
_DATEADD_MAP: dict[str, str] = {
    "dd": "addDays",
    "d": "addDays",
    "day": "addDays",
    "mm": "addMonths",
    "m": "addMonths",
    "month": "addMonths",
    "hh": "addHours",
    "h": "addHours",
    "hour": "addHours",
    "mi": "addMinutes",
    "n": "addMinutes",
    "minute": "addMinutes",
    "ss": "addSeconds",
    "s": "addSeconds",
    "second": "addSeconds",
    "yy": "/* TODO: addYears — no direct ADF equivalent */",
    "yyyy": "/* TODO: addYears — no direct ADF equivalent */",
    "year": "/* TODO: addYears — no direct ADF equivalent */",
}

# Cast mapping: SSIS (DT_xxx) → ADF function
_CAST_MAP: dict[str, str] = {
    "DT_I1": "toShort",
    "DT_I2": "toShort",
    "DT_I4": "toInteger",
    "DT_I8": "toLong",
    "DT_UI1": "toShort",
    "DT_UI2": "toInteger",
    "DT_UI4": "toLong",
    "DT_UI8": "toLong",
    "DT_R4": "toFloat",
    "DT_R8": "toDouble",
    "DT_DECIMAL": "toDecimal",
    "DT_NUMERIC": "toDecimal",
    "DT_CY": "toDecimal",
    "DT_BOOL": "toBoolean",
    "DT_STR": "toString",
    "DT_WSTR": "toString",
    "DT_DBDATE": "toDate",
    "DT_DBTIMESTAMP": "toTimestamp",
    "DT_DBTIMESTAMP2": "toTimestamp",
    "DT_DATE": "toTimestamp",
    "DT_GUID": "toString",
    "DT_BYTES": "toBinary",
}

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Column reference: [ColumnName]
_COL_REF_RE = re.compile(r"\[([^\]]+)\]")

# SSIS cast: (DT_XXX) expr  or  (DT_XXX, len, codepage) expr
_CAST_RE = re.compile(
    r"\(\s*(DT_\w+)(?:\s*,\s*\d+)*(?:\s*,\s*\d+)*\s*\)",
    re.IGNORECASE,
)

# Function call: FUNCNAME(...)
_FUNC_CALL_RE = re.compile(r"\b([A-Z_]+)\s*\(", re.IGNORECASE)

# DATEADD("part", n, expr)
_DATEADD_RE = re.compile(
    r'\bDATEADD\s*\(\s*"?(\w+)"?\s*,\s*(.+?)\s*,\s*(.+?)\s*\)',
    re.IGNORECASE,
)

# NULL(DT_XXX) — typed null
_TYPED_NULL_RE = re.compile(r"\bNULL\s*\(\s*(DT_\w+)\s*\)", re.IGNORECASE)

# Ternary: condition ? true_expr : false_expr
_TERNARY_RE = re.compile(
    r"(.+?)\s*\?\s*(.+?)\s*:\s*(.+)",
    re.DOTALL,
)

# String concatenation with + (only between string-like operands)
# We'll handle this heuristically in the main translator.


def translate_expression(ssis_expr: str | None) -> str:
    """
    Translate an SSIS expression to an ADF Mapping Data Flow expression.

    Returns the translated expression string.  Parts that cannot be translated
    are annotated with ``/* TODO */`` comments.
    """
    if not ssis_expr:
        return ""

    expr = ssis_expr.strip()

    # 1. Typed NULL: NULL(DT_I4) → toInteger(null())
    expr = _TYPED_NULL_RE.sub(_replace_typed_null, expr)

    # 2. DATEADD — handle before generic function mapping
    expr = _DATEADD_RE.sub(_replace_dateadd, expr)

    # 3. Cast expressions: (DT_I4)[col] → toInteger(col)
    expr = _translate_casts(expr)

    # 4. Ternary: cond ? a : b → iif(cond, a, b)
    expr = _translate_ternary(expr)

    # 5. Function calls
    expr = _translate_functions(expr)

    # 6. Column references: [ColName] → ColName
    expr = _COL_REF_RE.sub(r"\1", expr)

    # 7. SSIS boolean operators
    expr = expr.replace("&&", "&&").replace("||", "||")
    # SSIS uses == for equality (same as ADF), but != for not-equal
    # ADF uses != as well, so no change needed.

    # Emit structured warning when TODO markers remain
    if "/* TODO" in expr:
        _warn(
            phase="convert",
            severity="warning",
            source="ssis_expression_translator",
            message=f"Expression requires manual review: {ssis_expr}",
            detail=f"Translated with TODO markers: {expr}",
        )

    return expr


def _replace_typed_null(m: re.Match) -> str:
    dt = m.group(1).upper()
    func = _CAST_MAP.get(dt, "toString")
    return f"{func}(null())"


def _replace_dateadd(m: re.Match) -> str:
    part = m.group(1).lower().strip('"')
    amount = m.group(2).strip()
    date_expr = m.group(3).strip()
    adf_func = _DATEADD_MAP.get(part, f"/* TODO: DATEADD({part}) */")
    if adf_func.startswith("/*"):
        return f"{adf_func}({date_expr}, {amount})"
    return f"{adf_func}({date_expr}, {amount})"


def _translate_casts(expr: str) -> str:
    """Replace (DT_XXX)[col] or (DT_XXX,len,cp)[col] with adfFunc(col)."""
    # Pattern: (DT_XXX)<whitespace>[ColRef] or (DT_XXX)<ws>(subexpr)
    pattern = re.compile(
        r"\(\s*(DT_\w+)(?:\s*,\s*\d+)*(?:\s*,\s*\d+)*\s*\)\s*"
        r"(?:"
        r"\[([^\]]+)\]"          # [ColumnRef]
        r"|"
        r"\(([^)]+)\)"           # (sub-expression)
        r"|"
        r"(\w+)"                 # bare identifier
        r")",
        re.IGNORECASE,
    )

    def _cast_repl(m: re.Match) -> str:
        dt = m.group(1).upper()
        operand = m.group(2) or m.group(3) or m.group(4) or ""
        func = _CAST_MAP.get(dt, f"/* TODO: cast {dt} */")
        return f"{func}({operand})"

    return pattern.sub(_cast_repl, expr)


def _translate_ternary(expr: str) -> str:
    """Convert SSIS ternary ``cond ? a : b`` to ADF ``iif(cond, a, b)``."""
    m = _TERNARY_RE.match(expr)
    if m:
        cond = m.group(1).strip()
        true_val = m.group(2).strip()
        false_val = m.group(3).strip()
        return f"iif({cond}, {true_val}, {false_val})"
    return expr


def _translate_functions(expr: str) -> str:
    """Replace SSIS function names with ADF equivalents."""
    def _func_repl(m: re.Match) -> str:
        name = m.group(1).upper()
        adf_name = _FUNC_MAP.get(name)
        if adf_name:
            return f"{adf_name}("
        return m.group(0)  # leave unknown functions as-is

    return _FUNC_CALL_RE.sub(_func_repl, expr)
