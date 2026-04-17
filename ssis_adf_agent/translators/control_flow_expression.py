"""
Translate SSIS expressions to ADF **pipeline** expressions (control-flow context).

ADF pipeline expressions use prefix/functional syntax::

    @equals(variables('X'), 10)
    @and(greater(variables('Counter'), 0), less(variables('Counter'), 100))
    @add(variables('Counter'), 1)
    @concat('Hello ', variables('Name'))

SSIS expressions use infix syntax::

    @[User::X] == 10
    @[User::Counter] > 0 && @[User::Counter] < 100
    @[User::Counter] + 1
    "Hello " + @[User::Name]

This module tokenizes and parses common SSIS expression patterns and converts
them to ADF prefix-functional form.  Expressions that cannot be fully
translated are annotated with ``/* TODO */`` markers.

This is distinct from ``ssis_expression_translator.py`` which handles
*Data Flow* expressions (column-centric infix syntax that stays infix in ADF).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from ..warnings_collector import warn as _warn


# ---------------------------------------------------------------------------
# Token types
# ---------------------------------------------------------------------------

class _Tk(Enum):
    NUMBER = auto()
    STRING = auto()
    BOOL = auto()
    VARIABLE = auto()   # resolved to variables('Name')
    PARAMETER = auto()  # resolved to pipeline().parameters.Name (project/package params)
    IDENT = auto()      # bare identifier
    FUNC = auto()       # function name (followed by LPAREN)
    LPAREN = auto()
    RPAREN = auto()
    COMMA = auto()
    OP_EQ = auto()      # ==
    OP_NE = auto()      # !=
    OP_LE = auto()      # <=
    OP_GE = auto()      # >=
    OP_LT = auto()      # <
    OP_GT = auto()      # >
    OP_AND = auto()     # &&
    OP_OR = auto()      # ||
    OP_NOT = auto()     # !  (unary)
    OP_ADD = auto()     # +
    OP_SUB = auto()     # -
    OP_MUL = auto()     # *
    OP_DIV = auto()     # /
    OP_MOD = auto()     # %
    OP_BITAND = auto()  # &  (bitwise AND — must check not &&)
    OP_BITOR = auto()   # |  (bitwise OR — must check not ||)
    OP_BITXOR = auto()  # ^  (bitwise XOR)
    OP_BITNOT = auto()  # ~  (bitwise NOT)
    EOF = auto()


@dataclass
class _Token:
    kind: _Tk
    value: str


# ---------------------------------------------------------------------------
# ADF pipeline function mapping for SSIS functions
# ---------------------------------------------------------------------------

_FUNC_MAP: dict[str, str] = {
    # Date/time
    "GETDATE": "utcNow",
    "GETUTCDATE": "utcNow",
    "DATEADD": "__dateadd__",   # special-cased
    "DATEDIFF": "__datediff__", # special-cased
    "DATEPART": "__datepart__", # special-cased
    "YEAR": "__datepart_year__",
    "MONTH": "__datepart_month__",
    "DAY": "__datepart_day__",
    # String
    "LEN": "length",
    "UPPER": "toUpper",
    "LOWER": "toLower",
    "LTRIM": "trim",
    "RTRIM": "trim",
    "TRIM": "trim",
    "SUBSTRING": "__substring__",  # special-cased: 1-based → 0-based
    "REPLACE": "replace",
    "RIGHT": "__right__",          # special-cased
    "LEFT": "__left__",            # special-cased
    "FINDSTRING": "__findstring__", # special-cased: returns 1-based → 0-based
    "CHARINDEX": "__findstring__",  # SQL-style alias
    "PATINDEX": "__patindex__",     # special-cased
    "REVERSE": "/* TODO: REVERSE — no direct ADF equivalent */",
    # Null handling
    "ISNULL": "empty",
    "REPLACENULL": "coalesce",
    # Type checking
    "ISNUMERIC": "/* TODO: ISNUMERIC — validate manually */",
    # Math
    "ABS": "abs",
    "CEILING": "ceil",
    "FLOOR": "floor",
    "ROUND": "round",
    "POWER": "power",
    "SQRT": "sqrt",
    "SIGN": "sign",
    # Type casting
    "(DT_STR)": "string",
    "(DT_WSTR)": "string",
    "(DT_I4)": "int",
    "(DT_I8)": "int",
    "(DT_BOOL)": "bool",
    "(DT_DECIMAL)": "decimal",
    "(DT_DBTIMESTAMP)": "/* TODO: cast to datetime */",
}

_DATEADD_PART_MAP: dict[str, str] = {
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
    "yy": "addToTime",
    "yyyy": "addToTime",
    "year": "addToTime",
}

_DATEDIFF_PART_MAP: dict[str, str] = {
    "dd": "dayOfYear",
    "d": "dayOfYear",
    "day": "dayOfYear",
    "mm": "/* TODO: month diff */",
    "m": "/* TODO: month diff */",
    "month": "/* TODO: month diff */",
    "hh": "/* TODO: hour diff */",
    "h": "/* TODO: hour diff */",
    "hour": "/* TODO: hour diff */",
    "mi": "/* TODO: minute diff */",
    "n": "/* TODO: minute diff */",
    "minute": "/* TODO: minute diff */",
    "ss": "/* TODO: second diff */",
    "s": "/* TODO: second diff */",
    "second": "/* TODO: second diff */",
    "yy": "/* TODO: year diff */",
    "yyyy": "/* TODO: year diff */",
    "year": "/* TODO: year diff */",
}

_DATEPART_FUNC_MAP: dict[str, str] = {
    "dd": "dayOfMonth",
    "d": "dayOfMonth",
    "day": "dayOfMonth",
    "dw": "dayOfWeek",
    "weekday": "dayOfWeek",
    "dy": "dayOfYear",
    "dayofyear": "dayOfYear",
    "mm": "/* TODO: DATEPART month — use formatDateTime(expr, 'MM') */",
    "m": "/* TODO: DATEPART month — use formatDateTime(expr, 'MM') */",
    "month": "/* TODO: DATEPART month — use formatDateTime(expr, 'MM') */",
    "yy": "/* TODO: DATEPART year — use formatDateTime(expr, 'yyyy') */",
    "yyyy": "/* TODO: DATEPART year — use formatDateTime(expr, 'yyyy') */",
    "year": "/* TODO: DATEPART year — use formatDateTime(expr, 'yyyy') */",
    "hh": "/* TODO: DATEPART hour — use formatDateTime(expr, 'HH') */",
    "hour": "/* TODO: DATEPART hour — use formatDateTime(expr, 'HH') */",
    "mi": "/* TODO: DATEPART minute — use formatDateTime(expr, 'mm') */",
    "minute": "/* TODO: DATEPART minute — use formatDateTime(expr, 'mm') */",
}

# ---------------------------------------------------------------------------
# Comparison/boolean → ADF prefix function
# ---------------------------------------------------------------------------

_COMPARISON_MAP: dict[_Tk, str] = {
    _Tk.OP_EQ: "equals",
    _Tk.OP_NE: "not",       # not(equals(a, b))
    _Tk.OP_LT: "less",
    _Tk.OP_GT: "greater",
    _Tk.OP_LE: "lessOrEquals",
    _Tk.OP_GE: "greaterOrEquals",
}

_ARITH_MAP: dict[_Tk, str] = {
    _Tk.OP_ADD: "add",
    _Tk.OP_SUB: "sub",
    _Tk.OP_MUL: "mul",
    _Tk.OP_DIV: "div",
    _Tk.OP_MOD: "mod",
    _Tk.OP_BITAND: "/* TODO: bitwise AND */",
    _Tk.OP_BITOR: "/* TODO: bitwise OR */",
    _Tk.OP_BITXOR: "/* TODO: bitwise XOR */",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def translate_control_flow_expr(ssis_expr: str | None) -> str:
    """
    Translate an SSIS expression to ADF pipeline expression syntax.

    Returns the translated expression *without* a leading ``@`` — the caller
    should prepend ``@`` when embedding in an ADF JSON ``value`` field.

    Unrecognised constructs are passed through with ``/* TODO */`` markers.
    """
    if not ssis_expr:
        return ""
    expr = ssis_expr.strip()
    if not expr:
        return ""

    try:
        tokens = _tokenize(expr)
        parser = _Parser(tokens)
        result = parser.parse_expression()
        # Check for leftover tokens (indicates partial parse)
        if parser.peek().kind != _Tk.EOF:
            result = f"/* TODO: partially translated */ {result}"
    except _ParseError:
        # Fall back to best-effort regex translation
        result = _fallback_translate(expr)

    if "/* TODO" in result:
        _warn(
            phase="convert",
            severity="warning",
            source="control_flow_expression",
            message=f"Expression requires manual review: {ssis_expr}",
            detail=f"Translated with TODO markers: {result}",
        )

    return result


def strip_variable_namespace(var_ref: str) -> str:
    """
    Strip the SSIS variable namespace from a reference.

    ``User::Counter`` → ``Counter``
    ``System::PackageName`` → ``PackageName``
    ``Counter`` → ``Counter``
    """
    return var_ref.split("::")[-1]


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

# Variable patterns (order matters — longest match first)
# Project/package parameters use a leading '$' before the namespace,
# e.g. @[$Project::Database] or @[$Package::FileServer]
_PARAM_BRACKET_RE = re.compile(r"@\[\s*\$(?:Project|Package)::(\w+)\s*\]")
_VAR_BRACKET_RE = re.compile(r"@\[\s*([\w:]+)\s*\]")      # @[User::Var]
_VAR_PAREN_RE = re.compile(r"@\(\s*([\w:]+)\s*\)")         # @(User::Var)
_VAR_BARE_RE = re.compile(r"@(\w+::\w+)")                   # @User::Var

_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")
_STRING_RE = re.compile(r'"([^"]*)"')
_IDENT_RE = re.compile(r"[A-Za-z_]\w*")

_OPERATORS: list[tuple[str, _Tk]] = [
    ("==", _Tk.OP_EQ),
    ("!=", _Tk.OP_NE),
    ("<=", _Tk.OP_LE),
    (">=", _Tk.OP_GE),
    ("&&", _Tk.OP_AND),
    ("||", _Tk.OP_OR),
    ("<", _Tk.OP_LT),
    (">", _Tk.OP_GT),
    ("!", _Tk.OP_NOT),
    ("+", _Tk.OP_ADD),
    ("-", _Tk.OP_SUB),
    ("*", _Tk.OP_MUL),
    ("/", _Tk.OP_DIV),
    ("%", _Tk.OP_MOD),
    ("&", _Tk.OP_BITAND),
    ("|", _Tk.OP_BITOR),
    ("^", _Tk.OP_BITXOR),
    ("~", _Tk.OP_BITNOT),
    ("(", _Tk.LPAREN),
    (")", _Tk.RPAREN),
    (",", _Tk.COMMA),
]


def _tokenize(expr: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    while i < len(expr):
        # Skip whitespace
        if expr[i].isspace():
            i += 1
            continue

        # Project/Package parameter: @[$Project::X] or @[$Package::X]
        m = _PARAM_BRACKET_RE.match(expr, i)
        if m:
            tokens.append(_Token(_Tk.PARAMETER, m.group(1)))
            i = m.end()
            continue

        # Variable: @[User::X], @(User::X), @User::X
        m = _VAR_BRACKET_RE.match(expr, i)
        if m:
            var_name = strip_variable_namespace(m.group(1))
            tokens.append(_Token(_Tk.VARIABLE, var_name))
            i = m.end()
            continue

        m = _VAR_PAREN_RE.match(expr, i)
        if m:
            var_name = strip_variable_namespace(m.group(1))
            tokens.append(_Token(_Tk.VARIABLE, var_name))
            i = m.end()
            continue

        m = _VAR_BARE_RE.match(expr, i)
        if m:
            var_name = strip_variable_namespace(m.group(1))
            tokens.append(_Token(_Tk.VARIABLE, var_name))
            i = m.end()
            continue

        # String literal
        m = _STRING_RE.match(expr, i)
        if m:
            tokens.append(_Token(_Tk.STRING, m.group(1)))
            i = m.end()
            continue

        # Number literal
        m = _NUMBER_RE.match(expr, i)
        if m:
            tokens.append(_Token(_Tk.NUMBER, m.group(0)))
            i = m.end()
            continue

        # Boolean
        if expr[i:i+4].lower() == "true" and (i + 4 >= len(expr) or not expr[i+4].isalnum()):
            tokens.append(_Token(_Tk.BOOL, "true"))
            i += 4
            continue
        if expr[i:i+5].lower() == "false" and (i + 5 >= len(expr) or not expr[i+5].isalnum()):
            tokens.append(_Token(_Tk.BOOL, "false"))
            i += 5
            continue

        # Multi-char operators (must check before single-char)
        matched_op = False
        for op_str, op_tk in _OPERATORS:
            if expr[i:i+len(op_str)] == op_str:
                # Distinguish IDENT( → FUNC token
                tokens.append(_Token(op_tk, op_str))
                i += len(op_str)
                matched_op = True
                break
        if matched_op:
            continue

        # Identifier / function name
        m = _IDENT_RE.match(expr, i)
        if m:
            name = m.group(0)
            i = m.end()
            # Peek ahead for '(' to distinguish function calls
            while i < len(expr) and expr[i].isspace():
                i += 1
            if i < len(expr) and expr[i] == "(":
                tokens.append(_Token(_Tk.FUNC, name))
            else:
                tokens.append(_Token(_Tk.IDENT, name))
            continue

        # Unknown character — skip
        i += 1

    tokens.append(_Token(_Tk.EOF, ""))
    return tokens


# ---------------------------------------------------------------------------
# Recursive-descent parser — SSIS infix → ADF prefix
# ---------------------------------------------------------------------------

class _ParseError(Exception):
    pass


class _Parser:
    """
    Operator-precedence recursive-descent parser.

    Precedence (low → high):
      1. ``||``
      2. ``&&``
      3. ``==  !=``
      4. ``<  <=  >  >=``
      5. ``+  -``
      6. ``*  /  %``
      7. unary ``!  -``
      8. atoms (number, string, bool, variable, function call, parenthesised)
    """

    def __init__(self, tokens: list[_Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    def peek(self) -> _Token:
        return self._tokens[self._pos] if self._pos < len(self._tokens) else _Token(_Tk.EOF, "")

    def advance(self) -> _Token:
        t = self.peek()
        self._pos += 1
        return t

    def expect(self, kind: _Tk) -> _Token:
        t = self.advance()
        if t.kind != kind:
            raise _ParseError(f"Expected {kind}, got {t.kind} ({t.value!r})")
        return t

    # ---- entry ----

    def parse_expression(self) -> str:
        return self._or_expr()

    # ---- precedence levels ----

    def _or_expr(self) -> str:
        left = self._and_expr()
        while self.peek().kind == _Tk.OP_OR:
            self.advance()
            right = self._and_expr()
            left = f"or({left}, {right})"
        return left

    def _and_expr(self) -> str:
        left = self._equality_expr()
        while self.peek().kind == _Tk.OP_AND:
            self.advance()
            right = self._equality_expr()
            left = f"and({left}, {right})"
        return left

    def _equality_expr(self) -> str:
        left = self._comparison_expr()
        while self.peek().kind in (_Tk.OP_EQ, _Tk.OP_NE):
            op = self.advance()
            right = self._comparison_expr()
            if op.kind == _Tk.OP_NE:
                left = f"not(equals({left}, {right}))"
            else:
                left = f"equals({left}, {right})"
        return left

    def _comparison_expr(self) -> str:
        left = self._additive_expr()
        while self.peek().kind in (_Tk.OP_LT, _Tk.OP_GT, _Tk.OP_LE, _Tk.OP_GE):
            op = self.advance()
            right = self._additive_expr()
            func = _COMPARISON_MAP[op.kind]
            left = f"{func}({left}, {right})"
        return left

    def _additive_expr(self) -> str:
        left = self._multiplicative_expr()
        while self.peek().kind in (_Tk.OP_ADD, _Tk.OP_SUB):
            op = self.advance()
            right = self._multiplicative_expr()
            # Detect string concatenation: if either side is a string or concat
            if op.kind == _Tk.OP_ADD and (_looks_stringy(left) or _looks_stringy(right)):
                left = f"concat({left}, {right})"
            else:
                func = _ARITH_MAP[op.kind]
                left = f"{func}({left}, {right})"
        return left

    def _multiplicative_expr(self) -> str:
        left = self._unary_expr()
        while self.peek().kind in (_Tk.OP_MUL, _Tk.OP_DIV, _Tk.OP_MOD,
                                   _Tk.OP_BITAND, _Tk.OP_BITOR, _Tk.OP_BITXOR):
            op = self.advance()
            right = self._unary_expr()
            func = _ARITH_MAP[op.kind]
            left = f"{func}({left}, {right})"
        return left

    def _unary_expr(self) -> str:
        if self.peek().kind == _Tk.OP_NOT:
            self.advance()
            operand = self._unary_expr()
            return f"not({operand})"
        if self.peek().kind == _Tk.OP_SUB:
            self.advance()
            operand = self._unary_expr()
            return f"sub(0, {operand})"
        if self.peek().kind == _Tk.OP_BITNOT:
            self.advance()
            operand = self._unary_expr()
            return f"/* TODO: bitwise NOT */({operand})"
        return self._atom()

    def _atom(self) -> str:
        tok = self.peek()

        if tok.kind == _Tk.NUMBER:
            self.advance()
            return tok.value

        if tok.kind == _Tk.STRING:
            self.advance()
            return f"'{tok.value}'"

        if tok.kind == _Tk.BOOL:
            self.advance()
            return tok.value

        if tok.kind == _Tk.VARIABLE:
            self.advance()
            return f"variables('{tok.value}')"

        if tok.kind == _Tk.PARAMETER:
            self.advance()
            return f"pipeline().parameters.{tok.value}"

        if tok.kind == _Tk.IDENT:
            self.advance()
            return tok.value

        if tok.kind == _Tk.FUNC:
            return self._function_call()

        if tok.kind == _Tk.LPAREN:
            self.advance()
            inner = self.parse_expression()
            self.expect(_Tk.RPAREN)
            return inner

        raise _ParseError(f"Unexpected token: {tok.kind} ({tok.value!r})")

    def _function_call(self) -> str:
        name_tok = self.advance()  # FUNC token
        func_name = name_tok.value.upper()
        self.expect(_Tk.LPAREN)

        args: list[str] = []
        if self.peek().kind != _Tk.RPAREN:
            args.append(self.parse_expression())
            while self.peek().kind == _Tk.COMMA:
                self.advance()
                args.append(self.parse_expression())
        self.expect(_Tk.RPAREN)

        # Special-case DATEADD
        if func_name == "DATEADD" and len(args) >= 3:
            return _translate_dateadd(args)

        # Special-case DATEDIFF
        if func_name == "DATEDIFF" and len(args) >= 3:
            return _translate_datediff(args)

        # Special-case DATEPART
        if func_name == "DATEPART" and len(args) >= 2:
            return _translate_datepart(args)

        # Special-case YEAR / MONTH / DAY (single-arg date part extractors)
        if func_name == "YEAR" and len(args) == 1:
            return f"int(formatDateTime({args[0]}, 'yyyy'))"
        if func_name == "MONTH" and len(args) == 1:
            return f"int(formatDateTime({args[0]}, 'MM'))"
        if func_name == "DAY" and len(args) == 1:
            return f"int(formatDateTime({args[0]}, 'dd'))"

        # Special-case GETDATE with no args
        if func_name in ("GETDATE", "GETUTCDATE"):
            return "utcNow()"

        # Special-case SUBSTRING: SSIS is 1-based, ADF is 0-based
        if func_name == "SUBSTRING" and len(args) >= 3:
            return f"substring({args[0]}, sub({args[1]}, 1), {args[2]})"

        # Special-case RIGHT(str, n) → substring(str, sub(length(str), n), n)
        if func_name == "RIGHT" and len(args) >= 2:
            return f"substring({args[0]}, sub(length({args[0]}), {args[1]}), {args[1]})"

        # Special-case LEFT(str, n) → substring(str, 0, n)
        if func_name == "LEFT" and len(args) >= 2:
            return f"substring({args[0]}, 0, {args[1]})"

        # Special-case FINDSTRING / CHARINDEX: SSIS returns 1-based, ADF indexOf is 0-based
        if func_name in ("FINDSTRING", "CHARINDEX") and len(args) >= 2:
            return f"add(indexOf({args[0]}, {args[1]}), 1)"

        # Special-case PATINDEX: pattern matching — no direct ADF equivalent
        if func_name == "PATINDEX" and len(args) >= 2:
            return f"/* TODO: PATINDEX */ indexOf({args[0]}, {args[1]})"

        adf_name = _FUNC_MAP.get(func_name, func_name)
        if adf_name.startswith("/*"):
            return f"{adf_name}({', '.join(args)})"
        return f"{adf_name}({', '.join(args)})"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _translate_dateadd(args: list[str]) -> str:
    """Translate DATEADD('part', amount, date_expr) → addDays(date_expr, amount)."""
    part = args[0].strip("'\"").lower()
    amount = args[1]
    date_expr = args[2]
    adf_func = _DATEADD_PART_MAP.get(part, f"/* TODO: DATEADD({part}) */")
    if adf_func.startswith("/*"):
        return f"{adf_func}({date_expr}, {amount})"
    return f"{adf_func}({date_expr}, {amount})"


def _translate_datediff(args: list[str]) -> str:
    """Translate DATEDIFF('part', start, end) → ADF expression.

    ADF has ``ticks()`` but no direct ``dateDiff`` function.  For day-level
    differences we use ``div(sub(ticks(end), ticks(start)), 864000000000)``
    (ticks per day).  Other parts get a TODO marker with the closest approach.
    """
    part = args[0].strip("'\"").lower()
    start = args[1]
    end = args[2]

    if part in ("dd", "d", "day"):
        return f"div(sub(ticks({end}), ticks({start})), 864000000000)"
    if part in ("hh", "h", "hour"):
        return f"div(sub(ticks({end}), ticks({start})), 36000000000)"
    if part in ("mi", "n", "minute"):
        return f"div(sub(ticks({end}), ticks({start})), 600000000)"
    if part in ("ss", "s", "second"):
        return f"div(sub(ticks({end}), ticks({start})), 10000000)"

    return f"/* TODO: DATEDIFF {part} */ div(sub(ticks({end}), ticks({start})), 864000000000)"


def _translate_datepart(args: list[str]) -> str:
    """Translate DATEPART('part', expr) → ADF dayOfMonth/dayOfWeek/dayOfYear or formatDateTime."""
    part = args[0].strip("'\"").lower()
    date_expr = args[1]
    adf_func = _DATEPART_FUNC_MAP.get(part)
    if adf_func is None:
        return f"/* TODO: DATEPART({part}) */({date_expr})"
    if adf_func.startswith("/*"):
        return f"{adf_func}({date_expr})"
    return f"{adf_func}({date_expr})"


def _looks_stringy(expr: str) -> bool:
    """Heuristic: does this expression look like a string value?"""
    return expr.startswith("'") or expr.startswith("concat(")


def _fallback_translate(expr: str) -> str:
    """
    Best-effort regex fallback when the parser fails.
    Handles variable references at minimum.
    """
    # @[User::VarName] → variables('VarName')
    expr = re.sub(r"@\[\s*(?:\w+::)?(\w+)\s*\]", r"variables('\1')", expr)
    # @(User::VarName) → variables('VarName')
    expr = re.sub(r"@\(\s*(?:\w+::)?(\w+)\s*\)", r"variables('\1')", expr)
    # @User::VarName → variables('VarName')
    expr = re.sub(r"@\w+::(\w+)", r"variables('\1')", expr)
    return f"/* TODO: review translated expression */ {expr}"
